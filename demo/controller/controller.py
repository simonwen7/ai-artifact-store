#!/usr/bin/env python3
"""AI Artifact Store Interactive Systems Demo controller.

Localhost-only presentation layer that drives the production CLI against an
owned disposable PostgreSQL database and three owned StorageNode processes.

Python 3 standard library only.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

CONTROLLER_HOST = "127.0.0.1"
CONTROLLER_PORT = 8787
METADATA_PORT = 8080
NODE_SPECS = (
    ("node-a", 8081),
    ("node-b", 8082),
    ("node-c", 8083),
)
REQUIRED_PORTS = (METADATA_PORT, 8081, 8082, 8083)
FORBIDDEN_DB_NAMES = {"ai_artifact_store_dev", "ai_artifact_store_test", "postgres"}
MIGRATIONS = [
    "001_initial_schema.sql",
    "002_separate_object_layouts.sql",
    "003_content_addressed_artifact_versions.sql",
    "004_manifest_and_run_models.sql",
    "005_upload_sessions.sql",
    "006_upload_session_finalizations.sql",
    "007_fastcdc_chunking.sql",
    "008_garbage_collection.sql",
    "009_multi_node_replication.sql",
    "010_ai_aware_lifecycle.sql",
    "011_retired_finalization_reclamation.sql",
]
MAX_UPLOAD_BYTES = 256 * 1024 * 1024
STREAM_BLOCK = 1024 * 1024
EVENT_LIMIT = 200
GUIDED_BYTES = 32 * 1024 * 1024
LIFECYCLE_BYTES = 1 * 1024 * 1024
ARTIFACT_KINDS = {
    "generic",
    "model-checkpoint",
    "dataset-snapshot",
    "embedding-index",
    "evaluation-output",
}
CHUNKING_STRATEGIES = {"fixed-size", "fastcdc"}
NODE_IDS = {node_id for node_id, _ in NODE_SPECS}


class DemoError(Exception):
    def __init__(self, code: str, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


def die(message: str) -> None:
    print(f"demo-controller: {message}", file=sys.stderr)
    raise SystemExit(1)


def make_uuid_v7() -> str:
    millis = int(time.time() * 1000) & ((1 << 48) - 1)
    rand_a = secrets.randbits(12)
    rand_b = secrets.randbits(62)
    value = (millis << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b
    hex_digits = f"{value:032x}"
    return f"{hex_digits[0:8]}-{hex_digits[8:12]}-{hex_digits[12:16]}-{hex_digits[16:20]}-{hex_digits[20:32]}"


def rewrite_db_url(admin_url: str, db_name: str) -> str:
    parsed = urlparse(admin_url)
    if parsed.scheme.startswith("postgres") and not parsed.netloc:
        return f"{parsed.scheme}:///{db_name}"
    return parsed._replace(path=f"/{db_name}").geturl()


def port_is_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def assert_cluster_ports_free() -> None:
    busy = [port for port in REQUIRED_PORTS if not port_is_free(port)]
    if busy:
        die(f"required cluster ports are occupied: {busy}. Free them and retry.")


def sanitize_filename(name: str) -> str:
    base = Path(name.replace("\\", "/")).name.strip()
    if not base or base in {".", ".."}:
        return "artifact.bin"
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", base)
    return cleaned[:128] or "artifact.bin"


def xorshift_stream_to_file(path: Path, byte_count: int, seed: int) -> str:
    hasher = hashlib.sha256()
    state = seed & 0xFFFFFFFFFFFFFFFF
    remaining = byte_count
    block = bytearray(STREAM_BLOCK)
    with path.open("wb") as handle:
        while remaining > 0:
            n = min(len(block), remaining)
            for index in range(n):
                state ^= (state << 13) & 0xFFFFFFFFFFFFFFFF
                state ^= state >> 7
                state ^= (state << 17) & 0xFFFFFFFFFFFFFFFF
                block[index] = state & 0xFF
            view = memoryview(block)[:n]
            handle.write(view)
            hasher.update(view)
            remaining -= n
    return hasher.hexdigest()


def file_sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(STREAM_BLOCK)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def wait_health(url: str, timeout_s: float = 20.0) -> None:
    parsed = urlparse(url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port
    if port is None:
        die(f"health URL missing port: {url}")
    deadline = time.perf_counter() + timeout_s
    while time.perf_counter() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.2):
                pass
        except OSError:
            time.sleep(0.05)
            continue
        curl = shutil.which("curl")
        if curl is None:
            return
        probe = subprocess.run(
            [curl, "-fsS", url],
            check=False,
            capture_output=True,
            text=True,
        )
        if probe.returncode == 0:
            return
        time.sleep(0.05)
    die(f"service did not become ready: {url}")


class DemoController:
    def __init__(self, repo_root: Path, build_dir: Path, admin_db_url: str) -> None:
        self.repo_root = repo_root
        self.build_dir = build_dir
        self.admin_db_url = admin_db_url
        self.aistore = build_dir / "aistore"
        self.metadata_bin = build_dir / "metadata-service"
        self.storage_bin = build_dir / "storage-node"
        for binary in (self.aistore, self.metadata_bin, self.storage_bin):
            if not binary.is_file() or not os.access(binary, os.X_OK):
                die(f"missing executable: {binary}")

        self.lock = threading.RLock()
        self.op_lock = threading.Lock()
        self.ready = False
        self.shutting_down = False
        self.db_name = ""
        self.db_created = False
        self.work_dir: Path | None = None
        self.metadata_proc: subprocess.Popen[bytes] | None = None
        self.node_procs: dict[str, subprocess.Popen[bytes] | None] = {
            node_id: None for node_id, _ in NODE_SPECS
        }
        self.node_ports = {node_id: port for node_id, port in NODE_SPECS}
        self.events: list[dict[str, str]] = []
        self.artifact: dict[str, Any] | None = None
        self.source_path: Path | None = None
        self.source_sha256: str | None = None
        self.restored_path: Path | None = None
        self.guided: dict[str, Any] = self._fresh_guided()
        self.lifecycle: dict[str, Any] = self._fresh_lifecycle()
        self.last_push_stats: dict[str, Any] | None = None
        self.last_pull_stats: dict[str, Any] | None = None
        self.last_repair_stats: dict[str, Any] | None = None

    @staticmethod
    def _fresh_guided() -> dict[str, Any]:
        return {
            "step": "READY",
            "step_index": 0,
            "killed_node_id": None,
            "summary": {"push": None, "pull": None, "repair": None},
        }

    @staticmethod
    def _fresh_lifecycle() -> dict[str, Any]:
        return {"initialized": False, "policy_id": None, "versions": []}

    def emit(self, kind: str, message: str) -> None:
        event = {
            "timestamp": time.strftime("%H:%M:%S"),
            "kind": kind,
            "message": message,
        }
        self.events.append(event)
        if len(self.events) > EVENT_LIMIT:
            self.events = self.events[-EVENT_LIMIT:]

    def psql(self, sql: str, *, dbname: str | None = None) -> str:
        url = rewrite_db_url(self.admin_db_url, dbname) if dbname is not None else self.admin_db_url
        result = subprocess.run(
            ["psql", url, "-v", "ON_ERROR_STOP=1", "-At", "-c", sql],
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise DemoError(
                "database_error",
                "Database command failed while operating the owned demo database.",
                500,
            )
        return result.stdout.strip()

    def apply_migrations(self) -> None:
        url = rewrite_db_url(self.admin_db_url, self.db_name)
        for name in MIGRATIONS:
            path = self.repo_root / "migrations" / name
            if not path.is_file():
                die(f"missing migration: {name}")
            result = subprocess.run(
                ["psql", url, "-v", "ON_ERROR_STOP=1", "-f", str(path)],
                check=False,
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                die(f"migration failed: {name}")

    def create_owned_db(self) -> None:
        self.db_name = f"aistore_demo_{os.getpid()}_{secrets.token_hex(4)}"
        if self.db_name in FORBIDDEN_DB_NAMES or not self.db_name.startswith("aistore_demo_"):
            die("internal error: invalid demo database name")
        existing = self.psql(f"SELECT 1 FROM pg_database WHERE datname = '{self.db_name}';")
        if existing == "1":
            die(f"refusing to reuse pre-existing database name: {self.db_name}")
        self.psql(f'CREATE DATABASE "{self.db_name}";')
        self.db_created = True
        self.apply_migrations()

    def drop_owned_db(self) -> None:
        if not self.db_created or not self.db_name:
            return
        try:
            self.psql(f'DROP DATABASE IF EXISTS "{self.db_name}";')
        except DemoError:
            print(
                f"demo-controller warning: failed to drop owned database {self.db_name}",
                file=sys.stderr,
            )
        self.db_created = False
        self.db_name = ""

    def _stop_proc(self, proc: subprocess.Popen[bytes] | None) -> None:
        if proc is None or proc.poll() is not None:
            return
        try:
            proc.terminate()
        except ProcessLookupError:
            return
        try:
            proc.wait(timeout=3)
            return
        except subprocess.TimeoutExpired:
            pass
        try:
            proc.kill()
            proc.wait(timeout=2)
        except (ProcessLookupError, subprocess.TimeoutExpired):
            pass

    def stop_all_processes(self) -> None:
        for node_id in list(self.node_procs):
            self._stop_proc(self.node_procs.get(node_id))
            self.node_procs[node_id] = None
        self._stop_proc(self.metadata_proc)
        self.metadata_proc = None

    def prepare_workdir(self) -> None:
        self.work_dir = Path(tempfile.mkdtemp(prefix="aistore-demo-"))
        for name in ("uploads", "restores", "logs", "policy"):
            (self.work_dir / name).mkdir(parents=True, exist_ok=True)
        for node_id, _ in NODE_SPECS:
            (self.work_dir / f"cas-{node_id}").mkdir(parents=True, exist_ok=True)

    def remove_workdir(self) -> None:
        if self.work_dir is not None and self.work_dir.exists():
            shutil.rmtree(self.work_dir, ignore_errors=True)
        self.work_dir = None

    def start_metadata(self) -> None:
        assert self.work_dir is not None
        env = os.environ.copy()
        env["AISTORE_DB_URL"] = rewrite_db_url(self.admin_db_url, self.db_name)
        log_path = self.work_dir / "logs" / "metadata-service.log"
        log_file = log_path.open("ab")
        self.metadata_proc = subprocess.Popen(
            [str(self.metadata_bin)],
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        wait_health(f"http://127.0.0.1:{METADATA_PORT}/health")

    def start_node(self, node_id: str) -> None:
        assert self.work_dir is not None
        if node_id not in NODE_IDS:
            raise DemoError("invalid_node", f"Unknown node id: {node_id}", 404)
        port = self.node_ports[node_id]
        cas = self.work_dir / f"cas-{node_id}"
        cas.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        env["AISTORE_STORAGE_ROOT"] = str(cas)
        env["AISTORE_STORAGE_NODE_ID"] = node_id
        env["AISTORE_STORAGE_PORT"] = str(port)
        log_path = self.work_dir / "logs" / f"{node_id}.log"
        log_file = log_path.open("ab")
        proc = subprocess.Popen(
            [str(self.storage_bin)],
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        self.node_procs[node_id] = proc
        wait_health(f"http://127.0.0.1:{port}/health")

    def register_nodes(self) -> None:
        for node_id, port in NODE_SPECS:
            self.run_cli(
                [
                    str(self.aistore),
                    "node",
                    "register",
                    "--storage-node-id",
                    node_id,
                    "--storage-address",
                    "127.0.0.1",
                    "--storage-port",
                    str(port),
                    "--metadata-address",
                    "127.0.0.1",
                    "--metadata-port",
                    str(METADATA_PORT),
                ]
            )

    def bootstrap_cluster(self) -> None:
        assert_cluster_ports_free()
        self.prepare_workdir()
        self.create_owned_db()
        self.start_metadata()
        for node_id, _ in NODE_SPECS:
            self.start_node(node_id)
        self.register_nodes()
        self.ready = True
        self.emit("system", "Demo cluster ready: metadata-service + node-a/b/c Active.")

    def cleanup(self) -> None:
        with self.lock:
            self.ready = False
            self.stop_all_processes()
            self.drop_owned_db()
            self.remove_workdir()

    def reset_demo(self) -> dict[str, Any]:
        with self.lock:
            self.ready = False
            self.stop_all_processes()
            self.drop_owned_db()
            self.remove_workdir()
            self.artifact = None
            self.source_path = None
            self.source_sha256 = None
            self.restored_path = None
            self.guided = self._fresh_guided()
            self.lifecycle = self._fresh_lifecycle()
            self.last_push_stats = None
            self.last_pull_stats = None
            self.last_repair_stats = None
            self.events = []
            self.bootstrap_cluster()
            self.emit("system", "Demo reset complete.")
            return self.build_state()

    def run_cli(self, args: list[str]) -> dict[str, Any]:
        result = subprocess.run(
            args,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            detail = (result.stderr or result.stdout or "").strip().splitlines()
            safe = detail[-1] if detail else "production CLI command failed"
            if len(safe) > 240:
                safe = safe[:237] + "..."
            raise DemoError("cli_failed", safe, 500)
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        if not lines:
            return {}
        try:
            payload = json.loads(lines[-1])
        except json.JSONDecodeError as exc:
            raise DemoError("cli_json_error", "Production CLI did not emit JSON.", 500) from exc
        if not isinstance(payload, dict):
            raise DemoError("cli_json_error", "Production CLI JSON root must be an object.", 500)
        return payload

    def insert_artifact(self, artifact_id: str, name: str) -> None:
        sql = (
            "INSERT INTO artifacts (artifact_id, name, project) VALUES "
            f"('{artifact_id}'::uuid, '{name}', 'interactive-demo') "
            "ON CONFLICT (artifact_id) DO NOTHING;"
        )
        self.psql(sql, dbname=self.db_name)

    def process_online(self, node_id: str) -> bool:
        proc = self.node_procs.get(node_id)
        return proc is not None and proc.poll() is None

    def metadata_online(self) -> bool:
        return self.metadata_proc is not None and self.metadata_proc.poll() is None

    def registry_states(self) -> dict[str, str]:
        rows = self.psql(
            "SELECT node_id || '|' || state FROM storage_nodes ORDER BY node_id;",
            dbname=self.db_name,
        )
        states = {node_id: "active" for node_id, _ in NODE_SPECS}
        if not rows:
            return states
        for line in rows.splitlines():
            if "|" not in line:
                continue
            node_id, state = line.split("|", 1)
            states[node_id] = state
        return states

    def node_chunk_counts(self) -> dict[str, int]:
        counts = {node_id: 0 for node_id, _ in NODE_SPECS}
        rows = self.psql(
            "SELECT node_id || '|' || COUNT(*)::text FROM storage_locations "
            "WHERE state = 'available' GROUP BY node_id;",
            dbname=self.db_name,
        )
        if not rows:
            return counts
        for line in rows.splitlines():
            if "|" not in line:
                continue
            node_id, count = line.split("|", 1)
            if node_id in counts:
                counts[node_id] = int(count)
        return counts

    def query_chunk_placements(self, layout_id: str) -> list[dict[str, Any]]:
        chunk_rows = self.psql(
            "SELECT olc.chunk_id || '|' || c.size_bytes::text "
            "FROM object_layout_chunks olc "
            "INNER JOIN chunks c ON c.chunk_id = olc.chunk_id "
            f"WHERE olc.layout_id = '{layout_id}' "
            "ORDER BY olc.chunk_index;",
            dbname=self.db_name,
        )
        chunks: list[dict[str, Any]] = []
        if not chunk_rows:
            return chunks
        for line in chunk_rows.splitlines():
            chunk_id, size_text = line.split("|", 1)
            loc_rows = self.psql(
                "SELECT node_id || '|' || state FROM storage_locations "
                f"WHERE chunk_id = '{chunk_id}' ORDER BY node_id;",
                dbname=self.db_name,
            )
            locations: list[dict[str, Any]] = []
            desired: list[str] = []
            if loc_rows:
                for loc_line in loc_rows.splitlines():
                    node_id, state = loc_line.split("|", 1)
                    desired.append(node_id)
                    locations.append(
                        {
                            "node_id": node_id,
                            "metadata_state": state,
                            "process_status": "online" if self.process_online(node_id) else "offline",
                        }
                    )
            chunks.append(
                {
                    "chunk_id": chunk_id,
                    "size_bytes": int(size_text),
                    "desired_nodes": desired,
                    "locations": locations,
                }
            )
        return chunks

    @staticmethod
    def compute_health(chunks: list[dict[str, Any]], replication_factor: int = 1) -> str:
        if not chunks:
            return "unavailable"
        required = max(1, int(replication_factor))
        any_degraded = False
        for chunk in chunks:
            reachable = 0
            for loc in chunk["locations"]:
                if loc["metadata_state"] == "available" and loc["process_status"] == "online":
                    reachable += 1
            if reachable == 0:
                return "unavailable"
            if reachable < required:
                any_degraded = True
        return "degraded" if any_degraded else "healthy"

    def refresh_artifact(self) -> None:
        if self.artifact is None:
            return
        layout_id = self.artifact["layout_id"]
        chunks = self.query_chunk_placements(layout_id)
        self.artifact["chunks"] = chunks
        self.artifact["health"] = self.compute_health(
            chunks, int(self.artifact.get("replication_factor", 1))
        )

    def physical_representation(self, layout_id: str | None) -> str:
        if not layout_id:
            return "reclaimed"
        exists = self.psql(
            f"SELECT 1 FROM object_layouts WHERE layout_id = '{layout_id}';",
            dbname=self.db_name,
        )
        if exists != "1":
            return "reclaimed"
        total = self.psql(
            "SELECT COUNT(*)::text FROM object_layout_chunks "
            f"WHERE layout_id = '{layout_id}';",
            dbname=self.db_name,
        )
        available = self.psql(
            "SELECT COUNT(*)::text FROM object_layout_chunks olc "
            "INNER JOIN storage_locations sl ON sl.chunk_id = olc.chunk_id AND sl.state = 'available' "
            f"WHERE olc.layout_id = '{layout_id}';",
            dbname=self.db_name,
        )
        total_n = int(total or "0")
        available_n = int(available or "0")
        if total_n == 0 or available_n == 0:
            return "reclaimed"
        if available_n < total_n:
            return "partially-reclaimed"
        return "present"

    def refresh_lifecycle(self) -> None:
        if not self.lifecycle.get("initialized"):
            return
        versions = []
        for entry in self.lifecycle.get("versions", []):
            version_id = entry["version_id"]
            retired = self.psql(
                f"SELECT 1 FROM artifact_version_retirements WHERE version_id = '{version_id}';",
                dbname=self.db_name,
            )
            pinned = self.psql(
                f"SELECT 1 FROM artifact_version_pins WHERE version_id = '{version_id}';",
                dbname=self.db_name,
            )
            versions.append(
                {
                    "label": entry["label"],
                    "version_id": version_id,
                    "semantic_status": "retired" if retired == "1" else "active",
                    "pin": pinned == "1",
                    "last_decision": entry.get("last_decision"),
                    "reason": entry.get("reason"),
                    "physical_representation": self.physical_representation(entry.get("layout_id")),
                    "layout_id": entry.get("layout_id"),
                }
            )
        self.lifecycle["versions"] = versions

    def build_state(self) -> dict[str, Any]:
        with self.lock:
            if self.ready and self.db_created:
                try:
                    self.refresh_artifact()
                    self.refresh_lifecycle()
                    registry = self.registry_states()
                    counts = self.node_chunk_counts()
                except DemoError:
                    registry = {node_id: "active" for node_id, _ in NODE_SPECS}
                    counts = {node_id: 0 for node_id, _ in NODE_SPECS}
            else:
                registry = {node_id: "active" for node_id, _ in NODE_SPECS}
                counts = {node_id: 0 for node_id, _ in NODE_SPECS}

            nodes = []
            for node_id, port in NODE_SPECS:
                nodes.append(
                    {
                        "node_id": node_id,
                        "port": port,
                        "process_status": "online" if self.process_online(node_id) else "offline",
                        "registry_state": registry.get(node_id, "active"),
                        "chunk_count": counts.get(node_id, 0),
                    }
                )

            artifact_view = None
            if self.artifact is not None:
                artifact_view = {
                    key: value
                    for key, value in self.artifact.items()
                    if key != "source_path"
                }

            lifecycle_view = {
                "initialized": self.lifecycle.get("initialized", False),
                "policy_id": self.lifecycle.get("policy_id"),
                "versions": [
                    {
                        "label": v["label"],
                        "version_id": v["version_id"],
                        "semantic_status": v["semantic_status"],
                        "pin": v["pin"],
                        "last_decision": v.get("last_decision"),
                        "reason": v.get("reason"),
                        "physical_representation": v.get("physical_representation", "present"),
                    }
                    for v in self.lifecycle.get("versions", [])
                ],
            }

            return {
                "ready": self.ready and self.metadata_online(),
                "busy": self.op_lock.locked(),
                "cluster": {
                    "metadata_service": {
                        "status": "online" if self.metadata_online() else "offline",
                        "port": METADATA_PORT,
                    },
                    "nodes": nodes,
                },
                "artifact": artifact_view,
                "guided": dict(self.guided),
                "lifecycle": lifecycle_view,
                "events": list(self.events),
            }

    def push_file(
        self,
        source: Path,
        *,
        filename: str,
        chunking_strategy: str,
        replication_factor: int,
        artifact_kind: str,
        size_bytes: int,
        source_sha: str,
    ) -> dict[str, Any]:
        if chunking_strategy not in CHUNKING_STRATEGIES:
            raise DemoError("invalid_chunking", "chunking_strategy must be fixed-size or fastcdc")
        if replication_factor not in {1, 2, 3}:
            raise DemoError("invalid_rf", "replication_factor must be 1, 2, or 3")
        if artifact_kind not in ARTIFACT_KINDS:
            raise DemoError("invalid_kind", "unsupported artifact_kind")

        artifact_id = make_uuid_v7()
        self.insert_artifact(artifact_id, sanitize_filename(filename))
        args = [
            str(self.aistore),
            "push",
            "--file",
            str(source),
            "--artifact-id",
            artifact_id,
            "--replication-factor",
            str(replication_factor),
            "--metadata",
            f"aistore.artifact_kind={artifact_kind}",
            "--metadata-address",
            "127.0.0.1",
            "--metadata-port",
            str(METADATA_PORT),
        ]
        if chunking_strategy == "fixed-size":
            args.extend(["--chunking-strategy", "fixed-size", "--chunk-size", str(4 * 1024 * 1024)])
        else:
            args.extend(
                [
                    "--chunking-strategy",
                    "fastcdc",
                    "--min-chunk-size",
                    str(2 * 1024 * 1024),
                    "--avg-chunk-size",
                    str(4 * 1024 * 1024),
                    "--max-chunk-size",
                    str(8 * 1024 * 1024),
                ]
            )

        started = time.perf_counter()
        cli = self.run_cli(args)
        elapsed = time.perf_counter() - started
        layout_id = str(cli["layout_id"])
        chunks = self.query_chunk_placements(layout_id)
        artifact = {
            "artifact_id": artifact_id,
            "version_id": str(cli["version_id"]),
            "object_id": str(cli["object_id"]),
            "layout_id": layout_id,
            "filename": sanitize_filename(filename),
            "size_bytes": size_bytes,
            "chunking_strategy": chunking_strategy,
            "replication_factor": replication_factor,
            "artifact_kind": artifact_kind,
            "health": self.compute_health(chunks, replication_factor),
            "chunks": chunks,
        }
        self.artifact = artifact
        self.source_path = source
        self.source_sha256 = source_sha
        self.restored_path = None
        self.last_push_stats = {
            "label": "Push",
            "elapsed_seconds": elapsed,
            "chunk_count": len(chunks),
            "bytes": size_bytes,
            "bytes_sent_to_storage": cli.get("bytes_sent_to_storage"),
            "put_requests": cli.get("put_requests"),
        }
        self.emit(
            "push",
            f"Pushed {sanitize_filename(filename)} ({size_bytes} bytes) with "
            f"{chunking_strategy} RF={replication_factor}; {len(chunks)} chunks.",
        )
        return artifact

    def pull_current(self) -> dict[str, Any]:
        if self.artifact is None or self.source_sha256 is None:
            raise DemoError("no_artifact", "Push an artifact before Pull.", 400)
        assert self.work_dir is not None
        destination = self.work_dir / "restores" / f"{self.artifact['version_id']}.bin"
        if destination.exists():
            destination.unlink()
        offline = [
            node_id
            for node_id, _ in NODE_SPECS
            if not self.process_online(node_id)
        ]
        started = time.perf_counter()
        cli = self.run_cli(
            [
                str(self.aistore),
                "pull",
                "--version-id",
                self.artifact["version_id"],
                "--output",
                str(destination),
                "--metadata-address",
                "127.0.0.1",
                "--metadata-port",
                str(METADATA_PORT),
            ]
        )
        elapsed = time.perf_counter() - started
        digest = file_sha256(destination)
        if digest != self.source_sha256:
            raise DemoError("pull_hash_mismatch", "Restored file SHA-256 did not match source.", 500)
        self.restored_path = destination
        self.last_pull_stats = {
            "label": "Pull",
            "elapsed_seconds": elapsed,
            "bytes": int(cli.get("bytes_restored", self.artifact["size_bytes"])),
            "chunks_downloaded": cli.get("chunks_downloaded"),
        }
        if offline:
            self.emit(
                "pull",
                "Pull succeeded while "
                + ", ".join(offline)
                + " was offline; another available replica served the artifact. SHA-256 verified.",
            )
        else:
            self.emit("pull", "Pull succeeded. SHA-256 verified against source.")
        return cli

    def disable_offline_active_nodes(self) -> list[str]:
        registry = self.registry_states()
        disabled: list[str] = []
        for node_id, _ in NODE_SPECS:
            if not self.process_online(node_id) and registry.get(node_id) == "active":
                self.run_cli(
                    [
                        str(self.aistore),
                        "node",
                        "set-state",
                        "--storage-node-id",
                        node_id,
                        "--state",
                        "disabled",
                        "--metadata-address",
                        "127.0.0.1",
                        "--metadata-port",
                        str(METADATA_PORT),
                    ]
                )
                disabled.append(node_id)
                self.emit(
                    "node",
                    f"Marked {node_id} Disabled via production CLI so offline nodes "
                    "are excluded from automatic placement/repair.",
                )
        return disabled

    def repair_current(self) -> dict[str, Any]:
        if self.artifact is None:
            raise DemoError("no_artifact", "Push an artifact before Repair.", 400)
        disabled = self.disable_offline_active_nodes()
        started = time.perf_counter()
        cli = self.run_cli(
            [
                str(self.aistore),
                "repair",
                "--version-id",
                self.artifact["version_id"],
                "--replication-factor",
                str(self.artifact["replication_factor"]),
                "--metadata-address",
                "127.0.0.1",
                "--metadata-port",
                str(METADATA_PORT),
            ]
        )
        elapsed = time.perf_counter() - started
        self.refresh_artifact()
        self.last_repair_stats = {
            "label": "Repair",
            "elapsed_seconds": elapsed,
            "replicas_written": cli.get("replicas_written"),
            "replicas_verified": cli.get("replicas_verified"),
            "disabled_nodes": disabled,
            "message": "Repair completed via production CLI.",
        }
        self.emit(
            "repair",
            "Repair completed"
            + (f" after disabling {', '.join(disabled)}" if disabled else "")
            + ".",
        )
        return cli

    def kill_node(self, node_id: str) -> None:
        if node_id not in NODE_IDS:
            raise DemoError("invalid_node", "Unknown node id.", 404)
        proc = self.node_procs.get(node_id)
        if proc is None or proc.poll() is not None:
            raise DemoError("node_already_offline", f"{node_id} is already offline.", 400)
        self._stop_proc(proc)
        self.node_procs[node_id] = None
        self.emit("node", f"Storage node {node_id} went offline (process stopped; registry unchanged).")
        self.refresh_artifact()

    def restart_node(self, node_id: str) -> None:
        if node_id not in NODE_IDS:
            raise DemoError("invalid_node", "Unknown node id.", 404)
        if self.process_online(node_id):
            raise DemoError("node_already_online", f"{node_id} is already online.", 400)
        if not port_is_free(self.node_ports[node_id]):
            raise DemoError(
                "port_busy",
                f"Port {self.node_ports[node_id]} is occupied; cannot restart {node_id}.",
                500,
            )
        self.start_node(node_id)
        self.emit("node", f"Storage node {node_id} restarted (same CAS root; registry unchanged).")
        self.refresh_artifact()

    def set_node_state(self, node_id: str, state: str) -> None:
        if node_id not in NODE_IDS:
            raise DemoError("invalid_node", "Unknown node id.", 404)
        if state not in {"active", "draining", "disabled"}:
            raise DemoError("invalid_state", "state must be active, draining, or disabled")
        self.run_cli(
            [
                str(self.aistore),
                "node",
                "set-state",
                "--storage-node-id",
                node_id,
                "--state",
                state,
                "--metadata-address",
                "127.0.0.1",
                "--metadata-port",
                str(METADATA_PORT),
            ]
        )
        self.emit("node", f"Registry state for {node_id} set to {state} via production CLI.")

    def guided_push(self) -> dict[str, Any]:
        assert self.work_dir is not None
        if self.guided["step"] != "READY":
            raise DemoError("guided_step", "Guided demo expects READY before push.", 409)
        path = self.work_dir / "uploads" / "guided-artifact.bin"
        sha = xorshift_stream_to_file(path, GUIDED_BYTES, 0xD3A0015C00000001)
        artifact = self.push_file(
            path,
            filename="guided-artifact.bin",
            chunking_strategy="fastcdc",
            replication_factor=2,
            artifact_kind="model-checkpoint",
            size_bytes=GUIDED_BYTES,
            source_sha=sha,
        )
        self.guided["step"] = "PUSHED"
        self.guided["step_index"] = 1
        self.guided["summary"]["push"] = self.last_push_stats
        self.emit("system", "Guided step complete: artifact pushed with FastCDC RF=2.")
        return artifact

    def guided_kill(self) -> str:
        if self.guided["step"] != "PUSHED" or self.artifact is None:
            raise DemoError("guided_step", "Guided demo expects PUSHED before kill.", 409)
        counts: dict[str, int] = {node_id: 0 for node_id, _ in NODE_SPECS}
        for chunk in self.artifact.get("chunks", []):
            for loc in chunk.get("locations", []):
                if loc.get("metadata_state") == "available":
                    counts[loc["node_id"]] = counts.get(loc["node_id"], 0) + 1
        candidates = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        if not candidates or candidates[0][1] <= 0:
            raise DemoError("guided_kill", "No node stores current artifact chunks.", 500)
        target = candidates[0][0]
        self.kill_node(target)
        self.guided["step"] = "NODE_FAILED"
        self.guided["step_index"] = 2
        self.guided["killed_node_id"] = target
        return target

    def guided_pull(self) -> None:
        if self.guided["step"] != "NODE_FAILED":
            raise DemoError("guided_step", "Guided demo expects NODE_FAILED before pull.", 409)
        self.pull_current()
        self.guided["step"] = "PULL_VERIFIED"
        self.guided["step_index"] = 3
        self.guided["summary"]["pull"] = self.last_pull_stats

    def guided_repair(self) -> None:
        if self.guided["step"] != "PULL_VERIFIED":
            raise DemoError("guided_step", "Guided demo expects PULL_VERIFIED before repair.", 409)
        self.repair_current()
        self.refresh_artifact()
        if self.artifact is None or self.artifact.get("health") != "healthy":
            # After disabling offline node, healthy means remaining Active online nodes satisfy RF.
            # Recompute: each chunk needs RF reachable among non-disabled? Spec: replication healthy
            # according to actual locations/process state. With offline disabled node, desired may
            # still list offline node. Check each chunk has >= RF available+online locations OR
            # >= RF available locations on online nodes.
            rf = int(self.artifact["replication_factor"]) if self.artifact else 2
            ok = True
            for chunk in self.artifact.get("chunks", []) if self.artifact else []:
                reachable = sum(
                    1
                    for loc in chunk["locations"]
                    if loc["metadata_state"] == "available" and loc["process_status"] == "online"
                )
                if reachable < rf:
                    ok = False
                    break
            if not ok:
                raise DemoError("repair_incomplete", "Repair did not restore reachable RF replicas.", 500)
            self.artifact["health"] = "healthy"
        self.guided["step"] = "REPAIRED"
        self.guided["step_index"] = 4
        self.guided["summary"]["repair"] = self.last_repair_stats
        self.emit("system", "Replicas repaired. Finish Guided Demo to mark presentation complete.")

    def guided_complete(self) -> None:
        if self.guided["step"] != "REPAIRED":
            raise DemoError(
                "guided_step",
                "Guided demo expects REPAIRED before complete.",
                409,
            )
        self.guided["step"] = "COMPLETE"
        self.guided["step_index"] = 5
        self.emit("system", "Distributed recovery complete.")

    def lifecycle_setup(self) -> None:
        assert self.work_dir is not None
        artifact_id = make_uuid_v7()
        self.insert_artifact(artifact_id, "lifecycle-demo-artifact")
        versions = []
        seeds = (0xC1FE000000000001, 0xC1FE000000000002, 0xC1FE000000000003)
        labels = ("v1", "v2", "v3")
        for index, (label, seed) in enumerate(zip(labels, seeds)):
            path = self.work_dir / "uploads" / f"lifecycle-{label}.bin"
            sha = xorshift_stream_to_file(path, LIFECYCLE_BYTES, seed)
            args = [
                str(self.aistore),
                "push",
                "--file",
                str(path),
                "--artifact-id",
                artifact_id,
                "--replication-factor",
                "1",
                "--chunking-strategy",
                "fixed-size",
                "--chunk-size",
                str(4 * 1024 * 1024),
                "--metadata",
                "aistore.artifact_kind=model-checkpoint",
                "--metadata-address",
                "127.0.0.1",
                "--metadata-port",
                str(METADATA_PORT),
            ]
            cli = self.run_cli(args)
            versions.append(
                {
                    "label": label,
                    "version_id": str(cli["version_id"]),
                    "layout_id": str(cli["layout_id"]),
                    "semantic_status": "active",
                    "pin": False,
                    "last_decision": None,
                    "reason": None,
                    "physical_representation": "present",
                    "source_sha": sha,
                }
            )
            self.emit("lifecycle", f"Committed lifecycle {label} ({LIFECYCLE_BYTES} bytes).")
            if index < 2:
                time.sleep(1.05)

        v1 = versions[0]["version_id"]
        self.run_cli(
            [
                str(self.aistore),
                "lifecycle",
                "pin",
                "--version-id",
                v1,
                "--reason",
                "golden-checkpoint",
                "--metadata-address",
                "127.0.0.1",
                "--metadata-port",
                str(METADATA_PORT),
            ]
        )
        versions[0]["pin"] = True
        self.emit("lifecycle", "Pinned v1 with reason golden-checkpoint.")

        policy_id = make_uuid_v7()
        policy = {
            "name": "interactive-demo-lifecycle",
            "rules": [
                {"artifact_kind": kind, "keep_last_n": 1, "max_age_seconds": None}
                for kind in (
                    "generic",
                    "model-checkpoint",
                    "dataset-snapshot",
                    "embedding-index",
                    "evaluation-output",
                )
            ],
        }
        policy_path = self.work_dir / "policy" / "demo-policy.json"
        policy_path.write_text(json.dumps(policy), encoding="utf-8")
        self.run_cli(
            [
                str(self.aistore),
                "lifecycle",
                "policy",
                "create",
                "--file",
                str(policy_path),
                "--policy-id",
                policy_id,
                "--metadata-address",
                "127.0.0.1",
                "--metadata-port",
                str(METADATA_PORT),
            ]
        )
        self.lifecycle = {
            "initialized": True,
            "policy_id": policy_id,
            "versions": versions,
        }
        self.refresh_lifecycle()
        self.emit("lifecycle", "Lifecycle scenario initialized (v1 pinned, keep_last_n=1).")

    def lifecycle_dry_run(self) -> None:
        if not self.lifecycle.get("initialized"):
            raise DemoError("lifecycle_not_ready", "Initialize the lifecycle scenario first.", 400)
        run_id = make_uuid_v7()
        self.run_cli(
            [
                str(self.aistore),
                "lifecycle",
                "run",
                "--dry-run",
                "--policy-id",
                self.lifecycle["policy_id"],
                "--lifecycle-run-id",
                run_id,
                "--metadata-address",
                "127.0.0.1",
                "--metadata-port",
                str(METADATA_PORT),
            ]
        )
        explain = self.run_cli(
            [
                str(self.aistore),
                "lifecycle",
                "explain",
                "--lifecycle-run-id",
                run_id,
                "--metadata-address",
                "127.0.0.1",
                "--metadata-port",
                str(METADATA_PORT),
            ]
        )
        by_version = {
            entry["version_id"]: (entry.get("decision"), entry.get("reason"))
            for entry in explain.get("decisions", [])
        }
        for version in self.lifecycle["versions"]:
            decision, reason = by_version.get(version["version_id"], (None, None))
            version["last_decision"] = decision
            version["reason"] = reason
        retired = self.psql("SELECT COUNT(*)::text FROM artifact_version_retirements;", dbname=self.db_name)
        if int(retired or "0") != 0:
            raise DemoError("lifecycle_dry_run", "Dry-run unexpectedly created retirement rows.", 500)
        self.refresh_lifecycle()
        self.emit("lifecycle", "Lifecycle dry-run complete; no retirement rows written.")

    def lifecycle_apply(self) -> None:
        if not self.lifecycle.get("initialized"):
            raise DemoError("lifecycle_not_ready", "Initialize the lifecycle scenario first.", 400)
        run_id = make_uuid_v7()
        self.run_cli(
            [
                str(self.aistore),
                "lifecycle",
                "run",
                "--policy-id",
                self.lifecycle["policy_id"],
                "--lifecycle-run-id",
                run_id,
                "--metadata-address",
                "127.0.0.1",
                "--metadata-port",
                str(METADATA_PORT),
            ]
        )
        explain = self.run_cli(
            [
                str(self.aistore),
                "lifecycle",
                "explain",
                "--lifecycle-run-id",
                run_id,
                "--metadata-address",
                "127.0.0.1",
                "--metadata-port",
                str(METADATA_PORT),
            ]
        )
        by_version = {
            entry["version_id"]: (entry.get("decision"), entry.get("reason"))
            for entry in explain.get("decisions", [])
        }
        for version in self.lifecycle["versions"]:
            decision, reason = by_version.get(version["version_id"], (None, None))
            version["last_decision"] = decision
            version["reason"] = reason
        self.refresh_lifecycle()
        v2 = next(v for v in self.lifecycle["versions"] if v["label"] == "v2")
        if v2["semantic_status"] != "retired":
            raise DemoError("lifecycle_apply", "Expected v2 to be retired after Apply.", 500)
        if v2["physical_representation"] == "reclaimed":
            raise DemoError(
                "lifecycle_apply",
                "Expected v2 physical representation to remain present before GC.",
                500,
            )
        self.emit(
            "lifecycle",
            "Lifecycle Apply retired v2 semantically; physical representation still present.",
        )

    def lifecycle_gc(self) -> None:
        if not self.lifecycle.get("initialized"):
            raise DemoError("lifecycle_not_ready", "Initialize the lifecycle scenario first.", 400)
        for node_id, _ in NODE_SPECS:
            if not self.process_online(node_id):
                continue
            run_id = make_uuid_v7()
            self.run_cli(
                [
                    str(self.aistore),
                    "gc",
                    "--storage-node-id",
                    node_id,
                    "--gc-run-id",
                    run_id,
                    "--metadata-address",
                    "127.0.0.1",
                    "--metadata-port",
                    str(METADATA_PORT),
                ]
            )
            self.emit("gc", f"GC completed for {node_id}.")
        self.refresh_lifecycle()
        self.emit("gc", "Lifecycle GC finished; ArtifactVersion history remains preserved.")

    def reference_performance(self) -> dict[str, Any]:
        chunking_path = self.repo_root / "benchmarks" / "results" / "m10_reference_chunking.json"
        process_path = self.repo_root / "benchmarks" / "results" / "m10_reference_process.json"
        if not chunking_path.is_file() or not process_path.is_file():
            raise DemoError(
                "reference_missing",
                "M10 reference benchmark JSON files are missing from the repository.",
                503,
            )
        return {
            "chunking": json.loads(chunking_path.read_text(encoding="utf-8")),
            "process": json.loads(process_path.read_text(encoding="utf-8")),
        }

    def handle_push_upload(self, query: dict[str, list[str]], body_stream: Any, content_length: int | None) -> dict[str, Any]:
        assert self.work_dir is not None
        if content_length is not None and content_length > MAX_UPLOAD_BYTES:
            raise DemoError("payload_too_large", "Upload exceeds 256 MiB limit.", 413)
        filename = sanitize_filename((query.get("filename") or ["upload.bin"])[0])
        chunking = (query.get("chunking_strategy") or ["fixed-size"])[0]
        try:
            rf = int((query.get("replication_factor") or ["1"])[0])
        except ValueError as exc:
            raise DemoError("invalid_rf", "replication_factor must be an integer") from exc
        kind = (query.get("artifact_kind") or ["generic"])[0]
        path = self.work_dir / "uploads" / f"{make_uuid_v7()}-{filename}"
        hasher = hashlib.sha256()
        written = 0
        with path.open("wb") as handle:
            remaining = content_length if content_length is not None else None
            while True:
                to_read = STREAM_BLOCK if remaining is None else min(STREAM_BLOCK, remaining)
                if to_read <= 0:
                    break
                chunk = body_stream.read(to_read)
                if not chunk:
                    break
                written += len(chunk)
                if written > MAX_UPLOAD_BYTES:
                    handle.close()
                    path.unlink(missing_ok=True)
                    raise DemoError("payload_too_large", "Upload exceeds 256 MiB limit.", 413)
                handle.write(chunk)
                hasher.update(chunk)
                if remaining is not None:
                    remaining -= len(chunk)
        if written == 0:
            raise DemoError("empty_upload", "Uploaded body was empty.", 400)
        return self.push_file(
            path,
            filename=filename,
            chunking_strategy=chunking,
            replication_factor=rf,
            artifact_kind=kind,
            size_bytes=written,
            source_sha=hasher.hexdigest(),
        )


CONTROLLER: DemoController | None = None


def with_mutation(handler: Callable[[], Any]) -> dict[str, Any]:
    assert CONTROLLER is not None
    if not CONTROLLER.op_lock.acquire(blocking=False):
        raise DemoError("demo_operation_in_progress", "Another demo operation is already in progress.", 409)
    try:
        if not CONTROLLER.ready:
            raise DemoError("not_ready", "Demo cluster is not ready.", 503)
        handler()
        return CONTROLLER.build_state()
    finally:
        CONTROLLER.op_lock.release()


class DemoHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        sys.stderr.write("demo-http: " + (fmt % args) + "\n")

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, exc: DemoError) -> None:
        self._send_json(exc.status, {"error": exc.code, "message": exc.message})

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        raw = self.rfile.read(length) if length > 0 else b"{}"
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DemoError("invalid_json", "Request body must be JSON.") from exc
        if not isinstance(payload, dict):
            raise DemoError("invalid_json", "JSON root must be an object.")
        return payload

    def do_GET(self) -> None:  # noqa: N802
        assert CONTROLLER is not None
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            if path == "/api/health":
                self._send_json(200, {"status": "ok"})
                return
            if path == "/api/state":
                self._send_json(200, CONTROLLER.build_state())
                return
            if path == "/api/reference-performance":
                self._send_json(200, CONTROLLER.reference_performance())
                return
            if path == "/api/artifacts/restored":
                restored = CONTROLLER.restored_path
                if restored is None or not restored.is_file():
                    raise DemoError("no_restored_file", "No successful restore is available.", 404)
                data = restored.read_bytes()
                filename = "restored.bin"
                if CONTROLLER.artifact is not None:
                    filename = sanitize_filename(CONTROLLER.artifact.get("filename", "restored.bin"))
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
                self.end_headers()
                self.wfile.write(data)
                return
            raise DemoError("not_found", "Unknown endpoint.", 404)
        except DemoError as exc:
            self._send_error(exc)
        except Exception:
            traceback.print_exc()
            self._send_json(500, {"error": "internal_error", "message": "Unexpected controller failure."})

    def do_POST(self) -> None:  # noqa: N802
        assert CONTROLLER is not None
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)

        def run(mutator: Callable[[], Any]) -> None:
            state = with_mutation(mutator)
            self._send_json(200, state)

        try:
            if path == "/api/artifacts/push":
                length_header = self.headers.get("Content-Length")
                content_length = int(length_header) if length_header is not None else None

                def push() -> None:
                    CONTROLLER.handle_push_upload(query, self.rfile, content_length)

                run(push)
                return

            if path == "/api/artifacts/pull":
                run(CONTROLLER.pull_current)
                return
            if path == "/api/artifacts/repair":
                run(CONTROLLER.repair_current)
                return
            if path == "/api/demo/reset":
                # Reset acquires op lock and rebuilds cluster.
                if not CONTROLLER.op_lock.acquire(blocking=False):
                    raise DemoError(
                        "demo_operation_in_progress",
                        "Another demo operation is already in progress.",
                        409,
                    )
                try:
                    state = CONTROLLER.reset_demo()
                finally:
                    CONTROLLER.op_lock.release()
                self._send_json(200, state)
                return
            if path == "/api/guided/push":
                run(CONTROLLER.guided_push)
                return
            if path == "/api/guided/kill":
                run(CONTROLLER.guided_kill)
                return
            if path == "/api/guided/pull":
                run(CONTROLLER.guided_pull)
                return
            if path == "/api/guided/repair":
                run(CONTROLLER.guided_repair)
                return
            if path == "/api/guided/complete":
                run(CONTROLLER.guided_complete)
                return
            if path == "/api/lifecycle/setup":
                run(CONTROLLER.lifecycle_setup)
                return
            if path == "/api/lifecycle/dry-run":
                run(CONTROLLER.lifecycle_dry_run)
                return
            if path == "/api/lifecycle/apply":
                run(CONTROLLER.lifecycle_apply)
                return
            if path == "/api/lifecycle/gc":
                run(CONTROLLER.lifecycle_gc)
                return

            match = re.fullmatch(r"/api/nodes/(node-[abc])/(kill|restart|state)", path)
            if match:
                node_id, action = match.group(1), match.group(2)
                if action == "kill":
                    run(lambda: CONTROLLER.kill_node(node_id))
                    return
                if action == "restart":
                    run(lambda: CONTROLLER.restart_node(node_id))
                    return
                body = self._read_json()
                state = str(body.get("state", ""))
                run(lambda: CONTROLLER.set_node_state(node_id, state))
                return

            raise DemoError("not_found", "Unknown endpoint.", 404)
        except DemoError as exc:
            # Drain unread body on failed upload to keep connection sane.
            try:
                length = int(self.headers.get("Content-Length", "0") or "0")
                if length > 0 and path.endswith("/push"):
                    remaining = length
                    while remaining > 0:
                        chunk = self.rfile.read(min(STREAM_BLOCK, remaining))
                        if not chunk:
                            break
                        remaining -= len(chunk)
            except Exception:
                pass
            self._send_error(exc)
        except Exception:
            traceback.print_exc()
            self._send_json(500, {"error": "internal_error", "message": "Unexpected controller failure."})

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(204)
        self.end_headers()

    def end_headers(self) -> None:
        # Local Vite proxy only; no public CORS surface required.
        super().end_headers()


def main() -> int:
    global CONTROLLER
    repo_root = Path(__file__).resolve().parents[2]
    build_dir = Path(os.environ.get("AISTORE_DEMO_BUILD_DIR", "build-demo-release"))
    if not build_dir.is_absolute():
        build_dir = (repo_root / build_dir).resolve()
    admin_db_url = os.environ.get("AISTORE_DEMO_ADMIN_DB_URL", "postgresql:///postgres")
    parsed = urlparse(admin_db_url)
    admin_db_name = (parsed.path or "/").lstrip("/") or "postgres"
    if admin_db_name in {"ai_artifact_store_dev", "ai_artifact_store_test"}:
        die("AISTORE_DEMO_ADMIN_DB_URL must not target ai_artifact_store_dev or ai_artifact_store_test")

    if shutil.which("psql") is None:
        die("psql is required")

    if not port_is_free(CONTROLLER_PORT):
        die(f"controller port {CONTROLLER_PORT} is occupied")

    CONTROLLER = DemoController(repo_root=repo_root, build_dir=build_dir, admin_db_url=admin_db_url)
    server: ThreadingHTTPServer | None = None

    def handle_signal(signum: int, _frame: Any) -> None:
        print(f"demo-controller: received signal {signum}, cleaning up", file=sys.stderr)
        if CONTROLLER is not None and not CONTROLLER.shutting_down:
            CONTROLLER.shutting_down = True
            CONTROLLER.cleanup()
        if server is not None:
            threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        CONTROLLER.bootstrap_cluster()
    except Exception:
        CONTROLLER.cleanup()
        raise

    server = ThreadingHTTPServer((CONTROLLER_HOST, CONTROLLER_PORT), DemoHandler)
    print(f"demo-controller: listening on http://{CONTROLLER_HOST}:{CONTROLLER_PORT}", flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        if CONTROLLER is not None and not CONTROLLER.shutting_down:
            CONTROLLER.shutting_down = True
            CONTROLLER.cleanup()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
