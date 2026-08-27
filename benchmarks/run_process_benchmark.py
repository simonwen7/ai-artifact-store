#!/usr/bin/env python3
"""Localhost process benchmark harness for AI Artifact Store.

Uses production binaries against a uniquely named disposable PostgreSQL database.
Never touches ai_artifact_store_dev or ai_artifact_store_test.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunparse


FORBIDDEN_DB_NAMES = {"ai_artifact_store_dev", "ai_artifact_store_test", "postgres"}
REQUIRED_PORTS = (8080, 8081, 8082, 8083)
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


def die(message: str) -> None:
    print(f"process-benchmark error: {message}", file=sys.stderr)
    raise SystemExit(1)


def require_tool(name: str) -> str:
    path = shutil.which(name)
    if path is None:
        die(f"required tool not found on PATH: {name}")
    return path


def port_is_free(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("127.0.0.1", port))
        except OSError:
            return False
    return True


def assert_ports_free() -> None:
    busy = [port for port in REQUIRED_PORTS if not port_is_free(port)]
    if busy:
        die(f"required benchmark ports are occupied: {busy}")


def parse_admin_db_url(url: str) -> tuple[str, str]:
    parsed = urlparse(url)
    db_name = (parsed.path or "/").lstrip("/") or "postgres"
    if db_name in {"ai_artifact_store_dev", "ai_artifact_store_test"}:
        die("admin-db-url must not target ai_artifact_store_dev or ai_artifact_store_test")
    return url, db_name


def rewrite_db_url(admin_url: str, db_name: str) -> str:
    parsed = urlparse(admin_url)
    if parsed.scheme.startswith("postgres") and not parsed.netloc:
        # Preserve libpq unix-socket form: postgresql:///dbname
        return f"{parsed.scheme}:///{db_name}"
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            f"/{db_name}",
            parsed.params,
            parsed.query,
            parsed.fragment,
        )
    )


def run_checked(cmd: list[str], *, env: dict[str, str] | None = None, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        env=env,
        cwd=str(cwd) if cwd is not None else None,
    )
    if result.returncode != 0:
        die(
            "command failed ("
            + " ".join(cmd)
            + f") exit={result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def psql(admin_url: str, sql: str, *, dbname: str | None = None) -> str:
    require_tool("psql")
    url = rewrite_db_url(admin_url, dbname) if dbname is not None else admin_url
    result = run_checked(["psql", url, "-v", "ON_ERROR_STOP=1", "-At", "-c", sql])
    return result.stdout.strip()


def apply_migrations(admin_url: str, db_name: str, repo_root: Path) -> None:
    url = rewrite_db_url(admin_url, db_name)
    for name in MIGRATIONS:
        path = repo_root / "migrations" / name
        if not path.is_file():
            die(f"missing migration: {path.name}")
        run_checked(["psql", url, "-v", "ON_ERROR_STOP=1", "-f", str(path)])


def xorshift_stream_to_file(path: Path, byte_count: int, seed: int) -> str:
    """Stream deterministic bytes to disk; return SHA-256 hex of full file."""
    hasher = hashlib.sha256()
    state = seed & 0xFFFFFFFFFFFFFFFF
    remaining = byte_count
    block = bytearray(1024 * 1024)

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


def wait_health(url: str, timeout_s: float = 15.0) -> None:
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
            time.sleep(0.1)
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
        time.sleep(0.1)

    die(f"service did not become ready: {url}")


def discover_cpu_model() -> str | None:
    system = platform.system()
    try:
        if system == "Darwin":
            out = subprocess.run(
                ["sysctl", "-n", "machdep.cpu.brand_string"],
                check=False,
                capture_output=True,
                text=True,
            )
            text = out.stdout.strip()
            return text or None
        if system == "Linux" and Path("/proc/cpuinfo").is_file():
            for line in Path("/proc/cpuinfo").read_text(encoding="utf-8", errors="replace").splitlines():
                if line.lower().startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        return None
    return None


def compiler_version(binary: Path) -> str | None:
    # Prefer clang++ --version from PATH used for Release builds when available.
    clang = shutil.which("clang++")
    if clang is None:
        return None
    out = subprocess.run([clang, "--version"], check=False, capture_output=True, text=True)
    if out.returncode != 0 or not out.stdout.strip():
        return None
    return out.stdout.splitlines()[0].strip()


def cmake_version() -> str | None:
    cmake = shutil.which("cmake")
    if cmake is None:
        return None
    out = subprocess.run([cmake, "--version"], check=False, capture_output=True, text=True)
    if out.returncode != 0 or not out.stdout.strip():
        return None
    return out.stdout.splitlines()[0].strip()


def make_uuid_v7() -> str:
    """Generate an RFC 9562-style UUIDv7 for Artifact/session IDs."""
    millis = int(time.time() * 1000) & ((1 << 48) - 1)
    rand_a = secrets.randbits(12)
    rand_b = secrets.randbits(62)
    value = (millis << 80) | (0x7 << 76) | (rand_a << 64) | (0b10 << 62) | rand_b
    hex_digits = f"{value:032x}"
    return f"{hex_digits[0:8]}-{hex_digits[8:12]}-{hex_digits[12:16]}-{hex_digits[16:20]}-{hex_digits[20:32]}"


def scrub_result(obj: Any) -> Any:
    """Defensive scrub of accidental absolute personal paths / credentials."""
    if isinstance(obj, dict):
        cleaned: dict[str, Any] = {}
        for key, value in obj.items():
            key_l = str(key).lower()
            if key_l in {"db_url", "database_url", "password", "username", "home", "hostname", "user"}:
                continue
            cleaned[key] = scrub_result(value)
        return cleaned
    if isinstance(obj, list):
        return [scrub_result(item) for item in obj]
    if isinstance(obj, str):
        if obj.startswith("/Users/") or "://" in obj and "@" in obj:
            return "<redacted>"
        return obj
    return obj


class ProcessBenchmark:
    def __init__(self, build_dir: Path, admin_db_url: str, file_bytes: int, output: Path | None) -> None:
        self.repo_root = Path(__file__).resolve().parents[1]
        self.build_dir = build_dir.resolve()
        self.admin_db_url = admin_db_url
        self.file_bytes = file_bytes
        self.output = output
        self.db_name = f"aistore_benchmark_{os.getpid()}_{secrets.token_hex(4)}"
        self.work_dir = Path(tempfile.mkdtemp(prefix="aistore-bench-"))
        self.pids: list[int] = []
        self.db_created = False

        if self.db_name in FORBIDDEN_DB_NAMES:
            die("internal error: generated forbidden database name")

        self.aistore = self.build_dir / "aistore"
        self.metadata = self.build_dir / "metadata-service"
        self.storage = self.build_dir / "storage-node"

        for binary in (self.aistore, self.metadata, self.storage):
            if not binary.is_file() or not os.access(binary, os.X_OK):
                die(f"missing executable binary: {binary.name} under {self.build_dir}")

    def cleanup(self) -> None:
        for pid in reversed(self.pids):
            try:
                os.kill(pid, 15)
            except ProcessLookupError:
                continue
            for _ in range(50):
                try:
                    os.waitpid(pid, os.WNOHANG)
                except ChildProcessError:
                    break
                time.sleep(0.05)
            try:
                os.kill(pid, 9)
            except ProcessLookupError:
                pass

        if self.db_created:
            try:
                # DROP only the owned benchmark database.
                psql(self.admin_db_url, f'DROP DATABASE IF EXISTS "{self.db_name}";')
            except SystemExit:
                print(
                    f"process-benchmark warning: failed to drop owned database {self.db_name}",
                    file=sys.stderr,
                )

        shutil.rmtree(self.work_dir, ignore_errors=True)

    def create_db(self) -> None:
        existing = psql(self.admin_db_url, f"SELECT 1 FROM pg_database WHERE datname = '{self.db_name}';")
        if existing == "1":
            die(f"refusing to reuse pre-existing database name: {self.db_name}")
        psql(self.admin_db_url, f'CREATE DATABASE "{self.db_name}";')
        self.db_created = True
        apply_migrations(self.admin_db_url, self.db_name, self.repo_root)

    def start_services(self) -> None:
        assert_ports_free()
        db_url = rewrite_db_url(self.admin_db_url, self.db_name)

        meta_env = os.environ.copy()
        meta_env["AISTORE_DB_URL"] = db_url
        meta = subprocess.Popen(
            [str(self.metadata)],
            env=meta_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.pids.append(meta.pid)

        for node_id, port in (("bench-node-a", 8081), ("bench-node-b", 8082), ("bench-node-c", 8083)):
            cas = self.work_dir / f"cas-{node_id}"
            cas.mkdir(parents=True, exist_ok=True)
            env = os.environ.copy()
            env["AISTORE_STORAGE_ROOT"] = str(cas)
            env["AISTORE_STORAGE_NODE_ID"] = node_id
            env["AISTORE_STORAGE_PORT"] = str(port)
            proc = subprocess.Popen(
                [str(self.storage)],
                env=env,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
            self.pids.append(proc.pid)
            wait_health(f"http://127.0.0.1:{port}/health")

        wait_health("http://127.0.0.1:8080/health")

        for node_id, port in (("bench-node-a", 8081), ("bench-node-b", 8082), ("bench-node-c", 8083)):
            run_checked(
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
                    "8080",
                ]
            )

    def insert_artifact(self, artifact_id: str, name: str) -> None:
        sql = (
            "INSERT INTO artifacts (artifact_id, name, project) VALUES "
            f"('{artifact_id}'::uuid, '{name}', 'm10-process-benchmark') "
            "ON CONFLICT (artifact_id) DO NOTHING;"
        )
        psql(self.admin_db_url, sql, dbname=self.db_name)

    def run_cli_json(self, args: list[str]) -> dict[str, Any]:
        result = run_checked(args)
        line = result.stdout.strip().splitlines()[-1]
        try:
            payload = json.loads(line)
        except json.JSONDecodeError as exc:
            die(f"CLI did not emit JSON: {exc}\nstdout={result.stdout!r}")
        if not isinstance(payload, dict):
            die("CLI JSON root must be an object")
        return payload

    def scenario_push(
        self,
        *,
        name: str,
        source: Path,
        artifact_id: str,
        replication_factor: int,
        include_network_avoidance: bool = False,
    ) -> dict[str, Any]:
        self.insert_artifact(artifact_id, name)
        started = time.perf_counter()
        cli = self.run_cli_json(
            [
                str(self.aistore),
                "push",
                "--file",
                str(source),
                "--artifact-id",
                artifact_id,
                "--replication-factor",
                str(replication_factor),
                "--chunk-size",
                str(4 * 1024 * 1024),
                "--metadata-address",
                "127.0.0.1",
                "--metadata-port",
                "8080",
            ]
        )
        elapsed = time.perf_counter() - started
        bytes_read = int(cli["bytes_read"])
        bytes_sent = int(cli["bytes_sent_to_storage"])
        logical_mib_s = (bytes_read / (1024.0 * 1024.0) / elapsed) if elapsed > 0 else 0.0
        result: dict[str, Any] = {
            "name": name,
            "elapsed_seconds": elapsed,
            "logical_bytes": bytes_read,
            "logical_mib_per_second": logical_mib_s,
            "cli": {
                "status": cli.get("status"),
                "version_id": cli.get("version_id"),
                "object_id": cli.get("object_id"),
                "layout_id": cli.get("layout_id"),
                "bytes_read": bytes_read,
                "bytes_sent_to_storage": bytes_sent,
                "put_requests": cli.get("put_requests"),
                "verified_target_chunks": cli.get("verified_target_chunks"),
                "repaired_target_chunks": cli.get("repaired_target_chunks"),
                "total_chunks": cli.get("total_chunks"),
                "unique_chunks": cli.get("unique_chunks"),
            },
        }
        if bytes_read > 0:
            result["storage_byte_amplification"] = bytes_sent / bytes_read
        if include_network_avoidance and bytes_read > 0:
            avoided = bytes_read - bytes_sent
            result["network_bytes_avoided"] = avoided
            result["network_avoidance_ratio"] = avoided / bytes_read
        return result

    def scenario_pull(self, *, name: str, version_id: str, expected_sha256: str) -> dict[str, Any]:
        destination = self.work_dir / f"{name}.out"
        started = time.perf_counter()
        cli = self.run_cli_json(
            [
                str(self.aistore),
                "pull",
                "--version-id",
                version_id,
                "--output",
                str(destination),
                "--metadata-address",
                "127.0.0.1",
                "--metadata-port",
                "8080",
            ]
        )
        elapsed = time.perf_counter() - started
        digest = hashlib.sha256(destination.read_bytes()).hexdigest()
        if digest != expected_sha256:
            die(f"pull hash mismatch for {name}: got {digest} expected {expected_sha256}")
        bytes_restored = int(cli["bytes_restored"])
        logical_mib_s = (bytes_restored / (1024.0 * 1024.0) / elapsed) if elapsed > 0 else 0.0
        return {
            "name": name,
            "elapsed_seconds": elapsed,
            "logical_bytes": bytes_restored,
            "logical_mib_per_second": logical_mib_s,
            "cli": {
                "status": cli.get("status"),
                "version_id": cli.get("version_id"),
                "bytes_restored": bytes_restored,
                "bytes_received_from_storage": cli.get("bytes_received_from_storage"),
                "chunks_downloaded": cli.get("chunks_downloaded"),
                "chunks_reused_from_partial": cli.get("chunks_reused_from_partial"),
                "total_chunks": cli.get("total_chunks"),
            },
            "sha256_verified": True,
        }

    def run(self) -> dict[str, Any]:
        print("process-benchmark: creating disposable database", file=sys.stderr)
        self.create_db()
        print("process-benchmark: starting services", file=sys.stderr)
        self.start_services()

        source_a = self.work_dir / "source-A.bin"
        source_b = self.work_dir / "source-B.bin"
        print(f"process-benchmark: generating {self.file_bytes} byte sources", file=sys.stderr)
        sha_a = xorshift_stream_to_file(source_a, self.file_bytes, 0xA15C0DE500000001)
        sha_b = xorshift_stream_to_file(source_b, self.file_bytes, 0xA15C0DE500000002)

        artifact_a1 = make_uuid_v7()
        artifact_a2 = make_uuid_v7()
        artifact_b = make_uuid_v7()

        print("process-benchmark: cold_push_rf1", file=sys.stderr)
        cold_rf1 = self.scenario_push(
            name="cold_push_rf1",
            source=source_a,
            artifact_id=artifact_a1,
            replication_factor=1,
        )

        print("process-benchmark: warm_dedup_push_rf1", file=sys.stderr)
        warm = self.scenario_push(
            name="warm_dedup_push_rf1",
            source=source_a,
            artifact_id=artifact_a2,
            replication_factor=1,
            include_network_avoidance=True,
        )

        print("process-benchmark: pull_rf1", file=sys.stderr)
        pull_rf1 = self.scenario_pull(
            name="pull_rf1",
            version_id=str(cold_rf1["cli"]["version_id"]),
            expected_sha256=sha_a,
        )

        print("process-benchmark: cold_push_rf2", file=sys.stderr)
        cold_rf2 = self.scenario_push(
            name="cold_push_rf2",
            source=source_b,
            artifact_id=artifact_b,
            replication_factor=2,
        )

        print("process-benchmark: pull_rf2", file=sys.stderr)
        pull_rf2 = self.scenario_pull(
            name="pull_rf2",
            version_id=str(cold_rf2["cli"]["version_id"]),
            expected_sha256=sha_b,
        )

        result = {
            "schema_version": 1,
            "benchmark": "process_localhost",
            "environment": {
                "os": platform.system(),
                "os_version": platform.release(),
                "architecture": platform.machine(),
                "cpu_model": discover_cpu_model(),
                "logical_cpu_count": os.cpu_count(),
                "compiler_version": compiler_version(self.aistore),
                "cmake_version": cmake_version(),
                "build_type": "Release",
            },
            "configuration": {
                "file_bytes": self.file_bytes,
                "fixed_chunk_size_bytes": 4 * 1024 * 1024,
                "storage_node_count": 3,
            },
            "scenarios": {
                "cold_push_rf1": cold_rf1,
                "warm_dedup_push_rf1": warm,
                "pull_rf1": pull_rf1,
                "cold_push_rf2": cold_rf2,
                "pull_rf2": pull_rf2,
            },
        }
        return scrub_result(result)


def main() -> int:
    parser = argparse.ArgumentParser(description="AI Artifact Store localhost process benchmark")
    parser.add_argument("--build-dir", default="build-m10-release")
    parser.add_argument("--admin-db-url", default="postgresql:///postgres")
    parser.add_argument("--file-bytes", type=int, default=67108864)
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()

    if args.file_bytes <= 0:
        die("--file-bytes must be > 0")

    require_tool("psql")
    parse_admin_db_url(args.admin_db_url)

    build_dir = Path(args.build_dir)
    if not build_dir.is_absolute():
        build_dir = (Path.cwd() / build_dir).resolve()

    harness = ProcessBenchmark(
        build_dir=build_dir,
        admin_db_url=args.admin_db_url,
        file_bytes=args.file_bytes,
        output=args.output,
    )

    try:
        result = harness.run()
    finally:
        harness.cleanup()

    text = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
        print(f"process-benchmark: wrote {args.output}", file=sys.stderr)
    else:
        sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
