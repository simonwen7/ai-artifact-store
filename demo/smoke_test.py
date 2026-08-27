#!/usr/bin/env python3
"""HTTP smoke test for the Interactive Systems Demo controller.

Assumes the controller is already running on 127.0.0.1:8787.
Python 3 standard library only. Not CTest.
"""

from __future__ import annotations

import json
import sys
import urllib.error
import urllib.request
from typing import Any

BASE = "http://127.0.0.1:8787"


def request(method: str, path: str, body: bytes | None = None, content_type: str | None = None) -> Any:
    headers = {"Accept": "application/json"}
    if content_type is not None:
        headers["Content-Type"] = content_type
    req = urllib.request.Request(BASE + path, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=600) as response:
            raw = response.read().decode("utf-8")
            if not raw:
                return None
            return json.loads(raw)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"{method} {path} failed: HTTP {exc.code}: {detail}") from exc


def request_expect_status(
    method: str,
    path: str,
    expected_status: int,
    body: bytes | None = None,
    content_type: str | None = None,
) -> Any:
    headers = {"Accept": "application/json"}
    if content_type is not None:
        headers["Content-Type"] = content_type
    req = urllib.request.Request(BASE + path, data=body, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=600) as response:
            raise SystemExit(
                f"{method} {path} expected HTTP {expected_status}, got {response.status}"
            )
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        if exc.code != expected_status:
            raise SystemExit(
                f"{method} {path} expected HTTP {expected_status}, got {exc.code}: {detail}"
            ) from exc
        try:
            return json.loads(detail) if detail else None
        except json.JSONDecodeError:
            return detail


def main() -> int:
    health = request("GET", "/api/health")
    if health.get("status") != "ok":
        raise SystemExit(f"health failed: {health}")

    state = request("POST", "/api/demo/reset")
    if not state.get("ready"):
        raise SystemExit("reset did not yield ready cluster")

    premature = request_expect_status("POST", "/api/guided/complete", 409)
    if not isinstance(premature, dict) or premature.get("error") != "guided_step":
        raise SystemExit(f"premature complete expected guided_step 409, got {premature}")

    state = request("POST", "/api/guided/push")
    artifact = state.get("artifact")
    if not artifact:
        raise SystemExit("guided push did not create artifact")
    if int(artifact.get("replication_factor", 0)) != 2:
        raise SystemExit(f"expected RF2, got {artifact.get('replication_factor')}")
    if not artifact.get("chunks"):
        raise SystemExit("expected chunk placements after push")

    state = request("POST", "/api/guided/kill")
    offline = [n for n in state["cluster"]["nodes"] if n["process_status"] == "offline"]
    if len(offline) != 1:
        raise SystemExit(f"expected exactly one offline node, got {offline}")
    if state.get("guided", {}).get("step") != "NODE_FAILED":
        raise SystemExit(f"expected NODE_FAILED, got {state.get('guided')}")

    state = request("POST", "/api/guided/pull")
    if state.get("guided", {}).get("step") != "PULL_VERIFIED":
        raise SystemExit(f"expected PULL_VERIFIED, got {state.get('guided')}")

    state = request("POST", "/api/guided/repair")
    if state.get("guided", {}).get("step") != "REPAIRED":
        raise SystemExit(f"expected REPAIRED, got {state.get('guided')}")
    health_label = (state.get("artifact") or {}).get("health")
    if health_label != "healthy":
        # Allow healthy after RF reachable on remaining online nodes.
        raise SystemExit(f"expected healthy artifact after repair, got {health_label}")

    state = request("POST", "/api/guided/complete")
    if state.get("guided", {}).get("step") != "COMPLETE":
        raise SystemExit(f"expected COMPLETE, got {state.get('guided')}")

    state = request("POST", "/api/lifecycle/setup")
    life = state.get("lifecycle") or {}
    if not life.get("initialized") or len(life.get("versions", [])) != 3:
        raise SystemExit(f"lifecycle setup failed: {life}")

    state = request("POST", "/api/lifecycle/dry-run")
    versions = {v["label"]: v for v in state["lifecycle"]["versions"]}
    if versions["v1"].get("last_decision") != "retain" or versions["v1"].get("reason") != "pinned":
        raise SystemExit(f"v1 dry-run unexpected: {versions['v1']}")
    if versions["v2"].get("last_decision") != "retire" or versions["v2"].get("reason") != "policy-retire":
        raise SystemExit(f"v2 dry-run unexpected: {versions['v2']}")
    if versions["v3"].get("last_decision") != "retain" or versions["v3"].get("reason") != "keep-last-n":
        raise SystemExit(f"v3 dry-run unexpected: {versions['v3']}")
    if versions["v2"].get("semantic_status") != "active":
        raise SystemExit("dry-run must not retire v2")

    state = request("POST", "/api/lifecycle/apply")
    versions = {v["label"]: v for v in state["lifecycle"]["versions"]}
    if versions["v2"].get("semantic_status") != "retired":
        raise SystemExit("apply must retire v2")
    if versions["v2"].get("physical_representation") == "reclaimed":
        raise SystemExit("v2 physical representation must still be present before GC")

    state = request("POST", "/api/lifecycle/gc")
    versions = {v["label"]: v for v in state["lifecycle"]["versions"]}
    if versions["v2"].get("semantic_status") != "retired":
        raise SystemExit("v2 retirement history must remain after GC")
    if versions["v1"].get("semantic_status") != "active" or versions["v3"].get("semantic_status") != "active":
        raise SystemExit("v1/v3 must remain active after GC")
    if versions["v2"].get("physical_representation") not in {"reclaimed", "partially-reclaimed"}:
        raise SystemExit(f"expected v2 reclaimed after GC, got {versions['v2']}")

    ref = request("GET", "/api/reference-performance")
    if "chunking" not in ref or "process" not in ref:
        raise SystemExit("reference-performance missing chunking/process")
    if "fixed_size" not in ref["chunking"] or "fastcdc" not in ref["chunking"]:
        raise SystemExit("chunking reference JSON incomplete")
    if "scenarios" not in ref["process"]:
        raise SystemExit("process reference JSON incomplete")

    print("DEMO_SMOKE_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
