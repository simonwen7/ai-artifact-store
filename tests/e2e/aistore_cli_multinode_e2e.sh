#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 3 ]]; then
  echo "usage: $0 <aistore> <metadata-service> <storage-node>" >&2
  exit 1
fi

AISTORE_BIN="$1"
METADATA_BIN="$2"
STORAGE_BIN="$3"

DB_URL="${AISTORE_TEST_DB_URL:-dbname=ai_artifact_store_test}"

reset_registry_state() {
  psql "${DB_URL}" -v ON_ERROR_STOP=1 <<'SQL' >/dev/null
DELETE FROM upload_session_finalizations WHERE session_id IN (SELECT session_id FROM upload_sessions WHERE state = 'open');
DELETE FROM upload_session_metadata WHERE session_id IN (SELECT session_id FROM upload_sessions WHERE state = 'open');
DELETE FROM upload_sessions WHERE state = 'open';
DELETE FROM replication_runs;
DELETE FROM gc_runs;
DELETE FROM storage_nodes;
SQL
}
reset_registry_state

ARTIFACT_ID="018f1f7e-7b3c-7000-8000-000000000051"
SESSION_ID="018f1f7e-7b3c-7000-8000-000000000052"
REPAIR_RUN_ID="018f1f7e-7b3c-7000-8000-000000000053"
NODE_A="m8-e2e-node-a"
NODE_B="m8-e2e-node-b"
NODE_C="m8-e2e-node-c"

psql "${DB_URL}" -v ON_ERROR_STOP=1 <<SQL >/dev/null
DELETE FROM upload_session_finalizations WHERE session_id = '${SESSION_ID}'::uuid;
DELETE FROM upload_session_metadata WHERE session_id = '${SESSION_ID}'::uuid;
DELETE FROM upload_sessions WHERE session_id = '${SESSION_ID}'::uuid;
DELETE FROM replication_runs WHERE run_id = '${REPAIR_RUN_ID}'::uuid;
DELETE FROM replication_runs WHERE version_id IN (
  SELECT version_id FROM artifact_versions WHERE artifact_id = '${ARTIFACT_ID}'::uuid
);
DELETE FROM artifact_version_metadata WHERE version_id IN (
  SELECT version_id FROM artifact_versions WHERE artifact_id = '${ARTIFACT_ID}'::uuid
);
DELETE FROM artifact_versions WHERE artifact_id = '${ARTIFACT_ID}'::uuid;
DELETE FROM storage_locations WHERE node_id IN ('${NODE_A}', '${NODE_B}', '${NODE_C}');
SQL

WORK_DIR="$(mktemp -d)"
STORAGE_ROOT_A="$(mktemp -d)"
STORAGE_ROOT_B="$(mktemp -d)"
STORAGE_ROOT_C="$(mktemp -d)"
SOURCE_FILE="${WORK_DIR}/source.bin"
PULL_DEST="${WORK_DIR}/restored.bin"
PULL_DEST2="${WORK_DIR}/restored2.bin"
STDOUT_FILE="${WORK_DIR}/stdout.txt"
STDERR_FILE="${WORK_DIR}/stderr.txt"

METADATA_PID=""
STORAGE_PID_A=""
STORAGE_PID_B=""
STORAGE_PID_C=""
VERSION_ID=""
LAYOUT_ID=""

cleanup() {
  for pid in "${STORAGE_PID_A}" "${STORAGE_PID_B}" "${STORAGE_PID_C}" "${METADATA_PID}"; do
    if [[ -n "${pid}" ]] && kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" || true
      wait "${pid}" || true
    fi
  done

  if [[ -n "${VERSION_ID}" ]]; then
    psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
      "DELETE FROM replication_runs WHERE version_id = '${VERSION_ID}';" >/dev/null 2>&1 || true
    psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
      "DELETE FROM artifact_version_metadata WHERE version_id = '${VERSION_ID}';" >/dev/null 2>&1 || true
    psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
      "DELETE FROM artifact_versions WHERE version_id = '${VERSION_ID}';" >/dev/null 2>&1 || true
  fi

  if [[ -n "${LAYOUT_ID}" ]]; then
    psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
      "DELETE FROM object_layout_chunks WHERE layout_id = '${LAYOUT_ID}';" >/dev/null 2>&1 || true
  fi

  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM upload_session_finalizations WHERE session_id = '${SESSION_ID}'::uuid;" >/dev/null 2>&1 || true
  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM upload_session_metadata WHERE session_id = '${SESSION_ID}'::uuid;" >/dev/null 2>&1 || true
  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM upload_sessions WHERE session_id = '${SESSION_ID}'::uuid;" >/dev/null 2>&1 || true
  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM storage_locations WHERE node_id IN ('${NODE_A}', '${NODE_B}', '${NODE_C}');" >/dev/null 2>&1 || true
  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM storage_nodes WHERE node_id IN ('${NODE_A}', '${NODE_B}', '${NODE_C}');" >/dev/null 2>&1 || true
  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM artifacts WHERE artifact_id = '${ARTIFACT_ID}'::uuid;" >/dev/null 2>&1 || true

  rm -rf "${WORK_DIR}" "${STORAGE_ROOT_A}" "${STORAGE_ROOT_B}" "${STORAGE_ROOT_C}"
}

trap cleanup EXIT

wait_for_health() {
  local url="$1"
  local name="$2"
  local ready=0
  local _
  for _ in $(seq 1 50); do
    if curl -fsS "${url}" >/dev/null 2>&1; then
      ready=1
      break
    fi
    sleep 0.1
  done

  if [[ "${ready}" -ne 1 ]]; then
    echo "${name} did not become ready at ${url}" >&2
    exit 1
  fi
}

# Deterministic multi-chunk FixedSize payload (3 x 4-byte chunks).
printf 'AAAABBBBCCCC' >"${SOURCE_FILE}"

psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
  "INSERT INTO artifacts (artifact_id, name, project) VALUES ('${ARTIFACT_ID}'::uuid, 'multinode-e2e-artifact', 'm8-process-e2e') ON CONFLICT (artifact_id) DO NOTHING;"

AISTORE_DB_URL="${DB_URL}" "${METADATA_BIN}" &
METADATA_PID=$!
wait_for_health "http://127.0.0.1:8080/health" "metadata-service"

AISTORE_STORAGE_NODE_ID="${NODE_A}" AISTORE_STORAGE_PORT=8081 AISTORE_STORAGE_ROOT="${STORAGE_ROOT_A}" "${STORAGE_BIN}" &
STORAGE_PID_A=$!
AISTORE_STORAGE_NODE_ID="${NODE_B}" AISTORE_STORAGE_PORT=8082 AISTORE_STORAGE_ROOT="${STORAGE_ROOT_B}" "${STORAGE_BIN}" &
STORAGE_PID_B=$!
AISTORE_STORAGE_NODE_ID="${NODE_C}" AISTORE_STORAGE_PORT=8083 AISTORE_STORAGE_ROOT="${STORAGE_ROOT_C}" "${STORAGE_BIN}" &
STORAGE_PID_C=$!

wait_for_health "http://127.0.0.1:8081/health" "storage-node-a"
wait_for_health "http://127.0.0.1:8082/health" "storage-node-b"
wait_for_health "http://127.0.0.1:8083/health" "storage-node-c"

"${AISTORE_BIN}" node register --storage-node-id "${NODE_A}" --storage-address 127.0.0.1 --storage-port 8081
"${AISTORE_BIN}" node register --storage-node-id "${NODE_B}" --storage-address 127.0.0.1 --storage-port 8082
"${AISTORE_BIN}" node register --storage-node-id "${NODE_C}" --storage-address 127.0.0.1 --storage-port 8083

LIST_JSON="$("${AISTORE_BIN}" node list)"
python3 - "${LIST_JSON}" "${NODE_A}" "${NODE_B}" "${NODE_C}" <<'PY'
import json
import sys

body = json.loads(sys.argv[1])
nodes = body["nodes"]
assert list(body.keys()) == ["nodes"]
ids = [node["node_id"] for node in nodes]
for required in sys.argv[2:]:
    assert required in ids, required
by_id = {node["node_id"]: node for node in nodes}
assert by_id[sys.argv[2]]["port"] == 8081 and by_id[sys.argv[2]]["state"] == "active"
assert by_id[sys.argv[3]]["port"] == 8082 and by_id[sys.argv[3]]["state"] == "active"
assert by_id[sys.argv[4]]["port"] == 8083 and by_id[sys.argv[4]]["state"] == "active"
PY

set +e
"${AISTORE_BIN}" push \
  --file "${SOURCE_FILE}" \
  --artifact-id "${ARTIFACT_ID}" \
  --session-id "${SESSION_ID}" \
  --replication-factor 2 \
  --chunk-size 4 \
  --metadata source=multinode-e2e \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
PUSH_STATUS=$?
set -e

if [[ "${PUSH_STATUS}" -ne 0 ]]; then
  echo "multinode push expected exit 0, got ${PUSH_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

VERSION_ID="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["version_id"])' <"${STDOUT_FILE}")"
LAYOUT_ID="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["layout_id"])' <"${STDOUT_FILE}")"

python3 - "${DB_URL}" "${LAYOUT_ID}" "${NODE_A}" "${NODE_B}" "${NODE_C}" <<'PY'
import hashlib
import subprocess
import sys

db_url, layout_id, node_a, node_b, node_c = sys.argv[1:6]
placement = sorted([node_a, node_b, node_c])

rows = subprocess.check_output(
    [
        "psql",
        db_url,
        "-At",
        "-F",
        "\t",
        "-c",
        f"SELECT chunk_id FROM object_layout_chunks WHERE layout_id = '{layout_id}' ORDER BY chunk_index;",
    ],
    text=True,
).splitlines()
assert len(rows) == 3, rows

prefix = b"AISTORE_PLACEMENT_V1\x00"


def desired_nodes(chunk_id: str):
    ranked = []
    for node_id in placement:
        digest = hashlib.sha256(prefix + chunk_id.encode("ascii") + b"\x00" + node_id.encode("ascii")).digest()
        ranked.append((digest, node_id))
    ranked.sort(key=lambda item: (-int.from_bytes(item[0], "big"), item[1]))
    return [node_id for _, node_id in ranked[:2]]


primary_pairs = set()
for chunk_id in rows:
    desired = desired_nodes(chunk_id)
    primary_pairs.add(tuple(desired))
    for node_id in desired:
        state = subprocess.check_output(
            [
                "psql",
                db_url,
                "-At",
                "-c",
                "SELECT state FROM storage_locations "
                f"WHERE chunk_id = '{chunk_id}' AND node_id = '{node_id}';",
            ],
            text=True,
        ).strip()
        assert state == "available", (chunk_id, node_id, state)

assert len(primary_pairs) > 1, f"expected distributed primary pairs, got {primary_pairs}"
PY

if [[ -n "${STORAGE_PID_C}" ]] && kill -0 "${STORAGE_PID_C}" 2>/dev/null; then
  kill "${STORAGE_PID_C}"
  wait "${STORAGE_PID_C}" || true
fi
STORAGE_PID_C=""

set +e
"${AISTORE_BIN}" pull \
  --version-id "${VERSION_ID}" \
  --output "${PULL_DEST}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
PULL_STATUS=$?
set -e

if [[ "${PULL_STATUS}" -ne 0 ]]; then
  echo "pull with one node down expected exit 0, got ${PULL_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

if ! cmp -s "${SOURCE_FILE}" "${PULL_DEST}"; then
  echo "pull with one node down did not restore source bytes" >&2
  exit 1
fi

"${AISTORE_BIN}" node set-state --storage-node-id "${NODE_C}" --state disabled

set +e
"${AISTORE_BIN}" repair \
  --version-id "${VERSION_ID}" \
  --replication-factor 2 \
  --repair-run-id "${REPAIR_RUN_ID}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
REPAIR_STATUS=$?
set -e

if [[ "${REPAIR_STATUS}" -ne 0 ]]; then
  echo "repair expected exit 0 after disable, got ${REPAIR_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

python3 -c 'import json,sys; body=json.load(open(sys.argv[1])); assert body["status"]=="replication_repaired"; assert body["repair_run_id"]==sys.argv[2]' \
  "${STDOUT_FILE}" "${REPAIR_RUN_ID}"

python3 - "${DB_URL}" "${LAYOUT_ID}" "${NODE_A}" "${NODE_B}" "${STORAGE_ROOT_A}" "${STORAGE_ROOT_B}" <<'PY'
import hashlib
import os
import subprocess
import sys

db_url, layout_id, node_a, node_b, root_a, root_b = sys.argv[1:7]
chunks = subprocess.check_output(
    [
        "psql",
        db_url,
        "-At",
        "-c",
        f"SELECT chunk_id FROM object_layout_chunks WHERE layout_id = '{layout_id}' ORDER BY chunk_index;",
    ],
    text=True,
).splitlines()

for chunk_id in chunks:
    for node_id in (node_a, node_b):
        state = subprocess.check_output(
            [
                "psql",
                db_url,
                "-At",
                "-c",
                "SELECT state FROM storage_locations "
                f"WHERE chunk_id = '{chunk_id}' AND node_id = '{node_id}';",
            ],
            text=True,
        ).strip()
        assert state == "available", (chunk_id, node_id, state)

    for root in (root_a, root_b):
        found = False
        for dirpath, _, filenames in os.walk(root):
            if chunk_id in filenames:
                found = True
                break
        assert found, f"missing CAS object for {chunk_id} under {root}"
PY

if [[ -n "${STORAGE_PID_B}" ]] && kill -0 "${STORAGE_PID_B}" 2>/dev/null; then
  kill "${STORAGE_PID_B}"
  wait "${STORAGE_PID_B}" || true
fi
STORAGE_PID_B=""

set +e
"${AISTORE_BIN}" pull \
  --version-id "${VERSION_ID}" \
  --output "${PULL_DEST2}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
PULL2_STATUS=$?
set -e

if [[ "${PULL2_STATUS}" -ne 0 ]]; then
  echo "post-repair pull expected exit 0, got ${PULL2_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

if ! cmp -s "${SOURCE_FILE}" "${PULL_DEST2}"; then
  echo "post-repair pull did not restore source bytes" >&2
  exit 1
fi

if [[ -n "${STORAGE_PID_A}" ]] && kill -0 "${STORAGE_PID_A}" 2>/dev/null; then
  kill "${STORAGE_PID_A}"
  wait "${STORAGE_PID_A}" || true
fi
STORAGE_PID_A=""

set +e
"${AISTORE_BIN}" repair \
  --version-id "${VERSION_ID}" \
  --replication-factor 2 \
  --repair-run-id "${REPAIR_RUN_ID}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
RETRY_STATUS=$?
set -e

if [[ "${RETRY_STATUS}" -ne 0 ]]; then
  echo "completed repair retry expected exit 0, got ${RETRY_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

python3 -c 'import json,sys; body=json.load(open(sys.argv[1])); assert body["status"]=="replication_repaired"; assert body["repair_run_id"]==sys.argv[2]' \
  "${STDOUT_FILE}" "${REPAIR_RUN_ID}"

echo "aistore_cli_multinode_process_e2e passed"
