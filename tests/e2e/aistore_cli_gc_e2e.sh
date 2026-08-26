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

ARTIFACT_ID="018f1f7e-7b3c-7000-8000-000000000041"
LIVE_SESSION_ID="018f1f7e-7b3c-7000-8000-000000000042"
BLOCKING_SESSION_ID="018f1f7e-7b3c-7000-8000-000000000043"
DRY_RUN_GC_ID="018f1f7e-7b3c-7000-8000-000000000044"
APPLY_GC_ID="018f1f7e-7b3c-7000-8000-000000000045"
PUSH_DURING_GC_SESSION_ID="018f1f7e-7b3c-7000-8000-000000000046"
ORPHAN_SESSION_ID="018f1f7e-7b3c-7000-8000-000000000047"
NODE_ID="m7-node"

psql "${DB_URL}" -v ON_ERROR_STOP=1 <<SQL >/dev/null
DELETE FROM upload_session_finalizations WHERE session_id IN (
  '${LIVE_SESSION_ID}'::uuid, '${BLOCKING_SESSION_ID}'::uuid, '${PUSH_DURING_GC_SESSION_ID}'::uuid, '${ORPHAN_SESSION_ID}'::uuid
);
DELETE FROM upload_session_metadata WHERE session_id IN (
  '${LIVE_SESSION_ID}'::uuid, '${BLOCKING_SESSION_ID}'::uuid, '${PUSH_DURING_GC_SESSION_ID}'::uuid, '${ORPHAN_SESSION_ID}'::uuid
);
DELETE FROM upload_sessions WHERE session_id IN (
  '${LIVE_SESSION_ID}'::uuid, '${BLOCKING_SESSION_ID}'::uuid, '${PUSH_DURING_GC_SESSION_ID}'::uuid, '${ORPHAN_SESSION_ID}'::uuid
);
DELETE FROM gc_runs WHERE run_id IN ('${DRY_RUN_GC_ID}'::uuid, '${APPLY_GC_ID}'::uuid);
DELETE FROM storage_locations WHERE node_id = '${NODE_ID}';
SQL

WORK_DIR="$(mktemp -d)"
STORAGE_ROOT="$(mktemp -d)"
SOURCE_FILE="${WORK_DIR}/source.bin"
PULL_DEST="${WORK_DIR}/restored.bin"
STDOUT_FILE="${WORK_DIR}/stdout.txt"
STDERR_FILE="${WORK_DIR}/stderr.txt"
JSON_FILE="${WORK_DIR}/success.json"

METADATA_PID=""
STORAGE_PID=""
VERSION_ID=""
OBJECT_ID=""
LAYOUT_ID=""
CHUNK_A=""
CHUNK_B=""
ORPHAN_CHUNK_ID=""
UNTRACKED_CHUNK_ID=""

cleanup() {
  if [[ -n "${METADATA_PID}" ]] && kill -0 "${METADATA_PID}" 2>/dev/null; then
    kill "${METADATA_PID}"
    wait "${METADATA_PID}" || true
  fi

  if [[ -n "${STORAGE_PID}" ]] && kill -0 "${STORAGE_PID}" 2>/dev/null; then
    kill "${STORAGE_PID}"
    wait "${STORAGE_PID}" || true
  fi

  if [[ -z "${CHUNK_A}" || -z "${CHUNK_B}" || -z "${OBJECT_ID}" ]]; then
    read -r CHUNK_A CHUNK_B OBJECT_ID ORPHAN_CHUNK_ID UNTRACKED_CHUNK_ID < <(
      python3 - <<'PY'
import hashlib
chunk_a = hashlib.sha256(b"ABCD").hexdigest()
chunk_b = hashlib.sha256(b"EFGH").hexdigest()
object_id = hashlib.sha256(b"ABCDABCDEFGH").hexdigest()
orphan_chunk = hashlib.sha256(b"ORPH").hexdigest()
untracked_chunk = hashlib.sha256(b"UNTR").hexdigest()
print(chunk_a, chunk_b, object_id, orphan_chunk, untracked_chunk)
PY
    )
  fi

  if [[ -z "${VERSION_ID}" ]]; then
    VERSION_ID="$(
      psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
        "SELECT finalized_version_id FROM upload_sessions WHERE session_id = '${LIVE_SESSION_ID}'::uuid;" 2>/dev/null || true
    )"
  fi

  if [[ -z "${LAYOUT_ID}" ]]; then
    LAYOUT_ID="$(
      psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
        "SELECT layout_id FROM upload_session_finalizations WHERE session_id = '${LIVE_SESSION_ID}'::uuid;" 2>/dev/null || true
    )"
  fi

  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM gc_runs WHERE run_id IN ('${DRY_RUN_GC_ID}'::uuid, '${APPLY_GC_ID}'::uuid);" >/dev/null || true

  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM upload_session_finalizations WHERE session_id IN ('${LIVE_SESSION_ID}'::uuid, '${ORPHAN_SESSION_ID}'::uuid);" >/dev/null || true
  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM upload_session_metadata WHERE session_id IN ('${LIVE_SESSION_ID}'::uuid, '${BLOCKING_SESSION_ID}'::uuid, '${ORPHAN_SESSION_ID}'::uuid, '${PUSH_DURING_GC_SESSION_ID}'::uuid);" >/dev/null || true
  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM upload_sessions WHERE session_id IN ('${LIVE_SESSION_ID}'::uuid, '${BLOCKING_SESSION_ID}'::uuid, '${ORPHAN_SESSION_ID}'::uuid, '${PUSH_DURING_GC_SESSION_ID}'::uuid);" >/dev/null || true

  if [[ -n "${VERSION_ID}" ]]; then
    psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
      "DELETE FROM artifact_version_metadata WHERE version_id = '${VERSION_ID}';" >/dev/null || true
    psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
      "DELETE FROM artifact_versions WHERE version_id = '${VERSION_ID}';" >/dev/null || true
  fi

  if [[ -n "${LAYOUT_ID}" ]]; then
    psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
      "DELETE FROM object_layout_chunks WHERE layout_id = '${LAYOUT_ID}';" >/dev/null || true
    psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
      "DELETE FROM object_layouts WHERE layout_id = '${LAYOUT_ID}';" >/dev/null || true
  fi

  if [[ -n "${OBJECT_ID}" ]]; then
    psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
      "DELETE FROM objects WHERE object_id = '${OBJECT_ID}';" >/dev/null || true
  fi

  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM storage_locations WHERE node_id = '${NODE_ID}' AND chunk_id IN ('${CHUNK_A}', '${CHUNK_B}', '${ORPHAN_CHUNK_ID}');" >/dev/null || true
  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM chunks WHERE chunk_id IN ('${CHUNK_A}', '${CHUNK_B}', '${ORPHAN_CHUNK_ID}');" >/dev/null || true
  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM artifacts WHERE artifact_id = '${ARTIFACT_ID}'::uuid;" >/dev/null || true

  rm -rf "${WORK_DIR}" "${STORAGE_ROOT}"
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

chunk_exists_on_storage() {
  local chunk_id="$1"
  local status
  status="$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:8081/v1/chunks/${chunk_id}")"
  [[ "${status}" == "200" ]]
}

validate_gc_success_json() {
  local path="$1"
  local expected_status="$2"
  local expected_run_id="$3"
  python3 - "${path}" "${expected_status}" "${expected_run_id}" "${NODE_ID}" <<'PY'
import json
import sys

path, expected_status, expected_run_id, node_id = sys.argv[1:5]
with open(path, encoding="utf-8") as handle:
    text = handle.read()

lines = [line for line in text.splitlines() if line.strip()]
if len(lines) != 1:
    raise SystemExit(f"expected exactly one JSON line, got {len(lines)}")

body = json.loads(lines[0])
expected_keys = {
    "status",
    "gc_run_id",
    "storage_node_id",
    "dry_run",
    "physical_chunks_scanned",
    "physical_bytes_scanned",
    "collectible_chunks",
    "collectible_bytes",
    "physically_deleted_chunks",
    "physically_deleted_bytes",
    "storage_locations_swept",
    "chunk_rows_swept",
    "object_layouts_swept",
    "objects_swept",
}
if set(body.keys()) != expected_keys:
    raise SystemExit(f"unexpected JSON keys: {sorted(body.keys())}")

assert body["status"] == expected_status
assert body["gc_run_id"] == expected_run_id
assert body["storage_node_id"] == node_id
assert isinstance(body["dry_run"], bool)
for key in expected_keys - {"status", "gc_run_id", "storage_node_id", "dry_run"}:
    assert isinstance(body[key], int)
    assert body[key] >= 0
PY
}

printf 'ABCDABCDEFGH' >"${SOURCE_FILE}"

read -r CHUNK_A CHUNK_B OBJECT_ID ORPHAN_CHUNK_ID UNTRACKED_CHUNK_ID < <(
  python3 - <<'PY'
import hashlib
chunk_a = hashlib.sha256(b"ABCD").hexdigest()
chunk_b = hashlib.sha256(b"EFGH").hexdigest()
object_id = hashlib.sha256(b"ABCDABCDEFGH").hexdigest()
orphan_chunk = hashlib.sha256(b"ORPH").hexdigest()
untracked_chunk = hashlib.sha256(b"UNTR").hexdigest()
print(chunk_a, chunk_b, object_id, orphan_chunk, untracked_chunk)
PY
)

psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
  "INSERT INTO artifacts (artifact_id, name, project) VALUES ('${ARTIFACT_ID}'::uuid, 'gc-e2e-artifact', 'm7-process-e2e') ON CONFLICT (artifact_id) DO NOTHING;"

AISTORE_DB_URL="${DB_URL}" "${METADATA_BIN}" &
METADATA_PID=$!

wait_for_health "http://127.0.0.1:8080/health" "metadata-service"

AISTORE_STORAGE_NODE_ID="${NODE_ID}" AISTORE_STORAGE_ROOT="${STORAGE_ROOT}" "${STORAGE_BIN}" &
STORAGE_PID=$!

wait_for_health "http://127.0.0.1:8081/health" "storage-node"

"${AISTORE_BIN}" node register \
  --storage-node-id "${NODE_ID}" \
  --storage-address 127.0.0.1 \
  --storage-port 8081

# Step 1: live committed data via production push.
set +e
"${AISTORE_BIN}" push \
  --file "${SOURCE_FILE}" \
  --artifact-id "${ARTIFACT_ID}" \
  --session-id "${LIVE_SESSION_ID}" \
  --chunk-size 4 \
  --metadata source=gc-process-e2e \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
PUSH_STATUS=$?
set -e

if [[ "${PUSH_STATUS}" -ne 0 ]]; then
  echo "live push expected exit 0, got ${PUSH_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

PUSH_PARSE_OUT="$(
  python3 - "${STDOUT_FILE}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    body = json.loads([line for line in handle.read().splitlines() if line.strip()][0])
print(body["version_id"])
print(body["layout_id"])
print(body["object_id"])
PY
)"
VERSION_ID="$(printf '%s\n' "${PUSH_PARSE_OUT}" | sed -n '1p')"
LAYOUT_ID="$(printf '%s\n' "${PUSH_PARSE_OUT}" | sed -n '2p')"
OBJECT_ID="$(printf '%s\n' "${PUSH_PARSE_OUT}" | sed -n '3p')"

# Step 2: tracked orphan via production HTTP (session, negotiate, PUT, location, abort).
curl -fsS -X POST "http://127.0.0.1:8080/v1/upload-sessions" \
  -H 'Content-Type: application/json' \
  -d "$(python3 - "${ORPHAN_SESSION_ID}" "${ARTIFACT_ID}" "${NODE_ID}" <<'PY'
import json
import sys
session_id, artifact_id, node_id = sys.argv[1:4]
print(json.dumps({
    "session_id": session_id,
    "artifact_id": artifact_id,
    "target_node_id": node_id,
    "replication_factor": 1,
    "placement_node_ids": [node_id],
    "chunking_strategy": "fixed-size",
    "chunking_parameters": {"chunk_size_bytes": 4},
    "parent_version_id": None,
    "immutable_metadata": {"source": "gc-orphan"},
}))
PY
)" >/dev/null

curl -fsS -X POST "http://127.0.0.1:8080/v1/chunks/negotiate" \
  -H 'Content-Type: application/json' \
  -d "$(python3 - "${ORPHAN_SESSION_ID}" "${ORPHAN_CHUNK_ID}" <<'PY'
import json
import sys
session_id, chunk_id = sys.argv[1:3]
print(json.dumps({
    "session_id": session_id,
    "chunks": [{"chunk_id": chunk_id, "size_bytes": 4}],
}))
PY
)" >/dev/null

printf 'ORPH' | curl -fsS -X PUT "http://127.0.0.1:8081/v1/chunks/${ORPHAN_CHUNK_ID}" \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @- >/dev/null

curl -fsS -X PUT "http://127.0.0.1:8080/v1/chunks/${ORPHAN_CHUNK_ID}/locations/${NODE_ID}" \
  -H 'Content-Type: application/json' \
  -d "$(python3 - "${ORPHAN_CHUNK_ID}" <<'PY'
import json
import sys
chunk_id = sys.argv[1]
print(json.dumps({
    "storage_path": f"/v1/chunks/{chunk_id}",
    "state": "available",
}))
PY
)" >/dev/null

curl -fsS -X POST "http://127.0.0.1:8080/v1/upload-sessions/${ORPHAN_SESSION_ID}/abort" >/dev/null

# Step 3: untracked physical orphan via direct storage PUT.
printf 'UNTR' | curl -fsS -X PUT "http://127.0.0.1:8081/v1/chunks/${UNTRACKED_CHUNK_ID}" \
  -H 'Content-Type: application/octet-stream' \
  --data-binary @- >/dev/null

# Step 4: open session blocks dry-run GC.
curl -fsS -X POST "http://127.0.0.1:8080/v1/upload-sessions" \
  -H 'Content-Type: application/json' \
  -d "$(python3 - "${BLOCKING_SESSION_ID}" "${ARTIFACT_ID}" "${NODE_ID}" <<'PY'
import json
import sys
session_id, artifact_id, node_id = sys.argv[1:4]
print(json.dumps({
    "session_id": session_id,
    "artifact_id": artifact_id,
    "target_node_id": node_id,
    "replication_factor": 1,
    "placement_node_ids": [node_id],
    "chunking_strategy": "fixed-size",
    "chunking_parameters": {"chunk_size_bytes": 4},
    "parent_version_id": None,
    "immutable_metadata": {"source": "gc-block"},
}))
PY
)" >/dev/null

set +e
"${AISTORE_BIN}" gc \
  --dry-run \
  --gc-run-id "${DRY_RUN_GC_ID}" \
  --storage-node-id "${NODE_ID}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
BLOCK_STATUS=$?
set -e

if [[ "${BLOCK_STATUS}" -eq 0 ]]; then
  echo "gc dry-run unexpectedly succeeded with open upload session" >&2
  exit 1
fi

if [[ -s "${STDOUT_FILE}" ]]; then
  echo "blocked gc stdout must be empty" >&2
  exit 1
fi

if ! grep -q "gc_blocked_by_open_upload_sessions" "${STDERR_FILE}"; then
  echo "blocked gc stderr missing gc_blocked_by_open_upload_sessions" >&2
  cat "${STDERR_FILE}" >&2
  exit 1
fi

curl -fsS -X POST "http://127.0.0.1:8080/v1/upload-sessions/${BLOCKING_SESSION_ID}/abort" >/dev/null

# Step 5: dry-run succeeds, orphans remain.
set +e
"${AISTORE_BIN}" gc \
  --dry-run \
  --gc-run-id "${DRY_RUN_GC_ID}" \
  --storage-node-id "${NODE_ID}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
DRY_STATUS=$?
set -e

if [[ "${DRY_STATUS}" -ne 0 ]]; then
  echo "dry-run gc expected exit 0, got ${DRY_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

cp "${STDOUT_FILE}" "${JSON_FILE}"
validate_gc_success_json "${JSON_FILE}" "gc_dry_run" "${DRY_RUN_GC_ID}"

python3 - "${JSON_FILE}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    body = json.loads([line for line in handle.read().splitlines() if line.strip()][0])
if body["collectible_chunks"] < 2:
    raise SystemExit("dry-run expected at least two collectible orphan chunks")
if body["physically_deleted_chunks"] != 0:
    raise SystemExit("dry-run must not delete physical chunks")
PY

for chunk_id in "${CHUNK_A}" "${CHUNK_B}" "${ORPHAN_CHUNK_ID}" "${UNTRACKED_CHUNK_ID}"; do
  if ! chunk_exists_on_storage "${chunk_id}"; then
    echo "dry-run must preserve physical chunk ${chunk_id}" >&2
    exit 1
  fi
done

ORPHAN_CHUNK_ROW="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT COUNT(*) FROM chunks WHERE chunk_id = '${ORPHAN_CHUNK_ID}';"
)"
if [[ "${ORPHAN_CHUNK_ROW}" -ne 1 ]]; then
  echo "dry-run must preserve tracked orphan chunk metadata" >&2
  exit 1
fi

# Step 6: stop storage, apply GC fails and leaves run open.
if [[ -n "${STORAGE_PID}" ]] && kill -0 "${STORAGE_PID}" 2>/dev/null; then
  kill "${STORAGE_PID}"
  wait "${STORAGE_PID}" || true
fi
STORAGE_PID=""

set +e
"${AISTORE_BIN}" gc \
  --gc-run-id "${APPLY_GC_ID}" \
  --storage-node-id "${NODE_ID}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
APPLY_FAIL_STATUS=$?
set -e

if [[ "${APPLY_FAIL_STATUS}" -eq 0 ]]; then
  echo "apply gc unexpectedly succeeded while storage was down" >&2
  exit 1
fi

if [[ -s "${STDOUT_FILE}" ]]; then
  echo "failed apply gc stdout must be empty" >&2
  exit 1
fi

if ! grep -q "resume with --gc-run-id ${APPLY_GC_ID}" "${STDERR_FILE}"; then
  echo "failed apply gc stderr missing resume hint" >&2
  cat "${STDERR_FILE}" >&2
  exit 1
fi

APPLY_STATE="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT state FROM gc_runs WHERE run_id = '${APPLY_GC_ID}'::uuid;"
)"
if [[ "${APPLY_STATE}" != "open" ]]; then
  echo "apply gc run must remain open after storage failure" >&2
  exit 1
fi

# Step 7: push blocked while open GC and storage down.
PUSH_BLOCK_FILE="${WORK_DIR}/push-block.bin"
printf 'BLOCK-PUSH' >"${PUSH_BLOCK_FILE}"

set +e
"${AISTORE_BIN}" push \
  --file "${PUSH_BLOCK_FILE}" \
  --artifact-id "${ARTIFACT_ID}" \
  --session-id "${PUSH_DURING_GC_SESSION_ID}" \
  --chunk-size 4 \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
PUSH_BLOCK_STATUS=$?
set -e

if [[ "${PUSH_BLOCK_STATUS}" -eq 0 ]]; then
  echo "push unexpectedly succeeded during open gc" >&2
  exit 1
fi

if ! grep -q "gc_in_progress" "${STDERR_FILE}"; then
  echo "push during gc missing gc_in_progress" >&2
  cat "${STDERR_FILE}" >&2
  exit 1
fi

PUSH_SESSION_COUNT="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT COUNT(*) FROM upload_sessions WHERE session_id = '${PUSH_DURING_GC_SESSION_ID}'::uuid;"
)"
if [[ "${PUSH_SESSION_COUNT}" -ne 0 ]]; then
  echo "push during gc must not create a new upload session row" >&2
  exit 1
fi

# Step 8: restart storage and resume apply GC.
AISTORE_STORAGE_ROOT="${STORAGE_ROOT}" "${STORAGE_BIN}" &
STORAGE_PID=$!

wait_for_health "http://127.0.0.1:8081/health" "storage-node"

set +e
"${AISTORE_BIN}" gc \
  --gc-run-id "${APPLY_GC_ID}" \
  --storage-node-id "${NODE_ID}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
APPLY_STATUS=$?
set -e

if [[ "${APPLY_STATUS}" -ne 0 ]]; then
  echo "resume apply gc expected exit 0, got ${APPLY_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

cp "${STDOUT_FILE}" "${JSON_FILE}"
validate_gc_success_json "${JSON_FILE}" "gc_completed" "${APPLY_GC_ID}"

for chunk_id in "${CHUNK_A}" "${CHUNK_B}"; do
  if ! chunk_exists_on_storage "${chunk_id}"; then
    echo "apply gc must preserve live chunk ${chunk_id}" >&2
    exit 1
  fi
done

for chunk_id in "${ORPHAN_CHUNK_ID}" "${UNTRACKED_CHUNK_ID}"; do
  if chunk_exists_on_storage "${chunk_id}"; then
    echo "apply gc must delete orphan chunk ${chunk_id}" >&2
    exit 1
  fi
done

ORPHAN_LOCATION_COUNT="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT COUNT(*) FROM storage_locations WHERE node_id = '${NODE_ID}' AND chunk_id = '${ORPHAN_CHUNK_ID}';"
)"
if [[ "${ORPHAN_LOCATION_COUNT}" -ne 0 ]]; then
  echo "apply gc must sweep tracked orphan storage location" >&2
  exit 1
fi

ORPHAN_CHUNK_ROW_AFTER="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT COUNT(*) FROM chunks WHERE chunk_id = '${ORPHAN_CHUNK_ID}';"
)"
if [[ "${ORPHAN_CHUNK_ROW_AFTER}" -ne 0 ]]; then
  echo "apply gc must remove tracked orphan chunk metadata when unreferenced" >&2
  exit 1
fi

ORPHAN_SESSION_STATE="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT state FROM upload_sessions WHERE session_id = '${ORPHAN_SESSION_ID}'::uuid;"
)"
if [[ "${ORPHAN_SESSION_STATE}" != "aborted" ]]; then
  echo "aborted orphan upload session must remain after gc" >&2
  exit 1
fi

LIVE_VERSION_COUNT="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT COUNT(*) FROM artifact_versions WHERE version_id = '${VERSION_ID}';"
)"
if [[ "${LIVE_VERSION_COUNT}" -ne 1 ]]; then
  echo "live artifact version must remain after gc" >&2
  exit 1
fi

# Step 9: pull restores live bytes.
set +e
"${AISTORE_BIN}" pull \
  --version-id "${VERSION_ID}" \
  --output "${PULL_DEST}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
PULL_STATUS=$?
set -e

if [[ "${PULL_STATUS}" -ne 0 ]]; then
  echo "pull after gc expected exit 0, got ${PULL_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

printf 'ABCDABCDEFGH' | cmp -s - "${PULL_DEST}"

# Step 10: completed apply GC succeeds offline (no storage I/O).
if [[ -n "${STORAGE_PID}" ]] && kill -0 "${STORAGE_PID}" 2>/dev/null; then
  kill "${STORAGE_PID}"
  wait "${STORAGE_PID}" || true
fi
STORAGE_PID=""

set +e
"${AISTORE_BIN}" gc \
  --gc-run-id "${APPLY_GC_ID}" \
  --storage-node-id "${NODE_ID}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
COMPLETED_RETRY_STATUS=$?
set -e

if [[ "${COMPLETED_RETRY_STATUS}" -ne 0 ]]; then
  echo "completed gc retry expected exit 0, got ${COMPLETED_RETRY_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

cp "${STDOUT_FILE}" "${JSON_FILE}"
validate_gc_success_json "${JSON_FILE}" "gc_completed" "${APPLY_GC_ID}"

echo "aistore_cli_gc_process_e2e passed"
