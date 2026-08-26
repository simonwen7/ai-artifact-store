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

ARTIFACT_ID="018f1f7e-7b3c-7000-8000-000000000021"
SESSION_ID="018f1f7e-7b3c-7000-8000-000000000022"
NODE_ID="m4s6-node"

WORK_DIR="$(mktemp -d)"
STORAGE_ROOT="$(mktemp -d)"
SOURCE_FILE="${WORK_DIR}/source.bin"
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
    read -r CHUNK_A CHUNK_B OBJECT_ID < <(
      python3 - <<'PY'
import hashlib
chunk_a = hashlib.sha256(b"ABCD").hexdigest()
chunk_b = hashlib.sha256(b"EFGH").hexdigest()
object_id = hashlib.sha256(b"ABCDABCDEFGH").hexdigest()
print(chunk_a, chunk_b, object_id)
PY
    )
  fi

  if [[ -z "${VERSION_ID}" ]]; then
    VERSION_ID="$(
      psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
        "SELECT finalized_version_id FROM upload_sessions WHERE session_id = '${SESSION_ID}'::uuid;" 2>/dev/null || true
    )"
  fi

  if [[ -z "${LAYOUT_ID}" ]]; then
    LAYOUT_ID="$(
      psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
        "SELECT layout_id FROM upload_session_finalizations WHERE session_id = '${SESSION_ID}'::uuid;" 2>/dev/null || true
    )"
  fi

  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM upload_session_finalizations WHERE session_id = '${SESSION_ID}'::uuid;" >/dev/null || true
  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM upload_session_metadata WHERE session_id = '${SESSION_ID}'::uuid;" >/dev/null || true
  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM upload_sessions WHERE session_id = '${SESSION_ID}'::uuid;" >/dev/null || true

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
    "DELETE FROM storage_locations WHERE node_id = '${NODE_ID}' AND chunk_id IN ('${CHUNK_A}', '${CHUNK_B}');" >/dev/null || true
  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM chunks WHERE chunk_id IN ('${CHUNK_A}', '${CHUNK_B}');" >/dev/null || true
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

printf 'ABCDABCDEFGH' >"${SOURCE_FILE}"

read -r CHUNK_A CHUNK_B OBJECT_ID < <(
  python3 - <<'PY'
import hashlib
chunk_a = hashlib.sha256(b"ABCD").hexdigest()
chunk_b = hashlib.sha256(b"EFGH").hexdigest()
object_id = hashlib.sha256(b"ABCDABCDEFGH").hexdigest()
print(chunk_a, chunk_b, object_id)
PY
)

psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
  "INSERT INTO artifacts (artifact_id, name, project) VALUES ('${ARTIFACT_ID}'::uuid, 'process-e2e-artifact', 'm4-step6-process-e2e') ON CONFLICT (artifact_id) DO NOTHING;"

AISTORE_DB_URL="${DB_URL}" "${METADATA_BIN}" &
METADATA_PID=$!

wait_for_health "http://127.0.0.1:8080/health" "metadata-service"

set +e
"${AISTORE_BIN}" push \
  --file "${SOURCE_FILE}" \
  --artifact-id "${ARTIFACT_ID}" \
  --storage-node-id "${NODE_ID}" \
  --session-id "${SESSION_ID}" \
  --chunk-size 4 \
  --metadata source=process-e2e \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
PHASE1_STATUS=$?
set -e

if [[ "${PHASE1_STATUS}" -ne 1 ]]; then
  echo "phase 1 expected exit 1, got ${PHASE1_STATUS}" >&2
  echo "stderr:" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

if [[ -s "${STDOUT_FILE}" ]]; then
  echo "phase 1 stdout must be empty" >&2
  cat "${STDOUT_FILE}" >&2
  exit 1
fi

if ! grep -q "session_id=${SESSION_ID}" "${STDERR_FILE}"; then
  echo "phase 1 stderr missing session_id" >&2
  cat "${STDERR_FILE}" >&2
  exit 1
fi

if ! grep -q "resume with --session-id" "${STDERR_FILE}"; then
  echo "phase 1 stderr missing resume hint" >&2
  cat "${STDERR_FILE}" >&2
  exit 1
fi

SESSION_STATE="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT state FROM upload_sessions WHERE session_id = '${SESSION_ID}'::uuid;"
)"

if [[ "${SESSION_STATE}" != "open" ]]; then
  echo "phase 1 expected open session, got '${SESSION_STATE}'" >&2
  exit 1
fi

AISTORE_STORAGE_ROOT="${STORAGE_ROOT}" "${STORAGE_BIN}" &
STORAGE_PID=$!

wait_for_health "http://127.0.0.1:8081/health" "storage-node"

set +e
"${AISTORE_BIN}" push \
  --file "${SOURCE_FILE}" \
  --artifact-id "${ARTIFACT_ID}" \
  --storage-node-id "${NODE_ID}" \
  --session-id "${SESSION_ID}" \
  --chunk-size 4 \
  --metadata source=process-e2e \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
PHASE2_STATUS=$?
set -e

if [[ "${PHASE2_STATUS}" -ne 0 ]]; then
  echo "phase 2 expected exit 0, got ${PHASE2_STATUS}" >&2
  echo "stderr:" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

if [[ -s "${STDERR_FILE}" ]]; then
  echo "phase 2 stderr must be empty" >&2
  cat "${STDERR_FILE}" >&2
  exit 1
fi

cp "${STDOUT_FILE}" "${JSON_FILE}"

JSON_PARSE_OUT="$(
  python3 - "${JSON_FILE}" "${SESSION_ID}" "${ARTIFACT_ID}" "${NODE_ID}" "${OBJECT_ID}" <<'PY'
import json
import re
import sys

path, session_id, artifact_id, node_id, expected_object_id = sys.argv[1:6]
with open(path, encoding="utf-8") as handle:
    text = handle.read()

lines = [line for line in text.splitlines() if line.strip()]
if len(lines) != 1:
    raise SystemExit(f"expected exactly one JSON line, got {len(lines)}")

body = json.loads(lines[0])
expected_keys = {
    "status",
    "session_id",
    "artifact_id",
    "target_node_id",
    "version_id",
    "object_id",
    "layout_id",
    "bytes_read",
    "total_chunks",
    "unique_chunks",
    "put_requests",
    "verified_target_chunks",
    "repaired_target_chunks",
    "bytes_sent_to_storage",
}
if set(body.keys()) != expected_keys:
    raise SystemExit(f"unexpected JSON keys: {sorted(body.keys())}")

hex64 = re.compile(r"^[0-9a-f]{64}$")

assert body["status"] == "committed"
assert body["session_id"] == session_id
assert body["artifact_id"] == artifact_id
assert body["target_node_id"] == node_id
assert body["bytes_read"] == 12
assert body["total_chunks"] == 3
assert body["unique_chunks"] == 2
assert body["put_requests"] == 2
assert body["verified_target_chunks"] == 0
assert body["repaired_target_chunks"] == 0
assert body["bytes_sent_to_storage"] == 8
assert hex64.fullmatch(body["version_id"])
assert hex64.fullmatch(body["object_id"])
assert body["object_id"] == expected_object_id
assert hex64.fullmatch(body["layout_id"])

print(body["version_id"])
print(body["layout_id"])
print(body["object_id"])
PY
)"

VERSION_ID="$(printf '%s\n' "${JSON_PARSE_OUT}" | sed -n '1p')"
LAYOUT_ID="$(printf '%s\n' "${JSON_PARSE_OUT}" | sed -n '2p')"
OBJECT_ID="$(printf '%s\n' "${JSON_PARSE_OUT}" | sed -n '3p')"

psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
  "SELECT 1 FROM upload_sessions WHERE session_id = '${SESSION_ID}'::uuid AND state = 'committed' AND finalized_version_id = '${VERSION_ID}';" \
  | grep -q 1

psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
  "SELECT 1 FROM upload_session_finalizations WHERE session_id = '${SESSION_ID}'::uuid AND layout_id = '${LAYOUT_ID}';" \
  | grep -q 1

psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
  "SELECT 1 FROM artifact_versions WHERE version_id = '${VERSION_ID}' AND state = 'committed' AND root_object_id = '${OBJECT_ID}';" \
  | grep -q 1

psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
  "SELECT 1 FROM objects WHERE object_id = '${OBJECT_ID}' AND total_size_bytes = 12;" \
  | grep -q 1

psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
  "SELECT 1 FROM object_layouts WHERE layout_id = '${LAYOUT_ID}' AND object_id = '${OBJECT_ID}' AND chunk_count = 3;" \
  | grep -q 1

LAYOUT_ROWS="$(
  psql "${DB_URL}" -At -F $'\t' -v ON_ERROR_STOP=1 -c \
    "SELECT chunk_index, byte_offset, chunk_id FROM object_layout_chunks WHERE layout_id = '${LAYOUT_ID}' ORDER BY chunk_index;"
)"

python3 -c '
import sys
chunk_a, chunk_b = sys.argv[1], sys.argv[2]
rows = [line.split("\t") for line in sys.stdin.read().splitlines() if line.strip()]
if len(rows) != 3:
    raise SystemExit(f"expected 3 layout rows, got {len(rows)}")
expected = [
    ("0", "0", chunk_a),
    ("1", "4", chunk_a),
    ("2", "8", chunk_b),
]
for actual, want in zip(rows, expected):
    if tuple(actual) != want:
        raise SystemExit(f"unexpected layout row {actual}, want {want}")
' "${CHUNK_A}" "${CHUNK_B}" <<<"${LAYOUT_ROWS}"

LOC_COUNT="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT COUNT(*) FROM storage_locations WHERE node_id = '${NODE_ID}' AND chunk_id IN ('${CHUNK_A}', '${CHUNK_B}') AND state = 'available';"
)"

if [[ "${LOC_COUNT}" != "2" ]]; then
  echo "expected 2 available target locations, got ${LOC_COUNT}" >&2
  exit 1
fi

PATH_A="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT storage_path FROM storage_locations WHERE node_id = '${NODE_ID}' AND chunk_id = '${CHUNK_A}';"
)"
PATH_B="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT storage_path FROM storage_locations WHERE node_id = '${NODE_ID}' AND chunk_id = '${CHUNK_B}';"
)"

if [[ "${PATH_A}" != "/v1/chunks/${CHUNK_A}" ]]; then
  echo "unexpected storage_path for chunk A: ${PATH_A}" >&2
  exit 1
fi

if [[ "${PATH_B}" != "/v1/chunks/${CHUNK_B}" ]]; then
  echo "unexpected storage_path for chunk B: ${PATH_B}" >&2
  exit 1
fi

FILE_A="${STORAGE_ROOT}/chunks/${CHUNK_A:0:2}/${CHUNK_A}"
FILE_B="${STORAGE_ROOT}/chunks/${CHUNK_B:0:2}/${CHUNK_B}"

if [[ ! -f "${FILE_A}" || ! -f "${FILE_B}" ]]; then
  echo "missing CAS files under ${STORAGE_ROOT}" >&2
  exit 1
fi

printf 'ABCD' | cmp -s - "${FILE_A}"
printf 'EFGH' | cmp -s - "${FILE_B}"

echo "aistore_cli_push_process_e2e passed"
