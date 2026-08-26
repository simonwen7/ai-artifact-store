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

ARTIFACT_ID="018f1f7e-7b3c-7000-8000-000000000031"
SESSION_ID="018f1f7e-7b3c-7000-8000-000000000032"
NODE_ID="m6-fastcdc-node"

WORK_DIR="$(mktemp -d)"
STORAGE_ROOT="$(mktemp -d)"
SOURCE_FILE="${WORK_DIR}/source.bin"
RESTORED_FILE="${WORK_DIR}/restored.bin"
STDOUT_FILE="${WORK_DIR}/stdout.txt"
STDERR_FILE="${WORK_DIR}/stderr.txt"
JSON_FILE="${WORK_DIR}/success.json"

METADATA_PID=""
STORAGE_PID=""
VERSION_ID=""
OBJECT_ID=""
LAYOUT_ID=""

cleanup() {
  if [[ -n "${METADATA_PID}" ]] && kill -0 "${METADATA_PID}" 2>/dev/null; then
    kill "${METADATA_PID}"
    wait "${METADATA_PID}" || true
  fi

  if [[ -n "${STORAGE_PID}" ]] && kill -0 "${STORAGE_PID}" 2>/dev/null; then
    kill "${STORAGE_PID}"
    wait "${STORAGE_PID}" || true
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

  if [[ -z "${OBJECT_ID}" && -n "${LAYOUT_ID}" ]]; then
    OBJECT_ID="$(
      psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
        "SELECT object_id FROM object_layouts WHERE layout_id = '${LAYOUT_ID}';" 2>/dev/null || true
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
    if [[ -n "${LAYOUT_ID}" ]]; then
      psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
        "DELETE FROM storage_locations WHERE node_id = '${NODE_ID}' AND chunk_id IN (SELECT chunk_id FROM object_layout_chunks WHERE layout_id = '${LAYOUT_ID}');" >/dev/null || true
      psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
        "DELETE FROM chunks WHERE chunk_id IN (SELECT chunk_id FROM object_layout_chunks WHERE layout_id = '${LAYOUT_ID}');" >/dev/null || true
    fi
    psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
      "DELETE FROM objects WHERE object_id = '${OBJECT_ID}';" >/dev/null || true
  fi

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

python3 - "${SOURCE_FILE}" <<'PY'
import sys

path = sys.argv[1]
state = 0xDEADBEEFCAFEBABE
payload = bytearray()

for _ in range(64 * 1024):
    state ^= (state << 13) & 0xFFFFFFFFFFFFFFFF
    state ^= state >> 7
    state ^= (state << 17) & 0xFFFFFFFFFFFFFFFF
    payload.append(state & 0xFF)

with open(path, "wb") as handle:
    handle.write(payload)
PY

SOURCE_SHA256="$(
  python3 - "${SOURCE_FILE}" <<'PY'
import hashlib
import sys
print(hashlib.sha256(open(sys.argv[1], "rb").read()).hexdigest())
PY
)"

psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
  "INSERT INTO artifacts (artifact_id, name, project) VALUES ('${ARTIFACT_ID}'::uuid, 'fastcdc-process-e2e-artifact', 'm6-fastcdc-process-e2e') ON CONFLICT (artifact_id) DO NOTHING;"

AISTORE_DB_URL="${DB_URL}" "${METADATA_BIN}" &
METADATA_PID=$!

wait_for_health "http://127.0.0.1:8080/health" "metadata-service"

AISTORE_STORAGE_ROOT="${STORAGE_ROOT}" "${STORAGE_BIN}" &
STORAGE_PID=$!

wait_for_health "http://127.0.0.1:8081/health" "storage-node"

set +e
"${AISTORE_BIN}" push \
  --file "${SOURCE_FILE}" \
  --artifact-id "${ARTIFACT_ID}" \
  --storage-node-id "${NODE_ID}" \
  --session-id "${SESSION_ID}" \
  --chunking-strategy fastcdc \
  --min-chunk-size 512 \
  --avg-chunk-size 1024 \
  --max-chunk-size 2048 \
  --metadata source=fastcdc-process-e2e \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
PHASE1_STATUS=$?
set -e

if [[ "${PHASE1_STATUS}" -ne 0 ]]; then
  echo "phase 1 expected exit 0, got ${PHASE1_STATUS}" >&2
  echo "stderr:" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

if [[ -s "${STDERR_FILE}" ]]; then
  echo "phase 1 stderr must be empty" >&2
  cat "${STDERR_FILE}" >&2
  exit 1
fi

cp "${STDOUT_FILE}" "${JSON_FILE}"

JSON_PARSE_OUT="$(
  python3 - "${JSON_FILE}" "${SESSION_ID}" "${ARTIFACT_ID}" "${NODE_ID}" "${SOURCE_SHA256}" <<'PY'
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
assert body["bytes_read"] == 65536
assert body["total_chunks"] > 1
assert body["unique_chunks"] == body["total_chunks"]
assert body["put_requests"] == body["unique_chunks"]
assert body["verified_target_chunks"] == 0
assert body["repaired_target_chunks"] == 0
assert body["bytes_sent_to_storage"] > 0
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

LAYOUT_META="$(
  psql "${DB_URL}" -At -F $'\t' -v ON_ERROR_STOP=1 -c \
    "SELECT chunking_strategy, fastcdc_min_chunk_size_bytes, fastcdc_avg_chunk_size_bytes, fastcdc_max_chunk_size_bytes, chunk_count FROM object_layouts WHERE layout_id = '${LAYOUT_ID}';"
)"

python3 -c '
import sys
strategy, min_size, avg_size, max_size, chunk_count = sys.argv[1].split("\t")
if strategy != "fastcdc":
    raise SystemExit(f"expected fastcdc strategy, got {strategy}")
if min_size != "512" or avg_size != "1024" or max_size != "2048":
    raise SystemExit(f"unexpected fastcdc parameters: {min_size}/{avg_size}/{max_size}")
if int(chunk_count) <= 1:
    raise SystemExit(f"expected multiple chunks, got {chunk_count}")
' "${LAYOUT_META}"

LAYOUT_SIZES="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT chunk_size_bytes FROM object_layout_chunks WHERE layout_id = '${LAYOUT_ID}' ORDER BY chunk_index;"
)"

python3 -c '
import sys
sizes = [line.strip() for line in sys.stdin.read().splitlines() if line.strip()]
if len(sizes) <= 1:
    raise SystemExit("expected multiple chunk refs")
non_final = sizes[:-1]
if not non_final:
    raise SystemExit("expected non-final chunks")
if all(size == "1024" for size in non_final):
    raise SystemExit("non-final chunk sizes must not all equal avg chunk size")
' <<<"${LAYOUT_SIZES}"

PHASE1_VERSION_ID="${VERSION_ID}"
PHASE1_LAYOUT_ID="${LAYOUT_ID}"
PHASE1_OBJECT_ID="${OBJECT_ID}"

if [[ -n "${STORAGE_PID}" ]] && kill -0 "${STORAGE_PID}" 2>/dev/null; then
  kill "${STORAGE_PID}"
  wait "${STORAGE_PID}" || true
fi
STORAGE_PID=""

set +e
"${AISTORE_BIN}" push \
  --file "${SOURCE_FILE}" \
  --artifact-id "${ARTIFACT_ID}" \
  --storage-node-id "${NODE_ID}" \
  --session-id "${SESSION_ID}" \
  --chunking-strategy fastcdc \
  --min-chunk-size 512 \
  --avg-chunk-size 1024 \
  --max-chunk-size 2048 \
  --metadata source=fastcdc-process-e2e \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
PHASE2_STATUS=$?
set -e

if [[ "${PHASE2_STATUS}" -ne 0 ]]; then
  echo "phase 2 committed recovery expected exit 0, got ${PHASE2_STATUS}" >&2
  echo "stderr:" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

cp "${STDOUT_FILE}" "${JSON_FILE}"

python3 - "${JSON_FILE}" "${SESSION_ID}" "${PHASE1_VERSION_ID}" "${PHASE1_OBJECT_ID}" "${PHASE1_LAYOUT_ID}" <<'PY'
import json
import sys

path, session_id, version_id, object_id, layout_id = sys.argv[1:6]
with open(path, encoding="utf-8") as handle:
    body = json.loads(handle.read().strip())

assert body["status"] == "committed"
assert body["session_id"] == session_id
assert body["version_id"] == version_id
assert body["object_id"] == object_id
assert body["layout_id"] == layout_id
assert body["put_requests"] == 0
assert body["verified_target_chunks"] == 0
assert body["repaired_target_chunks"] == 0
assert body["bytes_sent_to_storage"] == 0
PY

AISTORE_STORAGE_ROOT="${STORAGE_ROOT}" "${STORAGE_BIN}" &
STORAGE_PID=$!

wait_for_health "http://127.0.0.1:8081/health" "storage-node"

set +e
"${AISTORE_BIN}" pull \
  --version-id "${PHASE1_VERSION_ID}" \
  --output "${RESTORED_FILE}" \
  --storage-node-id "${NODE_ID}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
PULL_STATUS=$?
set -e

if [[ "${PULL_STATUS}" -ne 0 ]]; then
  echo "pull expected exit 0, got ${PULL_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

cmp -s "${SOURCE_FILE}" "${RESTORED_FILE}"

echo "aistore_cli_fastcdc_process_e2e passed"
