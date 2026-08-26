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

ARTIFACT_ID="018f1f7e-7b3c-7000-8000-000000000031"
SESSION_ID="018f1f7e-7b3c-7000-8000-000000000032"
NODE_ID="m5-node"

psql "${DB_URL}" -v ON_ERROR_STOP=1 <<SQL >/dev/null
DELETE FROM upload_session_finalizations WHERE session_id = '${SESSION_ID}'::uuid;
DELETE FROM upload_session_metadata WHERE session_id = '${SESSION_ID}'::uuid;
DELETE FROM upload_sessions WHERE session_id = '${SESSION_ID}'::uuid;
DELETE FROM artifact_version_metadata WHERE version_id IN (
  SELECT version_id FROM artifact_versions WHERE artifact_id = '${ARTIFACT_ID}'::uuid
);
DELETE FROM artifact_versions WHERE artifact_id = '${ARTIFACT_ID}'::uuid;
DELETE FROM storage_locations WHERE node_id = '${NODE_ID}';
SQL

WORK_DIR="$(mktemp -d)"
STORAGE_ROOT="$(mktemp -d)"
SOURCE_FILE="${WORK_DIR}/source.bin"
PULL_DEST="${WORK_DIR}/restored.bin"
RESUME_DEST="${WORK_DIR}/resume.bin"
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
  "INSERT INTO artifacts (artifact_id, name, project) VALUES ('${ARTIFACT_ID}'::uuid, 'pull-e2e-artifact', 'm5-process-e2e') ON CONFLICT (artifact_id) DO NOTHING;"

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

set +e
"${AISTORE_BIN}" push \
  --file "${SOURCE_FILE}" \
  --artifact-id "${ARTIFACT_ID}" \
  --session-id "${SESSION_ID}" \
  --chunk-size 4 \
  --metadata source=process-e2e \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
PUSH_STATUS=$?
set -e

if [[ "${PUSH_STATUS}" -ne 0 ]]; then
  echo "push expected exit 0, got ${PUSH_STATUS}" >&2
  echo "stderr:" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

if [[ -s "${STDERR_FILE}" ]]; then
  echo "push stderr must be empty" >&2
  cat "${STDERR_FILE}" >&2
  exit 1
fi

cp "${STDOUT_FILE}" "${JSON_FILE}"

PUSH_PARSE_OUT="$(
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
if body["status"] != "committed":
    raise SystemExit("expected committed push status")

hex64 = re.compile(r"^[0-9a-f]{64}$")
assert body["session_id"] == session_id
assert body["artifact_id"] == artifact_id
assert body["target_node_id"] == node_id
assert body["object_id"] == expected_object_id
assert hex64.fullmatch(body["version_id"])
assert hex64.fullmatch(body["layout_id"])

print(body["version_id"])
print(body["layout_id"])
print(body["object_id"])
PY
)"

VERSION_ID="$(printf '%s\n' "${PUSH_PARSE_OUT}" | sed -n '1p')"
LAYOUT_ID="$(printf '%s\n' "${PUSH_PARSE_OUT}" | sed -n '2p')"
OBJECT_ID="$(printf '%s\n' "${PUSH_PARSE_OUT}" | sed -n '3p')"

# Phase 1: full pull.
set +e
"${AISTORE_BIN}" pull \
  --version-id "${VERSION_ID}" \
  --output "${PULL_DEST}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
PULL_STATUS=$?
set -e

if [[ "${PULL_STATUS}" -ne 0 ]]; then
  echo "full pull expected exit 0, got ${PULL_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

printf 'ABCDABCDEFGH' | cmp -s - "${PULL_DEST}"

cp "${STDOUT_FILE}" "${JSON_FILE}"

python3 - "${JSON_FILE}" "${VERSION_ID}" "${ARTIFACT_ID}" "${NODE_ID}" "${OBJECT_ID}" <<'PY'
import json
import re
import sys

path, version_id, artifact_id, node_id, object_id = sys.argv[1:6]
with open(path, encoding="utf-8") as handle:
    text = handle.read()

body = json.loads([line for line in text.splitlines() if line.strip()][0])
expected_keys = {
    "status",
    "version_id",
    "artifact_id",
    "source_node_id",
    "object_id",
    "layout_id",
    "destination",
    "bytes_restored",
    "total_chunks",
    "chunks_downloaded",
    "chunks_reused_from_partial",
    "bytes_received_from_storage",
}
if set(body.keys()) != expected_keys:
    raise SystemExit(f"unexpected JSON keys: {sorted(body.keys())}")

hex64 = re.compile(r"^[0-9a-f]{64}$")
assert body["status"] == "restored"
assert body["version_id"] == version_id
assert body["artifact_id"] == artifact_id
assert body["source_node_id"] == node_id
assert body["object_id"] == object_id
assert hex64.fullmatch(body["layout_id"])
assert body["bytes_restored"] == 12
assert body["total_chunks"] == 3
assert body["chunks_downloaded"] == 3
assert body["chunks_reused_from_partial"] == 0
assert body["bytes_received_from_storage"] == 12
PY

# Phase 2: no-clobber when destination already exists.
set +e
"${AISTORE_BIN}" pull \
  --version-id "${VERSION_ID}" \
  --output "${PULL_DEST}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
NOCLOBBER_STATUS=$?
set -e

if [[ "${NOCLOBBER_STATUS}" -eq 0 ]]; then
  echo "no-clobber pull unexpectedly succeeded" >&2
  exit 1
fi

if [[ -s "${STDOUT_FILE}" ]]; then
  echo "no-clobber pull stdout must be empty" >&2
  exit 1
fi

if ! grep -q "destination already exists" "${STDERR_FILE}"; then
  echo "no-clobber pull stderr missing destination exists message" >&2
  cat "${STDERR_FILE}" >&2
  exit 1
fi

printf 'ABCDABCDEFGH' | cmp -s - "${PULL_DEST}"

# Corrupt destination before overwrite so replacement is observable.
printf 'WRONG-BYTES' >"${PULL_DEST}"

# Phase 3: overwrite replaces existing destination.
set +e
"${AISTORE_BIN}" pull \
  --version-id "${VERSION_ID}" \
  --output "${PULL_DEST}" \
  --overwrite \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
OVERWRITE_STATUS=$?
set -e

if [[ "${OVERWRITE_STATUS}" -ne 0 ]]; then
  echo "overwrite pull expected exit 0, got ${OVERWRITE_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

printf 'ABCDABCDEFGH' | cmp -s - "${PULL_DEST}"

# Phase 4: deterministic partial resume with storage outage.
rm -f "${RESUME_DEST}"
PARTIAL_PATH="${RESUME_DEST}.aistore.${VERSION_ID}.part"
printf 'ABCD' >"${PARTIAL_PATH}"

if [[ -n "${STORAGE_PID}" ]] && kill -0 "${STORAGE_PID}" 2>/dev/null; then
  kill "${STORAGE_PID}"
  wait "${STORAGE_PID}" || true
fi
STORAGE_PID=""

set +e
"${AISTORE_BIN}" pull \
  --version-id "${VERSION_ID}" \
  --output "${RESUME_DEST}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
RESUME_FAIL_STATUS=$?
set -e

if [[ "${RESUME_FAIL_STATUS}" -eq 0 ]]; then
  echo "interrupted pull unexpectedly succeeded while storage was down" >&2
  exit 1
fi

if [[ -s "${STDOUT_FILE}" ]]; then
  echo "interrupted pull stdout must be empty" >&2
  exit 1
fi

if [[ ! -f "${PARTIAL_PATH}" ]]; then
  echo "interrupted pull did not leave partial file" >&2
  exit 1
fi

if [[ -f "${RESUME_DEST}" ]]; then
  echo "interrupted pull must not publish destination" >&2
  exit 1
fi

# Prefix must still be the verified ABCD fixture bytes.
printf 'ABCD' | cmp -s - "${PARTIAL_PATH}"

AISTORE_STORAGE_ROOT="${STORAGE_ROOT}" "${STORAGE_BIN}" &
STORAGE_PID=$!

wait_for_health "http://127.0.0.1:8081/health" "storage-node"

set +e
"${AISTORE_BIN}" pull \
  --version-id "${VERSION_ID}" \
  --output "${RESUME_DEST}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
RESUME_STATUS=$?
set -e

if [[ "${RESUME_STATUS}" -ne 0 ]]; then
  echo "resume pull expected exit 0, got ${RESUME_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

if [[ -s "${STDERR_FILE}" ]]; then
  echo "resume pull stderr must be empty" >&2
  cat "${STDERR_FILE}" >&2
  exit 1
fi

printf 'ABCDABCDEFGH' | cmp -s - "${RESUME_DEST}"

if [[ -f "${PARTIAL_PATH}" ]]; then
  echo "partial file must be removed after successful publish" >&2
  exit 1
fi

cp "${STDOUT_FILE}" "${JSON_FILE}"

python3 - "${JSON_FILE}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    body = json.loads([line for line in handle.read().splitlines() if line.strip()][0])

assert body["status"] == "restored"
assert body["bytes_restored"] == 12
assert body["total_chunks"] == 3
assert body["chunks_reused_from_partial"] == 1
assert body["chunks_downloaded"] == 2
assert body["bytes_received_from_storage"] == 8
PY

# Pull must remain metadata read-only aside from the original push UploadSession.
SESSION_COUNT="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT COUNT(*) FROM upload_sessions WHERE session_id = '${SESSION_ID}'::uuid;"
)"
VERSION_COUNT="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT COUNT(*) FROM artifact_versions WHERE version_id = '${VERSION_ID}';"
)"
LAYOUT_COUNT="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT COUNT(*) FROM object_layouts WHERE layout_id = '${LAYOUT_ID}';"
)"
OBJECT_COUNT="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT COUNT(*) FROM objects WHERE object_id = '${OBJECT_ID}';"
)"
LOCATION_COUNT="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT COUNT(*) FROM storage_locations WHERE node_id = '${NODE_ID}' AND chunk_id IN ('${CHUNK_A}', '${CHUNK_B}');"
)"
VERSION_STATE="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT state FROM artifact_versions WHERE version_id = '${VERSION_ID}';"
)"

if [[ "${SESSION_COUNT}" -ne 1 || "${VERSION_COUNT}" -ne 1 || "${LAYOUT_COUNT}" -ne 1 || "${OBJECT_COUNT}" -ne 1 ]]; then
  echo "pull mutated metadata identities unexpectedly" >&2
  exit 1
fi

if [[ "${LOCATION_COUNT}" -lt 2 ]]; then
  echo "expected storage locations from push to remain" >&2
  exit 1
fi

if [[ "${VERSION_STATE}" != "committed" ]]; then
  echo "artifact version must remain committed after pull" >&2
  exit 1
fi

echo "aistore_cli_pull_process_e2e passed"
