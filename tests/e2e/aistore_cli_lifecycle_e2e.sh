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
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MIGRATIONS_DIR="${SCRIPT_DIR}/../../migrations"

apply_migration_if_needed() {
  local version="$1"
  local name="$2"
  local filename="$3"
  local migration_file="${MIGRATIONS_DIR}/${filename}"
  local migration_applied
  migration_applied="$(
    psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
      "SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = ${version} AND name = '${name}');"
  )"

  if [[ "${migration_applied}" == "t" ]]; then
    return 0
  fi

  if [[ ! -f "${migration_file}" ]]; then
    echo "migration ${filename} not found at ${migration_file}" >&2
    exit 1
  fi

  psql "${DB_URL}" -v ON_ERROR_STOP=1 -f "${migration_file}" >/dev/null
}

apply_migration_if_needed 10 ai_aware_lifecycle 010_ai_aware_lifecycle.sql
apply_migration_if_needed 11 retired_finalization_reclamation 011_retired_finalization_reclamation.sql

reset_registry_state() {
  psql "${DB_URL}" -v ON_ERROR_STOP=1 <<'SQL' >/dev/null
DELETE FROM upload_session_finalizations WHERE session_id IN (SELECT session_id FROM upload_sessions WHERE state = 'open');
DELETE FROM upload_session_metadata WHERE session_id IN (SELECT session_id FROM upload_sessions WHERE state = 'open');
DELETE FROM upload_sessions WHERE state = 'open';
DELETE FROM replication_runs;
DELETE FROM gc_runs;
DELETE FROM lifecycle_run_decisions;
DELETE FROM artifact_version_retirements;
DELETE FROM lifecycle_runs;
DELETE FROM artifact_version_pins;
DELETE FROM lifecycle_policy_rules;
DELETE FROM lifecycle_policies;
DELETE FROM storage_nodes;
SQL
}
reset_registry_state

MODEL_ARTIFACT_ID="018f1f7e-7b3c-7000-8000-000000000061"
EVAL_ARTIFACT_ID="018f1f7e-7b3c-7000-8000-000000000062"
MODEL_V1_SESSION_ID="018f1f7e-7b3c-7000-8000-000000000063"
MODEL_V2_SESSION_ID="018f1f7e-7b3c-7000-8000-000000000064"
MODEL_V3_SESSION_ID="018f1f7e-7b3c-7000-8000-000000000065"
EVAL_V1_SESSION_ID="018f1f7e-7b3c-7000-8000-000000000066"
EVAL_V2_SESSION_ID="018f1f7e-7b3c-7000-8000-000000000067"
POLICY_ID="018f1f7e-7b3c-7000-8000-000000000068"
DRY_RUN_LIFECYCLE_ID="018f1f7e-7b3c-7000-8000-000000000069"
APPLY_LIFECYCLE_ID="018f1f7e-7b3c-7000-8000-000000000070"
GC_RUN_ID="018f1f7e-7b3c-7000-8000-000000000071"
NODE_ID="lifecycle-node"

MODEL_V1_CONTENT="MODEL-CHECKPOINT-V1-BYTES"
MODEL_V2_CONTENT="MODEL-CHECKPOINT-V2-BYTES"
MODEL_V3_CONTENT="MODEL-CHECKPOINT-V3-BYTES"
EVAL_V1_CONTENT="ABCD1111"
EVAL_V2_CONTENT="ABCD2222"

WORK_DIR="$(mktemp -d)"
STORAGE_ROOT="$(mktemp -d)"
STDOUT_FILE="${WORK_DIR}/stdout.txt"
STDERR_FILE="${WORK_DIR}/stderr.txt"
JSON_FILE="${WORK_DIR}/success.json"
POLICY_FILE="${WORK_DIR}/policy.json"
APPLY_LIFECYCLE_JSON="${WORK_DIR}/apply_lifecycle.json"

METADATA_PID=""
STORAGE_PID=""
MODEL_V1_ID=""
MODEL_V2_ID=""
MODEL_V3_ID=""
EVAL_V1_ID=""
EVAL_V2_ID=""
SHARED_CHUNK_ID=""

cleanup() {
  if [[ -n "${METADATA_PID}" ]] && kill -0 "${METADATA_PID}" 2>/dev/null; then
    kill "${METADATA_PID}"
    wait "${METADATA_PID}" || true
  fi

  if [[ -n "${STORAGE_PID}" ]] && kill -0 "${STORAGE_PID}" 2>/dev/null; then
    kill "${STORAGE_PID}"
    wait "${STORAGE_PID}" || true
  fi

  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM lifecycle_run_decisions WHERE run_id IN ('${DRY_RUN_LIFECYCLE_ID}'::uuid, '${APPLY_LIFECYCLE_ID}'::uuid);" >/dev/null || true
  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM artifact_version_retirements WHERE version_id IN (SELECT version_id FROM artifact_versions WHERE artifact_id IN ('${MODEL_ARTIFACT_ID}'::uuid, '${EVAL_ARTIFACT_ID}'::uuid));" >/dev/null || true
  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM lifecycle_runs WHERE run_id IN ('${DRY_RUN_LIFECYCLE_ID}'::uuid, '${APPLY_LIFECYCLE_ID}'::uuid);" >/dev/null || true
  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM artifact_version_pins WHERE version_id IN (SELECT version_id FROM artifact_versions WHERE artifact_id IN ('${MODEL_ARTIFACT_ID}'::uuid, '${EVAL_ARTIFACT_ID}'::uuid));" >/dev/null || true
  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM lifecycle_policy_rules WHERE policy_id = '${POLICY_ID}'::uuid;" >/dev/null || true
  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM lifecycle_policies WHERE policy_id = '${POLICY_ID}'::uuid;" >/dev/null || true
  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM gc_runs WHERE run_id = '${GC_RUN_ID}'::uuid;" >/dev/null || true

  for session_id in \
    "${MODEL_V1_SESSION_ID}" \
    "${MODEL_V2_SESSION_ID}" \
    "${MODEL_V3_SESSION_ID}" \
    "${EVAL_V1_SESSION_ID}" \
    "${EVAL_V2_SESSION_ID}"; do
    psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
      "DELETE FROM upload_session_finalizations WHERE session_id = '${session_id}'::uuid;" >/dev/null || true
    psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
      "DELETE FROM upload_session_metadata WHERE session_id = '${session_id}'::uuid;" >/dev/null || true
    psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
      "DELETE FROM upload_sessions WHERE session_id = '${session_id}'::uuid;" >/dev/null || true
  done

  for version_id in "${MODEL_V1_ID}" "${MODEL_V2_ID}" "${MODEL_V3_ID}" "${EVAL_V1_ID}" "${EVAL_V2_ID}"; do
    if [[ -n "${version_id}" ]]; then
      psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
        "DELETE FROM artifact_version_retirements WHERE version_id = '${version_id}';" >/dev/null || true
      psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
        "DELETE FROM lifecycle_run_decisions WHERE version_id = '${version_id}';" >/dev/null || true
      psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
        "DELETE FROM artifact_version_pins WHERE version_id = '${version_id}';" >/dev/null || true
      psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
        "DELETE FROM artifact_version_metadata WHERE version_id = '${version_id}';" >/dev/null || true
      psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
        "DELETE FROM artifact_versions WHERE version_id = '${version_id}';" >/dev/null || true
    fi
  done

  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM storage_locations WHERE node_id = '${NODE_ID}';" >/dev/null || true
  psql "${DB_URL}" -v ON_ERROR_STOP=1 -c \
    "DELETE FROM artifacts WHERE artifact_id IN ('${MODEL_ARTIFACT_ID}'::uuid, '${EVAL_ARTIFACT_ID}'::uuid);" >/dev/null || true

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

push_version() {
  local file_path="$1"
  local artifact_id="$2"
  local session_id="$3"
  local artifact_kind="$4"

  set +e
  "${AISTORE_BIN}" push \
    --file "${file_path}" \
    --artifact-id "${artifact_id}" \
    --session-id "${session_id}" \
    --metadata "aistore.artifact_kind=${artifact_kind}" \
    >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
  local status=$?
  set -e

  if [[ "${status}" -ne 0 ]]; then
    echo "push expected exit 0, got ${status}" >&2
    cat "${STDERR_FILE}" >&2 || true
    exit 1
  fi

  python3 - "${STDOUT_FILE}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    body = json.loads([line for line in handle.read().splitlines() if line.strip()][0])
print(body["version_id"])
PY
}

SHARED_CHUNK_ID="$(
  python3 - <<'PY'
import hashlib
print(hashlib.sha256(b"ABCD").hexdigest())
PY
)"

psql "${DB_URL}" -v ON_ERROR_STOP=1 <<SQL >/dev/null
DELETE FROM lifecycle_run_decisions WHERE run_id IN ('${DRY_RUN_LIFECYCLE_ID}'::uuid, '${APPLY_LIFECYCLE_ID}'::uuid);
DELETE FROM artifact_version_retirements WHERE version_id IN (
  SELECT version_id FROM artifact_versions WHERE artifact_id IN ('${MODEL_ARTIFACT_ID}'::uuid, '${EVAL_ARTIFACT_ID}'::uuid)
);
DELETE FROM lifecycle_runs WHERE run_id IN ('${DRY_RUN_LIFECYCLE_ID}'::uuid, '${APPLY_LIFECYCLE_ID}'::uuid);
DELETE FROM artifact_version_pins WHERE version_id IN (
  SELECT version_id FROM artifact_versions WHERE artifact_id IN ('${MODEL_ARTIFACT_ID}'::uuid, '${EVAL_ARTIFACT_ID}'::uuid)
);
DELETE FROM lifecycle_policy_rules WHERE policy_id = '${POLICY_ID}'::uuid;
DELETE FROM lifecycle_policies WHERE policy_id = '${POLICY_ID}'::uuid;
DELETE FROM gc_runs WHERE run_id = '${GC_RUN_ID}'::uuid;
DELETE FROM replication_runs;
DELETE FROM upload_session_finalizations WHERE session_id IN (
  '${MODEL_V1_SESSION_ID}'::uuid,
  '${MODEL_V2_SESSION_ID}'::uuid,
  '${MODEL_V3_SESSION_ID}'::uuid,
  '${EVAL_V1_SESSION_ID}'::uuid,
  '${EVAL_V2_SESSION_ID}'::uuid
);
DELETE FROM upload_session_metadata WHERE session_id IN (
  '${MODEL_V1_SESSION_ID}'::uuid,
  '${MODEL_V2_SESSION_ID}'::uuid,
  '${MODEL_V3_SESSION_ID}'::uuid,
  '${EVAL_V1_SESSION_ID}'::uuid,
  '${EVAL_V2_SESSION_ID}'::uuid
);
DELETE FROM upload_sessions WHERE session_id IN (
  '${MODEL_V1_SESSION_ID}'::uuid,
  '${MODEL_V2_SESSION_ID}'::uuid,
  '${MODEL_V3_SESSION_ID}'::uuid,
  '${EVAL_V1_SESSION_ID}'::uuid,
  '${EVAL_V2_SESSION_ID}'::uuid
);
DELETE FROM artifact_version_metadata WHERE version_id IN (
  SELECT version_id FROM artifact_versions WHERE artifact_id IN ('${MODEL_ARTIFACT_ID}'::uuid, '${EVAL_ARTIFACT_ID}'::uuid)
);
DELETE FROM object_layout_chunks WHERE layout_id IN (
  SELECT ol.layout_id
  FROM object_layouts ol
  INNER JOIN artifact_versions av ON av.root_object_id = ol.object_id
  WHERE av.artifact_id IN ('${MODEL_ARTIFACT_ID}'::uuid, '${EVAL_ARTIFACT_ID}'::uuid)
);
DELETE FROM object_layouts WHERE object_id IN (
  SELECT root_object_id FROM artifact_versions WHERE artifact_id IN ('${MODEL_ARTIFACT_ID}'::uuid, '${EVAL_ARTIFACT_ID}'::uuid)
);
DELETE FROM artifact_versions WHERE artifact_id IN ('${MODEL_ARTIFACT_ID}'::uuid, '${EVAL_ARTIFACT_ID}'::uuid);
DELETE FROM storage_locations WHERE node_id = '${NODE_ID}';
DELETE FROM artifacts WHERE artifact_id IN ('${MODEL_ARTIFACT_ID}'::uuid, '${EVAL_ARTIFACT_ID}'::uuid);
INSERT INTO artifacts (artifact_id, name, project) VALUES
  ('${MODEL_ARTIFACT_ID}'::uuid, 'lifecycle-model-artifact', 'm9-process-e2e'),
  ('${EVAL_ARTIFACT_ID}'::uuid, 'lifecycle-eval-artifact', 'm9-process-e2e');
SQL

printf '%s' "${MODEL_V1_CONTENT}" >"${WORK_DIR}/model_v1.bin"
printf '%s' "${MODEL_V2_CONTENT}" >"${WORK_DIR}/model_v2.bin"
printf '%s' "${MODEL_V3_CONTENT}" >"${WORK_DIR}/model_v3.bin"
printf '%s' "${EVAL_V1_CONTENT}" >"${WORK_DIR}/eval_v1.bin"
printf '%s' "${EVAL_V2_CONTENT}" >"${WORK_DIR}/eval_v2.bin"

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

MODEL_V1_ID="$(push_version "${WORK_DIR}/model_v1.bin" "${MODEL_ARTIFACT_ID}" "${MODEL_V1_SESSION_ID}" "model-checkpoint")"
MODEL_V2_ID="$(push_version "${WORK_DIR}/model_v2.bin" "${MODEL_ARTIFACT_ID}" "${MODEL_V2_SESSION_ID}" "model-checkpoint")"
MODEL_V3_ID="$(push_version "${WORK_DIR}/model_v3.bin" "${MODEL_ARTIFACT_ID}" "${MODEL_V3_SESSION_ID}" "model-checkpoint")"

set +e
"${AISTORE_BIN}" push \
  --file "${WORK_DIR}/eval_v1.bin" \
  --artifact-id "${EVAL_ARTIFACT_ID}" \
  --session-id "${EVAL_V1_SESSION_ID}" \
  --chunk-size 4 \
  --metadata "aistore.artifact_kind=evaluation-output" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
EVAL_V1_PUSH_STATUS=$?
set -e

if [[ "${EVAL_V1_PUSH_STATUS}" -ne 0 ]]; then
  echo "eval_v1 push expected exit 0, got ${EVAL_V1_PUSH_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

EVAL_V1_ID="$(
  python3 - "${STDOUT_FILE}" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as handle:
    body = json.loads([line for line in handle.read().splitlines() if line.strip()][0])
print(body["version_id"])
PY
)"

set +e
"${AISTORE_BIN}" push \
  --file "${WORK_DIR}/eval_v2.bin" \
  --artifact-id "${EVAL_ARTIFACT_ID}" \
  --session-id "${EVAL_V2_SESSION_ID}" \
  --chunk-size 4 \
  --metadata "aistore.artifact_kind=evaluation-output" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
EVAL_V2_PUSH_STATUS=$?
set -e

if [[ "${EVAL_V2_PUSH_STATUS}" -ne 0 ]]; then
  echo "eval_v2 push expected exit 0, got ${EVAL_V2_PUSH_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

EVAL_V2_ID="$(
  python3 - "${STDOUT_FILE}" <<'PY'
import json
import sys
with open(sys.argv[1], encoding="utf-8") as handle:
    body = json.loads([line for line in handle.read().splitlines() if line.strip()][0])
print(body["version_id"])
PY
)"

cat >"${POLICY_FILE}" <<'JSON'
{
  "name": "lifecycle-process-e2e",
  "rules": [
    {"artifact_kind": "generic", "keep_last_n": 1, "max_age_seconds": null},
    {"artifact_kind": "model-checkpoint", "keep_last_n": 1, "max_age_seconds": null},
    {"artifact_kind": "dataset-snapshot", "keep_last_n": 1, "max_age_seconds": null},
    {"artifact_kind": "embedding-index", "keep_last_n": 1, "max_age_seconds": null},
    {"artifact_kind": "evaluation-output", "keep_last_n": 1, "max_age_seconds": null}
  ]
}
JSON

set +e
"${AISTORE_BIN}" lifecycle policy create \
  --file "${POLICY_FILE}" \
  --policy-id "${POLICY_ID}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
POLICY_STATUS=$?
set -e

if [[ "${POLICY_STATUS}" -ne 0 ]]; then
  echo "lifecycle policy create expected exit 0, got ${POLICY_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

set +e
"${AISTORE_BIN}" lifecycle pin \
  --version-id "${MODEL_V1_ID}" \
  --reason "golden-checkpoint" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
PIN_STATUS=$?
set -e

if [[ "${PIN_STATUS}" -ne 0 ]]; then
  echo "lifecycle pin expected exit 0, got ${PIN_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

set +e
"${AISTORE_BIN}" lifecycle run \
  --dry-run \
  --policy-id "${POLICY_ID}" \
  --lifecycle-run-id "${DRY_RUN_LIFECYCLE_ID}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
DRY_STATUS=$?
set -e

if [[ "${DRY_STATUS}" -ne 0 ]]; then
  echo "lifecycle dry-run expected exit 0, got ${DRY_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

cp "${STDOUT_FILE}" "${JSON_FILE}"

python3 - "${JSON_FILE}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    body = json.loads([line for line in handle.read().splitlines() if line.strip()][0])

if body["versions_candidates"] != 2:
    raise SystemExit(f"dry-run expected versions_candidates=2, got {body['versions_candidates']}")
if body["versions_retired"] != 0:
    raise SystemExit(f"dry-run expected versions_retired=0, got {body['versions_retired']}")
PY

RETIREMENT_COUNT="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c "SELECT COUNT(*) FROM artifact_version_retirements;"
)"
if [[ "${RETIREMENT_COUNT}" -ne 0 ]]; then
  echo "dry-run must not create retirement rows" >&2
  exit 1
fi

set +e
"${AISTORE_BIN}" lifecycle explain \
  --lifecycle-run-id "${DRY_RUN_LIFECYCLE_ID}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
EXPLAIN_STATUS=$?
set -e

if [[ "${EXPLAIN_STATUS}" -ne 0 ]]; then
  echo "lifecycle explain expected exit 0, got ${EXPLAIN_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

python3 - \
  "${STDOUT_FILE}" \
  "${MODEL_V1_ID}" \
  "${MODEL_V2_ID}" \
  "${MODEL_V3_ID}" \
  "${EVAL_V1_ID}" \
  "${EVAL_V2_ID}" <<'PY'
import json
import sys

_, stdout_path, model_v1, model_v2, model_v3, eval_v1, eval_v2 = sys.argv
with open(stdout_path, encoding="utf-8") as handle:
    body = json.loads([line for line in handle.read().splitlines() if line.strip()][0])

expected = {
    model_v1: ("retain", "pinned"),
    model_v2: ("retire", "policy-retire"),
    model_v3: ("retain", "keep-last-n"),
    eval_v1: ("retire", "policy-retire"),
    eval_v2: ("retain", "keep-last-n"),
}

by_version = {entry["version_id"]: (entry["decision"], entry["reason"]) for entry in body["decisions"]}

for version_id, (decision, reason) in expected.items():
    if version_id not in by_version:
        raise SystemExit(f"missing decision for {version_id}")
    actual = by_version[version_id]
    if actual != (decision, reason):
        raise SystemExit(f"{version_id} expected {(decision, reason)}, got {actual}")
PY

set +e
"${AISTORE_BIN}" lifecycle run \
  --policy-id "${POLICY_ID}" \
  --lifecycle-run-id "${APPLY_LIFECYCLE_ID}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
APPLY_STATUS=$?
set -e

if [[ "${APPLY_STATUS}" -ne 0 ]]; then
  echo "lifecycle apply expected exit 0, got ${APPLY_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

cp "${STDOUT_FILE}" "${APPLY_LIFECYCLE_JSON}"

python3 - "${APPLY_LIFECYCLE_JSON}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as handle:
    body = json.loads([line for line in handle.read().splitlines() if line.strip()][0])

if body["versions_candidates"] != 2:
    raise SystemExit(f"apply expected versions_candidates=2, got {body['versions_candidates']}")
if body["versions_retired"] != 2:
    raise SystemExit(f"apply expected versions_retired=2, got {body['versions_retired']}")
PY

RETIRED_VERSIONS="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT version_id FROM artifact_version_retirements ORDER BY version_id;"
)"
EXPECTED_RETIRED="$(printf '%s\n%s' "${MODEL_V2_ID}" "${EVAL_V1_ID}" | sort)"
ACTUAL_RETIRED="$(printf '%s\n' ${RETIRED_VERSIONS} | sort)"
if [[ "${ACTUAL_RETIRED}" != "${EXPECTED_RETIRED}" ]]; then
  echo "unexpected retirement rows" >&2
  echo "expected:" >&2
  echo "${EXPECTED_RETIRED}" >&2
  echo "actual:" >&2
  echo "${ACTUAL_RETIRED}" >&2
  exit 1
fi

MODEL_V2_UNIQUE_CHUNKS="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT olc.chunk_id FROM artifact_versions av INNER JOIN object_layouts ol ON ol.object_id = av.root_object_id INNER JOIN object_layout_chunks olc ON olc.layout_id = ol.layout_id WHERE av.version_id = '${MODEL_V2_ID}';"
)"
EVAL_V1_UNIQUE_CHUNKS="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c \
    "SELECT olc.chunk_id FROM artifact_versions av INNER JOIN object_layouts ol ON ol.object_id = av.root_object_id INNER JOIN object_layout_chunks olc ON olc.layout_id = ol.layout_id WHERE av.version_id = '${EVAL_V1_ID}';"
)"

for chunk_id in ${MODEL_V2_UNIQUE_CHUNKS} ${EVAL_V1_UNIQUE_CHUNKS}; do
  if ! chunk_exists_on_storage "${chunk_id}"; then
    echo "retired version chunk ${chunk_id} must still exist before GC" >&2
    exit 1
  fi
done

if ! chunk_exists_on_storage "${SHARED_CHUNK_ID}"; then
  echo "shared chunk must exist before GC" >&2
  exit 1
fi

set +e
"${AISTORE_BIN}" pull \
  --version-id "${MODEL_V2_ID}" \
  --output "${WORK_DIR}/retired_pull.bin" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
RETIRED_PULL_STATUS=$?
set -e

if [[ "${RETIRED_PULL_STATUS}" -eq 0 ]]; then
  echo "pull retired model_v2 must fail" >&2
  exit 1
fi

if [[ ! -s "${STDERR_FILE}" ]]; then
  echo "retired pull stderr must be nonempty" >&2
  exit 1
fi

if ! grep -q '^aistore pull error:' "${STDERR_FILE}"; then
  echo "retired pull stderr missing aistore pull error prefix" >&2
  cat "${STDERR_FILE}" >&2
  exit 1
fi

if ! grep -q 'artifact_version_retired' "${STDERR_FILE}"; then
  echo "retired pull stderr missing artifact_version_retired" >&2
  cat "${STDERR_FILE}" >&2
  exit 1
fi

if [[ -f "${WORK_DIR}/retired_pull.bin" ]]; then
  echo "retired pull must not publish output destination" >&2
  exit 1
fi

pull_and_verify() {
  local version_id="$1"
  local expected_content="$2"
  local output_path="$3"

  set +e
  "${AISTORE_BIN}" pull \
    --version-id "${version_id}" \
    --output "${output_path}" \
    --overwrite \
    >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
  local status=$?
  set -e

  if [[ "${status}" -ne 0 ]]; then
    echo "pull ${version_id} expected exit 0, got ${status}" >&2
    cat "${STDERR_FILE}" >&2 || true
    exit 1
  fi

  printf '%s' "${expected_content}" | cmp -s - "${output_path}"
}

pull_and_verify "${MODEL_V1_ID}" "${MODEL_V1_CONTENT}" "${WORK_DIR}/pull_model_v1.bin"
pull_and_verify "${MODEL_V3_ID}" "${MODEL_V3_CONTENT}" "${WORK_DIR}/pull_model_v3.bin"
pull_and_verify "${EVAL_V2_ID}" "${EVAL_V2_CONTENT}" "${WORK_DIR}/pull_eval_v2.bin"

set +e
"${AISTORE_BIN}" repair \
  --version-id "${MODEL_V2_ID}" \
  --replication-factor 1 \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
REPAIR_STATUS=$?
set -e

if [[ "${REPAIR_STATUS}" -eq 0 ]]; then
  echo "repair retired model_v2 must fail" >&2
  exit 1
fi

if ! grep -q 'artifact_version_retired' "${STDERR_FILE}"; then
  echo "repair retired stderr missing artifact_version_retired" >&2
  cat "${STDERR_FILE}" >&2
  exit 1
fi

REPAIR_RUN_COUNT="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c "SELECT COUNT(*) FROM replication_runs;"
)"
if [[ "${REPAIR_RUN_COUNT}" -ne 0 ]]; then
  echo "repair retired must not create replication run" >&2
  exit 1
fi

set +e
"${AISTORE_BIN}" gc \
  --gc-run-id "${GC_RUN_ID}" \
  --storage-node-id "${NODE_ID}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
GC_STATUS=$?
set -e

if [[ "${GC_STATUS}" -ne 0 ]]; then
  echo "gc apply expected exit 0, got ${GC_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

if ! chunk_exists_on_storage "${SHARED_CHUNK_ID}"; then
  echo "shared ABCD chunk must remain after GC" >&2
  exit 1
fi

pull_and_verify "${MODEL_V1_ID}" "${MODEL_V1_CONTENT}" "${WORK_DIR}/pull_model_v1_post_gc.bin"
pull_and_verify "${MODEL_V3_ID}" "${MODEL_V3_CONTENT}" "${WORK_DIR}/pull_model_v3_post_gc.bin"
pull_and_verify "${EVAL_V2_ID}" "${EVAL_V2_CONTENT}" "${WORK_DIR}/pull_eval_v2_post_gc.bin"

if [[ -n "${STORAGE_PID}" ]] && kill -0 "${STORAGE_PID}" 2>/dev/null; then
  kill "${STORAGE_PID}"
  wait "${STORAGE_PID}" || true
fi
STORAGE_PID=""

set +e
"${AISTORE_BIN}" lifecycle run \
  --policy-id "${POLICY_ID}" \
  --lifecycle-run-id "${APPLY_LIFECYCLE_ID}" \
  >"${STDOUT_FILE}" 2>"${STDERR_FILE}"
LIFECYCLE_RETRY_STATUS=$?
set -e

if [[ "${LIFECYCLE_RETRY_STATUS}" -ne 0 ]]; then
  echo "completed lifecycle retry expected exit 0, got ${LIFECYCLE_RETRY_STATUS}" >&2
  cat "${STDERR_FILE}" >&2 || true
  exit 1
fi

python3 - "${APPLY_LIFECYCLE_JSON}" "${STDOUT_FILE}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as first_handle:
    first = json.loads([line for line in first_handle.read().splitlines() if line.strip()][0])
with open(sys.argv[2], encoding="utf-8") as second_handle:
    second = json.loads([line for line in second_handle.read().splitlines() if line.strip()][0])

for key in (
    "run_id",
    "policy_id",
    "mode",
    "versions_scanned",
    "versions_protected",
    "versions_retained_by_policy",
    "versions_candidates",
    "versions_retired",
    "logical_bytes_candidates",
    "logical_bytes_retired",
):
    if first[key] != second[key]:
        raise SystemExit(f"lifecycle retry stat mismatch for {key}: {first[key]} vs {second[key]}")
PY

RETIREMENT_COUNT_AFTER_RETRY="$(
  psql "${DB_URL}" -At -v ON_ERROR_STOP=1 -c "SELECT COUNT(*) FROM artifact_version_retirements;"
)"
if [[ "${RETIREMENT_COUNT_AFTER_RETRY}" -ne 2 ]]; then
  echo "lifecycle retry must not add retirement rows" >&2
  exit 1
fi

echo "aistore_cli_lifecycle_process_e2e passed"
