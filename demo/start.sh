#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${REPO_ROOT}"

BUILD_DIR="${AISTORE_DEMO_BUILD_DIR:-build-demo-release}"
ADMIN_DB_URL="${AISTORE_DEMO_ADMIN_DB_URL:-postgresql:///postgres}"
CONTROLLER_HOST="127.0.0.1"
CONTROLLER_PORT="8787"
FRONTEND_URL="http://127.0.0.1:5173"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "demo/start.sh: missing required tool: $1" >&2
    exit 1
  fi
}

require_cmd cmake
require_cmd psql
require_cmd python3
require_cmd node
require_cmd npm

resolve_vcpkg() {
  if [[ -n "${VCPKG_ROOT:-}" && -f "${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake" ]]; then
    printf '%s\n' "${VCPKG_ROOT}"
    return 0
  fi
  if [[ -f "${HOME}/vcpkg/scripts/buildsystems/vcpkg.cmake" ]]; then
    printf '%s\n' "${HOME}/vcpkg"
    return 0
  fi
  echo "demo/start.sh: vcpkg not found. Set VCPKG_ROOT or install vcpkg at \$HOME/vcpkg." >&2
  exit 1
}

ensure_binaries() {
  local need_build=0
  for bin in aistore metadata-service storage-node; do
    if [[ ! -x "${BUILD_DIR}/${bin}" ]]; then
      need_build=1
      break
    fi
  done
  if [[ "${need_build}" -eq 0 ]]; then
    return 0
  fi

  local vcpkg_root
  vcpkg_root="$(resolve_vcpkg)"
  echo "demo/start.sh: building Release core binaries into ${BUILD_DIR}" >&2
  cmake -S "${REPO_ROOT}" -B "${BUILD_DIR}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DBUILD_TESTING=OFF \
    -DAISTORE_BUILD_BENCHMARKS=OFF \
    -DCMAKE_TOOLCHAIN_FILE="${vcpkg_root}/scripts/buildsystems/vcpkg.cmake"
  cmake --build "${BUILD_DIR}" --parallel
}

ensure_frontend_deps() {
  if [[ ! -d "${REPO_ROOT}/demo/frontend/node_modules" ]]; then
    echo "demo/start.sh: installing frontend dependencies (npm ci)" >&2
    (cd "${REPO_ROOT}/demo/frontend" && npm ci)
  fi
}

wait_controller_health() {
  local deadline=$((SECONDS + 120))
  while (( SECONDS < deadline )); do
    if python3 - <<'PY' >/dev/null 2>&1
import urllib.request
urllib.request.urlopen("http://127.0.0.1:8787/api/health", timeout=1).read()
PY
    then
      return 0
    fi
    sleep 0.25
  done
  echo "demo/start.sh: controller health check timed out on :${CONTROLLER_PORT}" >&2
  return 1
}

ensure_binaries
ensure_frontend_deps

CONTROLLER_PID=""
FRONTEND_PID=""

cleanup() {
  set +e
  if [[ -n "${FRONTEND_PID}" ]] && kill -0 "${FRONTEND_PID}" 2>/dev/null; then
    kill "${FRONTEND_PID}" 2>/dev/null || true
    wait "${FRONTEND_PID}" 2>/dev/null || true
  fi
  if [[ -n "${CONTROLLER_PID}" ]] && kill -0 "${CONTROLLER_PID}" 2>/dev/null; then
    kill "${CONTROLLER_PID}" 2>/dev/null || true
    wait "${CONTROLLER_PID}" 2>/dev/null || true
  fi
}
trap cleanup EXIT INT TERM

export AISTORE_DEMO_BUILD_DIR="${BUILD_DIR}"
export AISTORE_DEMO_ADMIN_DB_URL="${ADMIN_DB_URL}"

echo "demo/start.sh: starting controller" >&2
python3 "${REPO_ROOT}/demo/controller/controller.py" &
CONTROLLER_PID=$!

wait_controller_health

echo "demo/start.sh: starting Vite frontend" >&2
(
  cd "${REPO_ROOT}/demo/frontend"
  npm run dev -- --host 127.0.0.1 --port 5173
) &
FRONTEND_PID=$!

cat <<EOF

AI Artifact Store Interactive Demo

${FRONTEND_URL}

Ctrl+C stops the frontend and controller (owned demo DB/processes cleaned up).

EOF

wait "${FRONTEND_PID}" "${CONTROLLER_PID}"
