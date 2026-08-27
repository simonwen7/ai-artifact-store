# Benchmarks

Engineering measurement tools for AI Artifact Store. **Not CTest. Not CI timing gates.**

## Build `aistore-bench`

```bash
cmake -S . -B build-m10-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DBUILD_TESTING=OFF \
  -DAISTORE_BUILD_BENCHMARKS=ON \
  -DCMAKE_TOOLCHAIN_FILE="$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake"

cmake --build build-m10-release
```

Executable: `build-m10-release/aistore-bench`

## Chunking microbenchmark

```bash
./build-m10-release/aistore-bench --help
./build-m10-release/aistore-bench chunking
./build-m10-release/aistore-bench chunking --bytes 134217728 --iterations 5 --shift-bytes 65536
```

Stdout: one JSON object (`schema_version`, FixedSize + FastCDC throughput medians, shifted-content reuse stats).

Stderr: progress diagnostics only.

Dataset memory allocation is for the microbenchmark only and is **not** a production Push/Pull RSS claim.

## Process localhost benchmark

```bash
python3 benchmarks/run_process_benchmark.py \
  --build-dir build-m10-release \
  --admin-db-url postgresql:///postgres \
  --file-bytes 67108864 \
  --output benchmarks/results/local_process.json
```

Prerequisites:

- `psql` and ability to `CREATE DATABASE` / `DROP DATABASE`
- free ports `8080`–`8083`
- Release binaries: `aistore`, `metadata-service`, `storage-node`

Safety:

- Creates a uniquely named disposable DB `aistore_benchmark_<pid>_<suffix>`
- Never uses `ai_artifact_store_dev` or `ai_artifact_store_test`
- Drops only the owned DB on cleanup
- Uses captured PIDs (no `pkill` / `killall`)

## Reference results

Committed illustrative localhost results:

- `benchmarks/results/m10_reference_chunking.json`
- `benchmarks/results/m10_reference_process.json`

Regenerate after intentional methodology changes; do not hand-edit numbers.

Interpretation: [`docs/performance.md`](../docs/performance.md)
