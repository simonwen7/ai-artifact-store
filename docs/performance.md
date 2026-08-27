# Performance

## Purpose

This document records **measured** localhost Release-build reference results for AI Artifact Store and separates them from **analytical architecture memory bounds**.

It does **not** define a production SLA, cloud benchmark, or competitive comparison.

Reference JSON (authoritative numbers):

- [`benchmarks/results/m10_reference_chunking.json`](../benchmarks/results/m10_reference_chunking.json)
- [`benchmarks/results/m10_reference_process.json`](../benchmarks/results/m10_reference_process.json)

## Methodology

### Chunking microbenchmark (`aistore-bench chunking`)

- Uses production `FixedSizeChunker` and `FastCdcChunker`.
- Each emitted chunk is SHA-256 hashed with the production hasher (chunk-ID cost included).
- Deterministic PRNG dataset (fixed seeds); default 128 MiB base + 64 KiB shift prefix.
- Timing: `std::chrono::steady_clock`, median of 5 iterations.
- Benchmark datasets are allocated in process memory. That allocation is **not** a claim about production Push/Pull RSS.

### Process end-to-end (`benchmarks/run_process_benchmark.py`)

- Starts real `metadata-service`, three `storage-node` processes, and a **disposable** PostgreSQL database owned by the harness.
- Never uses `ai_artifact_store_dev` or `ai_artifact_store_test`.
- Measures production CLI Push/Pull over localhost TCP.
- Timing: Python `time.perf_counter()`.
- Verifies restored file SHA-256 after Pull.

## Reference environment

From `m10_reference_process.json` → `environment`:

| Field | Value |
| --- | --- |
| OS | Darwin 24.6.0 |
| Architecture | arm64 |
| CPU model | Apple M4 Pro |
| Logical CPUs | 12 |
| Build type | Release |
| CMake | cmake version 4.4.2 |
| Compiler (PATH probe) | Apple clang version 17.0.0 |

These results are single-machine / localhost / illustrative.

## Chunking benchmark

Source: `m10_reference_chunking.json` (128 MiB dataset, 5 iterations, 64 KiB shift).

| Strategy | Median elapsed | Median throughput | Shifted reuse ratio |
| --- | ---: | ---: | ---: |
| FixedSize 4 MiB | 50.5 ms | 2533.5 MiB/s | 0.00 |
| FastCDC 2/4/8 MiB | 336.5 ms | 380.3 MiB/s | 0.9579 |

Notes:

- FixedSize shifted-content reuse is 0 for a 64 KiB prefix shift (boundaries move with fixed offsets).
- FastCDC reused ~95.8% of shifted-file logical bytes via exact chunk-ID intersection (`reused_logical_bytes / shifted_dataset_bytes`).

## Dedup shift benchmark

Exact fields from the same JSON:

| Strategy | base_chunk_count | shifted_chunk_count | shared_chunk_count | reused_logical_bytes | reuse_ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| FixedSize | 32 | 33 | 0 | 0 | 0 |
| FastCDC | 29 | 29 | 28 | 128626945 | 0.9578777069344993 |

## End-to-end localhost benchmark

Source: `m10_reference_process.json` (64 MiB files, FixedSize 4 MiB, 3 storage nodes).

| Scenario | Elapsed (s) | Logical MiB/s | Notes |
| --- | ---: | ---: | --- |
| cold_push_rf1 | 0.1488 | 430.1 | storage_byte_amplification = 1.0 |
| warm_dedup_push_rf1 | 0.0842 | 760.5 | bytes_sent_to_storage = 0; network_avoidance_ratio = 1.0 |
| pull_rf1 | 0.3066 | 208.8 | SHA-256 verified |
| cold_push_rf2 | 0.2243 | 285.4 | storage_byte_amplification = 2.0 |
| pull_rf2 | 0.3003 | 213.1 | SHA-256 verified |

### RF1 vs RF2 storage-byte amplification

`storage_byte_amplification = bytes_sent_to_storage / bytes_read`

- RF1 cold push: **1.0**
- RF2 cold push: **2.0**

This is physical byte send amplification on this run, not a synonym for configured RF under all dedup conditions.

### Warm dedup behavior

Warm push of identical bytes under a new Artifact identity:

- `bytes_sent_to_storage = 0`
- `verified_target_chunks = 16`
- `network_bytes_avoided = 67108864`
- `network_avoidance_ratio = 1.0`

## Architectural memory bounds

These are **analytical** bounds from engine constants, not measured RSS.

| Path | Bound |
| --- | --- |
| Push | 1 MiB read buffer; 4 workers; queue capacity 8; chunk ≤ 8 MiB |
| Pull | 4 workers; window capacity 8; chunk ≤ 8 MiB; logical ready payload ≈ 64 MiB plus ancillary buffers |
| FastCDC | buffer ≤ configured max chunk (default max 8 MiB) |
| Repair | one verified chunk class at a time; ≤ 8 MiB payload class |

Whole-process RSS can be higher (HTTP string bodies, metadata JSON, OS buffers, PostgreSQL client, etc.).

## Known bottlenecks

Documented current architecture costs (not redesigned in M10):

1. `HttpClient` opens a new TCP connection per request (`Connection: close`).
2. `PostgresMetadataRepository` uses one `pqxx::connection`.
3. `MetadataService` serializes repository access with a mutex.
4. `LocalChunkStore::list_chunks` materializes directory inventory before pagination.
5. Lifecycle evaluation loads all non-retired candidates into memory.
6. Per-replica PUT/verify work is sequential inside outer Push workers.

M10 leaves these unchanged because correctness is established, changing them would alter concurrency/architecture, and the new harnesses now provide a baseline for future optimization decisions.

These observations are **known architecture costs**. This document does **not** claim they are proven dominant bottlenecks on every workload unless isolated by a dedicated experiment.

## Interpretation / limitations

### What is measured?

- In-process chunk boundary detection + chunk-ID hashing throughput/medians.
- Exact chunk-ID reuse under a deterministic content shift.
- Localhost Push/Pull wall time and CLI-reported byte counters.

### What is analytically bounded?

- Engine buffer/window/worker constants (table above).

### What is not measured?

- Cross-machine WAN latency.
- Multi-client contention.
- Process RSS / allocator profiles.
- PostgreSQL query plans under large catalogs.
- Keep-alive / pooled HTTP or DB connection effects.

### What likely dominates current localhost performance?

On this localhost SSD/ARM machine, chunking+hash microbenchmarks are extremely fast relative to end-to-end Push/Pull. End-to-end time includes process orchestration, HTTP request/response bodies, CAS fsync/rename, and metadata RPCs. Correlation is not causation: new-TCP-per-request and single DB connection are plausible contributors, not isolated proof.

### Which optimizations are intentionally deferred?

HTTP keep-alive pools, PostgreSQL pools, lifecycle pagination, GC inventory indexing, and broader concurrency redesigns.

## How to reproduce

See [`benchmarks/README.md`](../benchmarks/README.md).
