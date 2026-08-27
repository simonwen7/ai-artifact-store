# Interactive Systems Demo

Portfolio presentation layer for **AI Artifact Store**.

The React/Python layer does **not** reimplement storage semantics. It operates the real completed system: production `aistore` CLI, Metadata Service, PostgreSQL, and three StorageNode processes.

## What it shows

1. Push a real artifact and inspect chunk placement across nodes
2. Kill a real StorageNode process
3. Pull successfully using replica fallback
4. Repair missing replicas and restore healthy replication
5. FixedSize vs FastCDC (from committed M10 reference measurements)
6. AI-aware Lifecycle + GC
7. M10 reference performance snapshot

## Architecture

```text
Browser (Vite :5173)
  → Python demo controller (:8787)
    → production aistore CLI / HTTP
      → Metadata Service (:8080) + PostgreSQL (owned disposable DB)
      → Storage Nodes node-a/b/c (:8081–8083)
```

## Prerequisites

- macOS or Linux
- CMake, C++20 toolchain, vcpkg
- PostgreSQL + `psql` (ability to `CREATE`/`DROP` databases)
- Python 3
- Node.js + npm

## Start

From the repository root:

```bash
./demo/start.sh
```

Open:

http://127.0.0.1:5173

Optional environment:

- `AISTORE_DEMO_BUILD_DIR` (default `build-demo-release`)
- `AISTORE_DEMO_ADMIN_DB_URL` (default `postgresql:///postgres`) — used only to create/drop the owned disposable demo DB
- `VCPKG_ROOT` if vcpkg is not at `$HOME/vcpkg`

## Guided Demo

Default experience on **Cluster Demo**:

1. Push Demo Artifact (32 MiB FastCDC, RF=2, model-checkpoint)
2. Kill a node that actually stores replicas (registry stays Active)
3. Pull while a replica is offline — SHA-256 verified
4. Disable the failed node via production CLI, then Repair
5. See recovery complete with real Push/Pull/Repair stats

## Explorer Mode

Manual controls: upload, kill/restart nodes, registry Active/Draining/Disabled, pull, repair, reset demo.

No arbitrary terminal, SQL, or filesystem path UI.

## Chunking Lab

Illustrative boundary view plus **numeric cards** loaded from:

- `benchmarks/results/m10_reference_chunking.json`

## Lifecycle

Real policy / pin / dry-run / apply / GC against the owned demo DB.

## Performance

Reads committed M10 reference JSON only. No live benchmark execution.

## Safety

- Creates owned DB `aistore_demo_<pid>_<hex>` only
- Never uses `ai_artifact_store_dev` or `ai_artifact_store_test`
- Tracks exact subprocess PIDs (no `pkill` / `killall`)
- Temporary CAS roots under a controller-owned temp directory
- Ctrl+C / SIGTERM cleans up owned processes, owned DB, and temp roots

## Smoke test

With the controller running:

```bash
python3 demo/smoke_test.py
```

Expect:

```text
DEMO_SMOKE_OK
```

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Ports occupied | Free `8080–8083` and `8787` (and `5173` for Vite) |
| PostgreSQL errors | Ensure local Postgres accepts `AISTORE_DEMO_ADMIN_DB_URL` and can create DBs |
| Missing binaries | `start.sh` builds Release into `build-demo-release` when needed |
| vcpkg missing | Set `VCPKG_ROOT` or install at `$HOME/vcpkg` |
| npm issues | Delete `demo/frontend/node_modules` and rerun `./demo/start.sh` (uses `npm ci`) |
