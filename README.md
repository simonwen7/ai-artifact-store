# AI Artifact Store

A content-addressed, versioned, distributed artifact storage system for AI/ML workflows.

## Project Goal

AI Artifact Store is a systems engineering project focused on efficiently storing, versioning, deduplicating, transferring, verifying, recovering, and lifecycle-managing large AI/ML artifacts.

The project uses AI/ML workloads as the design context, while storage systems engineering remains the core technical focus.

## Current Status

**Milestone 9 — AI-Aware Lifecycle Policy**

M9 adds an explainable, persistent, AI-aware lifecycle policy system for committed ArtifactVersions. Lifecycle evaluation decides which semantic versions may stop protecting their stored representations. Apply mode writes semantic retirement records only; M7 garbage collection reclaims unreferenced physical bytes afterward.

Reserved immutable metadata key:

- `aistore.artifact_kind`

Allowed values:

- `generic` (default when the key is absent)
- `model-checkpoint`
- `dataset-snapshot`
- `embedding-index`
- `evaluation-output`

Lifecycle capabilities:

- policy files with exactly five kind-specific rules
- keep-last-N retention per artifact kind
- optional max-age thresholds (`null` means no age barrier)
- operational pinning separate from version identity
- tag and manifest reference protection
- dry-run and explainable decisions
- semantic retirement (terminal; no unretire)
- lifecycle Apply does **not** delete CAS bytes — run `aistore gc` afterward
- retired versions cannot Pull or Repair
- shared Object/Chunk representations remain protected while any non-retired version still references them

Only **non-retired** committed ArtifactVersions act as GC roots in M9.

M9 does **not** implement:

- automatic scheduler or background daemon
- storage tiers or hot/cold placement
- capacity or cost models
- access-frequency policy
- heartbeat or automatic rebalance
- hard deletion of `artifact_versions` rows

Prior milestones remain in place through M8:

- content-addressed local CAS
- Object / immutable layout model
- ArtifactVersion identity
- metadata + storage services
- production HTTP clients
- persistent UploadSession with replication-factor and placement snapshots
- bounded one-pass multi-replica push
- atomic finalize requiring every desired replica Available
- resumable Open-session push
- production `aistore push` (registry-driven placement, no `--storage-node-id`)
- automatic multi-node RestorePlan resolution with per-chunk source fallback
- bounded pull with replica failover across registered Active/Draining nodes
- resumable partial restore via `.aistore.<version_id>.part`
- atomic publish (hard-link or rename)
- production `aistore pull` (no `--storage-node-id`)
- FastCDC content-defined chunking in PushEngine
- FixedSize chunking still supported
- strategy-specific UploadSession identity and committed retry
- production `aistore gc` with registry-resolved storage endpoints
- physical CAS inventory and idempotent chunk deletion before metadata sweep
- conservative Push/GC/Replication/Lifecycle mutual exclusion
- storage node registry (`aistore node register|list|set-state`)
- deterministic per-chunk Rendezvous placement (`select_replica_nodes`, SHA-256)
- replication runs and resumable `aistore repair` for under-replicated committed versions

Storage-node process identity:

- `AISTORE_STORAGE_ROOT` (required)
- `AISTORE_STORAGE_NODE_ID` (default `node-1`)
- `AISTORE_STORAGE_PORT` (default `8081`)

Node states: Active (placement/pull/repair), Draining (pull/repair only), Disabled (excluded from automatic pull/repair; GC may still target explicitly).

Not implemented in M9: heartbeat/failure detector, capacity-aware placement, rack/zone topology, automatic background rebalance, automatic lifecycle scheduling.

Fixed-size chunking remains the default and is fully supported. FastCDC improves deduplication when similar content shifts inside large artifacts because chunk boundaries follow content rather than fixed offsets.

Object IDs remain whole-object SHA-256 hashes and are independent of chunking strategy.

Pull does not require chunking flags or a source node flag; restore uses the committed layout descriptor and registered replica locations.

FastCDC CLI defaults: min 2 MiB, avg 4 MiB, max 8 MiB.

GC refuses to start while any Open UploadSession or Open ReplicationRun exists. Lifecycle runs refuse while any Open UploadSession, Open GcRun, or Open ReplicationRun exists. While a GcRun is Open, new UploadSessions are rejected; Pull and read-only restore APIs remain available.

Physical collectible chunks are removed from the configured storage node before metadata orphan sweep on apply runs.

Benchmarking and production performance tuning are future polish; no production benchmark claims are made here.

## CLI

Default local endpoints:

- metadata-service: `127.0.0.1:8080`
- storage-node: `127.0.0.1:8081`

`--artifact-id` must already exist in metadata. Artifact creation is outside the push CLI.

Assign AI artifact kind through existing push metadata flags, for example:

```bash
--metadata aistore.artifact_kind=model-checkpoint
```

Do not introduce a separate `--artifact-kind` push flag.

### Push (FixedSize)

Example:

```bash
./build/aistore node register --storage-node-id node-1 --storage-address 127.0.0.1 --storage-port 8081

./build/aistore push \
  --file ./artifact.bin \
  --artifact-id <uuidv7> \
  --metadata aistore.artifact_kind=model-checkpoint
```

Defaults: `--chunking-strategy fixed-size`, `--chunk-size 4194304`, `--replication-factor 1`.

Active storage nodes are discovered from the metadata registry; `--storage-node-id` is not required for push. The session snapshots sorted Active node IDs and uses deterministic Rendezvous placement per chunk. Finalize requires every desired replica Available.

```bash
./build/aistore push \
  --file ./artifact.bin \
  --artifact-id <uuidv7> \
  --replication-factor 2
```

### Push (FastCDC)

Example:

```bash
./build/aistore node register --storage-node-id node-1 --storage-address 127.0.0.1 --storage-port 8081

./build/aistore push \
  --file ./artifact.bin \
  --artifact-id <uuidv7> \
  --chunking-strategy fastcdc \
  --min-chunk-size 2097152 \
  --avg-chunk-size 4194304 \
  --max-chunk-size 8388608
```

FastCDC flags cannot be combined with `--chunk-size`. Fixed-size pushes cannot combine with FastCDC size flags.

Rerunning with the same `--session-id` handles both:

- an Open session after an interrupted data-plane upload
- a Committed session whose finalize acknowledgement may not have been observed by the client

For the latter, the CLI locally rescans the source, reconstructs the descriptor, and uses idempotent finalize.

### Pull

`aistore pull` restores a committed ArtifactVersion using automatic multi-node restore planning.

Required flags:

- `--version-id` — committed ArtifactVersion to restore
- `--output` — destination file path (parent directory must exist)

Optional flags:

- `--overwrite` — replace an existing destination file (default: fail if destination exists)

Example:

```bash
./build/aistore pull \
  --version-id <64-char-hex-version-id> \
  --output ./restored.bin
```

Retired versions return `410 artifact_version_retired`.

Interrupted pulls leave a resumable partial file beside the destination:

`<output>.aistore.<version_id>.part`

Rerunning the same command resumes verified prefix bytes and continues downloading remaining chunks.

### Garbage Collection

`aistore gc` runs garbage collection against a registered storage node. It inventories physical CAS chunks, classifies reachability through metadata, deletes collectible physical bytes (apply mode), and sweeps orphaned metadata representations.

Required flags:

- `--storage-node-id` — registered storage node to inventory and collect from

Optional flags:

- `--gc-run-id` — resume a previously started GC run (UUIDv7)
- `--dry-run` — inventory and classify without deleting physical bytes or sweeping metadata

If `--gc-run-id` is omitted, a new UUIDv7 run ID is generated locally before starting.

Example:

```bash
./build/aistore node register --storage-node-id node-1 --storage-address 127.0.0.1 --storage-port 8081

./build/aistore gc \
  --storage-node-id node-1
```

Dry-run example:

```bash
./build/aistore gc \
  --storage-node-id node-1 \
  --dry-run
```

Resume after an interrupted apply run:

```bash
./build/aistore gc \
  --storage-node-id node-1 \
  --gc-run-id <uuidv7>
```

In M9, only **non-retired** committed ArtifactVersions root their Objects for GC reachability. Semantic retirement preserves historical UploadSession and finalization records; later GC may reclaim dead representation metadata once no live version protects it. GC does not delete Artifact or ArtifactVersion metadata rows and does not implement retention policy evaluation — use lifecycle Apply first, then GC.

GC refuses to start while any Open UploadSession exists. While a GcRun is Open, new UploadSessions are rejected. Pull remains available.

Apply mode removes collectible physical chunks before metadata sweep.

### Lifecycle

`aistore lifecycle` manages lifecycle policies, pins, evaluation runs, and decision explanation. Lifecycle is metadata-only (no storage-node flags).

Subcommands:

```bash
aistore lifecycle policy create --file <policy.json> [--policy-id <uuidv7>]
aistore lifecycle policy show --policy-id <uuidv7>
aistore lifecycle pin --version-id <64hex> --reason "<text>"
aistore lifecycle unpin --version-id <64hex>
aistore lifecycle run --policy-id <uuidv7> [--lifecycle-run-id <uuidv7>] [--dry-run]
aistore lifecycle explain --lifecycle-run-id <uuidv7>
```

Example policy file shape:

```json
{
  "name": "ai-balanced",
  "rules": [
    {"artifact_kind": "generic", "keep_last_n": 3, "max_age_seconds": null},
    {"artifact_kind": "model-checkpoint", "keep_last_n": 3, "max_age_seconds": 2592000},
    {"artifact_kind": "dataset-snapshot", "keep_last_n": 2, "max_age_seconds": 7776000},
    {"artifact_kind": "embedding-index", "keep_last_n": 2, "max_age_seconds": 1209600},
    {"artifact_kind": "evaluation-output", "keep_last_n": 1, "max_age_seconds": 604800}
  ]
}
```

Dry-run evaluates and persists decisions without writing retirement rows. Apply writes semantic retirement records for policy-retire candidates. Run `aistore gc` afterward to reclaim unreferenced physical bytes.

Retirement is terminal: retired versions cannot be pinned, tagged, referenced by new manifests, pulled, or repaired.

### Storage Node Registry

```bash
./build/aistore node register \
  --storage-node-id node-1 \
  --storage-address 127.0.0.1 \
  --storage-port 8081

./build/aistore node list

./build/aistore node set-state --storage-node-id node-1 --state disabled
```

### Repair

`aistore repair` copies missing replicas for a committed version using a resumable replication run.

```bash
./build/aistore repair \
  --version-id <64-char-hex-version-id> \
  --replication-factor 2
```

Retired versions are rejected with `410 artifact_version_retired`.

## Technology

- C++20
- CMake
- Apple Clang
- GoogleTest
- PostgreSQL
- Boost.Asio / Boost.Beast / Boost.JSON
- OpenSSL

Additional dependencies are introduced only when required by a milestone.

## Planned Core Capabilities

- Content-addressed chunk storage
- Immutable artifact versioning
- Streaming uploads and downloads
- Chunk-level deduplication
- Integrity verification
- Resumable transfers
- Parallel data transfer
- Content-defined chunking
- Garbage collection (M7)
- Multi-node storage and replication (M8)
- AI-workload-aware lifecycle policies (M9)

Replication placement beyond M8 (capacity-aware / rack-aware / background rebalance) and automatic lifecycle scheduling remain future work.

Heartbeat-derived node health and automatic state transitions are not implemented.

## Development Philosophy

The project is being built incrementally with an emphasis on correctness, systems understanding, explicit engineering tradeoffs, failure handling, testing, and measured performance.
