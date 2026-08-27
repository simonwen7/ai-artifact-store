# CLI

Operational reference for production `aistore` commands. Behavior matches the current binary; this document does not change contracts.

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

Only **non-retired** committed ArtifactVersions root their Objects for GC reachability. Semantic retirement preserves historical UploadSession and finalization records; later GC may reclaim dead representation metadata once no live version protects it. GC does not delete Artifact or ArtifactVersion metadata rows and does not implement retention policy evaluation — use lifecycle Apply first, then GC.

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

