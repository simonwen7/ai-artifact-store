# AI Artifact Store

A content-addressed, versioned, distributed artifact storage system for AI/ML workflows.

## Project Goal

AI Artifact Store is a systems engineering project focused on efficiently storing, versioning, deduplicating, transferring, verifying, recovering, and lifecycle-managing large AI/ML artifacts.

The project uses AI/ML workloads as the design context, while storage systems engineering remains the core technical focus.

## Current Status

**Milestone 6 — Content-Defined Chunking**

The local content-addressed core, metadata model, push path, pull/restore path, and FastCDC chunking dispatch are in place through M6:

- content-addressed local CAS
- Object / immutable layout model
- ArtifactVersion identity
- metadata + storage services
- production HTTP clients
- persistent UploadSession
- bounded one-pass push
- atomic finalize
- resumable Open-session push
- production `aistore push`
- RestorePlan resolution for committed versions
- bounded pull with 4 download workers / window backpressure
- resumable partial restore via `.aistore.<version_id>.part`
- atomic publish (hard-link or rename)
- production `aistore pull`
- FastCDC content-defined chunking in PushEngine
- FixedSize chunking still supported
- strategy-specific UploadSession identity and committed retry
- pull without chunking flags (layout-driven restore)

Fixed-size chunking remains the default and is fully supported. FastCDC improves deduplication when similar content shifts inside large artifacts because chunk boundaries follow content rather than fixed offsets.

Object IDs remain whole-object SHA-256 hashes and are independent of chunking strategy.

Pull does not require chunking flags; restore uses the committed layout descriptor.

FastCDC CLI defaults: min 2 MiB, avg 4 MiB, max 8 MiB.

Benchmarking and production performance tuning are future polish; no production benchmark claims are made here.

## CLI

Default local endpoints:

- metadata-service: `127.0.0.1:8080`
- storage-node: `127.0.0.1:8081`

`--artifact-id` must already exist in metadata. Artifact creation is outside the push CLI.

### Push (FixedSize)

Example:

```bash
./build/aistore push \
  --file ./artifact.bin \
  --artifact-id <uuidv7> \
  --storage-node-id node-1
```

Defaults: `--chunking-strategy fixed-size`, `--chunk-size 4194304`.

### Push (FastCDC)

Example:

```bash
./build/aistore push \
  --file ./artifact.bin \
  --artifact-id <uuidv7> \
  --storage-node-id node-1 \
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

`aistore pull` restores a committed ArtifactVersion from a configured source storage node.

Required flags:

- `--version-id` — committed ArtifactVersion to restore
- `--output` — destination file path (parent directory must exist)
- `--storage-node-id` — source node that holds Available chunk locations

Optional flags:

- `--overwrite` — replace an existing destination file (default: fail if destination exists)

Example:

```bash
./build/aistore pull \
  --version-id <64-char-hex-version-id> \
  --output ./restored.bin \
  --storage-node-id node-1
```

Interrupted pulls leave a resumable partial file beside the destination:

`<output>.aistore.<version_id>.part`

Rerunning the same command resumes verified prefix bytes and continues downloading remaining chunks.

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
- Garbage collection
- Multi-node storage and replication
- AI-workload-aware lifecycle policies

Items such as GC, replication, multi-node placement, and AI lifecycle policies are planned and not claimed as implemented.

## Development Philosophy

The project is being built incrementally with an emphasis on correctness, systems understanding, explicit engineering tradeoffs, failure handling, testing, and measured performance.
