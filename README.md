# AI Artifact Store

A content-addressed, versioned, distributed artifact storage system for AI/ML workflows.

## Project Goal

AI Artifact Store is a systems engineering project focused on efficiently storing, versioning, deduplicating, transferring, verifying, recovering, and lifecycle-managing large AI/ML artifacts.

The project uses AI/ML workloads as the design context, while storage systems engineering remains the core technical focus.

## Current Status

**Milestone 4 — One-Pass Push Protocol**

The local content-addressed core, metadata model, and production push path are in place through M4 Step 6:

- content-addressed local CAS
- Object / immutable layout model
- ArtifactVersion identity
- metadata + storage services
- production HTTP clients
- persistent UploadSession
- batch dedup negotiation
- bounded one-pass push
- Hybrid Verification
- 4 upload workers / backpressure
- atomic finalize
- idempotent finalize
- resumable Open-session push
- production `aistore push`

## CLI

Default local endpoints:

- metadata-service: `127.0.0.1:8080`
- storage-node: `127.0.0.1:8081`

`--artifact-id` must already exist in metadata. Artifact creation is outside the push CLI.

Example:

```bash
./build/aistore push \
  --file ./artifact.bin \
  --artifact-id <uuidv7> \
  --storage-node-id node-1
```

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

Items such as CDC, GC, replication, multi-node placement, and AI lifecycle policies are planned and not claimed as implemented.

## Development Philosophy

The project is being built incrementally with an emphasis on correctness, systems understanding, explicit engineering tradeoffs, failure handling, testing, and measured performance.
