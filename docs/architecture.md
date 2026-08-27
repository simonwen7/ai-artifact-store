# Architecture

Factual current architecture of AI Artifact Store (through M10 polish). Details for CLI usage live in [`cli.md`](cli.md). Failure semantics: [`failure-model.md`](failure-model.md). Design tradeoffs: [`design-decisions.md`](design-decisions.md).

## High-level topology

```mermaid
flowchart LR
  CLI[aistore CLI]
  MS[Metadata Service<br/>Boost.Beast/Asio]
  PG[(PostgreSQL)]
  SN1[Storage Node A<br/>LocalChunkStore]
  SN2[Storage Node B]
  SN3[Storage Node C]

  CLI -->|HTTP JSON| MS
  MS --> PG
  CLI -->|HTTP chunk PUT/GET/HEAD| SN1
  CLI --> SN2
  CLI --> SN3
  MS -.->|registry endpoints| CLI
```

## Identity / data model

```mermaid
erDiagram
  ARTIFACT ||--o{ ARTIFACT_VERSION : has
  ARTIFACT_VERSION ||--|| OBJECT : root_object_id
  OBJECT ||--o{ OBJECT_LAYOUT : represented_by
  OBJECT_LAYOUT ||--o{ OBJECT_LAYOUT_CHUNK : contains
  CHUNK ||--o{ OBJECT_LAYOUT_CHUNK : referenced_by
  CHUNK ||--o{ STORAGE_LOCATION : placed_on
  STORAGE_NODE ||--o{ STORAGE_LOCATION : hosts
  MANIFEST ||--o{ MANIFEST_ENTRY : lists
  MANIFEST_ENTRY }o--|| ARTIFACT_VERSION : references
  UPLOAD_SESSION ||--o| UPLOAD_SESSION_FINALIZATION : finalizes
  ARTIFACT_VERSION ||--o| ARTIFACT_VERSION_RETIREMENT : may_have
```

Core identity rules:

- **Chunk ID** = SHA-256(chunk bytes)
- **Object ID** = SHA-256(entire raw object byte stream)
- **ObjectLayout ID** = SHA-256(canonical layout descriptor)
- **ArtifactVersion ID** = content-addressed version identity over immutable metadata + root object + parent

## Push flow

```mermaid
sequenceDiagram
  participant CLI
  participant Meta as MetadataService
  participant Store as StorageNode(s)

  CLI->>Meta: create UploadSession (RF + placement snapshot)
  CLI->>CLI: one-pass scan (object Sha256 + chunker)
  loop negotiation batches
    CLI->>Meta: negotiate chunk needs
    CLI->>Store: HEAD/PUT replicas for missing chunks
    CLI->>Meta: register StorageLocation(s)
  end
  CLI->>Meta: finalize_upload(layout)
  Meta-->>CLI: committed version + layout identity
```

Push is client-orchestrated, one-pass over the source file, with outer worker concurrency and serial per-replica work inside each chunk task.

## Pull flow

```mermaid
sequenceDiagram
  participant CLI
  participant Meta as MetadataService
  participant Store as StorageNode(s)

  CLI->>Meta: resolve restore plan (single- or multi-node)
  loop windowed download workers
    CLI->>Store: GET chunk (source fallback on failure)
    CLI->>CLI: ordered write into partial file
  end
  CLI->>CLI: whole-object verify + atomic publish
```

## Repair / GC / Lifecycle relationship

```mermaid
flowchart TD
  Life[Lifecycle Apply<br/>semantic retirement only]
  GC[GC Apply<br/>physical then metadata]
  Repair[Repair<br/>ReplicationRun]
  Push[Push / UploadSession]

  Life -->|retires versions| GC
  GC -->|collects unreferenced CAS| GC
  Repair -->|restores RF| Store[(CAS replicas)]
  Push -.->|mutual exclusion with open GC/replication| GC
  Life -.->|refuses while open Upload/GC/Replication| Push
```

- Lifecycle writes retirement records; it does not delete CAS bytes.
- GC deletes collectible physical chunks first, then sweeps metadata.
- Repair copies under-replicated chunks using planned sources/targets.

## Failure / transactional boundaries

| Boundary | Mechanism |
| --- | --- |
| UploadSession | Persistent Open/Committed/Aborted + finalization row |
| Finalize | Single PostgreSQL transaction committing version + layout + session state |
| Pull publish | Write partial → verify → hard-link/rename style atomic publish |
| ReplicationRun | Persistent Open/Completed with resume |
| GcRun | Persistent Open/Completed; physical-before-metadata |
| LifecycleRun | Metadata-only transaction; DryRun vs Apply |

See [`failure-model.md`](failure-model.md) for resume/ACK details.
