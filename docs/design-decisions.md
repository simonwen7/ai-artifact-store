# Design Decisions

Concise ADR-style notes grounded in the current codebase.

## Whole-object Object ID independent of chunking

- **Decision:** `object_id = SHA-256(entire raw byte stream)`.
- **Why:** Content identity must not change when chunking strategy changes.
- **Tradeoff:** Requires a full-object hash during Push scan.
- **Alternative not chosen:** Layout-hash-as-object-id (rejected; conflates representation with content).

## Immutable ObjectLayouts

- **Decision:** Layouts are content-addressed and immutable once registered.
- **Why:** Stable representation identity for restore/repair/GC.
- **Tradeoff:** Multiple layouts may exist for one Object.
- **Alternative not chosen:** Mutable in-place layout edits.

## ArtifactVersion content identity

- **Decision:** Version IDs are derived from immutable version inputs (artifact, root object, parent, metadata).
- **Why:** Idempotent finalize and conflict detection.
- **Tradeoff:** Metadata is immutable at commit time.
- **Alternative not chosen:** Server-assigned monotonic version numbers as primary identity.

## Client-orchestrated Push/Pull

- **Decision:** CLI/engines orchestrate chunk transfer; metadata service authorizes/records.
- **Why:** Explicit control plane vs data plane; simpler metadata service.
- **Tradeoff:** Clients must speak storage HTTP.
- **Alternative not chosen:** Metadata service proxying all chunk bytes.

## One-pass Push

- **Decision:** Single sequential read feeds object hash + chunker + upload pipeline.
- **Why:** Large artifacts should not require multi-pass disk scans.
- **Tradeoff:** Pipeline complexity (queue/workers).
- **Alternative not chosen:** Separate hash-then-chunk passes.

## FixedSize + FastCDC

- **Decision:** Support both; FixedSize default; FastCDC optional with fixed defaults (2/4/8 MiB).
- **Why:** Predictable ops vs shift-resilient dedup.
- **Tradeoff:** Two chunking configuration surfaces.
- **Alternative not chosen:** FastCDC-only.

## Hybrid metadata + HEAD verification

- **Decision:** Negotiation uses metadata intent plus storage HEAD/PUT verification.
- **Why:** Detect stale/missing CAS relative to metadata.
- **Tradeoff:** Extra RPCs.
- **Alternative not chosen:** Trust metadata locations blindly.

## Rendezvous placement

- **Decision:** Deterministic node selection from active registry + RF.
- **Why:** Stable, decentralized placement without a central balancer.
- **Tradeoff:** Not capacity-aware.
- **Alternative not chosen:** Random or load-based placement in current scope.

## Synchronous desired replication

- **Decision:** Push attempts to place desired RF replicas before finalize acceptance rules.
- **Why:** Read availability without async repair dependency for the common path.
- **Tradeoff:** Higher Push latency/amplification.
- **Alternative not chosen:** Async-only replication.

## Source fallback on Pull/Repair

- **Decision:** Per-chunk alternate sources when a replica fails/invalidates.
- **Why:** Survive single-node loss when RF>1.
- **Tradeoff:** More complex restore plans.
- **Alternative not chosen:** Single fixed source node forever.

## Semantic retirement instead of hard version delete

- **Decision:** Lifecycle Apply writes retirement rows; versions remain historically addressable as retired.
- **Why:** Explainable policy + terminal semantics without rewriting history.
- **Tradeoff:** Object identity rows may remain while representation is GC’d.
- **Alternative not chosen:** Hard-delete ArtifactVersion rows on retire.

## Physical-before-metadata GC

- **Decision:** Delete CAS bytes first; sweep metadata after successful physical phase.
- **Why:** Avoid metadata saying “gone” while bytes still consume disk if crash mid-GC.
- **Tradeoff:** Temporary orphans possible after physical delete before metadata sweep completes (resume handles).
- **Alternative not chosen:** Metadata-first deletion.

## Persistent operational run/session IDs

- **Decision:** UploadSession / GcRun / ReplicationRun / LifecycleRun are durable.
- **Why:** Crash-safe resume and idempotent completion.
- **Tradeoff:** Operational table growth.
- **Alternative not chosen:** Ephemeral in-memory-only jobs.

## No heartbeat/failure detector in current scope

- **Decision:** Node states are explicit registry fields; no automatic detector.
- **Why:** Keep failure model explicit and testable.
- **Tradeoff:** Operators must mark Draining/Disabled.
- **Alternative not chosen:** Gossip/heartbeat subsystem in M8–M10.

## No premature TCP/DB performance rewrite in M10

- **Decision:** Measure and document connection-per-request and single-DB-connection costs; do not rewrite them in M10.
- **Why:** Correctness is established; optimization without isolation risks regressions.
- **Tradeoff:** Localhost performance headroom remains on the table.
- **Alternative not chosen:** Keep-alive pools / pqxx pools as an unmeasured drive-by change.
