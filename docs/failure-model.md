# Failure Model

Factual crash/resume semantics implemented today. This is not a distributed consensus specification.

## Push

| Failure | Behavior |
| --- | --- |
| Open session interrupted mid-upload | Resume same `session_id` with matching creation identity; continue negotiation/PUT |
| Partial replica writes | Missing replicas remain negotiable; finalize requires RF availability on placement nodes |
| Lost finalize ACK after commit | Client may observe create conflict / GET committed session and retry finalize idempotently |
| Committed retry | Finalize with matching layout returns the committed result; retired finalized versions return `410 artifact_version_retired` |

## Pull

| Failure | Behavior |
| --- | --- |
| Interrupted download | Partial restore file retained; resume reuses verified prefix chunks |
| Source node failure | Per-chunk source fallback using multi-node restore plan when available |
| Hash-invalid chunk bytes | Reject that source attempt; try alternate sources |
| Whole-object verification failure | Do not publish destination |
| Successful publish | Atomic publish from partial; best-effort unlink of partial afterward |

## Repair

| Failure | Behavior |
| --- | --- |
| Interrupted repair | Open `ReplicationRun` can be resumed by run id |
| Source failure | Source failover counted in repair stats |
| Lost completion ACK | Completed run retry returns stored stats without repeating storage work |

## GC

| Property | Behavior |
| --- | --- |
| Ordering | Physical CAS delete before metadata sweep |
| Open run | Resumeable; Push blocked while Open |
| DryRun | Inventories/counts without deleting storage or layout metadata |
| Completed retry | Does not re-contact storage for already completed physical work |
| Push barrier | Open UploadSessions block GC start; Open GC blocks new UploadSessions |

## Lifecycle

| Property | Behavior |
| --- | --- |
| Scope | Metadata-only evaluation/persistence |
| DryRun | Persists decisions without retirement rows |
| Apply | Writes terminal `artifact_version_retirements` |
| Lost ACK | Run rows are durable; clients can re-query run/decisions |
| GC separation | Retirement does not delete CAS; run GC afterward |
| Terminality | No unretire; Pull/Repair/tag/pin/manifest reject retired versions |

## Node states

| State | Placement | Pull/Repair | Notes |
| --- | --- | --- | --- |
| Active | Eligible | Eligible | Default healthy node |
| Draining | Not for new placement | Eligible | Drain writes |
| Disabled | Excluded | Excluded from automatic selection | Explicit GC target still possible |

## Explicitly not solved

- Consensus / quorum replication protocols
- Automatic failure detection / heartbeats
- Network partition consensus semantics
- Cross-region durability guarantees
- Background rebalancing / capacity-aware placement
