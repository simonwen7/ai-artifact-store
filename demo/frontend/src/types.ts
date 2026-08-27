export type ProcessStatus = "online" | "offline";
export type RegistryState = "active" | "draining" | "disabled";
export type MetadataServiceStatus = "online" | "offline";

export type ChunkingStrategy = "fixed-size" | "fastcdc";
export type ArtifactKind =
  | "generic"
  | "model-checkpoint"
  | "dataset-snapshot"
  | "embedding-index"
  | "evaluation-output";

export type ArtifactHealth = "healthy" | "degraded" | "unavailable";
export type LocationMetadataState = "available" | "missing" | "corrupt";

export type GuidedStepName =
  | "READY"
  | "PUSHED"
  | "NODE_FAILED"
  | "PULL_VERIFIED"
  | "REPAIRED"
  | "COMPLETE";

export type EventKind =
  | "system"
  | "push"
  | "pull"
  | "node"
  | "repair"
  | "lifecycle"
  | "gc"
  | "error";

export type SemanticStatus = "active" | "retired";
export type LifecycleDecision = "retain" | "retire";
export type PhysicalRepresentation =
  | "present"
  | "partially-reclaimed"
  | "reclaimed";

export interface ApiErrorBody {
  error: string;
  message?: string;
}

export interface HealthResponse {
  status: string;
}

export interface NodeState {
  node_id: string;
  port: number;
  process_status: ProcessStatus;
  registry_state: RegistryState;
  chunk_count: number;
}

export interface MetadataServiceState {
  status: MetadataServiceStatus;
  port: number;
}

export interface ClusterState {
  metadata_service: MetadataServiceState;
  nodes: NodeState[];
}

export interface ChunkLocation {
  node_id: string;
  metadata_state: LocationMetadataState;
  process_status: ProcessStatus;
}

export interface ChunkPlacement {
  chunk_id: string;
  size_bytes: number;
  desired_nodes: string[];
  locations: ChunkLocation[];
}

export interface ArtifactState {
  artifact_id: string;
  version_id: string;
  object_id: string;
  layout_id: string;
  filename: string;
  size_bytes: number;
  chunking_strategy: string;
  replication_factor: number;
  artifact_kind: string;
  health: ArtifactHealth;
  chunks: ChunkPlacement[];
}

export interface DemoEvent {
  timestamp: string;
  kind: EventKind | string;
  message: string;
}

export interface GuidedOperationStat {
  label?: string;
  elapsed_seconds?: number;
  chunk_count?: number;
  bytes?: number;
  node_id?: string;
  message?: string;
  [key: string]: unknown;
}

export interface GuidedState {
  step: GuidedStepName | string;
  step_index?: number;
  killed_node_id?: string | null;
  summary?: {
    push?: GuidedOperationStat | null;
    pull?: GuidedOperationStat | null;
    repair?: GuidedOperationStat | null;
  } | null;
}

export interface LifecycleVersionState {
  label: "v1" | "v2" | "v3" | string;
  version_id: string;
  semantic_status: SemanticStatus | string;
  pin: boolean;
  last_decision: LifecycleDecision | string | null;
  reason: string | null;
  physical_representation: PhysicalRepresentation | string;
}

export interface LifecycleState {
  initialized: boolean;
  policy_id: string | null;
  versions: LifecycleVersionState[];
}

export interface DemoState {
  ready: boolean;
  busy?: boolean;
  cluster: ClusterState;
  artifact: ArtifactState | null;
  guided: GuidedState;
  lifecycle: LifecycleState;
  events: DemoEvent[];
}

export interface PushArtifactParams {
  filename: string;
  chunking_strategy: ChunkingStrategy;
  replication_factor: 1 | 2 | 3;
  artifact_kind: ArtifactKind;
}

export interface SetNodeStateRequest {
  state: RegistryState;
}

export interface ChunkingReference {
  benchmark?: string;
  dataset_bytes?: number;
  iterations?: number;
  shift_bytes?: number;
  shifted_dataset_bytes?: number;
  schema_version?: number;
  fixed_size: {
    chunk_size_bytes?: number;
    base_chunk_count?: number;
    shifted_chunk_count?: number;
    shared_chunk_count?: number;
    reused_logical_bytes?: number;
    reuse_ratio: number;
    median_elapsed_ms: number;
    median_mib_per_second: number;
  };
  fastcdc: {
    min_chunk_size_bytes?: number;
    avg_chunk_size_bytes?: number;
    max_chunk_size_bytes?: number;
    base_chunk_count?: number;
    shifted_chunk_count?: number;
    shared_chunk_count?: number;
    reused_logical_bytes?: number;
    reuse_ratio: number;
    median_elapsed_ms: number;
    median_mib_per_second: number;
  };
}

export interface ProcessScenario {
  name?: string;
  elapsed_seconds: number;
  logical_bytes?: number;
  logical_mib_per_second: number;
  storage_byte_amplification?: number;
  sha256_verified?: boolean;
  cli?: Record<string, unknown>;
}

export interface ProcessReference {
  benchmark?: string;
  configuration?: {
    file_bytes?: number;
    fixed_chunk_size_bytes?: number;
    storage_node_count?: number;
  };
  environment?: Record<string, unknown>;
  scenarios: Record<string, ProcessScenario>;
}

export interface ReferencePerformance {
  chunking: ChunkingReference;
  process: ProcessReference;
}

export type MutationResult = DemoState | { state: DemoState; [key: string]: unknown };
