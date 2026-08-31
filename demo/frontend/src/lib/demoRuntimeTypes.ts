import type { ReactNode } from "react";
import type {
  AppTab,
  DemoState,
  GuidedStepName,
  ReferencePerformance,
  RegistryState,
} from "../types";
import type { DemoMode } from "./demoMode";

export interface DemoCapabilities {
  mutateCluster: boolean;
  mutateLifecycle: boolean;
  uploadArtifact: boolean;
  downloadRestore: boolean;
  pollState: boolean;
  showLifecycleTab: boolean;
  scenarioExplorer: boolean;
}

export interface DemoLabels {
  productSubtitle: string;
  statusPrefix: string;
  disclosure: string | null;
  localDemoHint: string | null;
  localDemoUrl: string;
  metricNodesLabel: string;
  chunkPlacementNote: string;
  guidedCompleteBody: string;
  lifecycleIntro: string;
  waitingForController: string;
}

export interface DemoActions {
  guidedPush: () => Promise<DemoState>;
  guidedKill: () => Promise<DemoState>;
  guidedPull: () => Promise<DemoState>;
  guidedRepair: () => Promise<DemoState>;
  guidedComplete: () => Promise<DemoState>;
  resetDemo: () => Promise<DemoState>;
  pullArtifact: () => Promise<DemoState>;
  repairArtifact: () => Promise<DemoState>;
  killNode: (nodeId: string) => Promise<DemoState>;
  restartNode: (nodeId: string) => Promise<DemoState>;
  setNodeState: (nodeId: string, state: RegistryState) => Promise<DemoState>;
  pushArtifact: (
    body: ArrayBuffer | Blob | Uint8Array,
    params: {
      filename: string;
      chunking_strategy: "fixed-size" | "fastcdc";
      replication_factor: 1 | 2 | 3;
      artifact_kind:
        | "generic"
        | "model-checkpoint"
        | "dataset-snapshot"
        | "embedding-index"
        | "evaluation-output";
    },
  ) => Promise<DemoState>;
  lifecycleSetup: () => Promise<DemoState>;
  lifecycleDryRun: () => Promise<DemoState>;
  lifecycleApply: () => Promise<DemoState>;
  lifecycleGc: () => Promise<DemoState>;
  getRestoredUrl: () => string;
  selectScenario: (step: GuidedStepName) => Promise<void>;
}

export interface DemoRuntimeValue {
  mode: DemoMode;
  capabilities: DemoCapabilities;
  labels: DemoLabels;
  tabs: { id: AppTab; label: string }[];
  state: DemoState | null;
  busy: boolean;
  locked: boolean;
  pollError: string | null;
  actionError: string | null;
  actionNote: string | null;
  dismissActionError: () => void;
  referencePerformance: ReferencePerformance | null;
  referenceError: string | null;
  referenceLoading: boolean;
  runMutation: (fn: () => Promise<DemoState>, note?: string) => Promise<void>;
  actions: DemoActions;
}

export type DemoRuntimeProviderComponent = (props: {
  children: ReactNode;
}) => JSX.Element;
