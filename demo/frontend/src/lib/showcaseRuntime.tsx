import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import type { AppTab, DemoState, GuidedStepName, ReferencePerformance } from "../types";
import type {
  DemoActions,
  DemoCapabilities,
  DemoLabels,
  DemoRuntimeValue,
} from "./demoRuntimeTypes";
import {
  GUIDED_STEPS,
  loadGuidedFixture,
  loadShowcaseReferencePerformance,
  nextGuidedStep,
  stepFromState,
} from "./showcaseFixtures";

export const DemoRuntimeContext = createContext<DemoRuntimeValue | null>(null);

const SHOWCASE_LABELS: DemoLabels = {
  productSubtitle: "Public Interactive Showcase",
  statusPrefix: "recorded",
  disclosure:
    "Replays recorded states from a verified local 3-node execution of the real C++ storage system.",
  localDemoHint: "Run the repository locally to interact with the real C++ system.",
  localDemoUrl:
    "https://github.com/simonwen7/ai-artifact-store/blob/main/demo/README.md",
  metricNodesLabel: "3-node recorded demo",
  chunkPlacementNote:
    "Captured from the verified local execution; placement is not recomputed in the browser.",
  guidedCompleteBody:
    "You replayed push, failure, verified pull, and repair from a recorded local 3-node run of the production CLI.",
  lifecycleIntro:
    "Semantic retirement is separate from physical reclamation. Available in the real local demo.",
  waitingForController: "Showcase data unavailable",
};

function showcaseCapabilities(): DemoCapabilities {
  return {
    mutateCluster: false,
    mutateLifecycle: false,
    uploadArtifact: false,
    downloadRestore: false,
    pollState: false,
    showLifecycleTab: false,
    scenarioExplorer: true,
  };
}

function showcaseTabs(): { id: AppTab; label: string }[] {
  return [
    { id: "cluster", label: "Cluster Demo" },
    { id: "chunking", label: "Chunking Lab" },
    { id: "performance", label: "Performance" },
  ];
}

class ShowcaseError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ShowcaseError";
  }
}

function formatError(err: unknown): string {
  if (err instanceof Error) return err.message;
  return "Unexpected error";
}

function unsupportedShowcase(action: string): never {
  throw new ShowcaseError(
    `${action} is not available in the public showcase.`,
  );
}

export { ShowcaseDemoProvider as LocalOrShowcaseProvider };

export function ShowcaseDemoProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<DemoState | null>(null);
  const stateRef = useRef<DemoState | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const [actionNote, setActionNote] = useState<string | null>(null);
  const [referencePerformance, setReferencePerformance] =
    useState<ReferencePerformance | null>(null);
  const [referenceError, setReferenceError] = useState<string | null>(null);
  const [referenceLoading, setReferenceLoading] = useState(true);

  const setShowcaseState = useCallback((next: DemoState) => {
    stateRef.current = next;
    setState(next);
  }, []);

  useEffect(() => {
    let cancelled = false;
    void loadGuidedFixture("READY")
      .then((next) => {
        if (!cancelled) {
          setShowcaseState(next);
          setLoadError(null);
        }
      })
      .catch((err) => {
        if (!cancelled) setLoadError(formatError(err));
      });
    return () => {
      cancelled = true;
    };
  }, [setShowcaseState]);

  useEffect(() => {
    let cancelled = false;
    setReferenceLoading(true);
    void loadShowcaseReferencePerformance()
      .then((ref) => {
        if (!cancelled) {
          setReferencePerformance(ref);
          setReferenceError(null);
        }
      })
      .catch((err) => {
        if (!cancelled) setReferenceError(formatError(err));
      })
      .finally(() => {
        if (!cancelled) setReferenceLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const applyFixture = useCallback(
    async (step: GuidedStepName) => {
      const next = await loadGuidedFixture(step);
      setShowcaseState(next);
      return next;
    },
    [setShowcaseState],
  );

  const runMutation = useCallback(
    async (fn: () => Promise<DemoState>, note?: string) => {
      if (busy) return;
      setBusy(true);
      setActionError(null);
      setActionNote(note ?? null);
      try {
        const next = await fn();
        setShowcaseState(next);
        setActionNote(null);
      } catch (err) {
        setActionError(formatError(err));
        setActionNote(null);
      } finally {
        setBusy(false);
      }
    },
    [busy, setShowcaseState],
  );

  const actions: DemoActions = useMemo(() => {
    const advanceFrom = async (expected: GuidedStepName) => {
      const current = stepFromState(stateRef.current);
      if (current !== expected) {
        throw new ShowcaseError(
          `Expected ${expected}, current step is ${current}.`,
        );
      }
      const next = nextGuidedStep(current);
      if (!next) {
        throw new ShowcaseError("Guided sequence is complete.");
      }
      return applyFixture(next);
    };

    return {
      guidedPush: () => advanceFrom("READY"),
      guidedKill: () => advanceFrom("PUSHED"),
      guidedPull: () => advanceFrom("NODE_FAILED"),
      guidedRepair: () => advanceFrom("PULL_VERIFIED"),
      guidedComplete: () => advanceFrom("REPAIRED"),
      resetDemo: () => applyFixture("READY"),
      pullArtifact: () => unsupportedShowcase("Pull"),
      repairArtifact: () => unsupportedShowcase("Repair"),
      killNode: (_nodeId: string) => unsupportedShowcase("Kill node"),
      restartNode: (_nodeId: string) => unsupportedShowcase("Restart node"),
      setNodeState: (_nodeId: string, _state: "active" | "draining" | "disabled") =>
        unsupportedShowcase("Set node state"),
      pushArtifact: async (
        _body: ArrayBuffer | Blob | Uint8Array,
        _params: {
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
      ) => unsupportedShowcase("Push artifact"),
      lifecycleSetup: () => unsupportedShowcase("Lifecycle setup"),
      lifecycleDryRun: () => unsupportedShowcase("Lifecycle dry-run"),
      lifecycleApply: () => unsupportedShowcase("Lifecycle apply"),
      lifecycleGc: () => unsupportedShowcase("Lifecycle GC"),
      getRestoredUrl: () => "#",
      selectScenario: async (step: GuidedStepName) => {
        if (!(GUIDED_STEPS as string[]).includes(step)) return;
        setBusy(true);
        setActionError(null);
        try {
          await applyFixture(step);
        } catch (err) {
          setActionError(formatError(err));
        } finally {
          setBusy(false);
        }
      },
    };
  }, [applyFixture]);

  const value: DemoRuntimeValue = {
    mode: "showcase",
    capabilities: showcaseCapabilities(),
    labels: SHOWCASE_LABELS,
    tabs: showcaseTabs(),
    state,
    busy,
    locked: busy,
    pollError: loadError,
    actionError,
    actionNote,
    dismissActionError: () => setActionError(null),
    referencePerformance,
    referenceError,
    referenceLoading,
    runMutation,
    actions,
  };

  return (
    <DemoRuntimeContext.Provider value={value}>{children}</DemoRuntimeContext.Provider>
  );
}

export function useDemoRuntime(): DemoRuntimeValue {
  const ctx = useContext(DemoRuntimeContext);
  if (!ctx) {
    throw new Error("useDemoRuntime must be used within DemoRuntimeProvider");
  }
  return ctx;
}
