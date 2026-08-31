import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import type { AppTab, DemoState, ReferencePerformance } from "../types";
import * as api from "./api";
import type {
  DemoActions,
  DemoCapabilities,
  DemoLabels,
  DemoRuntimeValue,
} from "./demoRuntimeTypes";

export const DemoRuntimeContext = createContext<DemoRuntimeValue | null>(null);

const LOCAL_LABELS: DemoLabels = {
  productSubtitle: "Interactive Distributed Systems Demo",
  statusPrefix: "live",
  disclosure: null,
  localDemoHint: null,
  localDemoUrl:
    "https://github.com/simonwen7/ai-artifact-store/blob/main/demo/README.md",
  metricNodesLabel: "Storage nodes",
  chunkPlacementNote: "From controller state — not recomputed in the browser.",
  guidedCompleteBody:
    "You exercised push, failure, verified pull, and repair against the real local system.",
  lifecycleIntro:
    "Semantic retirement is separate from physical reclamation. Pin, dry-run, apply, then GC — against the real local system.",
  waitingForController: "Waiting for demo controller at 127.0.0.1:8787",
};

function localCapabilities(): DemoCapabilities {
  return {
    mutateCluster: true,
    mutateLifecycle: true,
    uploadArtifact: true,
    downloadRestore: true,
    pollState: true,
    showLifecycleTab: true,
    scenarioExplorer: false,
  };
}

function localTabs(): { id: AppTab; label: string }[] {
  return [
    { id: "cluster", label: "Cluster Demo" },
    { id: "chunking", label: "Chunking Lab" },
    { id: "lifecycle", label: "Lifecycle" },
    { id: "performance", label: "Performance" },
  ];
}

export { LocalDemoProvider as LocalOrShowcaseProvider };

export function LocalDemoProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<DemoState | null>(null);
  const [pollError, setPollError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const [actionNote, setActionNote] = useState<string | null>(null);
  const [referencePerformance, setReferencePerformance] =
    useState<ReferencePerformance | null>(null);
  const [referenceError, setReferenceError] = useState<string | null>(null);
  const [referenceLoading, setReferenceLoading] = useState(true);

  useEffect(() => {
    let cancelled = false;
    let timer: number | undefined;

    const tick = async () => {
      try {
        const next = await api.getState();
        if (!cancelled) {
          setState(next);
          setPollError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setPollError(api.formatSafeError(err));
        }
      } finally {
        if (!cancelled) {
          timer = window.setTimeout(tick, 1000);
        }
      }
    };

    void tick();
    return () => {
      cancelled = true;
      if (timer !== undefined) window.clearTimeout(timer);
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    setReferenceLoading(true);
    void api
      .getReferencePerformance()
      .then((ref) => {
        if (!cancelled) {
          setReferencePerformance(ref);
          setReferenceError(null);
        }
      })
      .catch((err) => {
        if (!cancelled) setReferenceError(api.formatSafeError(err));
      })
      .finally(() => {
        if (!cancelled) setReferenceLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const runMutation = useCallback(
    async (fn: () => Promise<DemoState>, note?: string) => {
      if (busy) return;
      setBusy(true);
      setActionError(null);
      setActionNote(note ?? null);
      try {
        const next = await fn();
        setState(next);
        setActionNote(null);
      } catch (err) {
        setActionError(api.formatSafeError(err));
        setActionNote(null);
      } finally {
        setBusy(false);
      }
    },
    [busy],
  );

  const actions: DemoActions = useMemo(
    () => ({
      guidedPush: api.guidedPush,
      guidedKill: api.guidedKill,
      guidedPull: api.guidedPull,
      guidedRepair: api.guidedRepair,
      guidedComplete: api.guidedComplete,
      resetDemo: api.resetDemo,
      pullArtifact: api.pullArtifact,
      repairArtifact: api.repairArtifact,
      killNode: api.killNode,
      restartNode: api.restartNode,
      setNodeState: api.setNodeState,
      pushArtifact: api.pushArtifact,
      lifecycleSetup: api.lifecycleSetup,
      lifecycleDryRun: api.lifecycleDryRun,
      lifecycleApply: api.lifecycleApply,
      lifecycleGc: api.lifecycleGc,
      getRestoredUrl: api.getRestoredUrl,
      selectScenario: async () => {
        /* local explorer does not use scenario selection */
      },
    }),
    [],
  );

  const value: DemoRuntimeValue = {
    mode: "local",
    capabilities: localCapabilities(),
    labels: LOCAL_LABELS,
    tabs: localTabs(),
    state,
    busy,
    locked: busy || Boolean(state?.busy),
    pollError,
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
