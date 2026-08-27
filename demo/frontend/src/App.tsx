import { useCallback, useEffect, useState } from "react";
import Header from "./components/Header";
import ClusterDemo from "./components/ClusterDemo";
import ChunkingLab from "./components/ChunkingLab";
import LifecycleDemo from "./components/LifecycleDemo";
import PerformanceView from "./components/PerformanceView";
import { formatSafeError, getState } from "./lib/api";
import type { DemoState } from "./types";

export type AppTab = "cluster" | "chunking" | "lifecycle" | "performance";

const TABS: { id: AppTab; label: string }[] = [
  { id: "cluster", label: "Cluster Demo" },
  { id: "chunking", label: "Chunking Lab" },
  { id: "lifecycle", label: "Lifecycle" },
  { id: "performance", label: "Performance" },
];

export default function App() {
  const [tab, setTab] = useState<AppTab>("cluster");
  const [state, setState] = useState<DemoState | null>(null);
  const [pollError, setPollError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);
  const [actionNote, setActionNote] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    let timer: number | undefined;

    const tick = async () => {
      try {
        const next = await getState();
        if (!cancelled) {
          setState(next);
          setPollError(null);
        }
      } catch (err) {
        if (!cancelled) {
          setPollError(formatSafeError(err));
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
        setActionError(formatSafeError(err));
        setActionNote(null);
      } finally {
        setBusy(false);
      }
    },
    [busy],
  );

  const locked = busy || Boolean(state?.busy);

  return (
    <div className="min-h-screen">
      <Header state={state} pollError={pollError} />

      <main className="mx-auto max-w-6xl px-4 pb-16 pt-6 sm:px-6">
        <nav
          className="mb-8 flex flex-wrap gap-1 rounded-xl border border-white/[0.06] bg-ink-900/80 p-1"
          aria-label="Demo sections"
        >
          {TABS.map((item) => {
            const active = tab === item.id;
            return (
              <button
                key={item.id}
                type="button"
                onClick={() => setTab(item.id)}
                className={[
                  "rounded-lg px-3.5 py-2 text-sm transition",
                  active
                    ? "bg-ink-700 text-mist-50 shadow-sm"
                    : "text-mist-400 hover:bg-white/[0.03] hover:text-mist-100",
                ].join(" ")}
              >
                {item.label}
              </button>
            );
          })}
        </nav>

        {(actionError || actionNote || pollError) && (
          <div className="mb-6 space-y-2">
            {actionNote && (
              <div className="rounded-lg border border-accent/30 bg-accent-muted px-4 py-3 text-sm text-mist-100">
                {actionNote}
              </div>
            )}
            {actionError && (
              <div className="flex items-start justify-between gap-3 rounded-lg border border-failure/35 bg-failure/10 px-4 py-3 text-sm text-[#f0c0c0]">
                <p>{actionError}</p>
                <button
                  type="button"
                  className="btn-ghost shrink-0 px-2 py-1 text-xs"
                  onClick={() => setActionError(null)}
                >
                  Dismiss
                </button>
              </div>
            )}
            {pollError && !state && (
              <div className="rounded-lg border border-warning/35 bg-warning/10 px-4 py-3 text-sm text-[#f0d9a8]">
                Waiting for demo controller at 127.0.0.1:8787 — {pollError}
              </div>
            )}
          </div>
        )}

        {tab === "cluster" && (
          <ClusterDemo
            state={state}
            busy={locked}
            runMutation={runMutation}
          />
        )}
        {tab === "chunking" && <ChunkingLab />}
        {tab === "lifecycle" && (
          <LifecycleDemo
            state={state}
            busy={locked}
            runMutation={runMutation}
          />
        )}
        {tab === "performance" && <PerformanceView />}
      </main>
    </div>
  );
}
