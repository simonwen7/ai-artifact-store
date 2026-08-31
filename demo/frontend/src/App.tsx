import { useState } from "react";
import Header from "./components/Header";
import ClusterDemo from "./components/ClusterDemo";
import ChunkingLab from "./components/ChunkingLab";
import LifecycleDemo from "./components/LifecycleDemo";
import PerformanceView from "./components/PerformanceView";
import { DemoRuntimeProvider, useDemoRuntime } from "./lib/demoRuntime";
import type { AppTab } from "./types";

export type { AppTab };

function AppShell() {
  const {
    tabs,
    capabilities,
    labels,
    state,
    locked,
    pollError,
    actionError,
    actionNote,
    dismissActionError,
  } = useDemoRuntime();

  const defaultTab = tabs[0]?.id ?? "cluster";
  const [tab, setTab] = useState<AppTab>(defaultTab);
  const activeTab = tabs.some((t) => t.id === tab) ? tab : defaultTab;

  return (
    <div className="min-h-screen">
      <Header />

      <main className="mx-auto max-w-6xl px-4 pb-16 pt-6 sm:px-6">
        {labels.disclosure && (
          <div className="mb-6 rounded-xl border border-accent/20 bg-accent-muted/60 px-4 py-3">
            <p className="text-sm font-medium text-mist-100">
              Public Interactive Showcase
            </p>
            <p className="mt-1 text-sm text-mist-300">{labels.disclosure}</p>
            {labels.localDemoHint && (
              <p className="mt-2 text-xs text-mist-500">
                {labels.localDemoHint}{" "}
                <a
                  href={labels.localDemoUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="text-accent underline-offset-2 hover:underline"
                >
                  Local demo instructions
                </a>
              </p>
            )}
          </div>
        )}

        <nav
          className="mb-8 flex flex-wrap gap-1 rounded-xl border border-white/[0.06] bg-ink-900/80 p-1"
          aria-label="Demo sections"
        >
          {tabs.map((item) => {
            const active = activeTab === item.id;
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

        {(actionError || actionNote || (pollError && !state)) && (
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
                  onClick={dismissActionError}
                >
                  Dismiss
                </button>
              </div>
            )}
            {pollError && !state && (
              <div className="rounded-lg border border-warning/35 bg-warning/10 px-4 py-3 text-sm text-[#f0d9a8]">
                {labels.waitingForController} — {pollError}
              </div>
            )}
          </div>
        )}

        {activeTab === "cluster" && <ClusterDemo busy={locked} />}
        {activeTab === "chunking" && <ChunkingLab />}
        {activeTab === "lifecycle" && capabilities.showLifecycleTab && (
          <LifecycleDemo busy={locked} />
        )}
        {activeTab === "performance" && <PerformanceView />}
      </main>
    </div>
  );
}

export default function App() {
  return (
    <DemoRuntimeProvider>
      <AppShell />
    </DemoRuntimeProvider>
  );
}
