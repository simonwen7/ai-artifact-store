import { useEffect, useMemo, useState, type ReactNode } from "react";
import type { DemoState } from "../types";
import {
  getReferencePerformance,
  guidedPush,
  pullArtifact,
  repairArtifact,
  resetDemo,
} from "../lib/api";
import GuidedDemo from "./GuidedDemo";
import NodeCard from "./NodeCard";
import ArtifactPanel from "./ArtifactPanel";
import ChunkPlacementTable from "./ChunkPlacementTable";
import OperationLog from "./OperationLog";

type Mode = "guided" | "explorer";

interface ClusterDemoProps {
  state: DemoState | null;
  busy: boolean;
  runMutation: (fn: () => Promise<DemoState>, note?: string) => Promise<void>;
}

export default function ClusterDemo({
  state,
  busy,
  runMutation,
}: ClusterDemoProps) {
  const [mode, setMode] = useState<Mode>("guided");
  const [fastcdcReuse, setFastcdcReuse] = useState<string>("—");

  useEffect(() => {
    let cancelled = false;
    void getReferencePerformance()
      .then((ref) => {
        if (cancelled) return;
        const ratio = ref.chunking?.fastcdc?.reuse_ratio;
        if (typeof ratio === "number") {
          setFastcdcReuse(`${(ratio * 100).toFixed(1)}%`);
        }
      })
      .catch(() => {
        /* keep placeholder */
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const nodes = useMemo(() => {
    const list = state?.cluster.nodes ?? [];
    return [...list].sort((a, b) => a.node_id.localeCompare(b.node_id));
  }, [state]);

  return (
    <div className="space-y-8">
      <section className="space-y-4">
        <div>
          <h2 className="max-w-xl text-3xl font-semibold tracking-tight text-mist-50 sm:text-4xl">
            Distributed storage,
            <br />
            built to survive failure.
          </h2>
          <p className="mt-3 text-base text-mist-400">
            Content-addressed. Versioned. Replicated. Recoverable.
          </p>
        </div>

        {mode === "guided" && guidedStep(state) === "READY" && (
          <button
            type="button"
            className="btn-primary"
            disabled={busy || !state?.ready}
            onClick={() =>
              void runMutation(guidedPush, "Pushing guided demo artifact…")
            }
          >
            Start Guided Demo
          </button>
        )}
      </section>

      <section className="panel overflow-hidden">
        <div className="grid grid-cols-2 divide-x divide-white/[0.06] sm:grid-cols-4">
          <Metric value="415" label="Correctness tests" />
          <Metric value="3" label="Storage nodes" />
          <Metric value={fastcdcReuse} label="FastCDC reuse*" />
          <Metric value="6" label="Process-level E2Es" />
        </div>
        <p className="border-t border-white/[0.06] px-4 py-2 text-[11px] text-mist-500">
          * deterministic local reference benchmark
        </p>
      </section>

      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="inline-flex rounded-lg border border-white/10 bg-ink-900 p-1">
          <ModeButton active={mode === "guided"} onClick={() => setMode("guided")}>
            Guided
          </ModeButton>
          <ModeButton active={mode === "explorer"} onClick={() => setMode("explorer")}>
            Explorer
          </ModeButton>
        </div>
        {mode === "explorer" && (
          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              className="btn-secondary"
              disabled={busy || !state?.artifact}
              onClick={() =>
                void runMutation(pullArtifact, "Pulling artifact…")
              }
            >
              Pull Artifact
            </button>
            <button
              type="button"
              className="btn-secondary"
              disabled={busy || !state?.artifact}
              onClick={() =>
                void runMutation(repairArtifact, "Repairing replicas…")
              }
            >
              Repair Replicas
            </button>
            <button
              type="button"
              className="btn-danger"
              disabled={busy}
              onClick={() => void runMutation(resetDemo, "Resetting demo…")}
            >
              Reset Demo
            </button>
          </div>
        )}
      </div>

      {mode === "guided" && (
        <GuidedDemo state={state} busy={busy} runMutation={runMutation} onExplore={() => setMode("explorer")} />
      )}

      <section className="grid gap-4 md:grid-cols-3">
        {nodes.length === 0 ? (
          <EmptyCard text="Nodes appear when the demo cluster is ready." />
        ) : (
          nodes.map((node) => (
            <NodeCard
              key={node.node_id}
              node={node}
              busy={busy}
              explorer={mode === "explorer"}
              runMutation={runMutation}
            />
          ))
        )}
      </section>

      <ArtifactPanel
        state={state}
        busy={busy}
        explorer={mode === "explorer"}
        runMutation={runMutation}
      />

      <ChunkPlacementTable artifact={state?.artifact ?? null} />

      <OperationLog events={state?.events ?? []} />
    </div>
  );
}

function guidedStep(state: DemoState | null): string {
  return state?.guided?.step ?? "READY";
}

function Metric({ value, label }: { value: string; label: string }) {
  return (
    <div className="px-4 py-4">
      <div className="text-2xl font-semibold tracking-tight text-mist-50">{value}</div>
      <div className="mt-1 text-xs text-mist-500">{label}</div>
    </div>
  );
}

function ModeButton({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: ReactNode;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={[
        "rounded-md px-3 py-1.5 text-sm transition",
        active ? "bg-ink-700 text-mist-50" : "text-mist-400 hover:text-mist-100",
      ].join(" ")}
    >
      {children}
    </button>
  );
}

function EmptyCard({ text }: { text: string }) {
  return (
    <div className="panel col-span-full px-4 py-8 text-center text-sm text-mist-500">
      {text}
    </div>
  );
}
