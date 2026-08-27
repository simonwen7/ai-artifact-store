import type { DemoState, GuidedStepName } from "../types";
import {
  guidedComplete,
  guidedKill,
  guidedPull,
  guidedPush,
  guidedRepair,
  resetDemo,
} from "../lib/api";

const STEP_ORDER: GuidedStepName[] = [
  "READY",
  "PUSHED",
  "NODE_FAILED",
  "PULL_VERIFIED",
  "REPAIRED",
  "COMPLETE",
];

interface GuidedDemoProps {
  state: DemoState | null;
  busy: boolean;
  runMutation: (fn: () => Promise<DemoState>, note?: string) => Promise<void>;
  onExplore: () => void;
}

export default function GuidedDemo({
  state,
  busy,
  runMutation,
  onExplore,
}: GuidedDemoProps) {
  const step = normalizeStep(state?.guided?.step);
  const index = Math.max(0, STEP_ORDER.indexOf(step));
  const displayStep = Math.min(index + 1, 5);
  const killed = state?.guided?.killed_node_id;
  const showSummary = step === "REPAIRED" || step === "COMPLETE";

  return (
    <section className="panel overflow-hidden">
      <div className="border-b border-white/[0.06] px-5 py-4">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div>
            <p className="label">Guided Demo</p>
            <h3 className="mt-1 text-lg font-medium text-mist-50">
              {titleFor(step)}
            </h3>
          </div>
          <div className="text-sm text-mist-400">
            {step === "COMPLETE" ? "Complete" : `Step ${displayStep} of 5`}
          </div>
        </div>
        <StepRail current={index} />
      </div>

      <div className="space-y-4 px-5 py-5">
        <p className="text-sm leading-relaxed text-mist-300">{copyFor(step, killed)}</p>

        {step === "PULL_VERIFIED" && (
          <ul className="space-y-1 text-sm text-mist-300">
            <li className="flex gap-2">
              <span className="text-healthy">●</span>
              Artifact restored successfully.
            </li>
            <li className="flex gap-2">
              <span className="text-healthy">●</span>
              SHA-256 verified.
            </li>
            <li className="flex gap-2">
              <span className="text-failure">●</span>
              One StorageNode remains offline
              {killed ? ` (${killed})` : ""}.
            </li>
          </ul>
        )}

        {showSummary && <SummaryBlock state={state} />}

        <div className="flex flex-wrap gap-2 pt-1">
          {step === "READY" && (
            <button
              type="button"
              className="btn-primary"
              disabled={busy || !state?.ready}
              onClick={() =>
                void runMutation(guidedPush, "Pushing demo artifact (FastCDC, RF=2)…")
              }
            >
              Push Demo Artifact
            </button>
          )}
          {step === "PUSHED" && (
            <button
              type="button"
              className="btn-danger"
              disabled={busy}
              onClick={() =>
                void runMutation(guidedKill, "Simulating storage-node failure…")
              }
            >
              Simulate failure
            </button>
          )}
          {step === "NODE_FAILED" && (
            <button
              type="button"
              className="btn-primary"
              disabled={busy}
              onClick={() =>
                void runMutation(guidedPull, "Pulling while a replica is offline…")
              }
            >
              Pull while offline
            </button>
          )}
          {step === "PULL_VERIFIED" && (
            <button
              type="button"
              className="btn-primary"
              disabled={busy}
              onClick={() =>
                void runMutation(guidedRepair, "Repairing replicas…")
              }
            >
              Repair
            </button>
          )}
          {step === "REPAIRED" && (
            <button
              type="button"
              className="btn-primary"
              disabled={busy}
              onClick={() =>
                void runMutation(guidedComplete, "Finishing guided demo…")
              }
            >
              Finish Guided Demo
            </button>
          )}
          {step === "COMPLETE" && (
            <>
              <button
                type="button"
                className="btn-primary"
                disabled={busy}
                onClick={() => void runMutation(resetDemo, "Restarting demo…")}
              >
                Restart Demo
              </button>
              <button
                type="button"
                className="btn-secondary"
                disabled={busy}
                onClick={onExplore}
              >
                Explore Manually
              </button>
            </>
          )}
        </div>
      </div>
    </section>
  );
}

function normalizeStep(step: string | undefined): GuidedStepName {
  const value = (step ?? "READY").toUpperCase();
  if ((STEP_ORDER as string[]).includes(value)) return value as GuidedStepName;
  return "READY";
}

function titleFor(step: GuidedStepName): string {
  switch (step) {
    case "READY":
      return "Healthy three-node cluster";
    case "PUSHED":
      return "Artifact committed";
    case "NODE_FAILED":
      return "Storage node offline";
    case "PULL_VERIFIED":
      return "Pull verified under failure";
    case "REPAIRED":
      return "Distributed recovery complete";
    case "COMPLETE":
      return "Distributed recovery complete";
  }
}

function copyFor(step: GuidedStepName, killed: string | null | undefined): string {
  switch (step) {
    case "READY":
      return "Start with a healthy three-node cluster. All three nodes should already be online and Active.";
    case "PUSHED":
      return "The demo artifact is content-addressed and replicated. Next, simulate a storage-node failure without changing registry state — metadata belief and physical availability diverge.";
    case "NODE_FAILED":
      return `Process ${killed ?? "target node"} is offline while registry may still say Active. Restore the artifact from remaining reachable replicas.`;
    case "PULL_VERIFIED":
      return "Pull succeeded against a degraded cluster. Repair will disable the failed node for placement eligibility, then restore replication factor.";
    case "REPAIRED":
      return "Real Push, Pull, and Repair finished. Review the stats below, then finish the guided presentation.";
    case "COMPLETE":
      return "You exercised push, failure, verified pull, and repair against the real local system.";
  }
}

function StepRail({ current }: { current: number }) {
  return (
    <div className="mt-4 flex gap-1.5">
      {STEP_ORDER.slice(0, 5).map((_, i) => {
        const done = i < current || current >= 5;
        const active = i === current && current < 5;
        return (
          <div
            key={i}
            className={[
              "h-1 flex-1 rounded-full transition",
              done || active ? "bg-accent" : "bg-ink-700",
            ].join(" ")}
          />
        );
      })}
    </div>
  );
}

function SummaryBlock({ state }: { state: DemoState | null }) {
  const summary = state?.guided?.summary;
  const rows = [
    { key: "Push", value: summary?.push },
    { key: "Pull", value: summary?.pull },
    { key: "Repair", value: summary?.repair },
  ];

  return (
    <div className="grid gap-2 sm:grid-cols-3">
      {rows.map((row) => (
        <div
          key={row.key}
          className="rounded-lg border border-white/[0.06] bg-ink-900/60 px-3 py-3"
        >
          <div className="label">{row.key}</div>
          <div className="mt-2 text-sm text-mist-200">{formatStat(row.value)}</div>
        </div>
      ))}
    </div>
  );
}

function formatStat(value: Record<string, unknown> | null | undefined): string {
  if (!value) return "Completed";
  if (typeof value.message === "string") return value.message;
  const parts: string[] = [];
  if (typeof value.elapsed_seconds === "number") {
    parts.push(`${value.elapsed_seconds.toFixed(3)}s`);
  }
  if (typeof value.chunk_count === "number") {
    parts.push(`${value.chunk_count} chunks`);
  }
  if (typeof value.node_id === "string") {
    parts.push(value.node_id);
  }
  return parts.length > 0 ? parts.join(" · ") : "Completed";
}
