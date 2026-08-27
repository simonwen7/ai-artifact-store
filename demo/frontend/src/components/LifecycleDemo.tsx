import type { DemoState, LifecycleVersionState } from "../types";
import {
  lifecycleApply,
  lifecycleDryRun,
  lifecycleGc,
  lifecycleSetup,
} from "../lib/api";

interface LifecycleDemoProps {
  state: DemoState | null;
  busy: boolean;
  runMutation: (fn: () => Promise<DemoState>, note?: string) => Promise<void>;
}

export default function LifecycleDemo({
  state,
  busy,
  runMutation,
}: LifecycleDemoProps) {
  const lifecycle = state?.lifecycle;
  const initialized = Boolean(lifecycle?.initialized);
  const versions = lifecycle?.versions ?? [];

  return (
    <div className="space-y-8">
      <section>
        <h2 className="text-2xl font-semibold tracking-tight text-mist-50">
          Lifecycle
        </h2>
        <p className="mt-2 max-w-2xl text-sm text-mist-400">
          Semantic retirement is separate from physical reclamation. Pin, dry-run,
          apply, then GC — against the real local system.
        </p>
      </section>

      <section className="panel p-5">
        {!initialized ? (
          <div className="space-y-4 py-4 text-center">
            <p className="text-sm text-mist-400">Not initialized.</p>
            <button
              type="button"
              className="btn-primary"
              disabled={busy || !state?.ready}
              onClick={() =>
                void runMutation(
                  lifecycleSetup,
                  "Initializing lifecycle scenario…",
                )
              }
            >
              Initialize Lifecycle Scenario
            </button>
          </div>
        ) : (
          <div className="space-y-6">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <p className="label">Policy</p>
                <p className="mono mt-1 text-mist-300">
                  {lifecycle?.policy_id
                    ? truncate(lifecycle.policy_id)
                    : "demo policy"}
                </p>
              </div>
              <div className="flex flex-wrap gap-2">
                <button
                  type="button"
                  className="btn-secondary"
                  disabled={busy}
                  onClick={() =>
                    void runMutation(lifecycleDryRun, "Running lifecycle dry-run…")
                  }
                >
                  Dry run
                </button>
                <button
                  type="button"
                  className="btn-secondary"
                  disabled={busy}
                  onClick={() =>
                    void runMutation(lifecycleApply, "Applying lifecycle…")
                  }
                >
                  Apply
                </button>
                <button
                  type="button"
                  className="btn-primary"
                  disabled={busy}
                  onClick={() =>
                    void runMutation(lifecycleGc, "Running GC…")
                  }
                >
                  GC
                </button>
              </div>
            </div>

            <Timeline versions={versions} />

            <FlowLegend />

            <p className="text-xs text-mist-500">
              ArtifactVersion history preserved after semantic retirement and GC.
            </p>
          </div>
        )}
      </section>
    </div>
  );
}

function Timeline({ versions }: { versions: LifecycleVersionState[] }) {
  const ordered = ["v1", "v2", "v3"].map(
    (label) => versions.find((v) => v.label === label) ?? null,
  );

  return (
    <div className="grid gap-3 md:grid-cols-3">
      {ordered.map((version, index) => {
        const label = `v${index + 1}`;
        const role =
          label === "v1" ? "Pinned" : label === "v2" ? "Old" : "Latest";
        return (
          <div
            key={label}
            className="relative rounded-xl border border-white/[0.06] bg-ink-900/50 p-4"
          >
            <div className="flex items-center justify-between">
              <span className="text-lg font-semibold text-mist-50">{label}</span>
              <span className="text-xs text-mist-500">{role}</span>
            </div>
            {version ? (
              <div className="mt-4 space-y-2">
                <Badge
                  label={String(version.semantic_status).toUpperCase()}
                  tone={
                    version.semantic_status === "retired" ? "warning" : "healthy"
                  }
                />
                {version.pin && <Badge label="PINNED" tone="accent" />}
                {version.last_decision && (
                  <Badge
                    label={`${String(version.last_decision).toUpperCase()}${
                      version.reason ? ` / ${version.reason}` : ""
                    }`}
                    tone="muted"
                  />
                )}
                <Badge
                  label={`Physical: ${String(version.physical_representation).toUpperCase()}`}
                  tone={
                    version.physical_representation === "reclaimed"
                      ? "failure"
                      : "muted"
                  }
                />
                <p className="mono truncate pt-1 text-[11px] text-mist-500" title={version.version_id}>
                  {truncate(version.version_id)}
                </p>
              </div>
            ) : (
              <p className="mt-4 text-sm text-mist-500">Awaiting setup</p>
            )}
          </div>
        );
      })}
    </div>
  );
}

function FlowLegend() {
  const steps = [
    "Policy",
    "Semantic retirement",
    "GC",
    "Representation reclamation",
  ];
  return (
    <div className="flex flex-wrap items-center gap-2 text-xs text-mist-400">
      {steps.map((step, i) => (
        <span key={step} className="inline-flex items-center gap-2">
          <span className="rounded-md border border-white/10 bg-ink-800 px-2 py-1">
            {step}
          </span>
          {i < steps.length - 1 && <span className="text-mist-600">↓</span>}
        </span>
      ))}
    </div>
  );
}

function Badge({
  label,
  tone,
}: {
  label: string;
  tone: "healthy" | "warning" | "failure" | "accent" | "muted";
}) {
  const cls =
    tone === "healthy"
      ? "border-healthy/30 bg-healthy/10 text-healthy"
      : tone === "warning"
        ? "border-warning/30 bg-warning/10 text-warning"
        : tone === "failure"
          ? "border-failure/30 bg-failure/10 text-failure"
          : tone === "accent"
            ? "border-accent/30 bg-accent-muted text-accent"
            : "border-white/10 bg-ink-800 text-mist-400";
  return (
    <span className={`inline-flex rounded-md border px-2 py-0.5 text-[11px] ${cls}`}>
      {label}
    </span>
  );
}

function truncate(id: string): string {
  if (id.length <= 18) return id;
  return `${id.slice(0, 10)}…${id.slice(-6)}`;
}
