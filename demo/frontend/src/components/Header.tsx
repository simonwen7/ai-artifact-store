import type { DemoState, NodeState } from "../types";

function clusterHealth(state: DemoState | null, pollError: string | null): {
  label: string;
  tone: "ok" | "warn" | "bad" | "idle";
} {
  if (!state) {
    return {
      label: pollError ? "Controller unreachable" : "Initializing",
      tone: pollError ? "bad" : "idle",
    };
  }
  if (!state.ready) {
    return { label: "Initializing", tone: "idle" };
  }

  const nodes = state.cluster.nodes ?? [];
  const metaOnline = state.cluster.metadata_service?.status === "online";
  const offline = nodes.filter((n) => n.process_status !== "online");
  const draining = nodes.filter((n) => n.registry_state === "draining");

  if (!metaOnline || offline.length === nodes.length) {
    return { label: "Cluster unavailable", tone: "bad" };
  }
  if (offline.length > 0 || draining.length > 0 || state.artifact?.health === "degraded") {
    return { label: "Degraded", tone: "warn" };
  }
  if (state.artifact?.health === "unavailable") {
    return { label: "Degraded", tone: "warn" };
  }
  return { label: "Demo Cluster Ready", tone: "ok" };
}

function toneClass(tone: "ok" | "warn" | "bad" | "idle"): string {
  switch (tone) {
    case "ok":
      return "text-healthy";
    case "warn":
      return "text-warning";
    case "bad":
      return "text-failure";
    default:
      return "text-mist-400";
  }
}

function pulseClass(tone: "ok" | "warn" | "bad" | "idle"): string {
  switch (tone) {
    case "ok":
      return "bg-healthy";
    case "warn":
      return "bg-warning";
    case "bad":
      return "bg-failure";
    default:
      return "bg-mist-500";
  }
}

interface HeaderProps {
  state: DemoState | null;
  pollError: string | null;
}

export default function Header({ state, pollError }: HeaderProps) {
  const health = clusterHealth(state, pollError);
  const nodeSummary = summarizeNodes(state?.cluster.nodes ?? []);

  return (
    <header className="border-b border-white/[0.06] bg-ink-900/70 backdrop-blur-sm">
      <div className="mx-auto flex max-w-6xl flex-col gap-4 px-4 py-5 sm:flex-row sm:items-end sm:justify-between sm:px-6">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight text-mist-50 sm:text-3xl">
            AI Artifact Store
          </h1>
          <p className="mt-1 text-sm text-mist-400">
            Interactive Distributed Systems Demo
          </p>
        </div>

        <div className="flex flex-col items-start gap-1 sm:items-end">
          <div className={`flex items-center gap-2 text-sm font-medium ${toneClass(health.tone)}`}>
            <span
              className={`inline-block h-2 w-2 rounded-full ${pulseClass(health.tone)} ${
                health.tone === "idle" ? "animate-pulse" : ""
              }`}
              aria-hidden
            />
            {health.label}
          </div>
          <p className="text-xs text-mist-500">
            {state?.ready
              ? `Metadata ${state.cluster.metadata_service.status} · ${nodeSummary}`
              : "Waiting for local demo controller"}
          </p>
        </div>
      </div>
    </header>
  );
}

function summarizeNodes(nodes: NodeState[]): string {
  if (nodes.length === 0) return "0 nodes";
  const online = nodes.filter((n) => n.process_status === "online").length;
  return `${online}/${nodes.length} nodes online`;
}
