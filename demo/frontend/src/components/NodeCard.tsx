import type { NodeState, RegistryState } from "../types";
import { useDemoRuntime } from "../lib/demoRuntime";

interface NodeCardProps {
  node: NodeState;
  busy: boolean;
  explorer: boolean;
}

export default function NodeCard({ node, busy, explorer }: NodeCardProps) {
  const { runMutation, actions } = useDemoRuntime();
  const title = displayName(node.node_id);
  const processTone =
    node.process_status === "online" ? "healthy" : "failure";
  const registryTone = registryToneOf(node.registry_state);

  return (
    <article className="panel flex flex-col gap-4 p-4 transition hover:border-white/10">
      <div className="flex items-start justify-between gap-3">
        <div>
          <h3 className="text-base font-medium text-mist-50">{title}</h3>
          <p className="mono mt-1 text-mist-500">{node.node_id}</p>
        </div>
        <span className="rounded-md bg-ink-800 px-2 py-1 text-xs text-mist-400">
          :{node.port}
        </span>
      </div>

      <dl className="grid grid-cols-2 gap-3 text-sm">
        <div>
          <dt className="label">Process</dt>
          <dd className={`mt-1 font-medium ${statusText(processTone)}`}>
            <StatusDot tone={processTone} />
            {node.process_status.toUpperCase()}
          </dd>
        </div>
        <div>
          <dt className="label">Registry</dt>
          <dd className={`mt-1 font-medium ${statusText(registryTone)}`}>
            <StatusDot tone={registryTone} />
            {node.registry_state}
          </dd>
        </div>
        <div className="col-span-2">
          <dt className="label">Chunk count</dt>
          <dd className="mt-1 text-mist-100">{node.chunk_count}</dd>
        </div>
      </dl>

      {explorer && (
        <div className="mt-auto space-y-2 border-t border-white/[0.06] pt-3">
          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              className="btn-danger px-2.5 py-1.5 text-xs"
              disabled={busy || node.process_status === "offline"}
              onClick={() =>
                void runMutation(
                  () => actions.killNode(node.node_id),
                  `Killing ${node.node_id}…`,
                )
              }
            >
              Kill
            </button>
            <button
              type="button"
              className="btn-secondary px-2.5 py-1.5 text-xs"
              disabled={busy || node.process_status === "online"}
              onClick={() =>
                void runMutation(
                  () => actions.restartNode(node.node_id),
                  `Restarting ${node.node_id}…`,
                )
              }
            >
              Restart
            </button>
          </div>
          <div className="flex flex-wrap gap-1.5">
            {(["active", "draining", "disabled"] as RegistryState[]).map((s) => (
              <button
                key={s}
                type="button"
                className={[
                  "rounded-md px-2 py-1 text-[11px] capitalize transition",
                  node.registry_state === s
                    ? "bg-accent-muted text-accent"
                    : "bg-ink-800 text-mist-400 hover:text-mist-100",
                ].join(" ")}
                disabled={busy || node.registry_state === s}
                onClick={() =>
                  void runMutation(
                    () => actions.setNodeState(node.node_id, s),
                    `Setting ${node.node_id} → ${s}…`,
                  )
                }
              >
                {s}
              </button>
            ))}
          </div>
        </div>
      )}
    </article>
  );
}

function displayName(nodeId: string): string {
  const suffix = nodeId.replace(/^node-/, "");
  return `Node ${suffix.toUpperCase()}`;
}

function registryToneOf(state: RegistryState): "healthy" | "warning" | "muted" {
  if (state === "active") return "healthy";
  if (state === "draining") return "warning";
  return "muted";
}

function statusText(tone: "healthy" | "warning" | "failure" | "muted"): string {
  switch (tone) {
    case "healthy":
      return "text-healthy";
    case "warning":
      return "text-warning";
    case "failure":
      return "text-failure";
    default:
      return "text-mist-500";
  }
}

function StatusDot({
  tone,
}: {
  tone: "healthy" | "warning" | "failure" | "muted";
}) {
  const cls =
    tone === "healthy"
      ? "bg-healthy"
      : tone === "warning"
        ? "bg-warning"
        : tone === "failure"
          ? "bg-failure"
          : "bg-mist-500";
  return (
    <span className={`mr-1.5 inline-block h-1.5 w-1.5 rounded-full ${cls}`} />
  );
}
