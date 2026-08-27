import type { ArtifactState, ChunkLocation, ChunkPlacement } from "../types";

const NODE_COLS = ["node-a", "node-b", "node-c"] as const;

interface ChunkPlacementTableProps {
  artifact: ArtifactState | null;
}

export default function ChunkPlacementTable({ artifact }: ChunkPlacementTableProps) {
  const chunks = artifact?.chunks ?? [];

  return (
    <section className="panel overflow-hidden">
      <div className="border-b border-white/[0.06] px-5 py-4">
        <p className="label">Placement</p>
        <h3 className="mt-1 text-lg font-medium text-mist-50">Chunk placement</h3>
        <p className="mt-1 text-xs text-mist-500">
          From controller state — not recomputed in the browser.
        </p>
      </div>

      {chunks.length === 0 ? (
        <div className="px-5 py-10 text-center text-sm text-mist-500">
          Chunk locations appear after an artifact is pushed.
        </div>
      ) : (
        <div className="max-h-[28rem] overflow-auto">
          <table className="w-full min-w-[40rem] border-collapse text-left text-sm">
            <thead className="sticky top-0 bg-ink-850">
              <tr className="border-b border-white/[0.06] text-mist-500">
                <th className="px-4 py-3 font-medium">Chunk</th>
                <th className="px-4 py-3 font-medium">Size</th>
                <th className="px-4 py-3 font-medium">Node A</th>
                <th className="px-4 py-3 font-medium">Node B</th>
                <th className="px-4 py-3 font-medium">Node C</th>
              </tr>
            </thead>
            <tbody>
              {chunks.map((chunk) => (
                <tr
                  key={chunk.chunk_id}
                  className="border-b border-white/[0.04] transition hover:bg-white/[0.02]"
                >
                  <td className="mono px-4 py-2.5 text-mist-200" title={chunk.chunk_id}>
                    {truncateId(chunk.chunk_id)}
                  </td>
                  <td className="px-4 py-2.5 text-mist-300">
                    {formatSize(chunk.size_bytes)}
                  </td>
                  {NODE_COLS.map((nodeId) => (
                    <td key={nodeId} className="px-4 py-2.5">
                      <Cell chunk={chunk} nodeId={nodeId} />
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      <div className="border-t border-white/[0.06] px-5 py-3 text-[11px] text-mist-500">
        ● Available / online · ○ metadata available, process offline · – not placed · !
        missing/corrupt
      </div>
    </section>
  );
}

function Cell({ chunk, nodeId }: { chunk: ChunkPlacement; nodeId: string }) {
  const loc = chunk.locations.find((l) => l.node_id === nodeId);
  if (!loc) {
    const desired = chunk.desired_nodes?.includes(nodeId);
    return (
      <span className="text-mist-500" title={desired ? "Desired, not placed" : "Not placed"}>
        –
      </span>
    );
  }
  return <LocationMark loc={loc} />;
}

function LocationMark({ loc }: { loc: ChunkLocation }) {
  if (loc.metadata_state === "missing" || loc.metadata_state === "corrupt") {
    return (
      <span className="font-medium text-failure" title={`${loc.metadata_state}`}>
        !
      </span>
    );
  }
  if (loc.metadata_state === "available" && loc.process_status === "online") {
    return (
      <span className="text-healthy" title="Available / online">
        ●
      </span>
    );
  }
  if (loc.metadata_state === "available" && loc.process_status === "offline") {
    return (
      <span className="text-warning" title="Available metadata, process offline">
        ○
      </span>
    );
  }
  return <span className="text-mist-500">–</span>;
}

function truncateId(id: string): string {
  if (id.length <= 14) return id;
  return `${id.slice(0, 8)}…${id.slice(-4)}`;
}

function formatSize(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / (1024 * 1024)).toFixed(2)} MiB`;
}
