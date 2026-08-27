import { useCallback, useRef, useState, type DragEvent } from "react";
import type {
  ArtifactKind,
  ArtifactState,
  ChunkingStrategy,
  DemoState,
} from "../types";
import { getRestoredUrl, pushArtifact } from "../lib/api";

interface ArtifactPanelProps {
  state: DemoState | null;
  busy: boolean;
  explorer: boolean;
  runMutation: (fn: () => Promise<DemoState>, note?: string) => Promise<void>;
}

export default function ArtifactPanel({
  state,
  busy,
  explorer,
  runMutation,
}: ArtifactPanelProps) {
  const artifact = state?.artifact ?? null;
  const inputRef = useRef<HTMLInputElement>(null);
  const [file, setFile] = useState<File | null>(null);
  const [dragOver, setDragOver] = useState(false);
  const [chunking, setChunking] = useState<ChunkingStrategy>("fastcdc");
  const [rf, setRf] = useState<1 | 2 | 3>(2);
  const [kind, setKind] = useState<ArtifactKind>("model-checkpoint");

  const onDrop = useCallback((event: DragEvent) => {
    event.preventDefault();
    setDragOver(false);
    const next = event.dataTransfer.files?.[0];
    if (next) setFile(next);
  }, []);

  const push = () => {
    if (!file) return;
    void runMutation(async () => {
      const buffer = await file.arrayBuffer();
      return pushArtifact(buffer, {
        filename: file.name,
        chunking_strategy: chunking,
        replication_factor: rf,
        artifact_kind: kind,
      });
    }, `Pushing ${file.name}…`);
  };

  return (
    <section className="panel p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <p className="label">Artifact</p>
          <h3 className="mt-1 text-lg font-medium text-mist-50">
            {artifact ? artifact.filename : "No artifact yet"}
          </h3>
        </div>
        {artifact && (
          <HealthBadge health={artifact.health} />
        )}
      </div>

      {artifact ? (
        <div className="mt-5 space-y-4">
          <div className="grid gap-3 sm:grid-cols-2">
            <Field label="Size" value={formatBytes(artifact.size_bytes)} />
            <Field label="Chunk count" value={String(artifact.chunks?.length ?? 0)} />
            <Field label="Chunking" value={artifact.chunking_strategy} />
            <Field label="Replication factor" value={String(artifact.replication_factor)} />
            <Field label="Artifact kind" value={artifact.artifact_kind} />
            <Field label="Demo health view" value={artifact.health} />
          </div>
          <div className="grid gap-2">
            <IdRow label="Object ID" value={artifact.object_id} />
            <IdRow label="Layout ID" value={artifact.layout_id} />
            <IdRow label="Version ID" value={artifact.version_id} />
            <IdRow label="Artifact ID" value={artifact.artifact_id} />
          </div>
          <a
            className="btn-secondary inline-flex text-sm"
            href={getRestoredUrl()}
            target="_blank"
            rel="noreferrer"
          >
            Download last restore
          </a>
          <p className="text-[11px] text-mist-500">
            Demo health view — derived from placement + process reachability; not a
            persisted production health status.
          </p>
        </div>
      ) : (
        <p className="mt-3 text-sm text-mist-500">
          Push a guided artifact or upload in Explorer to populate identity and placement.
        </p>
      )}

      {explorer && (
        <div className="mt-6 space-y-4 border-t border-white/[0.06] pt-5">
          <div
            onDragOver={(e) => {
              e.preventDefault();
              setDragOver(true);
            }}
            onDragLeave={() => setDragOver(false)}
            onDrop={onDrop}
            className={[
              "rounded-xl border border-dashed px-4 py-8 text-center transition",
              dragOver
                ? "border-accent bg-accent-muted"
                : "border-white/15 bg-ink-900/50",
            ].join(" ")}
          >
            <p className="text-sm text-mist-300">
              {file ? file.name : "Drop a file here, or choose one"}
            </p>
            <button
              type="button"
              className="btn-secondary mt-3"
              onClick={() => inputRef.current?.click()}
            >
              Choose file
            </button>
            <input
              ref={inputRef}
              type="file"
              className="hidden"
              onChange={(e) => setFile(e.target.files?.[0] ?? null)}
            />
          </div>

          <div className="grid gap-3 sm:grid-cols-3">
            <label className="block text-sm">
              <span className="label">Chunking</span>
              <select
                className="mt-1 w-full rounded-lg border border-white/10 bg-ink-800 px-3 py-2 text-mist-100"
                value={chunking}
                onChange={(e) => setChunking(e.target.value as ChunkingStrategy)}
              >
                <option value="fixed-size">FixedSize</option>
                <option value="fastcdc">FastCDC</option>
              </select>
            </label>
            <label className="block text-sm">
              <span className="label">Replication factor</span>
              <select
                className="mt-1 w-full rounded-lg border border-white/10 bg-ink-800 px-3 py-2 text-mist-100"
                value={rf}
                onChange={(e) => setRf(Number(e.target.value) as 1 | 2 | 3)}
              >
                <option value={1}>1</option>
                <option value={2}>2</option>
                <option value={3}>3</option>
              </select>
            </label>
            <label className="block text-sm">
              <span className="label">Artifact kind</span>
              <select
                className="mt-1 w-full rounded-lg border border-white/10 bg-ink-800 px-3 py-2 text-mist-100"
                value={kind}
                onChange={(e) => setKind(e.target.value as ArtifactKind)}
              >
                <option value="generic">Generic</option>
                <option value="model-checkpoint">Model Checkpoint</option>
                <option value="dataset-snapshot">Dataset Snapshot</option>
                <option value="embedding-index">Embedding Index</option>
                <option value="evaluation-output">Evaluation Output</option>
              </select>
            </label>
          </div>

          <button
            type="button"
            className="btn-primary"
            disabled={busy || !file || !state?.ready}
            onClick={push}
          >
            Push Artifact
          </button>
        </div>
      )}
    </section>
  );
}

function HealthBadge({ health }: { health: ArtifactState["health"] }) {
  const tone =
    health === "healthy"
      ? "text-healthy bg-healthy/15 border-healthy/30"
      : health === "degraded"
        ? "text-warning bg-warning/15 border-warning/30"
        : "text-failure bg-failure/15 border-failure/30";
  return (
    <span className={`rounded-md border px-2.5 py-1 text-xs font-medium capitalize ${tone}`}>
      {health}
    </span>
  );
}

function Field({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg border border-white/[0.05] bg-ink-900/40 px-3 py-2">
      <div className="label">{label}</div>
      <div className="mt-1 text-sm text-mist-100">{value}</div>
    </div>
  );
}

function IdRow({ label, value }: { label: string; value: string }) {
  const [copied, setCopied] = useState(false);
  const short =
    value.length > 20 ? `${value.slice(0, 10)}…${value.slice(-8)}` : value;

  return (
    <div className="flex items-center justify-between gap-3 rounded-lg border border-white/[0.05] bg-ink-900/40 px-3 py-2">
      <div className="min-w-0">
        <div className="label">{label}</div>
        <div className="mono truncate text-mist-200" title={value}>
          {short}
        </div>
      </div>
      <button
        type="button"
        className="btn-ghost shrink-0 px-2 py-1 text-xs"
        onClick={async () => {
          try {
            await navigator.clipboard.writeText(value);
            setCopied(true);
            window.setTimeout(() => setCopied(false), 1200);
          } catch {
            /* ignore */
          }
        }}
      >
        {copied ? "Copied" : "Copy"}
      </button>
    </div>
  );
}

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  if (n < 1024 * 1024 * 1024) return `${(n / (1024 * 1024)).toFixed(2)} MiB`;
  return `${(n / (1024 * 1024 * 1024)).toFixed(2)} GiB`;
}
