import { useDemoRuntime } from "../lib/demoRuntime";
import type { ChunkingReference } from "../types";

export default function ChunkingLab() {
  const { referencePerformance, referenceError, referenceLoading } =
    useDemoRuntime();
  const chunking = referencePerformance?.chunking ?? null;

  return (
    <div className="space-y-8">
      <section>
        <h2 className="text-2xl font-semibold tracking-tight text-mist-50">
          Chunking Lab
        </h2>
        <p className="mt-2 max-w-2xl text-sm text-mist-400">
          FixedSize vs FastCDC under a content shift. Boundary blocks are illustrative;
          numeric cards come from committed M10 reference JSON.
        </p>
      </section>

      <section className="panel p-5">
        <p className="label">Illustrative chunk-boundary view</p>
        <div className="mt-4 space-y-6">
          <BoundaryRow
            title="FixedSize"
            original={[4, 4, 4, 4, 4, 4]}
            shifted={[1, 4, 4, 4, 4, 4, 3]}
            note="Boundaries shift with offsets · reference reuse 0%"
            accent="warning"
          />
          <BoundaryRow
            title="FastCDC"
            original={[3, 5, 4, 6, 4, 2]}
            shifted={[1, 3, 5, 4, 6, 4, 1]}
            note="Content-defined regions realign · high reference reuse"
            accent="healthy"
          />
        </div>
        <p className="mt-4 text-[11px] text-mist-500">
          Conceptual blocks only — not the exact 128 MiB benchmark chunk count.
        </p>
      </section>

      {referenceLoading && (
        <div className="panel px-5 py-8 text-sm text-mist-400">
          Loading reference measurements…
        </div>
      )}
      {referenceError && (
        <div className="rounded-lg border border-failure/35 bg-failure/10 px-4 py-3 text-sm text-[#f0c0c0]">
          {referenceError}
        </div>
      )}
      {chunking && <ReferenceCards chunking={chunking} />}

      <p className="text-sm text-mist-500">
        Reference local Release measurement. Not a production SLA.
      </p>
    </div>
  );
}

function BoundaryRow({
  title,
  original,
  shifted,
  note,
  accent,
}: {
  title: string;
  original: number[];
  shifted: number[];
  note: string;
  accent: "healthy" | "warning";
}) {
  return (
    <div>
      <div className="mb-2 flex items-baseline justify-between gap-3">
        <h3 className="text-sm font-medium text-mist-100">{title}</h3>
        <span className="text-xs text-mist-500">{note}</span>
      </div>
      <div className="space-y-2">
        <FileStrip label="Original File" widths={original} accent={accent} />
        <FileStrip
          label="Shifted File (+64 KiB prefix)"
          widths={shifted}
          accent={accent}
          shift
        />
      </div>
    </div>
  );
}

function FileStrip({
  label,
  widths,
  accent,
  shift,
}: {
  label: string;
  widths: number[];
  accent: "healthy" | "warning";
  shift?: boolean;
}) {
  const total = widths.reduce((a, b) => a + b, 0);
  return (
    <div>
      <div className="mb-1 text-[11px] text-mist-500">{label}</div>
      <div className="flex h-9 overflow-hidden rounded-md border border-white/10 bg-ink-900">
        {shift && (
          <div
            className="border-r border-dashed border-white/20 bg-white/[0.04]"
            style={{ width: `${(1 / (total + 1)) * 100}%` }}
            title="64 KiB prefix"
          />
        )}
        {widths.map((w, i) => (
          <div
            key={`${label}-${i}`}
            className={[
              "border-r border-ink-950/80 last:border-r-0",
              accent === "healthy" ? "bg-healthy/25" : "bg-warning/20",
              i % 2 === 0 ? "opacity-100" : "opacity-70",
            ].join(" ")}
            style={{ width: `${(w / total) * (shift ? 92 : 100)}%` }}
          />
        ))}
      </div>
    </div>
  );
}

function ReferenceCards({ chunking }: { chunking: ChunkingReference }) {
  const fixed = chunking.fixed_size;
  const fast = chunking.fastcdc;

  return (
    <section className="grid gap-4 sm:grid-cols-2">
      <MetricCard
        title="FixedSize"
        throughput={`${fixed.median_mib_per_second.toFixed(1)} MiB/s`}
        subtitle={`median ${fixed.median_elapsed_ms.toFixed(1)} ms · chunk + hash`}
        reuse={`${(fixed.reuse_ratio * 100).toFixed(1)}%`}
        reuseLabel="Shift reuse ratio"
      />
      <MetricCard
        title="FastCDC"
        throughput={`${fast.median_mib_per_second.toFixed(1)} MiB/s`}
        subtitle={`median ${fast.median_elapsed_ms.toFixed(1)} ms · chunk + hash`}
        reuse={`${(fast.reuse_ratio * 100).toFixed(1)}%`}
        reuseLabel="Shift reuse ratio"
      />
    </section>
  );
}

function MetricCard({
  title,
  throughput,
  subtitle,
  reuse,
  reuseLabel,
}: {
  title: string;
  throughput: string;
  subtitle: string;
  reuse: string;
  reuseLabel: string;
}) {
  return (
    <div className="panel p-5">
      <p className="label">{title}</p>
      <div className="mt-3 text-3xl font-semibold tracking-tight text-mist-50">
        {throughput}
      </div>
      <p className="mt-1 text-xs text-mist-500">{subtitle}</p>
      <div className="mt-5 border-t border-white/[0.06] pt-4">
        <div className="label">{reuseLabel}</div>
        <div className="mt-1 text-xl font-medium text-accent">{reuse}</div>
      </div>
    </div>
  );
}
