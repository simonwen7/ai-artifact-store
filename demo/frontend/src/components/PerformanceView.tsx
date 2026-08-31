import { useDemoRuntime } from "../lib/demoRuntime";
import type { ReferencePerformance } from "../types";

const METHODOLOGY_URL =
  "https://github.com/simonwen7/ai-artifact-store/blob/main/docs/performance.md";

export default function PerformanceView() {
  const { referencePerformance, referenceError, referenceLoading } =
    useDemoRuntime();
  const chunking = referencePerformance?.chunking;
  const process = referencePerformance?.process;
  const scenarios = process?.scenarios;

  return (
    <div className="space-y-8">
      <section className="flex flex-wrap items-end justify-between gap-4">
        <div>
          <h2 className="text-2xl font-semibold tracking-tight text-mist-50">
            Performance
          </h2>
          <p className="mt-2 max-w-2xl text-sm text-mist-400">
            Committed M10 localhost Release reference measurements. No live benchmark
            runs from this page.
          </p>
        </div>
        <a
          href={METHODOLOGY_URL}
          target="_blank"
          rel="noreferrer"
          className="btn-secondary"
        >
          View methodology
        </a>
      </section>

      <div className="rounded-xl border border-accent/25 bg-accent-muted px-4 py-3 text-sm text-mist-100">
        Reference localhost Release measurements. Illustrative only — not a production
        SLA.
      </div>

      {referenceLoading && (
        <div className="panel px-5 py-8 text-sm text-mist-400">
          Loading reference JSON…
        </div>
      )}
      {referenceError && (
        <div className="rounded-lg border border-failure/35 bg-failure/10 px-4 py-3 text-sm text-[#f0c0c0]">
          {referenceError}
        </div>
      )}

      {chunking && scenarios && (
        <ScenarioCards chunking={chunking} scenarios={scenarios} />
      )}
    </div>
  );
}

function ScenarioCards({
  chunking,
  scenarios,
}: {
  chunking: ReferencePerformance["chunking"];
  scenarios: NonNullable<ReferencePerformance["process"]["scenarios"]>;
}) {
  return (
    <section className="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
      <Card
        title="FixedSize chunk + hash"
        value={`${chunking.fixed_size.median_mib_per_second.toFixed(1)} MiB/s`}
        hint="Logical throughput"
      />
      <Card
        title="FastCDC chunk + hash"
        value={`${chunking.fastcdc.median_mib_per_second.toFixed(1)} MiB/s`}
        hint="Logical throughput"
      />
      <Card
        title="FixedSize shifted reuse"
        value={`${(chunking.fixed_size.reuse_ratio * 100).toFixed(1)}%`}
      />
      <Card
        title="FastCDC shifted reuse"
        value={`${(chunking.fastcdc.reuse_ratio * 100).toFixed(1)}%`}
      />
      <Card
        title="Cold RF1 Push"
        value={`${scenarios.cold_push_rf1.logical_mib_per_second.toFixed(1)} MiB/s`}
        hint="Logical throughput"
      />
      <Card
        title="Warm dedup Push"
        value={`${scenarios.warm_dedup_push_rf1.logical_mib_per_second.toFixed(1)} MiB/s`}
        hint={warmHint(scenarios.warm_dedup_push_rf1)}
      />
      <Card
        title="RF1 Pull"
        value={`${scenarios.pull_rf1.logical_mib_per_second.toFixed(1)} MiB/s`}
        hint="Logical throughput"
      />
      <Card
        title="Cold RF2 Push"
        value={`${scenarios.cold_push_rf2.logical_mib_per_second.toFixed(1)} MiB/s`}
        hint="Logical throughput"
      />
      <Card
        title="RF2 Pull"
        value={`${scenarios.pull_rf2.logical_mib_per_second.toFixed(1)} MiB/s`}
        hint="Logical throughput"
      />
      <Card
        title="RF2 storage-byte amplification"
        value={String(scenarios.cold_push_rf2.storage_byte_amplification ?? "—")}
        hint="Physical / network bytes vs logical"
      />
      <Card
        title="Warm network bytes avoided"
        value={formatAvoided(scenarios.warm_dedup_push_rf1)}
        hint="Physical / network bytes"
      />
    </section>
  );
}

function Card({
  title,
  value,
  hint,
}: {
  title: string;
  value: string;
  hint?: string;
}) {
  return (
    <div className="panel p-4">
      <p className="label">{title}</p>
      <div className="mt-3 text-2xl font-semibold tracking-tight text-mist-50">
        {value}
      </div>
      {hint && <p className="mt-2 text-[11px] text-mist-500">{hint}</p>}
    </div>
  );
}

function warmHint(scenario: { cli?: Record<string, unknown> }): string {
  const sent = scenario.cli?.bytes_sent_to_storage;
  if (sent === 0) return "0 bytes sent to storage";
  return "Logical throughput";
}

function formatAvoided(scenario: { cli?: Record<string, unknown> }): string {
  const avoided = scenario.cli?.network_bytes_avoided;
  if (typeof avoided === "number") {
    return `${(avoided / (1024 * 1024)).toFixed(0)} MiB`;
  }
  const sent = scenario.cli?.bytes_sent_to_storage;
  if (sent === 0) return "all logical bytes";
  return "—";
}
