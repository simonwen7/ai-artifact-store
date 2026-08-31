import type { DemoState, GuidedStepName, ReferencePerformance } from "../types";

export const GUIDED_STEPS: GuidedStepName[] = [
  "READY",
  "PUSHED",
  "NODE_FAILED",
  "PULL_VERIFIED",
  "REPAIRED",
  "COMPLETE",
];

export const GUIDED_FIXTURE_FILES: Record<GuidedStepName, string> = {
  READY: "/showcase/guided/guided-ready.json",
  PUSHED: "/showcase/guided/guided-pushed.json",
  NODE_FAILED: "/showcase/guided/guided-node-failed.json",
  PULL_VERIFIED: "/showcase/guided/guided-pull-verified.json",
  REPAIRED: "/showcase/guided/guided-repaired.json",
  COMPLETE: "/showcase/guided/guided-complete.json",
};

export const SCENARIO_LABELS: Record<GuidedStepName, string> = {
  READY: "Ready",
  PUSHED: "Pushed",
  NODE_FAILED: "Node Failure",
  PULL_VERIFIED: "Pull Verified",
  REPAIRED: "Repaired",
  COMPLETE: "Complete",
};

const fixtureCache = new Map<string, DemoState>();

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url, { headers: { Accept: "application/json" } });
  if (!response.ok) {
    throw new Error(`Failed to load showcase data (${response.status}): ${url}`);
  }
  return (await response.json()) as T;
}

export async function loadGuidedFixture(step: GuidedStepName): Promise<DemoState> {
  const path = GUIDED_FIXTURE_FILES[step];
  const cached = fixtureCache.get(path);
  if (cached) return structuredClone(cached);

  const state = await fetchJson<DemoState>(path);
  fixtureCache.set(path, state);
  return structuredClone(state);
}

export async function loadShowcaseReferencePerformance(): Promise<ReferencePerformance> {
  const [chunking, process] = await Promise.all([
    fetchJson<ReferencePerformance["chunking"]>(
      "/showcase/reference/m10_reference_chunking.json",
    ),
    fetchJson<ReferencePerformance["process"]>(
      "/showcase/reference/m10_reference_process.json",
    ),
  ]);
  return { chunking, process };
}

export function stepFromState(state: DemoState | null): GuidedStepName {
  const raw = (state?.guided?.step ?? "READY").toUpperCase();
  if ((GUIDED_STEPS as string[]).includes(raw)) return raw as GuidedStepName;
  return "READY";
}

export function nextGuidedStep(current: GuidedStepName): GuidedStepName | null {
  const index = GUIDED_STEPS.indexOf(current);
  if (index < 0 || index >= GUIDED_STEPS.length - 1) return null;
  return GUIDED_STEPS[index + 1];
}
