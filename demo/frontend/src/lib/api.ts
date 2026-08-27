import type {
  ApiErrorBody,
  DemoState,
  HealthResponse,
  MutationResult,
  PushArtifactParams,
  ReferencePerformance,
  RegistryState,
} from "../types";

export class ApiError extends Error {
  readonly status: number;
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
  }
}

function unwrapState(payload: MutationResult): DemoState {
  if (payload && typeof payload === "object" && "ready" in payload && "cluster" in payload) {
    return payload as DemoState;
  }
  if (
    payload &&
    typeof payload === "object" &&
    "state" in payload &&
    payload.state &&
    typeof payload.state === "object"
  ) {
    return payload.state as DemoState;
  }
  throw new ApiError(500, "invalid_response", "Unexpected response shape from demo controller.");
}

async function parseError(response: Response): Promise<ApiError> {
  let code = `http_${response.status}`;
  let message = response.statusText || "Request failed";
  try {
    const body = (await response.json()) as ApiErrorBody;
    if (body?.error) code = body.error;
    if (body?.message) message = body.message;
    else if (body?.error === "demo_operation_in_progress") {
      message = "Another demo operation is already in progress.";
    }
  } catch {
    // keep defaults
  }
  return new ApiError(response.status, code, message);
}

async function requestJson<T>(
  path: string,
  init?: RequestInit,
): Promise<T> {
  const response = await fetch(path, {
    ...init,
    headers: {
      Accept: "application/json",
      ...(init?.headers ?? {}),
    },
  });
  if (!response.ok) {
    throw await parseError(response);
  }
  if (response.status === 204) {
    return undefined as T;
  }
  return (await response.json()) as T;
}

async function mutate(path: string, init?: RequestInit): Promise<DemoState> {
  const payload = await requestJson<MutationResult>(path, init);
  return unwrapState(payload);
}

export async function getHealth(): Promise<HealthResponse> {
  return requestJson<HealthResponse>("/api/health");
}

export async function getState(): Promise<DemoState> {
  return requestJson<DemoState>("/api/state");
}

export async function getReferencePerformance(): Promise<ReferencePerformance> {
  return requestJson<ReferencePerformance>("/api/reference-performance");
}

export async function pushArtifact(
  body: ArrayBuffer | Blob | Uint8Array,
  params: PushArtifactParams,
): Promise<DemoState> {
  const query = new URLSearchParams({
    filename: params.filename,
    chunking_strategy: params.chunking_strategy,
    replication_factor: String(params.replication_factor),
    artifact_kind: params.artifact_kind,
  });
  return mutate(`/api/artifacts/push?${query.toString()}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/octet-stream",
    },
    body: body as BodyInit,
  });
}

export async function pullArtifact(): Promise<DemoState> {
  return mutate("/api/artifacts/pull", { method: "POST" });
}

export async function repairArtifact(): Promise<DemoState> {
  return mutate("/api/artifacts/repair", { method: "POST" });
}

export async function resetDemo(): Promise<DemoState> {
  return mutate("/api/demo/reset", { method: "POST" });
}

export async function killNode(nodeId: string): Promise<DemoState> {
  return mutate(`/api/nodes/${encodeURIComponent(nodeId)}/kill`, {
    method: "POST",
  });
}

export async function restartNode(nodeId: string): Promise<DemoState> {
  return mutate(`/api/nodes/${encodeURIComponent(nodeId)}/restart`, {
    method: "POST",
  });
}

export async function setNodeState(
  nodeId: string,
  state: RegistryState,
): Promise<DemoState> {
  return mutate(`/api/nodes/${encodeURIComponent(nodeId)}/state`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ state }),
  });
}

export async function guidedPush(): Promise<DemoState> {
  return mutate("/api/guided/push", { method: "POST" });
}

export async function guidedKill(): Promise<DemoState> {
  return mutate("/api/guided/kill", { method: "POST" });
}

export async function guidedPull(): Promise<DemoState> {
  return mutate("/api/guided/pull", { method: "POST" });
}

export async function guidedRepair(): Promise<DemoState> {
  return mutate("/api/guided/repair", { method: "POST" });
}

export async function guidedComplete(): Promise<DemoState> {
  return mutate("/api/guided/complete", { method: "POST" });
}

export async function lifecycleSetup(): Promise<DemoState> {
  return mutate("/api/lifecycle/setup", { method: "POST" });
}

export async function lifecycleDryRun(): Promise<DemoState> {
  return mutate("/api/lifecycle/dry-run", { method: "POST" });
}

export async function lifecycleApply(): Promise<DemoState> {
  return mutate("/api/lifecycle/apply", { method: "POST" });
}

export async function lifecycleGc(): Promise<DemoState> {
  return mutate("/api/lifecycle/gc", { method: "POST" });
}

export function getRestoredUrl(): string {
  return "/api/artifacts/restored";
}

export function formatSafeError(err: unknown): string {
  if (err instanceof ApiError) return err.message;
  if (err instanceof Error) return err.message;
  return "Unexpected error";
}
