export type DemoMode = "local" | "showcase";

/**
 * Centralized demo mode. Default is local so ./demo/start.sh is unchanged.
 * Set VITE_DEMO_MODE=showcase for the public static build.
 */
export function getDemoMode(): DemoMode {
  const raw = import.meta.env.VITE_DEMO_MODE;
  if (typeof raw === "string" && raw.trim().toLowerCase() === "showcase") {
    return "showcase";
  }
  return "local";
}

export function isShowcaseMode(): boolean {
  return getDemoMode() === "showcase";
}

export function isLocalMode(): boolean {
  return getDemoMode() === "local";
}
