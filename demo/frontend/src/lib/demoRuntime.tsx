import type { ReactNode } from "react";
import { getDemoMode } from "./demoMode";
import type { DemoRuntimeValue } from "./demoRuntimeTypes";

// Resolved at build time via vite alias (@aistore/demo-runtime-impl).
// Showcase builds point only at showcaseRuntime (no /api client).
// Local builds point only at localRuntime.
import {
  LocalOrShowcaseProvider,
  useDemoRuntime as useImplRuntime,
} from "@aistore/demo-runtime-impl";

export type {
  DemoActions,
  DemoCapabilities,
  DemoLabels,
  DemoRuntimeValue,
} from "./demoRuntimeTypes";

export function DemoRuntimeProvider({ children }: { children: ReactNode }) {
  // Mode is also baked into the Vite alias selection at build time.
  void getDemoMode();
  return <LocalOrShowcaseProvider>{children}</LocalOrShowcaseProvider>;
}

export function useDemoRuntime(): DemoRuntimeValue {
  return useImplRuntime();
}
