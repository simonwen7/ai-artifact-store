/// Ambient module for the Vite-resolved runtime implementation alias.
declare module "@aistore/demo-runtime-impl" {
  import type { ReactNode } from "react";
  import type { DemoRuntimeValue } from "./demoRuntimeTypes";

  export function LocalOrShowcaseProvider(props: {
    children: ReactNode;
  }): JSX.Element;

  export function useDemoRuntime(): DemoRuntimeValue;
}
