#!/usr/bin/env node
/**
 * Build the public showcase static bundle with VITE_DEMO_MODE=showcase.
 * Cross-platform (macOS / Linux / Vercel) without shell-specific env syntax.
 */
import { spawnSync } from "node:child_process";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const frontendRoot = resolve(__dirname, "..");
const env = { ...process.env, VITE_DEMO_MODE: "showcase" };

function run(command, args) {
  const result = spawnSync(command, args, {
    cwd: frontendRoot,
    stdio: "inherit",
    env,
    shell: process.platform === "win32",
  });
  if (result.status !== 0) {
    process.exit(result.status ?? 1);
  }
}

run(process.execPath, ["scripts/prepare-showcase.mjs"]);
run("npm", ["run", "build"]);
console.log("build:showcase OK");
