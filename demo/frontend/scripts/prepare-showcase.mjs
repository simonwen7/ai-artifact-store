#!/usr/bin/env node
/**
 * Copy committed M10 reference JSON into public/showcase/reference/
 * for the static showcase build. Source of truth remains benchmarks/results/.
 */
import { copyFileSync, mkdirSync, existsSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const frontendRoot = resolve(__dirname, "..");
const repoRoot = resolve(frontendRoot, "../..");
const srcDir = join(repoRoot, "benchmarks", "results");
const destDir = join(frontendRoot, "public", "showcase", "reference");

const files = [
  "m10_reference_chunking.json",
  "m10_reference_process.json",
];

mkdirSync(destDir, { recursive: true });

for (const name of files) {
  const src = join(srcDir, name);
  const dest = join(destDir, name);
  if (!existsSync(src)) {
    console.error(`Missing benchmark source: ${src}`);
    process.exit(1);
  }
  copyFileSync(src, dest);
  console.log(`Copied ${name} → public/showcase/reference/`);
}

console.log("prepare:showcase OK");
