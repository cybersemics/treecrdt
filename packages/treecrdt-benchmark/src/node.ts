import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import type { BenchmarkResult } from "./index.js";
import { parseBenchCliArgs, type BenchCliArgs } from "./cli.js";

export { parseBenchCliArgs, type BenchCliArgs };

export type BenchmarkOutput = BenchmarkResult & {
  implementation: string;
  storage: string;
  workload: string;
  timestamp: string;
  env?: Record<string, unknown>;
  extra?: Record<string, unknown>;
  sourceFile?: string;
};

export async function writeResult(
  result: BenchmarkResult,
  opts: {
    implementation: string;
    storage: string;
    workload?: string;
    outFile: string;
    extra?: Record<string, unknown>;
  }
): Promise<BenchmarkOutput> {
  const mergedExtra =
    result.extra && opts.extra
      ? { ...result.extra, ...opts.extra }
      : result.extra ?? opts.extra;
  const workload = opts.workload ?? result.name;
  const payload: BenchmarkOutput = {
    implementation: opts.implementation,
    storage: opts.storage,
    workload,
    timestamp: new Date().toISOString(),
    env: {
      node: process.version,
      platform: process.platform,
      arch: process.arch,
      cpu: os.cpus()[0]?.model,
      cores: os.cpus().length,
    },
    ...result,
    extra: mergedExtra,
    sourceFile: (() => {
      const abs = path.resolve(opts.outFile);
      const parts = abs.split(path.sep);
      const idx = parts.lastIndexOf("benchmarks");
      return idx === -1 ? abs : parts.slice(idx).join(path.sep);
    })(),
  };
  await fs.mkdir(path.dirname(opts.outFile), { recursive: true });
  await fs.writeFile(opts.outFile, JSON.stringify(payload, null, 2), "utf-8");
  return payload;
}

export function dirnameFromImportMeta(importMetaUrl: string): string {
  return path.dirname(fileURLToPath(importMetaUrl));
}

export function repoRootFromImportMeta(importMetaUrl: string, levelsUp: number): string {
  if (!Number.isInteger(levelsUp) || levelsUp < 0) throw new Error(`invalid levelsUp: ${levelsUp}`);
  const dir = dirnameFromImportMeta(importMetaUrl);
  return path.resolve(dir, ...Array.from({ length: levelsUp }, () => ".."));
}

