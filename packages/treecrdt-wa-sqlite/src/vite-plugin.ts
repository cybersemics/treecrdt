import fs from "node:fs/promises";
import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath } from "node:url";

type Plugin = {
  name: string;
  apply?: "serve" | "build" | ((...args: any[]) => boolean);
  configResolved?: () => void | Promise<void>;
  buildStart?: () => void | Promise<void>;
};

export type WaSqlitePluginOptions = {
  /** Destination directory (relative to project root) where wa-sqlite assets are copied. Defaults to public/wa-sqlite. */
  outDir?: string;
};

const defaultFiles = [
  "wa-sqlite.mjs",
  "wa-sqlite.wasm",
  "wa-sqlite-async.mjs",
  "wa-sqlite-async.wasm",
  "sqlite-api.js",
  "sqlite-constants.js",
];

/**
 * Vite plugin that copies the repo's vendored wa-sqlite artifacts into your app's public folder.
 * This removes the need for ad-hoc copy scripts in example apps.
 */
export function treecrdt(opts: WaSqlitePluginOptions = {}): Plugin {
  const outDir = opts.outDir ?? "public/wa-sqlite";
  const here = path.dirname(fileURLToPath(import.meta.url));
  const vendorPkgRoot = (() => {
    try {
      const require = createRequire(import.meta.url);
      const pkgJson = require.resolve("@treecrdt/wa-sqlite-vendor/package.json");
      return path.dirname(pkgJson);
    } catch {
      return path.resolve(here, "../../treecrdt-wa-sqlite-vendor");
    }
  })();
  const vendorWaSqliteRoot = path.join(vendorPkgRoot, "wa-sqlite");
  const vendorDistRoot = path.join(vendorPkgRoot, "dist");

  const copyOnce = async () => {
    const srcDir = vendorDistRoot;
    const srcExtra = path.join(vendorWaSqliteRoot, "src");
    await fs.mkdir(outDir, { recursive: true });
    for (const file of defaultFiles) {
      const from = file.endsWith(".js") && !file.startsWith("wa-sqlite") ? path.join(srcExtra, file) : path.join(srcDir, file);
      const to = path.join(outDir, file);
      try {
        const [fromStat, toStat] = await Promise.all([fs.stat(from), fs.stat(to)]);
        if (toStat.size === fromStat.size && toStat.mtimeMs >= fromStat.mtimeMs) continue;
      } catch {
        // fall through: destination missing or stat failed
      }
      await fs.copyFile(from, to);
    }
  };

  return {
    name: "treecrdt-wa-sqlite-assets",
    async configResolved() {
      await copyOnce();
    },
    async buildStart() {
      await copyOnce();
    },
  };
}
