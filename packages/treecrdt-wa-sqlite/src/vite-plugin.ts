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
  const vendorRoot = (() => {
    try {
      const require = createRequire(import.meta.url);
      const pkgJson = require.resolve("@treecrdt/wa-sqlite-vendor/package.json");
      return path.resolve(path.dirname(pkgJson), "wa-sqlite");
    } catch {
      return path.resolve(here, "../../../wa-sqlite-vendor/wa-sqlite");
    }
  })();

  const copyOnce = async () => {
    const srcDir = path.join(vendorRoot, "dist");
    const srcExtra = path.join(vendorRoot, "src");
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
