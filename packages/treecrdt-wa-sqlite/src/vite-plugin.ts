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
  /** Multiple destination directories (relative to project root). Useful for apps served from a base path. */
  outDirs?: string[];
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
  const outDirsRaw = opts.outDirs ?? (opts.outDir ? [opts.outDir] : ["public/wa-sqlite"]);
  const outDirs = Array.from(new Set(outDirsRaw.filter((d) => typeof d === "string" && d.length > 0)));
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

  let copied: Promise<void> | null = null;
  const copyOnce = async () => {
    if (copied) return copied;
    copied = (async () => {
      const srcDir = vendorDistRoot;
      const srcExtra = path.join(vendorWaSqliteRoot, "src");
      await Promise.all(outDirs.map((dir) => fs.mkdir(dir, { recursive: true })));
      for (const file of defaultFiles) {
        const from =
          file.endsWith(".js") && !file.startsWith("wa-sqlite")
            ? path.join(srcExtra, file)
            : path.join(srcDir, file);
        const fromStat = await fs.stat(from);

        await Promise.all(
          outDirs.map(async (dir) => {
            const to = path.join(dir, file);
            try {
              const toStat = await fs.stat(to);
              if (toStat.size === fromStat.size && toStat.mtimeMs >= fromStat.mtimeMs) return;
            } catch {
              // destination missing or stat failed; copy below
            }
            await fs.copyFile(from, to);
          })
        );
      }
    })();
    return copied;
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
