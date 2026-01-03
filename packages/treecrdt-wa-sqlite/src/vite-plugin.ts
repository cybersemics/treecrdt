import fs from "node:fs/promises";
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
  const vendorRoot = path.resolve(here, "../../../vendor/wa-sqlite");

  let copied: Promise<void> | null = null;
  const copyOnce = async () => {
    if (copied) return copied;
    copied = (async () => {
    const srcDir = path.join(vendorRoot, "dist");
    const srcExtra = path.join(vendorRoot, "src");
    await Promise.all(outDirs.map((dir) => fs.mkdir(dir, { recursive: true })));
    for (const file of defaultFiles) {
      const from =
        file.endsWith(".js") && !file.startsWith("wa-sqlite") ? path.join(srcExtra, file) : path.join(srcDir, file);
      await Promise.all(outDirs.map((dir) => fs.copyFile(from, path.join(dir, file))));
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
