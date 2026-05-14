import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

type Plugin = {
  name: string;
  apply?: 'serve' | 'build' | ((...args: any[]) => boolean);
  config?: () => Record<string, unknown>;
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
  'wa-sqlite.mjs',
  'wa-sqlite.wasm',
  'wa-sqlite-async.mjs',
  'wa-sqlite-async.wasm',
  'sqlite-api.js',
  'sqlite-constants.js',
];

/**
 * Vite plugin that copies the repo's vendored wa-sqlite artifacts into your app's public folder.
 * This removes the need for ad-hoc copy scripts in example apps.
 */
export function treecrdt(opts: WaSqlitePluginOptions = {}): Plugin {
  const outDirsRaw = opts.outDirs ?? (opts.outDir ? [opts.outDir] : ['public/wa-sqlite']);
  const outDirs = Array.from(
    new Set(outDirsRaw.filter((d) => typeof d === 'string' && d.length > 0)),
  );
  const here = path.dirname(fileURLToPath(import.meta.url));
  const packagedAssetsRoot = path.resolve(here, '../dist/wa-sqlite');

  let copied: Promise<void> | null = null;
  const copyOnce = async () => {
    if (copied) return copied;
    copied = (async () => {
      const srcDir = packagedAssetsRoot;
      await Promise.all(outDirs.map((dir) => fs.mkdir(dir, { recursive: true })));
      for (const file of defaultFiles) {
        const from = path.join(srcDir, file);
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
          }),
        );
      }
    })();
    return copied;
  };

  return {
    name: 'treecrdt-wa-sqlite-assets',
    config() {
      return {
        worker: {
          format: 'es',
        },
      };
    },
    async configResolved() {
      await copyOnce();
    },
    async buildStart() {
      await copyOnce();
    },
  };
}
