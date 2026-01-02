import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, "../../..");
const targetRelease = path.join(repoRoot, "target", "release", "deps");

if (!fs.existsSync(targetRelease)) {
  console.error("No target/release/deps directory. Run build:ext:native first.");
  process.exit(1);
}

const ext = (() => {
  switch (process.platform) {
    case "darwin":
      return ".dylib";
    case "win32":
      return ".dll";
    default:
      return ".so";
  }
})();

const matches = fs
  .readdirSync(targetRelease)
  .filter(
    (f) =>
      f.includes("treecrdt_sqlite_ext") &&
      (f.endsWith(ext) || f.endsWith(ext.toLowerCase()))
  );

if (matches.length === 0) {
  console.error(
    `No built extension found in ${targetRelease}. Expected treecrdt_sqlite_ext*${ext}`
  );
  process.exit(1);
}

const candidates = matches
  .map((file) => {
    const fullPath = path.join(targetRelease, file);
    const stat = fs.statSync(fullPath);
    return { file, fullPath, mtimeMs: stat.mtimeMs };
  })
  .sort((a, b) => b.mtimeMs - a.mtimeMs || a.file.localeCompare(b.file));

if (candidates.length > 1) {
  console.warn(
    `Multiple built extensions found; using newest: ${candidates[0].file}`
  );
}

const src = candidates[0].fullPath;
const destDir = path.resolve(__dirname, "../native");
fs.mkdirSync(destDir, { recursive: true });
const destBase = ext === ".dll" ? "treecrdt_sqlite_ext" : "libtreecrdt_sqlite_ext";
const dest = path.join(destDir, `${destBase}${ext}`);

fs.copyFileSync(src, dest);
console.log(`Copied ${src} -> ${dest}`);
