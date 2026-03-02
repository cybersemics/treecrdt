import { cpSync, existsSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const pkgRoot = join(__dirname, "..");
const targetDir = join(pkgRoot, "..", "..", "target", "release");

const ext = process.platform === "darwin" ? "dylib" : process.platform === "win32" ? "dll" : "so";
const libPrefix = process.platform === "win32" ? "" : "lib";
const src = join(targetDir, `${libPrefix}treecrdt_postgres_napi.${ext}`);

if (!existsSync(src)) {
  throw new Error(`native build output not found at ${src}`);
}

const outDir = join(pkgRoot, "native");
mkdirSync(outDir, { recursive: true });
const dst = join(outDir, "treecrdt-postgres-napi.node");
cpSync(src, dst);

