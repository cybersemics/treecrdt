import path from "node:path";
import { fileURLToPath } from "node:url";

export function dirnameFromImportMeta(importMetaUrl) {
  return path.dirname(fileURLToPath(importMetaUrl));
}

export function repoRootFromImportMeta(importMetaUrl, levelsUp) {
  const start = dirnameFromImportMeta(importMetaUrl);
  return path.resolve(start, ...Array.from({ length: levelsUp }, () => ".."));
}

