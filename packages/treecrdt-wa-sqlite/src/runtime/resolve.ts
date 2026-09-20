import { detectOpfsSupport, opfsFilenameForDocId } from '../opfs.js';
import type { ClientOptions, RuntimeMode, StorageMode } from '../types.js';

export type ResolvedBrowserEnvironment = {
  docId: string;
  storage: StorageMode;
  runtime: RuntimeMode;
  /** OPFS path derived from docId; undefined for in-memory databases. */
  filename?: string;
  baseUrl: string;
};

/** docId is part of CRDT/sync identity and cannot be defaulted or detected. */
export function validateDocId(docId: string): string {
  if (typeof docId !== 'string' || docId.length === 0) {
    throw new Error('createTreecrdtClient requires a non-empty docId');
  }
  return docId;
}

/** wa-sqlite JS assets are served from the app's public root (Vite BASE_URL or "/"). */
export function browserAssetsBaseUrl(): string {
  const base: string =
    typeof import.meta !== 'undefined' && (import.meta as any).env?.BASE_URL
      ? (import.meta as any).env.BASE_URL
      : '/';
  return base.endsWith('/') ? base : `${base}/`;
}

/**
 * Maps the public options onto storage, runtime, filename, and assets:
 * memory runs in-process, persistence runs OPFS in a dedicated worker and throws
 * when OPFS is unavailable (never a silent memory fallback).
 */
export function resolveBrowserEnvironment(opts: ClientOptions): ResolvedBrowserEnvironment {
  const docId = validateDocId(opts.docId);
  const baseUrl = browserAssetsBaseUrl();

  if (!opts.persistent) {
    return { docId, storage: 'memory', runtime: 'direct', baseUrl };
  }

  const support = detectOpfsSupport();
  if (!support.available) {
    throw new Error(
      `TreeCRDT persistent storage requires OPFS, unavailable in this environment: ${support.reason ?? 'unknown reason'}`,
    );
  }
  return {
    docId,
    storage: 'opfs',
    runtime: 'dedicated-worker',
    filename: opts.filename ?? opfsFilenameForDocId(docId),
    baseUrl,
  };
}
