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

export function validateCrossTab(crossTab: boolean | undefined): boolean {
  if (crossTab !== undefined && typeof crossTab !== 'boolean') {
    throw new Error('createTreecrdtClient crossTab must be a boolean');
  }
  if (crossTab && typeof SharedWorker === 'undefined') {
    throw new Error('TreeCRDT cross-tab mode requires SharedWorker support');
  }
  return crossTab ?? false;
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
 * cross-tab clients run in a shared worker; otherwise memory runs in-process and
 * persistence runs OPFS in a dedicated worker. OPFS never silently falls back.
 */
export function resolveBrowserEnvironment(opts: ClientOptions): ResolvedBrowserEnvironment {
  const docId = validateDocId(opts.docId);
  const crossTab = validateCrossTab(opts.crossTab);
  const baseUrl = browserAssetsBaseUrl();

  if (!opts.persistent) {
    return {
      docId,
      storage: 'memory',
      runtime: crossTab ? 'shared-worker' : 'direct',
      baseUrl,
    };
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
    runtime: crossTab ? 'shared-worker' : 'dedicated-worker',
    filename: opts.filename ?? opfsFilenameForDocId(docId),
    baseUrl,
  };
}
