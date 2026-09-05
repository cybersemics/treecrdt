import { detectOpfsSupport } from '../opfs.js';
import type {
  ClientOptions,
  NormalizedRuntimeOptions,
  NormalizedStorageOptions,
  RuntimeMode,
  TreecrdtRuntime,
} from '../types.js';

export type ResolvedBrowserEnvironment = {
  storage: NormalizedStorageOptions;
  runtime: NormalizedRuntimeOptions;
  baseUrl?: string;
  shouldUseOpfs: boolean;
  resolvedRuntime: RuntimeMode;
  docId: string;
};

export function normalizeStorageOptions(opts: ClientOptions): NormalizedStorageOptions {
  const raw = opts.storage ?? { type: 'auto' };
  if (!raw || typeof raw !== 'object') {
    throw new Error(
      'createTreecrdtClient storage must use object options, e.g. { type: "memory" } or { type: "opfs" }',
    );
  }

  if (raw.type === 'memory') {
    return { type: 'memory', requireOpfs: false, fallback: 'memory' };
  }
  if (raw.type === 'opfs') {
    const fallback = raw.fallback ?? 'throw';
    return {
      type: 'opfs',
      filename: raw.filename,
      requireOpfs: fallback === 'throw',
      fallback,
    };
  }
  if (raw.type !== 'auto') {
    throw new Error('createTreecrdtClient storage.type must be "memory", "opfs", or "auto"');
  }
  const fallback = raw.fallback ?? 'memory';
  return {
    type: 'auto',
    filename: raw.filename,
    requireOpfs: fallback === 'throw',
    fallback,
  };
}

export function normalizeRuntimeOptions(opts: ClientOptions): NormalizedRuntimeOptions {
  return opts.runtime ?? { type: 'auto' };
}

export function normalizeAssetsBaseUrl(baseUrl?: string): string | undefined {
  if (baseUrl === undefined) return undefined;
  return baseUrl.endsWith('/') ? baseUrl : `${baseUrl}/`;
}

export function defaultBrowserAssetsBaseUrl(): string {
  if (typeof import.meta !== 'undefined' && (import.meta as any).env?.BASE_URL) {
    return (import.meta as any).env.BASE_URL;
  }
  return '/';
}

export function resolveRuntimeMode(runtime: TreecrdtRuntime, shouldUseOpfs: boolean): RuntimeMode {
  if (runtime.type === 'direct') return 'direct';
  if (runtime.type === 'dedicated-worker') return 'dedicated-worker';
  if (runtime.type === 'shared-worker') {
    if (typeof SharedWorker === 'undefined') {
      throw new Error('TreeCRDT shared-worker runtime is unavailable in this environment');
    }
    return 'shared-worker';
  }
  if (shouldUseOpfs) return 'dedicated-worker';
  return 'direct';
}

export function defaultSharedWorkerName(docId: string, filename?: string): string {
  return `treecrdt:${docId}:${filename ?? '/treecrdt.db'}`;
}

export function resolveBrowserEnvironment(opts: ClientOptions): ResolvedBrowserEnvironment {
  const storage = normalizeStorageOptions(opts);
  const runtime = normalizeRuntimeOptions(opts);
  const docId = opts.docId ?? 'treecrdt';
  const baseUrl = normalizeAssetsBaseUrl(opts.assets?.baseUrl ?? defaultBrowserAssetsBaseUrl());
  const support = detectOpfsSupport();
  const shouldUseOpfs = storage.type === 'opfs' || (storage.type === 'auto' && support.available);
  const resolvedRuntime = resolveRuntimeMode(runtime, shouldUseOpfs);

  assertBrowserOpfsRequirements(storage, support, shouldUseOpfs, resolvedRuntime);

  return { storage, runtime, baseUrl, shouldUseOpfs, resolvedRuntime, docId };
}

function assertBrowserOpfsRequirements(
  storage: NormalizedStorageOptions,
  support: ReturnType<typeof detectOpfsSupport>,
  shouldUseOpfs: boolean,
  resolvedRuntime: RuntimeMode,
): void {
  const unavailable = () =>
    new Error(`OPFS unavailable in this environment: ${support.reason ?? 'unknown reason'}`);

  if (storage.type === 'auto' && !support.available && storage.fallback === 'throw') {
    throw unavailable();
  }
  if (shouldUseOpfs && resolvedRuntime === 'direct' && !support.available && storage.requireOpfs) {
    throw unavailable();
  }
}
