import type {
  DiscoveryAttachment,
  DiscoveryAttachmentProtocol,
  DocAttachmentPlan,
  DocDiscoveryService,
  ResolveDocResponse,
  ResolveDocRequest,
} from './types.js';

type Awaitable<T> = T | Promise<T>;

export type DiscoveryStringStore = {
  getItem(key: string): string | null;
  setItem(key: string, value: string): void;
  removeItem?(key: string): void;
};

export type CachedResolvedDoc = {
  resolvedAtMs: number;
  response: ResolveDocResponse;
};

export interface DiscoveryRouteCache {
  get(key: string): Awaitable<CachedResolvedDoc | undefined>;
  set(key: string, entry: CachedResolvedDoc): Awaitable<void>;
  delete(key: string): Awaitable<void>;
}

export type ResolveDocHttpClientOptions = {
  baseUrl: string;
  resolveDocPath?: string;
  fetch?: typeof fetch;
  headers?: Record<string, string>;
};

export type ResolveWebSocketAttachmentResult = {
  url: URL;
  source: 'direct' | 'cache' | 'network';
  response?: ResolveDocResponse;
  cacheKey?: string;
};

function normalizeBaseUrl(raw: string, defaultProtocol: 'http' | 'https' | 'ws' | 'wss'): URL {
  let input = raw.trim();
  if (input.length === 0) throw new Error('Discovery endpoint is empty');
  if (!/^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(input)) input = `${defaultProtocol}://${input}`;
  return new URL(input);
}

function normalizeDiscoveryBaseUrl(raw: string): URL {
  const url = normalizeBaseUrl(raw, 'https');
  if (url.protocol === 'ws:') url.protocol = 'http:';
  if (url.protocol === 'wss:') url.protocol = 'https:';
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error('Discovery endpoint must use http://, https://, ws://, or wss://');
  }
  if (url.pathname.endsWith('/sync')) {
    url.pathname = url.pathname.slice(0, -'/sync'.length) || '/';
    url.search = '';
  }
  return url;
}

export function normalizeDirectSyncWebSocketUrl(raw: string, docId: string): URL {
  const url = normalizeBaseUrl(raw, 'ws');
  if (url.protocol === 'http:') url.protocol = 'ws:';
  if (url.protocol === 'https:') url.protocol = 'wss:';
  if (url.protocol !== 'ws:' && url.protocol !== 'wss:') {
    throw new Error('Sync server URL must use ws://, wss://, http://, or https://');
  }
  if (url.pathname === '/' || url.pathname.length === 0) {
    url.pathname = '/sync';
  }
  url.searchParams.set('docId', docId);
  return url;
}

export function isDiscoveryBootstrapUrl(raw: string): boolean {
  const url = normalizeBaseUrl(raw, 'ws');
  if (url.protocol !== 'http:' && url.protocol !== 'https:') return false;
  return url.pathname !== '/sync';
}

export function buildResolveDocUrl(
  baseUrl: string,
  docId: string,
  resolveDocPath = '/resolve-doc',
): URL {
  const url = normalizeDiscoveryBaseUrl(baseUrl);
  url.pathname = resolveDocPath.startsWith('/') ? resolveDocPath : `/${resolveDocPath}`;
  url.search = '';
  url.searchParams.set('docId', docId);
  return url;
}

export function pickAttachment(
  plan: DocAttachmentPlan,
  protocol: DiscoveryAttachmentProtocol,
  role?: DiscoveryAttachment['role'],
): DiscoveryAttachment | undefined {
  const exact = plan.attachments.find(
    (attachment) => attachment.protocol === protocol && (role == null || attachment.role === role),
  );
  if (exact) return exact;
  return plan.attachments.find((attachment) => attachment.protocol === protocol);
}

export function pickWebSocketAttachment(plan: DocAttachmentPlan): DiscoveryAttachment | undefined {
  return pickAttachment(plan, 'websocket', 'preferred') ?? pickAttachment(plan, 'websocket');
}

export function createDiscoveryCacheKey(baseUrl: string, docId: string): string {
  const normalized = normalizeDiscoveryBaseUrl(baseUrl);
  return `${normalized.origin}${normalized.pathname}|${docId}`;
}

export function isCachedResolvedDocFresh(entry: CachedResolvedDoc, nowMs = Date.now()): boolean {
  const ttlMs = entry.response.plan.cacheTtlMs;
  if (ttlMs == null) return true;
  return entry.resolvedAtMs + ttlMs > nowMs;
}

export function createStringStoreRouteCache(
  store: DiscoveryStringStore,
  prefix = 'treecrdt.discovery.',
): DiscoveryRouteCache {
  return {
    get(key) {
      const raw = store.getItem(`${prefix}${key}`);
      if (!raw) return undefined;
      try {
        return JSON.parse(raw) as CachedResolvedDoc;
      } catch {
        store.removeItem?.(`${prefix}${key}`);
        return undefined;
      }
    },
    set(key, entry) {
      store.setItem(`${prefix}${key}`, JSON.stringify(entry));
    },
    delete(key) {
      store.removeItem?.(`${prefix}${key}`);
    },
  };
}

export function createHttpDocDiscoveryClient(
  opts: ResolveDocHttpClientOptions,
): Pick<DocDiscoveryService, 'resolveDoc'> {
  const fetchImpl = opts.fetch ?? fetch;
  if (typeof fetchImpl !== 'function') {
    throw new Error('fetch is not available; pass options.fetch explicitly');
  }

  const resolveDocPath = opts.resolveDocPath ?? '/resolve-doc';

  const requestJson = async <T>(url: URL, init: RequestInit): Promise<T> => {
    const res = await fetchImpl(url, {
      ...init,
      headers: {
        accept: 'application/json',
        'content-type': 'application/json',
        ...opts.headers,
        ...(init.headers as Record<string, string> | undefined),
      },
    });
    if (!res.ok) {
      throw new Error(`Discovery request failed (${res.status} ${res.statusText})`);
    }
    return (await res.json()) as T;
  };

  return {
    async resolveDoc(request: ResolveDocRequest): Promise<ResolveDocResponse> {
      const url = buildResolveDocUrl(opts.baseUrl, request.docId, resolveDocPath);
      return await requestJson<ResolveDocResponse>(url, { method: 'GET' });
    },
  };
}

export async function resolveDocWithCache(opts: {
  baseUrl: string;
  docId: string;
  client?: Pick<DocDiscoveryService, 'resolveDoc'>;
  cache?: DiscoveryRouteCache;
  forceRefresh?: boolean;
  fetch?: typeof fetch;
  resolveDocPath?: string;
}): Promise<{ response: ResolveDocResponse; source: 'cache' | 'network'; cacheKey: string }> {
  const cacheKey = createDiscoveryCacheKey(opts.baseUrl, opts.docId);
  if (!opts.forceRefresh && opts.cache) {
    const cached = await opts.cache.get(cacheKey);
    if (cached && isCachedResolvedDocFresh(cached)) {
      return { response: cached.response, source: 'cache', cacheKey };
    }
  }

  const client =
    opts.client ??
    createHttpDocDiscoveryClient({
      baseUrl: opts.baseUrl,
      fetch: opts.fetch,
      resolveDocPath: opts.resolveDocPath,
    });
  const response = await client.resolveDoc({ docId: opts.docId });
  await opts.cache?.set(cacheKey, {
    resolvedAtMs: Date.now(),
    response,
  });
  return { response, source: 'network', cacheKey };
}

export async function resolveWebSocketAttachment(opts: {
  endpoint: string;
  docId: string;
  cache?: DiscoveryRouteCache;
  fetch?: typeof fetch;
  resolveDocPath?: string;
  forceRefresh?: boolean;
}): Promise<ResolveWebSocketAttachmentResult> {
  if (!isDiscoveryBootstrapUrl(opts.endpoint)) {
    return {
      url: normalizeDirectSyncWebSocketUrl(opts.endpoint, opts.docId),
      source: 'direct',
    };
  }

  const { response, source, cacheKey } = await resolveDocWithCache({
    baseUrl: opts.endpoint,
    docId: opts.docId,
    cache: opts.cache,
    fetch: opts.fetch,
    resolveDocPath: opts.resolveDocPath,
    forceRefresh: opts.forceRefresh,
  });
  const attachment = pickWebSocketAttachment(response.plan);
  if (!attachment) {
    throw new Error(`Resolved doc ${opts.docId} does not include a websocket attachment`);
  }
  const url = normalizeDirectSyncWebSocketUrl(attachment.url, opts.docId);
  return {
    url,
    source,
    response,
    cacheKey,
  };
}
