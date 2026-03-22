import path from "node:path";
import { randomUUID } from "node:crypto";
import { pathToFileURL } from "node:url";

import { base64urlDecode, describeTreecrdtCapabilityTokenV1 } from "@treecrdt/auth";
import type { Operation } from "@treecrdt/interface";
import { createReplayOnlySyncAuth } from "@treecrdt/sync";
import type { SyncBackend, SyncPeer, SyncPeerOptions } from "@treecrdt/sync";
import {
  createCapabilityMaterialStore,
  createOpAuthStore,
} from "@treecrdt/sync-postgres";
import type {
  PostgresCapabilityMaterialStore,
  PostgresOpAuthStore,
} from "@treecrdt/sync-postgres";
import { treecrdtSyncV0ProtobufCodec } from "@treecrdt/sync/protobuf";
import type {
  WebSocketSyncServerDocHandle,
  WebSocketSyncServerDocProvider,
  WebSocketSyncServerHooks,
  WebSocketSyncServerUpgradeContext,
  WebSocketSyncServerUpgradeHook,
} from "@treecrdt/sync-server-core";
import { startWebSocketSyncServer } from "@treecrdt/sync-server-core";
import { Client as PgClient } from "pg";

type Awaitable<T> = T | Promise<T>;

export type PostgresSyncBackendFactory = {
  ensureSchema?: () => Awaitable<void>;
  open: (docId: string) => Awaitable<SyncBackend<Operation>>;
};

export type PostgresSyncBackendModule = {
  createPostgresNapiSyncBackendFactory: (url: string) => PostgresSyncBackendFactory;
};

export type PostgresNodeDocStoreOptions = {
  backendFactory: PostgresSyncBackendFactory;
  idleCloseMs?: number;
  broadcastDocUpdate?: (docId: string) => Awaitable<void>;
  peerOptionsFactory?: (docId: string) => Awaitable<SyncPeerOptions<Operation> | undefined>;
};

export type SyncServerOptions = {
  host?: string;
  port?: number;
  postgresUrl: string;
  backendModule?: string;
  maxCodewords?: number;
  directSendThreshold?: number;
  idleCloseMs?: number;
  maxPayloadBytes?: number;
  authToken?: string;
  authCapabilityIssuerPublicKeys?: Uint8Array[];
  docIdPattern?: string | RegExp;
  allowDocCreate?: boolean;
  enablePgNotify?: boolean;
  pgNotifyChannel?: string;
  rateLimitMaxUpgrades?: number;
  rateLimitWindowMs?: number;
  packageName?: string;
  packageVersion?: string;
  gitSha?: string;
  gitDirty?: boolean;
  startedAt?: string;
  hooks?: WebSocketSyncServerHooks;
};

export type SyncServerHandle = {
  host: string;
  port: number;
  idleCloseMs: number;
  backendModule: string;
  close: () => Promise<void>;
};

type SyncServerReadinessProbe = {
  check: () => Promise<void>;
  close?: () => Promise<void>;
};

type DocContext = {
  docId: string;
  backend: SyncBackend<Operation>;
  peerOptions?: SyncPeerOptions<Operation>;
  peers: Set<SyncPeer<Operation>>;
  connections: number;
  applyQueue: Promise<void>;
  closed: boolean;
  closeTimer?: NodeJS.Timeout;
};

type PostgresNodeDocStore = {
  provider: WebSocketSyncServerDocProvider<Operation>;
  notifyDocUpdate: (docId: string) => void;
  closeAll: () => Promise<void>;
};

type PostgresDocUpdateBusOptions = {
  postgresUrl: string;
  channel: string;
  onDocUpdate: (docId: string) => void;
};

export {
  createCapabilityMaterialStore,
  createOpAuthStore,
};
export type {
  PostgresCapabilityMaterialStore,
  PostgresOpAuthStore,
};

function ensurePostgresChannelName(value: string): string {
  const trimmed = value.trim();
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(trimmed)) {
    throw new Error(`invalid Postgres NOTIFY channel: ${value}`);
  }
  return trimmed;
}

function describeAuthMode(
  authToken: string | undefined,
  issuerPublicKeys: Uint8Array[]
): "none" | "static_token" | "capability_cwt" {
  if (issuerPublicKeys.length > 0) return "capability_cwt";
  if (authToken) return "static_token";
  return "none";
}

function errorMessage(error: unknown): string {
  if (error instanceof Error && error.message.trim().length > 0) return error.message;
  return String(error);
}

async function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  return await new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timeout after ${ms}ms: ${label}`)), ms);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      }
    );
  });
}

function parseDocIdRegex(input: string | RegExp | undefined): RegExp | undefined {
  if (!input) return undefined;
  if (input instanceof RegExp) return new RegExp(input.source, input.flags.replace(/[gy]/g, ""));
  const trimmed = input.trim();
  if (trimmed.length === 0) return undefined;
  return new RegExp(trimmed);
}

type DocUpdatePayload = {
  docId: string;
  source?: string;
};

class PostgresDocUpdateBus {
  private readonly channel: string;
  private readonly queryClient: PgClient;
  private readonly listenClient: PgClient;
  private readonly sourceId: string;
  private closed = false;

  private constructor(private readonly opts: PostgresDocUpdateBusOptions) {
    this.channel = ensurePostgresChannelName(opts.channel);
    this.queryClient = new PgClient({ connectionString: opts.postgresUrl });
    this.listenClient = new PgClient({ connectionString: opts.postgresUrl });
    this.sourceId = randomUUID();
  }

  static async create(opts: PostgresDocUpdateBusOptions): Promise<PostgresDocUpdateBus> {
    const bus = new PostgresDocUpdateBus(opts);
    await bus.start();
    return bus;
  }

  private async start(): Promise<void> {
    await Promise.all([this.queryClient.connect(), this.listenClient.connect()]);

    this.listenClient.on("notification", (msg: { channel: string; payload?: string }) => {
      if (msg.channel !== this.channel) return;
      if (!msg.payload) return;
      let payload: DocUpdatePayload;
      try {
        payload = JSON.parse(msg.payload) as DocUpdatePayload;
      } catch {
        payload = { docId: msg.payload };
      }
      if (!payload || typeof payload.docId !== "string" || payload.docId.length === 0) return;
      if (payload.source && payload.source === this.sourceId) return;
      this.opts.onDocUpdate(payload.docId);
    });

    await this.listenClient.query(`LISTEN ${this.channel}`);
  }

  async hasDoc(docId: string): Promise<boolean> {
    const res = await this.queryClient.query("SELECT 1 FROM treecrdt_meta WHERE doc_id = $1 LIMIT 1", [docId]);
    return (res.rowCount ?? 0) > 0;
  }

  async publishDocUpdate(docId: string): Promise<void> {
    if (this.closed || docId.length === 0) return;
    const payload = JSON.stringify({ docId, source: this.sourceId } satisfies DocUpdatePayload);
    await this.queryClient.query("SELECT pg_notify($1, $2)", [this.channel, payload]);
  }

  async ping(): Promise<void> {
    await this.queryClient.query("SELECT 1");
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    try {
      await this.listenClient.query(`UNLISTEN ${this.channel}`);
    } catch {
      // ignore
    }
    await Promise.allSettled([this.listenClient.end(), this.queryClient.end()]);
  }
}

function isDeniedDecision(
  decision: void | boolean | { allow: boolean; statusCode?: number; body?: string }
): decision is false | { allow: false; statusCode?: number; body?: string } {
  if (decision === false) return true;
  if (typeof decision !== "object" || decision === null) return false;
  return decision.allow === false;
}

function combineUpgradeHooks(
  ...hooks: Array<WebSocketSyncServerUpgradeHook | undefined>
): WebSocketSyncServerUpgradeHook | undefined {
  const active = hooks.filter((hook): hook is WebSocketSyncServerUpgradeHook => Boolean(hook));
  if (active.length === 0) return undefined;
  return async (ctx) => {
    for (const hook of active) {
      const decision = await hook(ctx);
      if (isDeniedDecision(decision)) return decision;
    }
    return { allow: true };
  };
}

function extractAuthToken(ctx: WebSocketSyncServerUpgradeContext): string | null {
  const queryToken = ctx.url.searchParams.get("token")?.trim();
  if (queryToken) return queryToken;

  const rawAuth = ctx.req.headers.authorization;
  if (typeof rawAuth === "string") {
    const match = rawAuth.match(/^Bearer\s+(.+)$/i);
    return match?.[1]?.trim() || null;
  }
  return null;
}

function createStaticTokenAuthHook(expectedToken: string): WebSocketSyncServerUpgradeHook {
  return (ctx) => {
    const token = extractAuthToken(ctx);
    if (token && token === expectedToken) return { allow: true };
    return {
      allow: false,
      statusCode: 401,
      body: "missing or invalid auth token",
    };
  };
}

function createCapabilityTokenAuthHook(issuerPublicKeys: Uint8Array[]): WebSocketSyncServerUpgradeHook {
  return async (ctx) => {
    const token = extractAuthToken(ctx);
    if (!token) {
      return {
        allow: false,
        statusCode: 401,
        body: "missing capability token",
      };
    }
    try {
      const tokenBytes = base64urlDecode(token);
      await describeTreecrdtCapabilityTokenV1({
        tokenBytes,
        issuerPublicKeys,
        docId: ctx.docId,
      });
      return { allow: true };
    } catch (err) {
      return {
        allow: false,
        statusCode: 401,
        body: `invalid capability token: ${err instanceof Error ? err.message : String(err)}`,
      };
    }
  };
}

function createDocIdPatternHook(pattern: RegExp): WebSocketSyncServerUpgradeHook {
  return (ctx) => {
    if (pattern.test(ctx.docId)) return { allow: true };
    return {
      allow: false,
      statusCode: 400,
      body: "invalid docId format",
    };
  };
}

function createKnownDocHook(hasDoc: (docId: string) => Promise<boolean>): WebSocketSyncServerUpgradeHook {
  return async (ctx) => {
    const known = await hasDoc(ctx.docId);
    if (known) return { allow: true };
    return {
      allow: false,
      statusCode: 403,
      body: "docId creation disabled",
    };
  };
}

function createIpRateLimitHook(maxUpgrades: number, windowMs: number): WebSocketSyncServerUpgradeHook {
  const buckets = new Map<string, { startedAt: number; count: number }>();
  let lastPrunedAt = 0;
  return (ctx) => {
    const now = Date.now();
    if (buckets.size > 0 && now - lastPrunedAt >= windowMs) {
      lastPrunedAt = now;
      const cutoff = now - windowMs;
      for (const [key, bucket] of buckets) {
        if (bucket.startedAt <= cutoff) buckets.delete(key);
      }
    }

    const key = ctx.remoteAddress ?? "unknown";
    const bucket = buckets.get(key);
    if (!bucket || now - bucket.startedAt >= windowMs) {
      buckets.set(key, { startedAt: now, count: 1 });
      return { allow: true };
    }

    bucket.count += 1;
    if (bucket.count <= maxUpgrades) return { allow: true };

    return {
      allow: false,
      statusCode: 429,
      body: "too many upgrade requests",
    };
  };
}

function ensureNonEmptyString(name: string, value: unknown): string {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${name} must be a non-empty string`);
  }
  return value;
}

function moduleSpecifier(input: string): string {
  if (input.startsWith("file://")) return input;
  if (input.startsWith(".") || input.startsWith("/") || input.startsWith("\\")) {
    return pathToFileURL(path.resolve(input)).href;
  }
  return input;
}

export async function loadPostgresBackendModule(moduleName: string): Promise<PostgresSyncBackendModule> {
  const name = ensureNonEmptyString("backendModule", moduleName);
  const specifier = moduleSpecifier(name);

  let imported: unknown;
  try {
    imported = await import(specifier);
  } catch (err) {
    const detail = err instanceof Error ? `${err.name}: ${err.message}` : String(err);
    throw new Error(`failed to import backend module "${name}" (${specifier}): ${detail}`);
  }

  const mod = imported as Partial<PostgresSyncBackendModule> | null;
  if (!mod || typeof mod.createPostgresNapiSyncBackendFactory !== "function") {
    throw new Error(
      `backend module "${name}" does not export createPostgresNapiSyncBackendFactory(url)`
    );
  }
  return { createPostgresNapiSyncBackendFactory: mod.createPostgresNapiSyncBackendFactory };
}

export function createPostgresNodeDocStore(opts: PostgresNodeDocStoreOptions): PostgresNodeDocStore {
  const idleCloseMs = Number(opts.idleCloseMs ?? 30_000);
  if (!Number.isFinite(idleCloseMs) || idleCloseMs < 0) throw new Error(`invalid idleCloseMs: ${opts.idleCloseMs}`);

  const docs = new Map<string, DocContext>();
  const openInFlight = new Map<string, Promise<DocContext>>();
  let closing = false;
  let closeAllPromise: Promise<void> | undefined;

  const notifyDocUpdate = (docId: string): void => {
    const ctx = docs.get(docId);
    if (!ctx) return;
    for (const peer of ctx.peers) void peer.notifyLocalUpdate();
  };

  const closeBackend = async (backend: SyncBackend<Operation>): Promise<void> => {
    try {
      await (backend as any)?.close?.();
    } catch {
      // ignore close failures
    }
  };

  const closeContext = async (ctx: DocContext): Promise<void> => {
    if (ctx.closed) return;
    ctx.closed = true;
    if (ctx.closeTimer) {
      clearTimeout(ctx.closeTimer);
      ctx.closeTimer = undefined;
    }
    docs.delete(ctx.docId);
    ctx.peers.clear();
    await closeBackend(ctx.backend);
  };

  const scheduleClose = (ctx: DocContext): void => {
    if (ctx.closed) return;
    if (closing || idleCloseMs === 0) {
      void closeContext(ctx);
      return;
    }
    if (ctx.closeTimer) return;
    ctx.closeTimer = setTimeout(() => {
      ctx.closeTimer = undefined;
      if (ctx.connections > 0) return;
      void closeContext(ctx);
    }, idleCloseMs);
  };

  const wrapBackendForContext = (ctx: DocContext, backend: SyncBackend<Operation>): SyncBackend<Operation> => {
    const applyOps = async (ops: Operation[]) => {
      if (ops.length === 0) return;
      const run = ctx.applyQueue.then(async () => {
        await backend.applyOps(ops);
      });
      // Keep the queue usable even after a failed apply.
      ctx.applyQueue = run.catch(() => undefined);
      await run;
      try {
        await opts.broadcastDocUpdate?.(ctx.docId);
      } catch {
        // ignore notify failures; local update path still proceeds
      }
      notifyDocUpdate(ctx.docId);
    };
    return {
      ...backend,
      docId: ctx.docId,
      applyOps,
    };
  };

  const openContext = async (docId: string): Promise<DocContext> => {
    if (closing) throw new Error("doc store is closing");

    const existing = docs.get(docId);
    if (existing) return existing;

    const pending = openInFlight.get(docId);
    if (pending) return pending;

    const opening = (async () => {
      const rawBackend = await opts.backendFactory.open(docId);
      let peerOptions: SyncPeerOptions<Operation> | undefined;
      try {
        peerOptions = await opts.peerOptionsFactory?.(docId);
      } catch (err) {
        await closeBackend(rawBackend);
        throw err;
      }
      if (closing) {
        await closeBackend(rawBackend);
        throw new Error("doc store is closing");
      }

      const alreadyOpened = docs.get(docId);
      if (alreadyOpened) {
        await closeBackend(rawBackend);
        return alreadyOpened;
      }
      const ctx: DocContext = {
        docId,
        backend: rawBackend,
        peerOptions,
        peers: new Set(),
        connections: 0,
        applyQueue: Promise.resolve(),
        closed: false,
      };
      ctx.backend = wrapBackendForContext(ctx, rawBackend);
      docs.set(docId, ctx);
      return ctx;
    })();

    openInFlight.set(docId, opening);
    try {
      return await opening;
    } finally {
      openInFlight.delete(docId);
    }
  };

  return {
    provider: {
      open: async (docId: string): Promise<WebSocketSyncServerDocHandle<Operation>> => {
        const cleanDocId = ensureNonEmptyString("docId", docId);
        const ctx = await openContext(cleanDocId);
        if (closing || ctx.closed) throw new Error("doc store is closing");

        ctx.connections += 1;
        if (ctx.closeTimer) {
          clearTimeout(ctx.closeTimer);
          ctx.closeTimer = undefined;
        }

        let released = false;
        return {
          backend: ctx.backend,
          peerOptions: ctx.peerOptions,
          onPeerAdded: (peer) => ctx.peers.add(peer),
          onPeerRemoved: (peer) => ctx.peers.delete(peer),
          release: async () => {
            if (released) return;
            released = true;
            if (ctx.closed) return;
            ctx.connections = Math.max(0, ctx.connections - 1);
            if (ctx.connections > 0) return;
            if (closing) {
              await closeContext(ctx);
              return;
            }
            scheduleClose(ctx);
          },
        };
      },
    },
    notifyDocUpdate,
    closeAll: async () => {
      if (closeAllPromise) return closeAllPromise;
      closing = true;
      closeAllPromise = (async () => {
        await Promise.allSettled(Array.from(openInFlight.values()));
        await Promise.allSettled(Array.from(docs.values()).map((ctx) => closeContext(ctx)));
      })();
      await closeAllPromise;
    },
  };
}

async function createReadinessProbe(postgresUrl: string): Promise<SyncServerReadinessProbe> {
  const client = new PgClient({ connectionString: postgresUrl });
  await client.connect();
  return {
    check: async () => {
      await withTimeout(client.query("SELECT 1"), 3_000, "postgres readiness ping");
    },
    close: async () => {
      await client.end();
    },
  };
}

export async function startSyncServer(opts: SyncServerOptions): Promise<SyncServerHandle> {
  const host = opts.host ?? "0.0.0.0";
  const port = Number(opts.port ?? 8787);
  const backendModule = ensureNonEmptyString(
    "backendModule",
    opts.backendModule ?? "@treecrdt/postgres-napi"
  );
  const postgresUrl = ensureNonEmptyString("postgresUrl", opts.postgresUrl);
  const maxCodewords =
    opts.maxCodewords == null ? undefined : Number(opts.maxCodewords);
  const directSendThreshold =
    opts.directSendThreshold == null ? undefined : Number(opts.directSendThreshold);
  const idleCloseMs = Number(opts.idleCloseMs ?? 30_000);
  const maxPayloadBytes = Number(opts.maxPayloadBytes ?? 10 * 1024 * 1024);
  const authToken =
    typeof opts.authToken === "string" && opts.authToken.trim().length > 0 ? opts.authToken.trim() : undefined;
  const authCapabilityIssuerPublicKeys = (opts.authCapabilityIssuerPublicKeys ?? []).filter(
    (value): value is Uint8Array => value instanceof Uint8Array && value.length > 0
  );
  const docIdPattern = parseDocIdRegex(opts.docIdPattern);
  const allowDocCreate = opts.allowDocCreate ?? true;
  const enablePgNotify = opts.enablePgNotify ?? true;
  const pgNotifyChannel = ensurePostgresChannelName(opts.pgNotifyChannel ?? "treecrdt_sync_doc_updates");
  const rateLimitMaxUpgrades = Number(opts.rateLimitMaxUpgrades ?? 0);
  const rateLimitWindowMs = Number(opts.rateLimitWindowMs ?? 60_000);
  const packageName = opts.packageName?.trim() || "@treecrdt/sync-server-postgres-node";
  const packageVersion = opts.packageVersion?.trim() || undefined;
  const gitSha = opts.gitSha?.trim() || undefined;
  const gitDirty = Boolean(opts.gitDirty);
  const startedAt = opts.startedAt?.trim() || new Date().toISOString();
  const startedAtMs = Date.parse(startedAt);

  if (!Number.isFinite(port) || port <= 0) throw new Error(`invalid port: ${opts.port}`);
  if (maxCodewords != null && (!Number.isFinite(maxCodewords) || maxCodewords <= 0)) {
    throw new Error(`invalid maxCodewords: ${opts.maxCodewords}`);
  }
  if (directSendThreshold != null && (!Number.isFinite(directSendThreshold) || directSendThreshold < 0)) {
    throw new Error(`invalid directSendThreshold: ${opts.directSendThreshold}`);
  }
  if (!Number.isFinite(idleCloseMs) || idleCloseMs < 0) throw new Error(`invalid idleCloseMs: ${opts.idleCloseMs}`);
  if (!Number.isFinite(maxPayloadBytes) || maxPayloadBytes <= 0) {
    throw new Error(`invalid maxPayloadBytes: ${opts.maxPayloadBytes}`);
  }
  if (!Number.isFinite(rateLimitMaxUpgrades) || rateLimitMaxUpgrades < 0) {
    throw new Error(`invalid rateLimitMaxUpgrades: ${opts.rateLimitMaxUpgrades}`);
  }
  if (!Number.isFinite(rateLimitWindowMs) || rateLimitWindowMs <= 0) {
    throw new Error(`invalid rateLimitWindowMs: ${opts.rateLimitWindowMs}`);
  }

  const module = await loadPostgresBackendModule(backendModule);
  const backendFactory = module.createPostgresNapiSyncBackendFactory(postgresUrl);
  if (backendFactory.ensureSchema) await backendFactory.ensureSchema();
  const opAuthStore = createOpAuthStore({ postgresUrl });
  const capabilityMaterialStore = createCapabilityMaterialStore({ postgresUrl });
  await opAuthStore.init();
  await capabilityMaterialStore.init();

  let docUpdateBus: PostgresDocUpdateBus | undefined;
  const docs = createPostgresNodeDocStore({
    backendFactory,
    idleCloseMs,
    broadcastDocUpdate: async (docId) => {
      await docUpdateBus?.publishDocUpdate(docId);
    },
    peerOptionsFactory: async (docId) => ({
      auth: createReplayOnlySyncAuth({
        docId,
        authMaterialStore: {
          opAuth: opAuthStore.forDoc(docId),
          capabilities: capabilityMaterialStore.forDoc(docId),
        },
      }),
      requireAuthForFilters: false,
      ...(maxCodewords != null ? { maxCodewords } : {}),
      ...(directSendThreshold != null ? { directSendThreshold } : {}),
    }),
  });
  if (enablePgNotify || !allowDocCreate) {
    try {
      docUpdateBus = await PostgresDocUpdateBus.create({
        postgresUrl,
        channel: pgNotifyChannel,
        onDocUpdate: (docId) => docs.notifyDocUpdate(docId),
      });
    } catch (err) {
      await docs.closeAll();
      await opAuthStore.close();
      await capabilityMaterialStore.close();
      throw err;
    }
  }

  let readinessProbe: SyncServerReadinessProbe | undefined;
  try {
    readinessProbe = await createReadinessProbe(postgresUrl);
  } catch (err) {
    await docs.closeAll();
    await docUpdateBus?.close();
    await opAuthStore.close();
    await capabilityMaterialStore.close();
    throw err;
  }

  const builtInAuthHook =
    authCapabilityIssuerPublicKeys.length > 0
      ? createCapabilityTokenAuthHook(authCapabilityIssuerPublicKeys)
      : authToken
      ? createStaticTokenAuthHook(authToken)
      : undefined;

  const builtInAuthorizeHook = combineUpgradeHooks(
    docIdPattern ? createDocIdPatternHook(docIdPattern) : undefined,
    !allowDocCreate ? createKnownDocHook((docId) => docUpdateBus!.hasDoc(docId)) : undefined
  );

  const hooks: WebSocketSyncServerHooks = {
    onRateLimit: combineUpgradeHooks(
      rateLimitMaxUpgrades > 0 ? createIpRateLimitHook(rateLimitMaxUpgrades, rateLimitWindowMs) : undefined,
      opts.hooks?.onRateLimit
    ),
    onAuthenticate: combineUpgradeHooks(builtInAuthHook, opts.hooks?.onAuthenticate),
    onAuthorize: combineUpgradeHooks(builtInAuthorizeHook, opts.hooks?.onAuthorize),
    onError: opts.hooks?.onError,
  };

  const server = await (async () => {
    try {
      return await startWebSocketSyncServer<Operation>({
        host,
        port,
        maxPayloadBytes,
        codec: treecrdtSyncV0ProtobufCodec,
        docs: docs.provider,
        hooks,
        healthCheck: async () => {
          try {
            await readinessProbe!.check();
            return { ok: true };
          } catch {
            return {
              ok: false,
              statusCode: 503,
              body: "postgres unavailable",
            };
          }
        },
        statusInfo: async () => {
          let ready = true;
          let readyDetail = "ok";
          try {
            await readinessProbe!.check();
          } catch (error) {
            ready = false;
            readyDetail = errorMessage(error);
          }

          return {
            ok: ready,
            ready,
            readyDetail,
            service: packageName,
            version: packageVersion ?? null,
            gitSha: gitSha ?? null,
            gitDirty,
            buildRef: gitSha ? `${gitSha}${gitDirty ? "-dirty" : ""}` : null,
            protocolVersion: 0,
            startedAt,
            uptimeMs: Number.isFinite(startedAtMs) ? Math.max(0, Date.now() - startedAtMs) : null,
            backendModule,
            authMode: describeAuthMode(authToken, authCapabilityIssuerPublicKeys),
            pgNotifyEnabled: Boolean(docUpdateBus),
            pgNotifyChannel: docUpdateBus ? pgNotifyChannel : null,
            docIdPattern: docIdPattern?.source ?? null,
            allowDocCreate,
            idleCloseMs,
            maxPayloadBytes,
          };
        },
      });
    } catch (err) {
      await docs.closeAll();
      await docUpdateBus?.close();
      await opAuthStore.close();
      await capabilityMaterialStore.close();
      await readinessProbe?.close?.();
      throw err;
    }
  })();

  return {
    host: server.host,
    port: server.port,
    idleCloseMs,
    backendModule,
    close: async () => {
      await server.close();
      await docs.closeAll();
      await docUpdateBus?.close();
      await opAuthStore.close();
      await capabilityMaterialStore.close();
      await readinessProbe?.close?.();
    },
  };
}
