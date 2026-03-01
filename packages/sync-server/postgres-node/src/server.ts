import path from "node:path";
import { pathToFileURL } from "node:url";

import type { Operation } from "@treecrdt/interface";
import type { SyncBackend, SyncPeer } from "@treecrdt/sync";
import { treecrdtSyncV0ProtobufCodec } from "@treecrdt/sync/protobuf";
import type { WebSocketSyncServerDocHandle, WebSocketSyncServerDocProvider } from "@treecrdt/sync-server-core";
import { startWebSocketSyncServer } from "@treecrdt/sync-server-core";

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
};

export type SyncServerOptions = {
  host?: string;
  port?: number;
  postgresUrl: string;
  backendModule?: string;
  idleCloseMs?: number;
  maxPayloadBytes?: number;
};

export type SyncServerHandle = {
  host: string;
  port: number;
  idleCloseMs: number;
  backendModule: string;
  close: () => Promise<void>;
};

type DocContext = {
  docId: string;
  backend: SyncBackend<Operation>;
  peers: Set<SyncPeer<Operation>>;
  connections: number;
  applyQueue: Promise<void>;
  closeTimer?: NodeJS.Timeout;
};

type PostgresNodeDocStore = {
  provider: WebSocketSyncServerDocProvider<Operation>;
  closeAll: () => Promise<void>;
};

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

  const closeContext = async (ctx: DocContext): Promise<void> => {
    if (ctx.closeTimer) {
      clearTimeout(ctx.closeTimer);
      ctx.closeTimer = undefined;
    }
    docs.delete(ctx.docId);
    try {
      await (ctx.backend as any)?.close?.();
    } catch {
      // ignore close failures
    }
  };

  const scheduleClose = (ctx: DocContext): void => {
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
      for (const peer of ctx.peers) void peer.notifyLocalUpdate();
    };
    return {
      ...backend,
      docId: ctx.docId,
      applyOps,
    };
  };

  const openContext = async (docId: string): Promise<DocContext> => {
    const existing = docs.get(docId);
    if (existing) return existing;

    const rawBackend = await opts.backendFactory.open(docId);
    const ctx: DocContext = {
      docId,
      backend: rawBackend,
      peers: new Set(),
      connections: 0,
      applyQueue: Promise.resolve(),
    };
    ctx.backend = wrapBackendForContext(ctx, rawBackend);
    docs.set(docId, ctx);
    return ctx;
  };

  return {
    provider: {
      open: async (docId: string): Promise<WebSocketSyncServerDocHandle<Operation>> => {
        const cleanDocId = ensureNonEmptyString("docId", docId);
        const ctx = await openContext(cleanDocId);

        ctx.connections += 1;
        if (ctx.closeTimer) {
          clearTimeout(ctx.closeTimer);
          ctx.closeTimer = undefined;
        }

        return {
          backend: ctx.backend,
          onPeerAdded: (peer) => ctx.peers.add(peer),
          onPeerRemoved: (peer) => ctx.peers.delete(peer),
          release: async () => {
            ctx.connections -= 1;
            if (ctx.connections <= 0) scheduleClose(ctx);
          },
        };
      },
    },
    closeAll: async () => {
      await Promise.all(Array.from(docs.values()).map((ctx) => closeContext(ctx)));
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
  const idleCloseMs = Number(opts.idleCloseMs ?? 30_000);
  const maxPayloadBytes = Number(opts.maxPayloadBytes ?? 10 * 1024 * 1024);

  if (!Number.isFinite(port) || port <= 0) throw new Error(`invalid port: ${opts.port}`);
  if (!Number.isFinite(idleCloseMs) || idleCloseMs < 0) throw new Error(`invalid idleCloseMs: ${opts.idleCloseMs}`);
  if (!Number.isFinite(maxPayloadBytes) || maxPayloadBytes <= 0) {
    throw new Error(`invalid maxPayloadBytes: ${opts.maxPayloadBytes}`);
  }

  const module = await loadPostgresBackendModule(backendModule);
  const backendFactory = module.createPostgresNapiSyncBackendFactory(postgresUrl);
  if (backendFactory.ensureSchema) await backendFactory.ensureSchema();

  const docs = createPostgresNodeDocStore({ backendFactory, idleCloseMs });
  const server = await startWebSocketSyncServer<Operation>({
    host,
    port,
    maxPayloadBytes,
    codec: treecrdtSyncV0ProtobufCodec,
    docs: docs.provider,
  });

  return {
    host: server.host,
    port: server.port,
    idleCloseMs,
    backendModule,
    close: async () => {
      await server.close();
      await docs.closeAll();
    },
  };
}
