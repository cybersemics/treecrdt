import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

import Database from "better-sqlite3";

import type { Operation } from "@treecrdt/interface";
import { nodeIdToBytes16, replicaIdToBytes } from "@treecrdt/interface/ids";
import { decodeSqliteOpRefs, decodeSqliteOps } from "@treecrdt/interface/sqlite";
import { createSqliteNodeApi, loadTreecrdtExtension } from "@treecrdt/sqlite-node";
import type { SyncBackend } from "@treecrdt/sync";
import type { SyncPeer } from "@treecrdt/sync";
import { treecrdtSyncV0ProtobufCodec } from "@treecrdt/sync/protobuf";
import type { WebSocketSyncServerDocHandle, WebSocketSyncServerDocProvider } from "@treecrdt/sync-server-core";
import { startWebSocketSyncServer } from "@treecrdt/sync-server-core";

export type SqliteNodeDocStoreOptions = {
  dbDir: string;
  idleCloseMs?: number;
};

export type SyncServerOptions = {
  host?: string;
  port?: number;
  dbDir?: string;
  idleCloseMs?: number;
  maxPayloadBytes?: number;
};

export type SyncServerHandle = {
  host: string;
  port: number;
  dbDir: string;
  idleCloseMs: number;
  close: () => Promise<void>;
};

type DocContext = {
  docId: string;
  dbPath: string;
  db: Database.Database;
  backend: SyncBackend<Operation>;
  peers: Set<SyncPeer<Operation>>;
  connections: number;
  applyQueue: Promise<void>;
  closeTimer?: NodeJS.Timeout;
};

function docDbPath(dbDir: string, docId: string): string {
  const hash = crypto.createHash("sha256").update(docId, "utf8").digest("hex");
  return path.join(dbDir, `doc-${hash}.sqlite3`);
}

export function createSqliteNodeDocStore(
  opts: SqliteNodeDocStoreOptions
): WebSocketSyncServerDocProvider<Operation> {
  const dbDir = path.resolve(opts.dbDir);
  const idleCloseMs = Number(opts.idleCloseMs ?? 30_000);
  if (!Number.isFinite(idleCloseMs) || idleCloseMs < 0) throw new Error(`invalid idleCloseMs: ${opts.idleCloseMs}`);

  const docs = new Map<string, DocContext>();

  const scheduleClose = (ctx: DocContext): void => {
    if (ctx.closeTimer) return;
    ctx.closeTimer = setTimeout(() => {
      ctx.closeTimer = undefined;
      if (ctx.connections > 0) return;
      docs.delete(ctx.docId);
      try {
        ctx.db.close();
      } catch {}
    }, idleCloseMs);
  };

  const openDocContext = async (docId: string): Promise<DocContext> => {
    const existing = docs.get(docId);
    if (existing) return existing;

    await fs.mkdir(dbDir, { recursive: true });

    const dbPath = docDbPath(dbDir, docId);
    const db = new Database(dbPath);
    db.pragma("journal_mode = WAL");

    loadTreecrdtExtension(db);
    const api = createSqliteNodeApi(db, { maxBulkOps: 5_000 });

    const storedDocId = await api.docId();
    if (!storedDocId) {
      await api.setDocId(docId);
    } else if (storedDocId !== docId) {
      db.close();
      throw new Error(`docId mismatch for ${dbPath}: expected ${docId}, got ${storedDocId}`);
    }

    const ctx: DocContext = {
      docId,
      dbPath,
      db,
      peers: new Set(),
      connections: 0,
      applyQueue: Promise.resolve(),
      backend: undefined as any,
    };
    docs.set(docId, ctx);

    ctx.backend = {
      docId,
      maxLamport: async () => BigInt(await api.headLamport()),
      listOpRefs: async (filter) => {
        if ("all" in filter) {
          return decodeSqliteOpRefs(await api.opRefsAll());
        }
        const parent = Buffer.from(filter.children.parent);
        return decodeSqliteOpRefs(await api.opRefsChildren(parent));
      },
      getOpsByOpRefs: async (opRefs) => {
        if (opRefs.length === 0) return [];
        return decodeSqliteOps(await api.opsByOpRefs(opRefs));
      },
      applyOps: async (ops) => {
        if (ops.length === 0) return;

        const serializeNodeId = (val: string) => Buffer.from(nodeIdToBytes16(val));
        const serializeReplica = (replica: Operation["meta"]["id"]["replica"]) =>
          Buffer.from(replicaIdToBytes(replica));
        ctx.applyQueue = ctx.applyQueue.then(async () => {
          await api.appendOps?.(ops, serializeNodeId, serializeReplica);
        });
        await ctx.applyQueue;
        for (const peer of ctx.peers) void peer.notifyLocalUpdate();
      },
    };

    return ctx;
  };

  return {
    async open(docId: string): Promise<WebSocketSyncServerDocHandle<Operation>> {
      const ctx = await openDocContext(docId);
      ctx.connections += 1;
      if (ctx.closeTimer) {
        clearTimeout(ctx.closeTimer);
        ctx.closeTimer = undefined;
      }

      return {
        backend: ctx.backend,
        onPeerAdded: (peer) => ctx.peers.add(peer),
        onPeerRemoved: (peer) => ctx.peers.delete(peer),
        release: () => {
          ctx.connections -= 1;
          if (ctx.connections <= 0) scheduleClose(ctx);
        },
      };
    },
  };
}

export async function startSyncServer(opts: SyncServerOptions = {}): Promise<SyncServerHandle> {
  const host = opts.host ?? "0.0.0.0";
  const port = Number(opts.port ?? 8787);
  const dbDir = path.resolve(opts.dbDir ?? path.join(process.cwd(), "data"));
  const idleCloseMs = Number(opts.idleCloseMs ?? 30_000);
  const maxPayloadBytes = Number(opts.maxPayloadBytes ?? 10 * 1024 * 1024);

  if (!Number.isFinite(port) || port < 0) throw new Error(`invalid port: ${opts.port}`);
  if (!Number.isFinite(idleCloseMs) || idleCloseMs < 0) throw new Error(`invalid idleCloseMs: ${opts.idleCloseMs}`);

  const docs = createSqliteNodeDocStore({ dbDir, idleCloseMs });

  const server = await startWebSocketSyncServer<Operation>({
    host,
    port,
    maxPayloadBytes,
    codec: treecrdtSyncV0ProtobufCodec,
    docs,
    onPeerError: (err, ctx) => {
      console.error("TreeCRDT peer message handler failed", {
        docId: ctx.docId,
        type: ctx.messageType,
        err,
      });
    },
  });

  return {
    host: server.host,
    port: server.port,
    dbDir,
    idleCloseMs,
    close: server.close,
  };
}

