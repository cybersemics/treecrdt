import { dbGetText } from './sql.js';
import type { Database } from './types.js';
import { nodeIdToBytes16, replicaIdToBytes } from '@treecrdt/interface/ids';
import type { Operation } from '@treecrdt/interface';
import type { TreecrdtAdapter } from '@treecrdt/interface';
import type { OpenTreecrdtDbResult } from './open.js';
import { clearOpfsStorage, type OpfsVfsKind } from './opfs.js';
import { rpcBinaryResult, type RpcInitResult, type RpcSqlParams } from './rpc.js';

export function openedToRpcInitResult(opened: OpenTreecrdtDbResult): RpcInitResult {
  return opened.opfsError
    ? { storage: opened.storage, filename: opened.filename, opfsError: opened.opfsError }
    : { storage: opened.storage, filename: opened.filename };
}

/**
 * Shared wa-sqlite + TreeCRDT adapter state for dedicated or shared worker entrypoints.
 * Lifecycle (`init`, port `close`, `broadcastMaterialized`) stays in each worker file.
 */
export class CommonWorkerSession {
  db: Database | null = null;
  api: TreecrdtAdapter | null = null;
  storedFilename: string | undefined;
  storedStorage: 'memory' | 'opfs' = 'memory';
  storedOpfsVfsKind: OpfsVfsKind | undefined;
  storedOpfsVfsName: string | undefined;

  protected onAfterReset(): void {}

  applyOpened(opened: OpenTreecrdtDbResult): void {
    this.db = opened.db;
    this.api = opened.api;
    this.storedFilename = opened.filename;
    this.storedStorage = opened.storage;
    this.storedOpfsVfsKind = opened.opfsVfsKind;
    this.storedOpfsVfsName = opened.opfsVfsName;
  }

  async closeDbAndReset(): Promise<void> {
    if (this.db?.close) await this.db.close();
    this.db = null;
    this.api = null;
    this.storedFilename = undefined;
    this.storedStorage = 'memory';
    this.storedOpfsVfsKind = undefined;
    this.storedOpfsVfsName = undefined;
    this.onAfterReset();
  }

  async drop(): Promise<null> {
    const filename = this.storedFilename;
    const storage = this.storedStorage;
    const opfsVfsKind = this.storedOpfsVfsKind;
    const opfsVfsName = this.storedOpfsVfsName;
    await this.closeDbAndReset();
    if (storage === 'opfs' && filename) {
      await clearOpfsStorage(filename, { vfsKind: opfsVfsKind, vfsName: opfsVfsName });
    }
    return null;
  }

  ensureApi(): TreecrdtAdapter {
    if (!this.db || !this.api) throw new Error('db not initialized');
    return this.api;
  }

  ensureDb(): Database {
    if (!this.db) throw new Error('db not initialized');
    return this.db;
  }

  async sqlExec(sql: string): Promise<null> {
    await this.ensureDb().exec(sql);
    return null;
  }

  async sqlGetText(sql: string, params?: RpcSqlParams): Promise<string | null> {
    return dbGetText(this.ensureDb(), sql, params ?? []);
  }

  async append(op: Operation) {
    return await this.ensureApi().appendOp(op, nodeIdToBytes16, replicaIdToBytes);
  }

  async appendMany(ops: Operation[]) {
    return await this.ensureApi().appendOps!(ops, nodeIdToBytes16, replicaIdToBytes);
  }

  async opsSince(lamport: number, root: string | undefined) {
    return await this.ensureApi().opsSince(lamport, root);
  }

  async opRefsAll() {
    return await this.ensureApi().opRefsAll();
  }

  async opRefsChildren(parent: string) {
    return await this.ensureApi().opRefsChildren(nodeIdToBytes16(parent));
  }

  async opsByOpRefs(opRefs: number[][]) {
    return await this.ensureApi().opsByOpRefs(opRefs.map((r) => Uint8Array.from(r)));
  }

  async treeChildren(parent: string) {
    return await this.ensureApi().treeChildren(nodeIdToBytes16(parent));
  }

  async treeChildrenPage(
    parent: string,
    cursor: { orderKey: number[]; node: number[] } | null,
    limit: number,
  ) {
    const cursorBytes = cursor
      ? {
          orderKey: Uint8Array.from(cursor.orderKey),
          node: Uint8Array.from(cursor.node),
        }
      : null;
    return await this.ensureApi().treeChildrenPage!(nodeIdToBytes16(parent), cursorBytes, limit);
  }

  async treeDump() {
    return await this.ensureApi().treeDump();
  }

  async treePayload(node: string) {
    const payload = await this.ensureApi().treePayload(nodeIdToBytes16(node));
    return rpcBinaryResult(payload);
  }

  async treeNodeCount() {
    return await this.ensureApi().treeNodeCount();
  }

  async treeParent(node: string) {
    const result = await this.ensureApi().treeParent(nodeIdToBytes16(node));
    return rpcBinaryResult(result);
  }

  async treeExists(node: string) {
    return await this.ensureApi().treeExists(nodeIdToBytes16(node));
  }

  async headLamport() {
    return await this.ensureApi().headLamport();
  }

  async replicaMaxCounter(replica: number[]) {
    return await this.ensureApi().replicaMaxCounter(Uint8Array.from(replica));
  }
}

/** RPC methods shared by dedicated and shared workers (excluding lifecycle). */
export function createCommonWorkerRpcHandlers(session: CommonWorkerSession) {
  return {
    sqlExec: (sql: string) => session.sqlExec(sql),
    sqlGetText: (sql: string, params?: RpcSqlParams) => session.sqlGetText(sql, params),
    append: (op: Operation) => session.append(op),
    appendMany: (ops: Operation[]) => session.appendMany(ops),
    opsSince: (lamport: number, root?: string) => session.opsSince(lamport, root),
    opRefsAll: () => session.opRefsAll(),
    opRefsChildren: (parent: string) => session.opRefsChildren(parent),
    opsByOpRefs: (opRefs: number[][]) => session.opsByOpRefs(opRefs),
    treeChildren: (parent: string) => session.treeChildren(parent),
    treeChildrenPage: (
      parent: string,
      cursor: { orderKey: number[]; node: number[] } | null,
      limit: number,
    ) => session.treeChildrenPage(parent, cursor, limit),
    treeDump: () => session.treeDump(),
    treeNodeCount: () => session.treeNodeCount(),
    treeParent: (node: string) => session.treeParent(node),
    treeExists: (node: string) => session.treeExists(node),
    treePayload: (node: string) => session.treePayload(node),
    headLamport: () => session.headLamport(),
    replicaMaxCounter: (replica: number[]) => session.replicaMaxCounter(replica),
  } as const;
}
