import type { Operation, TreecrdtAdapter } from '@treecrdt/interface';
import { nodeIdToBytes16, replicaIdToBytes } from '@treecrdt/interface/ids';
import type { MaterializationEvent, MaterializationOutcome } from '@treecrdt/interface/engine';
import type { OpfsVfsKind } from './opfs.js';
import { clearOpfsStorage } from './opfs.js';
import type { OpenTreecrdtDbOptions, OpenTreecrdtDbResult } from './open-core.js';
import { dbGetText } from './sql.js';
import type { Database, StorageMode } from './types.js';

export type BackendOpenFn = (opts: OpenTreecrdtDbOptions) => Promise<OpenTreecrdtDbResult>;

export type BackendInitConfig = {
  baseUrl?: string;
  filename?: string;
  storage: StorageMode;
  docId: string;
  fallback: 'memory' | 'throw';
  opfsVfs?: OpfsVfsKind;
};

export type BackendInitResult = {
  storage: StorageMode;
  filename: string;
  opfsError?: string;
};

export type MaterializationListener = (event: MaterializationEvent) => void;

export type SqlParam = number | string | null | Uint8Array;
export type TreeChildrenCursor = { orderKey: Uint8Array; node: Uint8Array };

/**
 * Single TreeCRDT + wa-sqlite session used by every runtime strategy.
 * Callers talk to these methods directly (in-process or via Comlink).
 */
export class TreecrdtBackend {
  private db: Database | null = null;
  private api: TreecrdtAdapter | null = null;
  private storedFilename: string | undefined;
  private storedStorage: StorageMode = 'memory';
  private readonly listeners = new Set<MaterializationListener>();
  private queue: Promise<void> = Promise.resolve();

  constructor(private readonly openDb: BackendOpenFn) {}

  get storage(): StorageMode {
    return this.storedStorage;
  }

  get filename(): string | undefined {
    return this.storedFilename;
  }

  subscribeMaterialized(listener: MaterializationListener): void {
    this.listeners.add(listener);
  }

  unsubscribeMaterialized(listener: MaterializationListener): void {
    this.listeners.delete(listener);
  }

  async init(config: BackendInitConfig): Promise<BackendInitResult> {
    return this.run(async () => {
      await this.closeDbUnlocked();
      const opened = await this.openDb({
        baseUrl: config.baseUrl,
        filename: config.filename,
        storage: config.storage,
        docId: config.docId,
        requireOpfs: config.fallback === 'throw',
        opfsVfs: config.opfsVfs,
        onMaterialized: (event) => this.emitMaterialized(event),
      });
      this.applyOpened(opened);
      return toInitResult(opened);
    });
  }

  async close(): Promise<void> {
    return this.run(() => this.closeDbUnlocked());
  }

  async drop(): Promise<void> {
    return this.run(async () => {
      const filename = this.storedFilename;
      const storage = this.storedStorage;
      await this.closeDbUnlocked();
      if (storage === 'opfs' && filename) {
        await clearOpfsStorage(filename);
      }
    });
  }

  async sqlExec(sql: string): Promise<void> {
    return this.run(async () => {
      await this.ensureDb().exec(sql);
    });
  }

  async sqlGetText(sql: string, params: SqlParam[] = []): Promise<string | null> {
    return this.run(() => dbGetText(this.ensureDb(), sql, params));
  }

  async append(op: Operation): Promise<MaterializationOutcome> {
    return this.run(async () => this.ensureApi().appendOp(op, nodeIdToBytes16, replicaIdToBytes));
  }

  async appendMany(ops: Operation[]): Promise<MaterializationOutcome> {
    return this.run(async () => this.ensureApi().appendOps!(ops, nodeIdToBytes16, replicaIdToBytes));
  }

  async opsSince(lamport: number, root?: string): Promise<unknown[]> {
    return this.run(async () => this.ensureApi().opsSince(lamport, root));
  }

  async opRefsAll(): Promise<unknown[]> {
    return this.run(async () => this.ensureApi().opRefsAll());
  }

  async opRefsChildren(parent: string): Promise<unknown[]> {
    return this.run(async () => this.ensureApi().opRefsChildren(nodeIdToBytes16(parent)));
  }

  async opsByOpRefs(opRefs: Uint8Array[]): Promise<unknown[]> {
    return this.run(async () => this.ensureApi().opsByOpRefs(opRefs));
  }

  async treeChildren(parent: string): Promise<unknown[]> {
    return this.run(async () => this.ensureApi().treeChildren(nodeIdToBytes16(parent)));
  }

  async treeChildrenPage(
    parent: string,
    cursor: TreeChildrenCursor | null,
    limit: number,
  ): Promise<unknown[]> {
    return this.run(async () =>
      this.ensureApi().treeChildrenPage!(nodeIdToBytes16(parent), cursor, limit),
    );
  }

  async treeDump(): Promise<unknown[]> {
    return this.run(async () => this.ensureApi().treeDump());
  }

  async treePayload(node: string): Promise<Uint8Array | null> {
    return this.run(async () => {
      const payload = await this.ensureApi().treePayload(nodeIdToBytes16(node));
      return cloneBinary(payload);
    });
  }

  async treeNodeCount(): Promise<number> {
    return this.run(async () => this.ensureApi().treeNodeCount());
  }

  async treeParent(node: string): Promise<Uint8Array | null> {
    return this.run(async () => {
      const parent = await this.ensureApi().treeParent(nodeIdToBytes16(node));
      return cloneBinary(parent);
    });
  }

  async treeExists(node: string): Promise<boolean> {
    return this.run(async () => this.ensureApi().treeExists(nodeIdToBytes16(node)));
  }

  async headLamport(): Promise<number> {
    return this.run(async () => this.ensureApi().headLamport());
  }

  async replicaMaxCounter(replica: Uint8Array): Promise<number> {
    return this.run(async () => this.ensureApi().replicaMaxCounter(replica));
  }

  /**
   * Fan out a materialization event to subscribed listeners.
   * Shared-worker peer notify uses `exclude` so the originating tab is not double-notified.
   */
  emitMaterialized(event: MaterializationEvent, exclude?: MaterializationListener): void {
    if (event.changes.length === 0) return;
    for (const listener of this.listeners) {
      if (listener === exclude) continue;
      try {
        listener(event);
      } catch {
        // Listener failures must not break the session.
      }
    }
  }

  private applyOpened(opened: OpenTreecrdtDbResult): void {
    this.db = opened.db;
    this.api = opened.api;
    this.storedFilename = opened.filename;
    this.storedStorage = opened.storage;
  }

  private async closeDbUnlocked(): Promise<void> {
    if (this.db?.close) await this.db.close();
    this.db = null;
    this.api = null;
    this.storedFilename = undefined;
    this.storedStorage = 'memory';
  }

  private ensureApi(): TreecrdtAdapter {
    if (!this.db || !this.api) throw new Error('db not initialized');
    return this.api;
  }

  private ensureDb(): Database {
    if (!this.db) throw new Error('db not initialized');
    return this.db;
  }

  private run<T>(work: () => Promise<T>): Promise<T> {
    const next = this.queue.then(work, work);
    this.queue = next.then(
      () => undefined,
      () => undefined,
    );
    return next;
  }
}

function toInitResult(opened: OpenTreecrdtDbResult): BackendInitResult {
  return opened.opfsError
    ? { storage: opened.storage, filename: opened.filename, opfsError: opened.opfsError }
    : { storage: opened.storage, filename: opened.filename };
}

/** Ensure binary results are structured-clone / transferable friendly. */
function cloneBinary(bytes: Uint8Array | null): Uint8Array | null {
  if (bytes === null) return null;
  const buffer = bytes.buffer;
  if (
    buffer instanceof ArrayBuffer &&
    bytes.byteOffset === 0 &&
    bytes.byteLength === buffer.byteLength
  ) {
    return bytes;
  }
  return Uint8Array.from(bytes);
}
