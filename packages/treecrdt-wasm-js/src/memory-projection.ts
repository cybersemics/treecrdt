import { normalizeNodeId } from '@treecrdt/interface/ids';
import type { MemoryChanges, MemoryContent, MemoryReader } from './memory-client.js';

export type MemoryProjectionOptions<T> = {
  /** The decoder is synchronous and pure; decoded values must be treated as immutable. */
  decode(content: MemoryContent): T | undefined;
};

export interface MemoryProjection<T> {
  get(id: string): T | undefined;
  /** Filters decoded-out children; a decoded-out parent has no visible children. */
  getChildren(id: string): readonly string[];
  /** Projection-local invalidation revision, including provisional writes and their rollback. */
  readonly revision: number;
}

function sameContent(before: MemoryContent | null, after: MemoryContent | null): boolean {
  if (!before || !after) return before === after;
  const a = before.payload;
  const b = after.payload;
  return (
    before.parentId === after.parentId &&
    (a === null || b === null
      ? a === b
      : a.length === b.length && a.every((byte, index) => byte === b[index]))
  );
}

/** Caches requested decoded content; native readers continue to own topology. */
export function createMemoryProjection<T>(
  reader: Pick<MemoryReader, 'getContent' | 'getChildren' | 'nodeIds'>,
  options: MemoryProjectionOptions<T>,
  check: () => void,
  evaluate: <R>(callback: () => R) => R,
) {
  const entries = new Map<string, T | undefined>();
  const children = new Map<
    string,
    { raw: readonly string[] | undefined; filtered: readonly string[]; dirty?: boolean }
  >();
  let revision = 0;
  let checkpoint: number | undefined;
  const decode = (content: MemoryContent | undefined): T | undefined =>
    content ? evaluate(() => options.decode(content)) : undefined;
  const get = (id: string): T | undefined => {
    check();
    const key = normalizeNodeId(id);
    if (!entries.has(key)) entries.set(key, decode(reader.getContent(key)));
    return entries.get(key);
  };
  const apply = ({ reset, changes }: Pick<MemoryChanges, 'reset' | 'changes'>) => {
    if (!reset && !changes.length) return;
    const updates = new Map<string, T | undefined>();
    if (reset) {
      for (const id of reader.nodeIds()) decode(reader.getContent(id));
    } else {
      for (const { id, before, after } of changes) {
        if (sameContent(before, after)) continue;
        const old = entries.has(id) ? entries.get(id) : decode(before ?? undefined);
        const next = decode(after ?? undefined);
        updates.set(id, next);
        if ((old === undefined) !== (next === undefined)) {
          for (const parent of [before?.parentId, after?.parentId]) {
            const cached = parent ? children.get(parent) : undefined;
            if (cached) cached.dirty = true;
          }
        }
      }
    }
    // Validate every changed row before publishing, including rows that have not been requested.
    if (reset) {
      entries.clear();
      children.clear();
    }
    for (const [id, entry] of updates) if (entries.has(id)) entries.set(id, entry);
    revision++;
  };
  const projection: MemoryProjection<T> = {
    get,
    getChildren: (id) => {
      check();
      const key = normalizeNodeId(id);
      const raw = get(key) === undefined ? undefined : reader.getChildren(key);
      const previous = children.get(key);
      if (!previous || previous.raw !== raw || previous.dirty) {
        const next = raw?.filter((child) => get(child) !== undefined) ?? [];
        const unchanged =
          previous &&
          previous.filtered.length === next.length &&
          previous.filtered.every((child, i) => child === next[i]);
        children.set(key, { raw, filtered: unchanged ? previous.filtered : Object.freeze(next) });
      }
      return children.get(key)!.filtered;
    },
    get revision() {
      check();
      return revision;
    },
  };
  apply({ reset: true, changes: [] });
  revision = 0;
  return {
    projection,
    apply,
    begin: () => {
      checkpoint = revision;
    },
    commit: () => {
      checkpoint = undefined;
    },
    rollback: () => {
      if (checkpoint === undefined) return;
      if (revision !== checkpoint) revision++;
      checkpoint = undefined;
      entries.clear();
      children.clear();
    },
    close: () => {
      entries.clear();
      children.clear();
    },
  };
}
