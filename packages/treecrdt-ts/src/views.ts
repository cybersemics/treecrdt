/**
 * A view orders groups of nodes as subtrees of the document, so the order replicates and can hold
 * folders and nesting. A group is a key of a payload index: the nodes whose payload holds it.
 *
 * - Membership is the payload index's, read with each call, so it always matches the payloads and
 *   nothing derives or stores it. Each member's rank and data are read from its payload alike.
 * - A member lists at a default key built from its `rank` until someone places it, and nothing is
 *   stored for it. A placement adds the group's node under the view root if it is missing, and a
 *   reference node for the item placed. The top level of a group merges the group node's children,
 *   by order key, with the members that have no reference, by default key, in one key space, so an
 *   item dropped between two members takes a key between theirs. Below the top level, the order is
 *   TreeCRDT's: references and folders as ordinary siblings, walked depth first.
 * - The group's node holds the group key as its payload, in plaintext like the keys the payload
 *   index reads, so a change under it maps back to the group.
 * - The view root, a group's node and the reference of a member that an item goes under are written
 *   with create-only inserts (`insertIfAbsent`) at keys every device derives alike, so devices that
 *   write one at once agree, and none moves a node someone placed. A drag inserts its reference, or
 *   moves it.
 * - A reference never creates membership. One whose member left the group is skipped, its children
 *   shown in its place, and applies again when the member returns. A member whose reference the
 *   walk does not reach, such as a deleted one, lists at its default key.
 * - View nodes live in the same document, under a view root that the first placement adds among the
 *   root's children and every replica then has. Everything that reads the root's children, walks
 *   the tree or listens to `onMaterialized` must skip ids that start with `VIEW_NODE_PREFIX`, and a
 *   writer whose capability covers only a subtree cannot order a group.
 * - Views read the SQLite extension's `tree_nodes` and `tree_payload` tables directly, for order
 *   keys, deleted nodes and nodes not yet in the tree, which `tree.read` leaves out, so they run on
 *   the SQLite engines only and follow that schema. With a local `authSession`, their reads and
 *   `onChange` have the payload index's limit (see `@treecrdt/interface/payload-index`).
 */
import { sha256 } from '@noble/hashes/sha2';
import { randomBytes } from '@noble/hashes/utils';
import type { LocalWriteOptions, MaterializationEvent, TreecrdtEngine } from './engine.js';
import { bytesToHex, hexToBytes, ROOT_NODE_ID_HEX } from './ids.js';
import type { Operation, ReplicaId } from './index.js';
import {
  createTreecrdtPayloadIndex,
  type TreecrdtPayloadIndex,
  type TreecrdtPayloadIndexMember,
  type TreecrdtPayloadIndexOptions,
} from './payload-index.js';
import type { SqliteRunner, TreecrdtSqlitePlacement } from './sqlite.js';

/**
 * First 6 id bytes of every node a view writes, so a consumer can skip them. Shorter would risk
 * matching random node ids. The other 10 bytes are a hash, or random for a folder. The low bit of
 * the byte after the prefix is set in a folder's id and clear in every other view node's, so a
 * replica that has not received a reference's member can still tell the reference from a folder.
 */
export const VIEW_NODE_PREFIX = 'e7c5e1f5a0d1';
const MAX_RANK = 2 ** 48 - 1;
/** Marks a folder id: see `VIEW_NODE_PREFIX`. */
const FOLDER_BIT = 0x01;

export type TreecrdtViewClient = Pick<TreecrdtEngine, 'docId' | 'local' | 'onMaterialized'> & {
  runner: SqliteRunner;
};

export type TreecrdtViewNode = {
  id: string;
  /** Its payload as JSON, as SQLite's JSON functions read it to find its keys. */
  payload: unknown;
};

export type TreecrdtViewOptions = {
  /** Names the view, whose node ids derive from it and the doc id. */
  name: string;
  /**
   * The payload index whose keys are the groups, or the options to open one, which the view then
   * closes with itself.
   */
  index: TreecrdtPayloadIndex | TreecrdtPayloadIndexOptions;
  /**
   * A member's default position, such as its creation time: where it lists until someone places
   * it. Clamped into [0, 2^48); a non-finite rank counts as 0. Read with the members, so a new rank
   * shows at once, though `onChange` reports none.
   */
  rank: (node: TreecrdtViewNode) => number;
  /** A value returned with each member, read from its payload. `onChange` reports no new value. */
  data?: (node: TreecrdtViewNode) => unknown;
  /**
   * What a reference stands for. 'member' (default): the member. 'parent': the member in its
   * context, its parent, so members of a group under one parent share a reference, and a member
   * deleted and created again under the same parent gets its place back. A move re-derives it.
   * Another value gives every reference another id, so groups lose their order: members list at
   * their default keys again. Choose it before any group is ordered.
   */
  reference?: 'member' | 'parent';
  /** Replica that writes the view's ops. */
  replica: ReplicaId;
  /** Options for each write, such as a write id or an auth session. */
  writeOptions?: () => LocalWriteOptions;
  /**
   * Receives what the view skipped: a member whose `rank` or `data` threw, which is in no group
   * while they throw, or a failing `onChange` listener.
   */
  onError?: (error: unknown, node?: string) => void;
};

export type TreecrdtViewMember = {
  id: string;
  /** Whether its reference is in the group's subtree; false where it lists at its default key. */
  placed: boolean;
  /** What `options.data` returned for the member, if given. */
  data?: unknown;
};

/**
 * An item of a group as `tree` returns it. Members that share a reference (`reference: 'parent'`)
 * list one after another, with the reference's children under the first.
 */
export type TreecrdtViewItem =
  | { kind: 'member'; id: string; data?: unknown; children: TreecrdtViewItem[] }
  | { kind: 'folder'; id: string; payload: Uint8Array | null; children: TreecrdtViewItem[] };

/** Where an item goes, in the order shown. */
export type TreecrdtViewPosition = {
  /** The member or folder to go into. Null or omitted: the top level. */
  parent?: string | null;
  /** The item of `parent` to go right before. Null or omitted: last. */
  before?: string | null;
};

export type TreecrdtView = {
  /** Members of each requested group in display order. Groups without members are absent. */
  members: (groups: readonly string[]) => Promise<Map<string, TreecrdtViewMember[]>>;
  /** Members and folders of `group`, nested as in its subtree, in display order. */
  tree: (group: string) => Promise<TreecrdtViewItem[]>;
  /**
   * Moves `item`, a member or folder of `group`, to `position` in the order this replica shows now,
   * and returns the ops written: the group's node if it is missing, the reference of `parent` if it
   * is a member without one, and the item's reference. Writes nothing if the item is already there
   * or no longer in the group. Goes to the top level if `parent` is no longer in the group, and
   * last if `before` is no longer in `parent`.
   */
  place: (group: string, item: string, position?: TreecrdtViewPosition) => Promise<Operation[]>;
  /** Adds a folder at `position`, writing what `place` would write before an item. */
  createFolder: (
    group: string,
    options?: TreecrdtViewPosition & { payload?: Uint8Array | null },
  ) => Promise<{ id: string; ops: Operation[] }>;
  /** Moves the folder's items to its place, in order, then deletes it. */
  removeFolder: (group: string, folder: string) => Promise<Operation[]>;
  /**
   * Called after each materialization event with the groups whose members or order may have
   * changed (possibly none) and the event itself: the keys the payload index reports, and the
   * groups whose subtree changed.
   */
  onChange: (listener: (groups: string[], event: MaterializationEvent) => void) => () => void;
  /** The payload index the view reads. */
  index: TreecrdtPayloadIndex;
  /** Resolves once `onChange` listeners have been called for every event emitted before the call. */
  idle: () => Promise<void>;
  /** Stops `onChange`, and closes the payload index if the view opened it. */
  close: () => Promise<void>;
};

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const hex4 = (digit: number) => digit.toString(16).padStart(4, '0');

const clampRank = (rank: number) =>
  Number.isFinite(rank) ? Math.min(MAX_RANK, Math.max(0, Math.floor(rank))) : 0;

/**
 * The first three order-key digits of a rank: a prefix of every default key with that rank. Small
 * numbers convert to hex much faster than the whole rank does.
 */
const rankDigits = (rank: number): string => {
  const hi = Math.floor(rank / 2 ** 32);
  const lo = rank % 2 ** 32;
  return hex4(hi & 0xffff) + hex4((lo >>> 16) & 0xffff) + hex4(lo & 0xffff);
};

/** The rank that a key's first three digits encode. */
const keyRank = (key: string) => parseInt(key.slice(0, 12).padEnd(12, '0'), 16);

/** A bound above `key` and below every default key of a later rank, if there is one. */
const rankBound = (key: string): string | null => {
  const bound = rankDigits(clampRank(keyRank(key) + 1));
  return bound > key ? bound : null;
};

const isFolder = (node: string) => {
  const marker = parseInt(node.slice(VIEW_NODE_PREFIX.length, VIEW_NODE_PREFIX.length + 2), 16);
  return node.startsWith(VIEW_NODE_PREFIX) && (marker & FOLDER_BIT) !== 0;
};

const between = (left: string | null, right: string | null): TreecrdtSqlitePlacement => ({
  type: 'between',
  left: left === null ? null : hexToBytes(left),
  right: right === null ? null : hexToBytes(right),
});
const at = (key: string): TreecrdtSqlitePlacement => ({ type: 'at', key: hexToBytes(key) });
/** Open bounds look up no sibling, unlike 'last'. */
const anywhere = between(null, null);

/** A member of a group, as its payload places it. */
type Membership = {
  node: string;
  /** What its reference stands for: the member, or with `reference: 'parent'`, its parent. */
  target: string;
  defaultKey: string;
  /** Present with `options.data`. */
  data?: unknown;
};

/** An item of a group's order: a node of its subtree, or members whose reference it lacks. */
type Slot = {
  /** Node id. Empty for members without a reference until a write needs its id. */
  node: string;
  /** Order key, hex. Members without a reference list at the least of their default keys. */
  key: string;
  folder: boolean;
  /** A folder's payload. */
  payload: Uint8Array | null;
  /** The members a reference stands for, by default key. None: its children show in its place. */
  members: Membership[];
  /**
   * Members whose reference the walk does not reach: none yet, one deleted or under a deleted node,
   * or one under a node whose insert has not arrived. They list at the top level.
   */
  implicit: boolean;
  /** Null at the top level. */
  parent: Slot | null;
  children: Slot[];
};

type Layout = {
  group: string;
  groupNode: string;
  /** Whether the group node is in the tree, deleted or not. */
  ordered: boolean;
  /** The group node's children merged with the members the walk does not reach, by key. */
  top: Slot[];
  /** Slots of the subtree's nodes. */
  nodes: Map<string, Slot>;
  /** The slot of each member. */
  slotOf: Map<string, Slot>;
};

const compareHex = (a: string, b: string) => (a < b ? -1 : a > b ? 1 : 0);

/** Siblings sort as TreeCRDT orders children: by order key, then node id. */
const slotOrder = (a: Slot, b: Slot) => compareHex(a.key, b.key) || compareHex(a.node, b.node);

/** Whether a slot shows as an item: a folder, or a reference with members. */
const shows = (slot: Slot) => slot.folder || slot.members.length > 0;

/** The nearest ancestor that shows as an item. */
const shownParent = (slot: Slot): Slot | null => {
  let parent = slot.parent;
  while (parent && !shows(parent)) parent = parent.parent;
  return parent;
};

/** The first item `slot` shows: itself, or for a reference whose member left, its children's. */
const firstShown = (slot: Slot): Slot | null => {
  if (shows(slot)) return slot;
  for (const child of slot.children) {
    const shown = firstShown(child);
    if (shown) return shown;
  }
  return null;
};

/** The item shown right after `slot` under its shown parent, null for the last. */
const nextShown = (top: Slot[], slot: Slot): Slot | null => {
  for (let current = slot; ; ) {
    const siblings = current.parent ? current.parent.children : top;
    for (const sibling of siblings.slice(siblings.indexOf(current) + 1)) {
      const shown = firstShown(sibling);
      if (shown) return shown;
    }
    // The children of a reference whose member left show in its place, so its next items follow.
    if (!current.parent || shows(current.parent)) return null;
    current = current.parent;
  }
};

/**
 * The placement under `container` (null: the top level) right after `left` and before `right`,
 * null for an end.
 */
const placementBetween = (
  container: Slot | null,
  left: { key: string } | null,
  right: { key: string } | null,
) =>
  between(
    left?.key ?? null,
    // Last at the top level: stay below every default key of a later rank, so members that join
    // later still list after this one.
    right ? right.key : !container && left ? rankBound(left.key) : null,
  );

const withData = (member: Membership) => ('data' in member ? { data: member.data } : {});

/** `make`, keeping what it returns for each key. */
const kept = <T>(make: (key: string) => T) => {
  const known = new Map<string, T>();
  return (key: string) => {
    let value = known.get(key);
    if (value === undefined) known.set(key, (value = make(key)));
    return value;
  };
};

/**
 * Opens the view `options.name` over `client`. It keeps nothing of its own besides the nodes it
 * writes: the payload index finds the members, and the document holds the order.
 */
export async function createTreecrdtView(
  client: TreecrdtViewClient,
  options: TreecrdtViewOptions,
): Promise<TreecrdtView> {
  const { runner } = client;
  const { name } = options;
  const byParent = options.reference === 'parent';
  const report = (error: unknown, node?: string) => {
    if (options.onError) options.onError(error, node);
    else console.error(`TreeCRDT view ${name}:`, error, node ?? '');
  };
  // Synchronous, since a read of an ordered group needs the id of each member's reference. The doc
  // id is in the message, so each document gets its own ids.
  const viewId = (...parts: string[]) => {
    const message = encoder.encode([client.docId, name, ...parts].join('\u0000'));
    const id = sha256(message).subarray(0, 10);
    id[0] &= ~FOLDER_BIT;
    return VIEW_NODE_PREFIX + bytesToHex(id);
  };
  const folderId = () => {
    const id = randomBytes(10);
    id[0] |= FOLDER_BIT;
    return VIEW_NODE_PREFIX + bytesToHex(id);
  };
  const root = viewId('root');
  const groupNode = kept((group) => viewId('group', group));
  // Each reference id is tagged by what it stands for, so switching `reference` never reuses one.
  const references = kept((group) =>
    kept((target) => viewId(byParent ? 'parent-ref' : 'ref', group, target)),
  );

  const index =
    'lookup' in options.index
      ? options.index
      : await createTreecrdtPayloadIndex(client, options.index);

  /**
   * Groups above `nodes`, deleted ones included: each group node holds its key, which must also
   * derive its id, so a payload written to it later maps it nowhere.
   */
  const groupsAbove = async (nodes: Set<string>): Promise<string[]> =>
    (
      JSON.parse(
        (await runner.getText(
          `WITH RECURSIVE up(node, parent) AS (
             SELECT node, parent FROM tree_nodes WHERE node IN (SELECT unhex(value) FROM json_each(?1))
             UNION
             SELECT t.node, t.parent FROM up JOIN tree_nodes t ON t.node = up.parent
           )
           SELECT json_group_array(json_array(lower(hex(up.node)), lower(hex(p.payload))))
           FROM up JOIN tree_payload p ON p.node = up.node
           WHERE up.parent = ?2 AND p.payload IS NOT NULL`,
          [JSON.stringify([...nodes]), hexToBytes(root)],
        )) ?? '[]',
      ) as [string, string][]
    )
      .map(([node, payload]) => [node, decoder.decode(hexToBytes(payload))] as const)
      .filter(([node, group]) => groupNode(group) === node)
      .map(([, group]) => group);

  const listeners = new Set<(groups: string[], event: MaterializationEvent) => void>();
  const stopReporting = index.onChange(async (keys, event) => {
    if (listeners.size === 0) return;
    const groups = new Set(keys);
    const viewNodes = new Set<string>();
    for (const change of event.changes) {
      if (change.node.startsWith(VIEW_NODE_PREFIX)) viewNodes.add(change.node);
      // A move also changes the group the node left. When the node went under a node whose insert
      // has not arrived, the walk up from it ends there, so that group is the only one found.
      const from = 'parentBefore' in change ? change.parentBefore : null;
      if (from?.startsWith(VIEW_NODE_PREFIX)) viewNodes.add(from);
    }
    if (viewNodes.size > 0) for (const group of await groupsAbove(viewNodes)) groups.add(group);
    const changed = [...groups].sort();
    for (const listener of listeners) {
      try {
        listener(changed, event);
      } catch (error) {
        report(error);
      }
    }
  });

  /** Where each member lists and what its reference stands for, read from its payload. */
  const memberships = (found: TreecrdtPayloadIndexMember[]): Membership[] =>
    found.flatMap(({ id, parent, payload }) => {
      // A view node is no member, and a member in its context needs a parent, which only a root
      // lacks.
      if (id.startsWith(VIEW_NODE_PREFIX) || (byParent && !parent)) return [];
      const node = { id, payload };
      const target = byParent ? parent! : id;
      try {
        return [
          {
            node: id,
            target,
            // Rank digits, then what the reference stands for, so equal ranks sort by id and no two
            // references share a default key, then a final 1 digit, so a key can always be placed
            // below it.
            defaultKey: rankDigits(clampRank(options.rank(node))) + target + '0001',
            ...(options.data ? { data: options.data(node) ?? null } : {}),
          },
        ];
      } catch (error) {
        report(error, id);
        return [];
      }
    });

  /** Groups seen ordered. A group node stays in the tree, so their reads skip the check for one. */
  const seenOrdered = new Set<string>();

  /**
   * Rows `[node, parent, order key, payload hex]` of the subtrees of the groups' nodes. A group
   * node counts while it is in the tree, deleted or not; below it, only live nodes do. Most groups
   * have none, which a cheaper query than the walk finds.
   */
  const subtrees = async (groups: string[]): Promise<[string, string, string, string | null][]> => {
    let nodes = groups.map(groupNode);
    if (!groups.some((group) => seenOrdered.has(group))) {
      nodes = JSON.parse(
        (await runner.getText(
          `SELECT json_group_array(lower(hex(node))) FROM tree_nodes
           WHERE node IN (SELECT unhex(value) FROM json_each(?1)) AND parent IS NOT NULL`,
          [JSON.stringify(nodes)],
        )) ?? '[]',
      );
      if (nodes.length === 0) return [];
    }
    return JSON.parse(
      (await runner.getText(
        `WITH RECURSIVE sub(node, parent, order_key) AS (
           SELECT node, parent, order_key FROM tree_nodes
           WHERE node IN (SELECT unhex(value) FROM json_each(?1)) AND parent IS NOT NULL
           UNION
           SELECT t.node, t.parent, t.order_key FROM tree_nodes t JOIN sub ON t.parent = sub.node
           WHERE t.tombstone = 0
         )
         SELECT json_group_array(json_array(lower(hex(sub.node)), lower(hex(sub.parent)),
                                            lower(hex(sub.order_key)), lower(hex(p.payload))))
         FROM sub LEFT JOIN tree_payload p ON p.node = sub.node`,
        [JSON.stringify(nodes)],
      )) ?? '[]',
    );
  };

  /** Reads each group's members and, if it is ordered, the live nodes of its subtree. */
  const layouts = async (groups: readonly string[]): Promise<Map<string, Layout>> => {
    const out = new Map<string, Layout>();
    const requested = [...new Set(groups)];
    if (requested.length === 0) return out;
    const found = await index.lookup(requested, { payloads: true });
    const inTree = new Set<string>();
    const childrenOf = new Map<string, [string, string, string | null][]>();
    for (const [node, parent, key, payload] of await subtrees(requested)) {
      inTree.add(node);
      const siblings = childrenOf.get(parent) ?? [];
      siblings.push([node, key, payload]);
      childrenOf.set(parent, siblings);
    }
    for (const group of requested) {
      const layout: Layout = {
        group,
        groupNode: groupNode(group),
        ordered: inTree.has(groupNode(group)),
        top: [],
        nodes: new Map(),
        slotOf: new Map(),
      };
      // Members by what their reference stands for, in default key order.
      const targets = new Map<string, Membership[]>();
      const sorted = memberships(found.get(group) ?? []).sort(
        (a, b) => compareHex(a.defaultKey, b.defaultKey) || compareHex(a.node, b.node),
      );
      for (const member of sorted) {
        const shared = targets.get(member.target);
        if (shared) shared.push(member);
        else targets.set(member.target, [member]);
      }
      // Once the walk reaches a reference, the members it stands for are its own.
      let unreached: Iterable<Membership[]> = targets.values();
      if (layout.ordered) {
        seenOrdered.add(group);
        const standFor = new Map<string, Membership[]>();
        const ids = references(group);
        for (const [target, shared] of targets) standFor.set(ids(target), shared);
        unreached = standFor.values();
        const build = (node: string, parent: Slot | null): Slot[] =>
          (childrenOf.get(node) ?? [])
            .map(([child, key, payload]) => {
              const folder = isFolder(child);
              const slot: Slot = {
                node: child,
                key,
                folder,
                payload: folder && payload ? hexToBytes(payload) : null,
                members: (!folder && standFor.get(child)) || [],
                implicit: false,
                parent,
                children: [],
              };
              standFor.delete(child);
              layout.nodes.set(child, slot);
              slot.children = build(child, slot);
              return slot;
            })
            .sort(slotOrder);
        layout.top = build(layout.groupNode, null);
      }
      for (const shared of unreached) {
        layout.top.push({
          node: '',
          key: shared[0]!.defaultKey,
          folder: false,
          payload: null,
          members: shared,
          implicit: true,
          parent: null,
          children: [],
        });
      }
      layout.top.sort(slotOrder);
      for (const slot of [...layout.nodes.values(), ...layout.top]) {
        for (const member of slot.members) layout.slotOf.set(member.node, slot);
      }
      out.set(group, layout);
    }
    return out;
  };

  const items = (slots: Slot[]): TreecrdtViewItem[] =>
    slots.flatMap((slot): TreecrdtViewItem[] => {
      if (slot.folder) {
        return [
          { kind: 'folder', id: slot.node, payload: slot.payload, children: items(slot.children) },
        ];
      }
      // A reference whose member left: its children show in its place.
      if (slot.members.length === 0) return items(slot.children);
      return slot.members.map((member, i) => ({
        kind: 'member' as const,
        id: member.node,
        ...withData(member),
        children: i === 0 ? items(slot.children) : [],
      }));
    });

  const flatten = (slots: Slot[], out: TreecrdtViewMember[]) => {
    for (const slot of slots) {
      for (const member of slot.members) {
        out.push({ id: member.node, placed: !slot.implicit, ...withData(member) });
      }
      flatten(slot.children, out);
    }
    return out;
  };

  const members = async (groups: readonly string[]) => {
    const out = new Map<string, TreecrdtViewMember[]>();
    for (const [group, layout] of await layouts(groups)) {
      const list = flatten(layout.top, []);
      if (list.length > 0) out.set(group, list);
    }
    return out;
  };

  const tree = async (group: string) => items((await layouts([group])).get(group)!.top);

  /** The slot of a member or folder of the group. */
  const find = (layout: Layout, item: string): Slot | undefined => {
    const member = layout.slotOf.get(item);
    if (member) return member;
    const slot = layout.nodes.get(item);
    return slot?.folder ? slot : undefined;
  };

  /**
   * The key an item right after `slot` goes above. A member without a reference lists at its
   * default key, but the reference that `prepare` writes 'at' that key gets the key, its id and a
   * final 1 digit, so an item between the two would list before the member once it has one.
   */
  const keyAfter = (layout: Layout, slot: Slot) =>
    slot.implicit
      ? slot.key + references(layout.group)(slot.members[0]!.target) + '0001'
      : slot.key;

  /**
   * Where `slot` (null: a new node) goes for `position`: under `container` (null: the top level),
   * right after `left` and before `right`, null for an end. Null when the slot already shows there.
   */
  const dropPoint = (layout: Layout, slot: Slot | null, position: TreecrdtViewPosition) => {
    // A parent no longer in the group means the top level.
    const parent = position.parent == null ? null : (find(layout, position.parent) ?? null);
    let before = position.before == null ? null : (find(layout, position.before) ?? null);
    // Before itself, or a member that shares its reference: where it is.
    if (slot && before === slot) return null;
    // A `before` no longer in `parent` means last. One under a reference whose member left shows
    // in that reference's place, so the item goes under that reference too.
    if (before && shownParent(before) !== parent) before = null;
    const container = before ? before.parent : parent;
    for (let up = container; up; up = up.parent) {
      if (up === slot) throw new Error('TreeCRDT view: an item cannot go inside itself');
    }
    // Already there: under `parent`, right before `before` in the order shown.
    if (slot && shownParent(slot) === parent && nextShown(layout.top, slot) === before) return null;
    const siblings = container ? container.children : layout.top;
    const rest = siblings.filter((sibling) => sibling !== slot);
    const left = rest[(before ? rest.indexOf(before) : rest.length) - 1] ?? null;
    return { container, left: left && { key: keyAfter(layout, left) }, right: before };
  };

  const writer = client.local.forReplica(options.replica);
  const writeOptions = () => options.writeOptions?.();

  /**
   * Those of `nodes` that are in the tree, each with whether a deletion hides it under `groupNode`:
   * it, or a node between it and the group node, is deleted. The walk misses any other because a
   * node above it has not arrived, such as a folder a peer moved it into, or the group node.
   */
  const locate = async (nodes: string[], groupNode: string): Promise<Map<string, boolean>> => {
    if (nodes.length === 0) return new Map();
    const rows = JSON.parse(
      (await runner.getText(
        `WITH RECURSIVE up(start, parent, deleted) AS (
           SELECT node, parent, tombstone <> 0 FROM tree_nodes
           WHERE node IN (SELECT unhex(value) FROM json_each(?1)) AND parent IS NOT NULL
           UNION
           SELECT up.start, t.parent, up.deleted OR t.tombstone <> 0
           FROM up JOIN tree_nodes t ON t.node = up.parent
           WHERE up.parent <> unhex(?2)
         )
         SELECT json_group_array(json_array(lower(hex(start)), hidden)) FROM (
           SELECT start, MAX(parent = unhex(?2) AND deleted) AS hidden FROM up GROUP BY start
         )`,
        [JSON.stringify(nodes), groupNode],
      )) ?? '[]',
    ) as [string, number | null][];
    return new Map(rows.map(([node, hidden]) => [node, hidden === 1]));
  };

  /**
   * Writes what an item needs before it goes under `container`: the view root and the group node
   * if the group is unordered, and the container's reference if it is a member without one. These
   * are create-only inserts at keys all devices derive alike, so concurrent ones agree and none
   * moves a node someone placed. A container that a deletion hides is moved back to hold the item.
   * Returns which references of `container` and `item` that the walk does not reach are in the
   * tree, as `locate` does.
   */
  const prepare = async (
    layout: Layout,
    container: Slot | null,
    item: Slot | null,
    ops: Operation[],
  ) => {
    const missing = [container, item].filter((slot): slot is Slot => !!slot?.implicit);
    for (const slot of missing) slot.node = references(layout.group)(slot.members[0]!.target);
    const located = await locate(
      missing.map((slot) => slot.node),
      layout.groupNode,
    );
    const create = async (
      parent: string,
      node: string,
      placement: TreecrdtSqlitePlacement,
      payload: Uint8Array | null = null,
    ) => {
      const op = await writer.insertIfAbsent(parent, node, placement, payload, writeOptions());
      if (op) ops.push(op);
    };
    if (!layout.ordered) {
      await create(ROOT_NODE_ID_HEX, root, anywhere);
      await create(root, layout.groupNode, anywhere, encoder.encode(layout.group));
    }
    if (container?.implicit) {
      const hidden = located.get(container.node);
      // At the least default key of the members it stands for, where they list. Writes nothing if
      // a peer created it meanwhile, which leaves it where the peer put it.
      if (hidden === undefined) await create(layout.groupNode, container.node, at(container.key));
      // Only a move shows a deleted container again, as it must to hold the item.
      else if (hidden) {
        ops.push(
          await writer.move(container.node, layout.groupNode, at(container.key), writeOptions()),
        );
      }
      // Any other waits on a node that has not arrived. Moving it would undo the peer's move that
      // put it there, so the item goes under it as it is, and shows with it.
    }
    return located;
  };

  const placeNow = async (group: string, item: string, position: TreecrdtViewPosition) => {
    const layout = (await layouts([group])).get(group)!;
    const slot = find(layout, item);
    // The member left the group meanwhile, such as by a concurrent edit, or the folder is gone.
    if (!slot) return [];
    const drop = dropPoint(layout, slot, position);
    if (!drop) return [];
    const ops: Operation[] = [];
    const located = await prepare(layout, drop.container, slot, ops);
    const parent = drop.container?.node ?? layout.groupNode;
    const placement = placementBetween(drop.container, drop.left, drop.right);
    ops.push(
      slot.implicit && !located.has(slot.node)
        ? // The drag creates the reference it finds missing with a plain insert, which wins over
          // every create-only insert of it, wherever that falls in the operation order.
          await writer.insert(parent, slot.node, placement, null, writeOptions())
        : await writer.move(slot.node, parent, placement, writeOptions()),
    );
    return ops;
  };

  const createFolderNow = async (
    group: string,
    { payload = null, ...position }: TreecrdtViewPosition & { payload?: Uint8Array | null },
  ) => {
    const layout = (await layouts([group])).get(group)!;
    const drop = dropPoint(layout, null, position)!;
    const ops: Operation[] = [];
    await prepare(layout, drop.container, null, ops);
    const id = folderId();
    const parent = drop.container?.node ?? layout.groupNode;
    const placement = placementBetween(drop.container, drop.left, drop.right);
    ops.push(await writer.insert(parent, id, placement, payload, writeOptions()));
    return { id, ops };
  };

  const removeFolderNow = async (group: string, folder: string) => {
    const layout = (await layouts([group])).get(group)!;
    const slot = layout.nodes.get(folder);
    if (!slot?.folder) return [];
    const siblings = slot.parent ? slot.parent.children : layout.top;
    const after = siblings[siblings.indexOf(slot) + 1] ?? null;
    const parent = slot.parent?.node ?? layout.groupNode;
    const ops: Operation[] = [];
    // Each item goes right after the one before, starting right after the folder, so the items
    // take its place once it is deleted, and at the last spot stay below later ranks as it did.
    let left: { key: string } = slot;
    for (const child of slot.children) {
      const placement = placementBetween(slot.parent, left, after);
      const op = await writer.move(child.node, parent, placement, writeOptions());
      ops.push(op);
      if (op.kind.type === 'move') left = { key: bytesToHex(op.kind.orderKey) };
    }
    ops.push(await writer.delete(folder, writeOptions()));
    return ops;
  };

  // One write at a time: a second drop is placed against the order the first left, which is the
  // order the user saw when making it.
  let writing: Promise<unknown> = Promise.resolve();
  const serially = <T>(work: () => Promise<T>): Promise<T> => {
    const next = writing.then(work);
    writing = next.catch(() => undefined);
    return next;
  };

  return {
    members,
    tree,
    place: (group, item, position = {}) => serially(() => placeNow(group, item, position)),
    createFolder: (group, opts = {}) => serially(() => createFolderNow(group, opts)),
    removeFolder: (group, folder) => serially(() => removeFolderNow(group, folder)),
    onChange: (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    index,
    idle: () => index.idle(),
    close: async () => {
      stopReporting();
      await (index === options.index ? index.idle() : index.close());
    },
  };
}
