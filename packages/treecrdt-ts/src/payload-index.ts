/**
 * Finds nodes by a string field of their JSON payload. SQLite keeps the index on `tree_payload` in
 * the write that changes a payload, so a lookup always matches the payloads, and no table, listener
 * or catch-up keeps it.
 *
 * - A node is indexed under the string that SQLite's JSON functions read at `path` of its payload
 *   as text. A payload they do not read as JSON, such as an empty or encrypted one, or that has no
 *   string there, indexes nothing. They read some text otherwise than `JSON.parse`,
 *   none of which an app that writes `JSON.stringify` output meets: the first of duplicate fields
 *   wins, text after a NUL byte is ignored, and a leading byte order mark is not JSON.
 * - A node counts while it is in the tree (attached, or the root) and not deleted. It reads each
 *   node's own tombstone, so a node under a deleted node counts where the engine leaves that node's
 *   descendants undeleted.
 * - `onChange` reports the keys that TEMP triggers record on `tree_payload` and `tree_nodes` in each
 *   write, read after each materialization event. The opens of a name on a connection, such as tabs
 *   sharing a worker, share one set of triggers and one log of keys that each reads from its own
 *   position, so an open that is never closed adds nothing to the cost of a write. Triggers see
 *   this connection's writes only: an event relayed from another connection, such as another tab's
 *   worker, reports none of its keys. They hold nothing across a restart, which an app that reads
 *   on demand does not need.
 * - A local write with an `authSession` holds a SQLite savepoint while it is authorized, and the
 *   statements run meanwhile join it. If authorization fails, its rollback can undo writes whose
 *   keys `onChange` already reported and whose nodes a lookup returned, and leave keys written
 *   next unreported, as it also takes back the log ids read. No event corrects these.
 *   https://github.com/cybersemics/treecrdt/pull/284 stops holding that savepoint.
 * - It reads the SQLite extension's `tree_nodes` and `tree_payload` tables directly, so it runs on
 *   the SQLite engines only and follows that schema.
 */
import { randomBytes } from '@noble/hashes/utils';
import type { MaterializationEvent, TreecrdtEngine } from './engine.js';
import { bytesToHex, ROOT_NODE_ID_HEX } from './ids.js';
import type { SqliteRunner } from './sqlite.js';

export type TreecrdtPayloadIndexClient = Pick<TreecrdtEngine, 'onMaterialized'> & {
  runner: SqliteRunner;
};

export type TreecrdtPayloadIndexOptions = {
  /** Names the SQLite index `treecrdt_payload_index_<name>`: lowercase letters, digits and `_`. */
  name: string;
  /**
   * JSON path of the key: `$`, then one or more `.field` of letters, digits and `_`. Opening the
   * index with another path builds it again. That throws while another open of the name on the
   * connection is left, which keeps its path until it closes.
   */
  path: string;
  /** Receives a failing `onChange` listener, or a failed read of the changed keys. */
  onError?: (error: unknown) => void;
};

export type TreecrdtPayloadIndexMember = {
  id: string;
  /** Null for the root. */
  parent: string | null;
  /** Only with `payloads: true`: the payload as JSON, as SQLite's JSON functions read it. */
  payload?: unknown;
};

export type TreecrdtPayloadIndexListener = (
  keys: string[],
  event: MaterializationEvent,
) => void | Promise<void>;

export type TreecrdtPayloadIndex = {
  /** Members of each key by id, read in one query. Keys without members are absent. */
  lookup: (
    keys: readonly string[],
    opts?: { payloads?: boolean },
  ) => Promise<Map<string, TreecrdtPayloadIndexMember[]>>;
  /**
   * Called after each materialization event with the keys, sorted, whose members may have changed
   * since the previous call: both keys of a node whose key changed, and the key of a node added,
   * deleted, restored or moved, a move among its siblings included. Not for a payload edit that
   * keeps the key. The next call waits for a promise the listener returns. An open that leaves
   * keys unread for five minutes while another open of the name reads on, such as one in a tab the
   * browser froze, loses them, and then also reports every key it has looked up.
   */
  onChange: (listener: TreecrdtPayloadIndexListener) => () => void;
  /** Resolves once listeners have been called for every event emitted before the call. */
  idle: () => Promise<void>;
  /**
   * Stops `onChange`. The last open of the name on the connection drops the triggers. The SQLite
   * index stays for the next open.
   */
  close: () => Promise<void>;
};

const NAME = /^[a-z0-9_]{1,40}$/;
const PATH = /^\$(\.[A-Za-z_][A-Za-z0-9_]*)+$/;
/** The opens on a connection: each one's name, path, and how far it has read its name's log. */
const OPENS = 'treecrdt_payload_index_opens';
/**
 * How many log rows an open reads between writes of its position to OPENS, which the opens of a
 * name prune the log by, so that most events take one statement.
 */
const PUBLISH_ROWS = 64;
/**
 * How long an open may be seen behind before the other opens of its name stop keeping its unread
 * keys. A live open reads them within moments of each event, so this lets go of an open that was
 * never closed, such as one of a tab that reloaded while other tabs kept its worker.
 */
const STALE_MS = 5 * 60_000;

/**
 * SQL for the key of payload column `column`: the string at `path` of the payload read as JSON
 * text, else NULL. Guarded, since json_extract raises on text that is not JSON, which would fail
 * every write of such a payload once the index exists.
 */
const keySql = (column: string, path: string) => {
  const text = `CAST(${column} AS TEXT)`;
  return `CASE WHEN json_valid(${text}) THEN CASE json_type(${text}, '${path}') WHEN 'text' THEN json_extract(${text}, '${path}') END END`;
};

/**
 * Opens the payload index `options.name` over `client`, building its SQLite index on first use,
 * and reports changed keys until `close`.
 */
export async function createTreecrdtPayloadIndex(
  client: TreecrdtPayloadIndexClient,
  options: TreecrdtPayloadIndexOptions,
): Promise<TreecrdtPayloadIndex> {
  const { runner } = client;
  const { name, path } = options;
  // Both end up in SQL.
  if (!NAME.test(name)) throw new Error(`TreeCRDT payload index: invalid name ${name}`);
  if (!PATH.test(path)) throw new Error(`TreeCRDT payload index: invalid path ${path}`);
  const report = (error: unknown) => {
    if (options.onError) options.onError(error);
    else console.error(`TreeCRDT payload index ${name}:`, error);
  };
  const key = (column: string) => keySql(column, path);
  const indexName = `treecrdt_payload_index_${name}`;
  const log = `${indexName}_log`;
  const triggers = ['insert', 'update', 'delete', 'node'].map((t) => `temp.${indexName}_${t}`);
  const self = bytesToHex(randomBytes(8));

  // Registered first, so that an open of another path racing this one sees it. AUTOINCREMENT, so
  // ids keep rising after every row has been read and dropped.
  await runner.exec(`
    CREATE TEMP TABLE IF NOT EXISTS ${OPENS} (
      open TEXT PRIMARY KEY, name TEXT NOT NULL, path TEXT NOT NULL, cursor INTEGER NOT NULL,
      behind REAL);
    CREATE TEMP TABLE IF NOT EXISTS ${log} (
      id INTEGER PRIMARY KEY AUTOINCREMENT, key TEXT NOT NULL);
    INSERT INTO ${OPENS} (open, name, path, cursor)
    VALUES ('${self}', '${name}', '${path}', (SELECT coalesce(max(id), 0) FROM ${log}));
  `);
  const [paths, cursor, stored] = JSON.parse(
    (await runner.getText(
      `SELECT json_array(
         (SELECT json_group_array(DISTINCT path) FROM ${OPENS} WHERE name = ?1),
         (SELECT cursor FROM ${OPENS} WHERE open = ?2),
         (SELECT sql FROM sqlite_master WHERE type = 'index' AND name = ?3))`,
      [name, self, indexName],
    ))!,
  ) as [string[], number, string | null];
  const other = paths.find((p) => p !== path);
  if (other) {
    await runner.run(`DELETE FROM ${OPENS} WHERE open = ?1`, [self]);
    throw new Error(`TreeCRDT payload index ${name}: open on this connection with path ${other}`);
  }

  // Partial, so a payload without the key adds no entry. SQLite keeps the statement as written,
  // without IF NOT EXISTS, so an index built for another path reads differently.
  const indexed = `ON tree_payload (${key('payload')}) WHERE ${key('payload')} IS NOT NULL`;
  const rebuild =
    stored === `CREATE INDEX ${indexName} ${indexed}`
      ? ''
      : `DROP INDEX IF EXISTS ${indexName}; CREATE INDEX IF NOT EXISTS ${indexName} ${indexed};`;
  const record = (keys: string) =>
    `INSERT INTO ${log} (key) SELECT key FROM (${keys}) WHERE key IS NOT NULL;`;
  // Every open of the name reads this path now, so the triggers may be replaced, in one exec that
  // no write runs inside.
  await runner.exec(`
    ${rebuild}
    ${triggers.map((trigger) => `DROP TRIGGER IF EXISTS ${trigger};`).join(' ')}
    CREATE TEMP TRIGGER ${indexName}_insert AFTER INSERT ON tree_payload
    BEGIN ${record(`SELECT ${key('NEW.payload')} AS key`)} END;
    CREATE TEMP TRIGGER ${indexName}_update AFTER UPDATE OF payload ON tree_payload
    WHEN ${key('OLD.payload')} IS NOT ${key('NEW.payload')}
    BEGIN ${record(`SELECT ${key('OLD.payload')} AS key UNION ALL SELECT ${key('NEW.payload')}`)} END;
    CREATE TEMP TRIGGER ${indexName}_delete AFTER DELETE ON tree_payload
    BEGIN ${record(`SELECT ${key('OLD.payload')} AS key`)} END;
    CREATE TEMP TRIGGER ${indexName}_node AFTER UPDATE OF parent, tombstone ON tree_nodes
    WHEN OLD.parent IS NOT NEW.parent OR OLD.tombstone IS NOT NEW.tombstone
    BEGIN ${record(`SELECT ${key('payload')} AS key FROM tree_payload WHERE node = NEW.node`)} END;
  `);

  /** Every key looked up: what the app may hold, which a drain that lost keys reports. */
  const looked = new Set<string>();
  /** The last log id this open read, and the one it last wrote to OPENS. */
  let read = cursor;
  let published = cursor;

  /** Takes the keys recorded since the last call. */
  const drain = async (): Promise<string[]> => {
    const [keys, top, at, behind] = JSON.parse(
      (await runner.getText(
        `SELECT json_array(
           (SELECT json_group_array(DISTINCT key) FROM ${log} WHERE id > ?2 AND id <= top),
           top, cursor, behind IS NOT NULL)
         FROM (SELECT max(coalesce((SELECT max(id) FROM ${log}), 0), ?2, cursor) AS top,
                      cursor, behind
               FROM ${OPENS} WHERE open = ?1)`,
        [self, read],
      ))!,
    ) as [string[], number, number, number];
    // Another open moved this one past keys it had not read: it was taken for gone.
    const lost = at > read;
    read = top;
    // The position is written now and then, and at once when another open has seen this one
    // behind. Rows stay until every open of the name has read them, and one seen behind for
    // STALE_MS is moved past them. A write in between adds rows after `top`, which the next event
    // reads.
    if (behind || top - published >= PUBLISH_ROWS) {
      published = top;
      const now = Date.now();
      await runner.exec(`
        UPDATE ${OPENS} SET cursor = max(cursor, ${top}), behind = NULL WHERE open = '${self}';
        UPDATE ${OPENS} SET behind = ${now}
        WHERE name = '${name}' AND cursor < ${top - PUBLISH_ROWS} AND behind IS NULL;
        UPDATE ${OPENS} SET cursor = max(cursor, ${top}), behind = NULL
        WHERE name = '${name}' AND behind < ${now - STALE_MS};
        DELETE FROM ${log} WHERE id <= (SELECT min(cursor) FROM ${OPENS} WHERE name = '${name}');
      `);
    }
    return (lost ? [...new Set([...looked, ...keys])] : keys).sort();
  };

  const listeners = new Set<TreecrdtPayloadIndexListener>();
  let tail: Promise<void> = Promise.resolve();
  const unsubscribe = client.onMaterialized((event) => {
    tail = tail
      .then(async () => {
        const keys = await drain();
        for (const listener of listeners) {
          try {
            await listener(keys, event);
          } catch (error) {
            report(error);
          }
        }
      })
      // A failed read leaves its keys to the next event.
      .catch(report);
  });

  const lookup: TreecrdtPayloadIndex['lookup'] = async (keys, opts = {}) => {
    const out = new Map<string, TreecrdtPayloadIndexMember[]>();
    if (keys.length === 0) return out;
    const requested = [...new Set(keys)];
    for (const key of requested) looked.add(key);
    // Payloads go into the result as JSON, which one parse reads, rather than as hex. Sorting in
    // JS is cheaper than SQLite sorting rows that carry payloads.
    const text = await runner.getText(
      `SELECT json_group_array(json_array(k.value, lower(hex(p.node)), lower(hex(n.parent)),
                                          CASE WHEN ?2 THEN json(CAST(p.payload AS TEXT)) END))
       FROM json_each(?1) AS k
       JOIN tree_payload AS p ON ${key('p.payload')} = k.value
       JOIN tree_nodes AS n ON n.node = p.node
       WHERE n.tombstone = 0 AND (n.parent IS NOT NULL OR n.node = X'${ROOT_NODE_ID_HEX}')`,
      [JSON.stringify(requested), opts.payloads ? 1 : 0],
    );
    const rows = JSON.parse(text ?? '[]') as [string, string, string, unknown][];
    for (const [key, id, parent, payload] of rows) {
      let members = out.get(key);
      if (!members) out.set(key, (members = []));
      // hex reads a NULL parent as ''.
      members.push({ id, parent: parent || null, ...(opts.payloads ? { payload } : {}) });
    }
    for (const members of out.values()) members.sort((a, b) => (a.id < b.id ? -1 : 1));
    return out;
  };

  let closing: Promise<void> | null = null;
  return {
    lookup,
    onChange: (listener) => {
      listeners.add(listener);
      return () => {
        listeners.delete(listener);
      };
    },
    idle: () => tail,
    close: () =>
      (closing ??= (async () => {
        unsubscribe();
        await tail;
        try {
          // One exec, so that no open starts between the check and the drops. While another open
          // of the name is left, json() fails on purpose, which ends the exec before them.
          await runner.exec(`
            DELETE FROM ${OPENS} WHERE open = '${self}';
            DELETE FROM ${log} WHERE id <= (SELECT min(cursor) FROM ${OPENS} WHERE name = '${name}');
            SELECT json('in use') WHERE EXISTS (SELECT 1 FROM ${OPENS} WHERE name = '${name}');
            ${triggers.map((trigger) => `DROP TRIGGER IF EXISTS ${trigger};`).join(' ')}
            DROP TABLE IF EXISTS temp.${log};
          `);
        } catch {
          // Another open still uses them, or a closed connection has dropped them already.
        }
      })()),
  };
}
