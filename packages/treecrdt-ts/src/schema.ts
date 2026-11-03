/**
 * Hand-written Kysely schema for the TreeCRDT SQLite tables.
 * Column types mirror packages/treecrdt-sqlite-ext/src/extension/functions/schema.sql.
 */

export type MetaTable = {
  key: string;
  value: string;
};

export type OpsTable = {
  replica: Uint8Array;
  counter: number;
  lamport: number;
  kind: string;
  parent: Uint8Array | null;
  node: Uint8Array;
  new_parent: Uint8Array | null;
  order_key: Uint8Array | null;
  op_ref: Uint8Array | null;
  known_state: Uint8Array | null;
  payload: Uint8Array | null;
};

export type TreeMetaTable = {
  id: number;
  head_lamport: number;
  head_replica: Uint8Array;
  head_counter: number;
  head_seq: number;
  replay_lamport: number | null;
  replay_replica: Uint8Array | null;
  replay_counter: number | null;
};

export type TreeNodesTable = {
  node: Uint8Array;
  parent: Uint8Array | null;
  order_key: Uint8Array | null;
  tombstone: number;
  last_change: Uint8Array | null;
  deleted_at: Uint8Array | null;
};

export type OprefsChildrenTable = {
  parent: Uint8Array;
  op_ref: Uint8Array;
  seq: number;
};

export type TreePayloadTable = {
  node: Uint8Array;
  payload: Uint8Array | null;
  last_lamport: number;
  last_replica: Uint8Array;
  last_counter: number;
};

/** Core TreeCRDT SQLite database shape used by `createTreecrdtDb`. */
export type TreecrdtDb = {
  meta: MetaTable;
  ops: OpsTable;
  tree_meta: TreeMetaTable;
  tree_nodes: TreeNodesTable;
  oprefs_children: OprefsChildrenTable;
  tree_payload: TreePayloadTable;
};
