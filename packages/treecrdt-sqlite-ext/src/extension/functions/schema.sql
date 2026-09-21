-- Shared by native initialization and the wa-sqlite adapter.

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ops (
  replica BLOB NOT NULL,
  counter INTEGER NOT NULL,
  lamport INTEGER NOT NULL,
  kind TEXT NOT NULL,
  parent BLOB,
  node BLOB NOT NULL,
  new_parent BLOB,
  order_key BLOB,
  op_ref BLOB,
  known_state BLOB,
  payload BLOB,
  PRIMARY KEY (replica, counter)
);

CREATE TABLE IF NOT EXISTS tree_meta (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  head_lamport INTEGER NOT NULL DEFAULT 0,
  head_replica BLOB NOT NULL DEFAULT X'',
  head_counter INTEGER NOT NULL DEFAULT 0,
  head_seq INTEGER NOT NULL DEFAULT 0,
  replay_lamport INTEGER,
  replay_replica BLOB,
  replay_counter INTEGER
);
INSERT OR IGNORE INTO tree_meta(id) VALUES (1);

CREATE TABLE IF NOT EXISTS tree_nodes (
  node BLOB PRIMARY KEY,
  parent BLOB,
  order_key BLOB,
  tombstone INTEGER NOT NULL DEFAULT 0,
  last_change BLOB,
  deleted_at BLOB
);

CREATE TABLE IF NOT EXISTS oprefs_children (
  parent BLOB NOT NULL,
  op_ref BLOB NOT NULL,
  seq INTEGER NOT NULL,
  PRIMARY KEY (parent, op_ref)
);

CREATE TABLE IF NOT EXISTS tree_payload (
  node BLOB PRIMARY KEY,
  payload BLOB,
  last_lamport INTEGER NOT NULL,
  last_replica BLOB NOT NULL,
  last_counter INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ops_lamport ON ops(lamport, replica, counter);
CREATE INDEX IF NOT EXISTS idx_ops_op_ref ON ops(op_ref);
CREATE INDEX IF NOT EXISTS idx_ops_node_kind_order ON ops(node, kind, lamport, replica, counter);
CREATE INDEX IF NOT EXISTS idx_tree_nodes_parent_order_key_node ON tree_nodes(parent, order_key, node);
CREATE INDEX IF NOT EXISTS idx_tree_nodes_parent_tombstone_order_key_node ON tree_nodes(parent, tombstone, order_key, node);
CREATE INDEX IF NOT EXISTS idx_oprefs_children_parent_seq ON oprefs_children(parent, seq);

-- Seed the root only for an empty operation log; preserve existing materialized state.
INSERT OR IGNORE INTO tree_nodes(node, parent, order_key, tombstone)
SELECT X'00000000000000000000000000000000', NULL, X'', 0
WHERE NOT EXISTS (SELECT 1 FROM ops);
