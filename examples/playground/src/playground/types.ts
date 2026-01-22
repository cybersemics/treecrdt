export type DisplayNode = {
  id: string;
  label: string;
  value: string;
  children: DisplayNode[];
};

export type NodeMeta = {
  parentId: string | null;
  order: number;
  childCount: number;
  deleted: boolean;
};

export type TreeState = {
  index: Record<string, NodeMeta>;
  childrenByParent: Record<string, string[]>;
};

export type CollapseState = {
  defaultCollapsed: boolean;
  overrides: Set<string>;
};

export type Status = "booting" | "ready" | "error";
export type StorageMode = "memory" | "opfs";

export type PeerInfo = { id: string; lastSeen: number };

export type PayloadRecord = {
  lamport: number;
  replica: string;
  counter: number;
  payload: Uint8Array | null;
};

