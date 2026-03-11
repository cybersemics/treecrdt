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
export type SyncTransportMode = "local" | "remote" | "hybrid";
export type RemoteSyncStatus =
  | { state: "disabled"; detail: string }
  | { state: "missing_url"; detail: string }
  | { state: "invalid"; detail: string }
  | { state: "connecting"; detail: string }
  | { state: "connected"; detail: string }
  | { state: "error"; detail: string };

export type PeerInfo = { id: string; lastSeen: number };

export type PayloadRecord = {
  payload: Uint8Array | null;
  encrypted?: boolean;
};
