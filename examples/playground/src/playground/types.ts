import type { SupportedImageMime } from "@treecrdt/content";

export type PayloadDisplay =
  | { kind: "root"; label: string; value: "" }
  | { kind: "empty"; label: string; value: "" }
  | { kind: "encrypted"; label: string; value: "" }
  | { kind: "text"; label: string; value: string }
  | {
      kind: "image";
      label: string;
      value: "";
      mime: SupportedImageMime;
      name?: string;
      size: number;
      url: string;
    };

export type DisplayNode = {
  id: string;
  label: string;
  value: string;
  payload: PayloadDisplay;
};

export type ImagePayloadViewMetric = {
  nodeId: string;
  mime: string;
  name?: string;
  bytes: number;
  coldMs: number | null;
  loadedAtMs: number;
};

export type NodeMeta = {
  parentId: string | null;
  order: number;
  childCount: number;
};

export type TreeState = {
  index: Record<string, NodeMeta>;
  childrenByParent: Record<string, string[]>;
};

export type CollapseState = {
  defaultCollapsed: boolean;
  overrides: Set<string>;
};

export type BulkAddProgress = {
  total: number;
  completed: number;
  phase: "creating" | "applying";
  startedAtMs: number;
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
