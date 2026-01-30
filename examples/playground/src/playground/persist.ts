import { bytesToHex } from "@treecrdt/interface/ids";

import type { StorageMode } from "./types";

export function pickReplicaLabel(): string {
  if (typeof window === "undefined") return `replica-${Math.random().toString(16).slice(2, 6)}`;
  const override = new URLSearchParams(window.location.search).get("replica");
  if (override && override.trim()) return override.trim();
  const key = "treecrdt-playground-replica";
  const existing = window.localStorage.getItem(key);
  if (existing) return existing;
  const next = `replica-${crypto.randomUUID().slice(0, 8)}`;
  window.localStorage.setItem(key, next);
  return next;
}

export function initialStorage(): StorageMode {
  if (typeof window === "undefined") return "memory";
  const param = new URLSearchParams(window.location.search).get("storage");
  return param === "opfs" ? "opfs" : "memory";
}

export function initialDocId(): string {
  if (typeof window === "undefined") return "treecrdt-playground";
  const param = new URLSearchParams(window.location.search).get("doc");
  if (param && param.trim()) return param.trim();
  const key = "treecrdt-playground-doc";
  const existing = window.localStorage.getItem(key);
  if (existing) return existing;
  const next = "treecrdt-playground";
  window.localStorage.setItem(key, next);
  return next;
}

export function persistDocId(docId: string) {
  if (typeof window === "undefined") return;
  const key = "treecrdt-playground-doc";
  window.localStorage.setItem(key, docId);
  const url = new URL(window.location.href);
  if (docId) url.searchParams.set("doc", docId);
  else url.searchParams.delete("doc");
  window.history.replaceState({}, "", url);
}

export function persistStorage(mode: StorageMode) {
  if (typeof window === "undefined") return;
  const url = new URL(window.location.href);
  if (mode === "opfs") {
    url.searchParams.set("storage", "opfs");
  } else {
    url.searchParams.delete("storage");
  }
  window.history.replaceState({}, "", url);
}

export function makeNodeId(): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return bytesToHex(bytes);
}

export function makeSessionKey(): string {
  const bytes = new Uint8Array(8);
  crypto.getRandomValues(bytes);
  return bytesToHex(bytes);
}

function opfsKeyStore(): { get: () => string | null; set: (val: string) => string } {
  if (typeof window === "undefined") {
    return { get: () => null, set: (val) => val };
  }
  const key = "treecrdt-playground-opfs-key";
  return {
    get: () => window.localStorage.getItem(key),
    set: (val: string) => {
      window.localStorage.setItem(key, val);
      return val;
    },
  };
}

export function ensureOpfsKey(): string {
  const store = opfsKeyStore();
  const existing = store.get();
  if (existing) return existing;
  return store.set(makeSessionKey());
}

export function persistOpfsKey(val: string): string {
  const store = opfsKeyStore();
  return store.set(val);
}
