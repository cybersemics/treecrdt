import { nodeIdToBytes16 } from "@treecrdt/interface/ids";
import { base64urlDecode, base64urlEncode, coseSign1Ed25519 } from "@treecrdt/sync";

import { hashes as ed25519Hashes, getPublicKey, utils as ed25519Utils } from "@noble/ed25519";
import { sha512 } from "@noble/hashes/sha512";
import { encode as cborEncode, rfc8949EncodeOptions } from "cborg";

ed25519Hashes.sha512 = sha512;

const AUTH_ENABLED_KEY = "treecrdt-playground-auth-enabled";
const ISSUER_PK_KEY_PREFIX = "treecrdt-playground-auth-issuer-pk:";
const ISSUER_SK_KEY_PREFIX = "treecrdt-playground-auth-issuer-sk:";
const LOCAL_PK_KEY_PREFIX = "treecrdt-playground-auth-local-pk:";
const LOCAL_SK_KEY_PREFIX = "treecrdt-playground-auth-local-sk:";
const LOCAL_TOKENS_KEY_PREFIX = "treecrdt-playground-auth-local-tokens:";

function lsGet(key: string): string | null {
  if (typeof window === "undefined") return null;
  return window.sessionStorage.getItem(key);
}

function lsSet(key: string, val: string) {
  if (typeof window === "undefined") return;
  window.sessionStorage.setItem(key, val);
}

function lsDel(key: string) {
  if (typeof window === "undefined") return;
  window.sessionStorage.removeItem(key);
}

function gsGet(key: string): string | null {
  if (typeof window === "undefined") return null;
  return window.localStorage.getItem(key);
}

function gsSet(key: string, val: string) {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(key, val);
}

export function initialAuthEnabled(): boolean {
  // Default to enabled so the playground demos signed+authorized sync out of the box.
  if (typeof window === "undefined") return true;
  const param = new URLSearchParams(window.location.search).get("auth");
  if (param === "0") return false;
  if (param === "1") return true;
  const stored = lsGet(AUTH_ENABLED_KEY);
  if (stored === "0") return false;
  if (stored === "1") return true;
  return true;
}

export function persistAuthEnabled(enabled: boolean) {
  if (typeof window === "undefined") return;
  lsSet(AUTH_ENABLED_KEY, enabled ? "1" : "0");
  const url = new URL(window.location.href);
  url.searchParams.set("auth", enabled ? "1" : "0");
  window.history.replaceState({}, "", url);
}

export type StoredAuthMaterial = {
  issuerPkB64: string | null;
  issuerSkB64: string | null;
  localPkB64: string | null;
  localSkB64: string | null;
  localTokensB64: string[];
};

export function loadAuthMaterial(docId: string, replicaLabel: string): StoredAuthMaterial {
  // Issuer keys are shared across tabs for the same doc so multiple peers can sync without manually exchanging invites.
  const issuerPkB64 = gsGet(`${ISSUER_PK_KEY_PREFIX}${docId}`);
  const issuerSkB64 = gsGet(`${ISSUER_SK_KEY_PREFIX}${docId}`);
  const localPkB64 = lsGet(`${LOCAL_PK_KEY_PREFIX}${docId}:${replicaLabel}`);
  const localSkB64 = lsGet(`${LOCAL_SK_KEY_PREFIX}${docId}:${replicaLabel}`);

  const tokensRaw = lsGet(`${LOCAL_TOKENS_KEY_PREFIX}${docId}:${replicaLabel}`);
  let localTokensB64: string[] = [];
  if (tokensRaw) {
    try {
      const parsed = JSON.parse(tokensRaw) as unknown;
      if (Array.isArray(parsed) && parsed.every((x) => typeof x === "string")) {
        localTokensB64 = parsed;
      }
    } catch {
      // ignore invalid tokens payload
    }
  }

  return { issuerPkB64, issuerSkB64, localPkB64, localSkB64, localTokensB64 };
}

export function saveIssuerKeys(
  docId: string,
  issuerPkB64: string,
  issuerSkB64?: string,
  opts: { forcePk?: boolean } = {}
) {
  const pkKey = `${ISSUER_PK_KEY_PREFIX}${docId}`;
  const skKey = `${ISSUER_SK_KEY_PREFIX}${docId}`;

  // Avoid clobbering issuer keys when multiple tabs initialize concurrently.
  if (issuerSkB64 && !gsGet(skKey)) gsSet(skKey, issuerSkB64);
  if (opts.forcePk) gsSet(pkKey, issuerPkB64);
  else if (!gsGet(pkKey)) gsSet(pkKey, issuerPkB64);
}

export function saveLocalKeys(docId: string, replicaLabel: string, localPkB64: string, localSkB64: string) {
  lsSet(`${LOCAL_PK_KEY_PREFIX}${docId}:${replicaLabel}`, localPkB64);
  lsSet(`${LOCAL_SK_KEY_PREFIX}${docId}:${replicaLabel}`, localSkB64);
}

export function saveLocalTokens(docId: string, replicaLabel: string, tokensB64: string[]) {
  lsSet(`${LOCAL_TOKENS_KEY_PREFIX}${docId}:${replicaLabel}`, JSON.stringify(tokensB64));
}

export function clearAuthMaterial(docId: string, replicaLabel: string) {
  lsDel(`${LOCAL_PK_KEY_PREFIX}${docId}:${replicaLabel}`);
  lsDel(`${LOCAL_SK_KEY_PREFIX}${docId}:${replicaLabel}`);
  lsDel(`${LOCAL_TOKENS_KEY_PREFIX}${docId}:${replicaLabel}`);
}

export async function generateEd25519KeyPair(): Promise<{ sk: Uint8Array; pk: Uint8Array }> {
  const sk = ed25519Utils.randomSecretKey();
  const pk = await getPublicKey(sk);
  return { sk, pk };
}

export async function deriveEd25519PublicKey(secretKey: Uint8Array): Promise<Uint8Array> {
  return await getPublicKey(secretKey);
}

export function createCapabilityTokenV1(opts: {
  issuerPrivateKey: Uint8Array;
  subjectPublicKey: Uint8Array;
  docId: string;
  rootNodeId: string;
  actions: string[];
  maxDepth?: number;
  excludeNodeIds?: string[];
}): Uint8Array {
  const cnf = new Map<unknown, unknown>([["pub", opts.subjectPublicKey]]);

  const resEntries: Array<[unknown, unknown]> = [
    ["doc_id", opts.docId],
    ["root", nodeIdToBytes16(opts.rootNodeId)],
  ];
  if (opts.maxDepth !== undefined) resEntries.push(["max_depth", opts.maxDepth]);
  if (opts.excludeNodeIds && opts.excludeNodeIds.length > 0) {
    resEntries.push(["exclude", opts.excludeNodeIds.map((id) => nodeIdToBytes16(id))]);
  }
  const res = new Map<unknown, unknown>(resEntries);

  const cap = new Map<unknown, unknown>([
    ["res", res],
    ["actions", opts.actions],
  ]);

  // CWT claims (numeric keys).
  const claims = new Map<unknown, unknown>([
    [3, opts.docId], // aud
    [8, cnf], // cnf
    [-1, [cap]], // private claim `caps`
  ]);

  const payload = cborEncode(claims, rfc8949EncodeOptions);
  return coseSign1Ed25519({ payload, privateKey: opts.issuerPrivateKey });
}

export type InvitePayloadV1 = {
  v: 1;
  t: "treecrdt.playground.invite";
  docId: string;
  issuerPkB64: string;
  subjectSkB64: string;
  tokenB64: string;
};

export function encodeInvitePayload(payload: InvitePayloadV1): string {
  const text = JSON.stringify(payload);
  const bytes = new TextEncoder().encode(text);
  return base64urlEncode(bytes);
}

export function decodeInvitePayload(inviteB64: string): InvitePayloadV1 {
  const bytes = base64urlDecode(inviteB64);
  const text = new TextDecoder().decode(bytes);
  const parsed = JSON.parse(text) as Partial<InvitePayloadV1>;
  if (parsed.v !== 1) throw new Error("unsupported invite version");
  if (parsed.t !== "treecrdt.playground.invite") throw new Error("invalid invite type");
  if (!parsed.docId || typeof parsed.docId !== "string") throw new Error("invite docId missing");
  if (!parsed.issuerPkB64 || typeof parsed.issuerPkB64 !== "string") throw new Error("invite issuerPkB64 missing");
  if (!parsed.subjectSkB64 || typeof parsed.subjectSkB64 !== "string") throw new Error("invite subjectSkB64 missing");
  if (!parsed.tokenB64 || typeof parsed.tokenB64 !== "string") throw new Error("invite tokenB64 missing");
  return parsed as InvitePayloadV1;
}
