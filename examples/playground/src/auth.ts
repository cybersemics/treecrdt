import {
  base64urlDecode,
  base64urlEncode,
  generateTreecrdtDeviceWrapKeyV1,
  generateTreecrdtDocPayloadKeyV1,
  issueDeviceCertV1,
  issueReplicaCertV1,
  issueTreecrdtCapabilityTokenV1,
  openTreecrdtDocPayloadKeyV1,
  openTreecrdtIssuerKeyV1,
  openTreecrdtLocalIdentityV1,
  sealTreecrdtDocPayloadKeyV1,
  sealTreecrdtIssuerKeyV1,
  sealTreecrdtLocalIdentityV1,
  type TreecrdtIdentityChainV1,
  type TreecrdtDeviceWrapKeyV1,
} from "@treecrdt/sync";

import { hashes as ed25519Hashes, getPublicKey, utils as ed25519Utils } from "@noble/ed25519";
import { sha512 } from "@noble/hashes/sha512";

ed25519Hashes.sha512 = sha512;

const AUTH_ENABLED_KEY = "treecrdt-playground-auth-enabled";
const REVEAL_IDENTITY_KEY = "treecrdt-playground-reveal-identity";
const DEVICE_WRAP_KEY_KEY = "treecrdt-playground-device-wrap-key:v1";

const ISSUER_PK_KEY_PREFIX = "treecrdt-playground-auth-issuer-pk:";
const ISSUER_SK_SEALED_KEY_PREFIX = "treecrdt-playground-auth-issuer-sk-sealed:";
const LOCAL_IDENTITY_SEALED_KEY_PREFIX = "treecrdt-playground-auth-local-identity-sealed:";
const DOC_PAYLOAD_KEY_SEALED_KEY_PREFIX = "treecrdt-playground-e2ee-doc-payload-key-sealed:";
const IDENTITY_SK_SEALED_KEY = "treecrdt-playground-identity-sk-sealed:v1";
const DEVICE_SIGNING_SK_SEALED_KEY = "treecrdt-playground-device-signing-sk-sealed:v1";

// Legacy (plaintext) keys: auto-migrated and deleted on load.
const LEGACY_ISSUER_SK_KEY_PREFIX = "treecrdt-playground-auth-issuer-sk:";
const LEGACY_LOCAL_PK_KEY_PREFIX = "treecrdt-playground-auth-local-pk:";
const LEGACY_LOCAL_SK_KEY_PREFIX = "treecrdt-playground-auth-local-sk:";
const LEGACY_LOCAL_TOKENS_KEY_PREFIX = "treecrdt-playground-auth-local-tokens:";

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

function gsDel(key: string) {
  if (typeof window === "undefined") return;
  window.localStorage.removeItem(key);
}

function base64urlDecodeSafe(b64: string): Uint8Array | null {
  try {
    return base64urlDecode(b64);
  } catch {
    return null;
  }
}

async function withGlobalLock<T>(name: string, run: () => Promise<T>): Promise<T> {
  const locks = typeof navigator === "undefined" ? null : (navigator as any).locks;
  if (locks?.request) return await locks.request(name, run);

  // Fallback for browsers without Web Locks API.
  if (typeof window === "undefined") return await run();
  const lockKey = `treecrdt-playground-lock:${name}`;
  const lockId = typeof crypto !== "undefined" && "randomUUID" in crypto ? crypto.randomUUID() : `${Math.random()}`;
  const now = () => Date.now();
  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
  const tryParseLock = (raw: string | null): { id: string; ts: number } | null => {
    if (!raw) return null;
    try {
      const parsed = JSON.parse(raw) as unknown;
      if (!parsed || typeof parsed !== "object") return null;
      const rec = parsed as Partial<{ id: unknown; ts: unknown }>;
      if (typeof rec.id !== "string" || typeof rec.ts !== "number") return null;
      return { id: rec.id, ts: rec.ts };
    } catch {
      return null;
    }
  };

  const ttlMs = 10_000;
  const started = now();
  while (true) {
    const t = now();
    const existing = tryParseLock(window.localStorage.getItem(lockKey));
    if (!existing || t - existing.ts > ttlMs) {
      window.localStorage.setItem(lockKey, JSON.stringify({ id: lockId, ts: t }));
    }
    const confirm = tryParseLock(window.localStorage.getItem(lockKey));
    if (confirm?.id === lockId) break;
    if (t - started > ttlMs) break;
    await sleep(25);
  }

  try {
    return await run();
  } finally {
    const confirm = tryParseLock(window.localStorage.getItem(lockKey));
    if (confirm?.id === lockId) window.localStorage.removeItem(lockKey);
  }
}

function requireDeviceWrapKeyBytes(): TreecrdtDeviceWrapKeyV1 {
  if (typeof window === "undefined") throw new Error("window is undefined");
  const existing = gsGet(DEVICE_WRAP_KEY_KEY);
  if (!existing) {
    const wrapKey = generateTreecrdtDeviceWrapKeyV1();
    const b64 = base64urlEncode(wrapKey);
    gsSet(DEVICE_WRAP_KEY_KEY, b64);
    return wrapKey;
  }

  const bytes = base64urlDecodeSafe(existing);
  if (!bytes || bytes.length !== 32) {
    throw new Error("device wrap key is invalid; reset storage or import a valid key");
  }
  return bytes;
}

export function getDeviceWrapKeyB64(): string | null {
  return gsGet(DEVICE_WRAP_KEY_KEY);
}

export function importDeviceWrapKeyB64(b64: string) {
  if (typeof window === "undefined") return;
  const bytes = base64urlDecodeSafe(b64.trim());
  if (!bytes || bytes.length !== 32) {
    throw new Error("device wrap key must be a base64url-encoded 32-byte value");
  }
  gsSet(DEVICE_WRAP_KEY_KEY, base64urlEncode(bytes));
}

export function clearDeviceWrapKey() {
  gsDel(DEVICE_WRAP_KEY_KEY);
}

export async function loadOrCreateDocPayloadKeyB64(docId: string): Promise<string> {
  if (!docId || docId.trim().length === 0) throw new Error("docId must not be empty");

  return await withGlobalLock(`treecrdt-playground-doc-payload-key:${docId}`, async () => {
    const wrapKey = requireDeviceWrapKeyBytes();
    const sealedKey = `${DOC_PAYLOAD_KEY_SEALED_KEY_PREFIX}${docId}`;

    if (!gsGet(sealedKey)) {
      const { payloadKey } = generateTreecrdtDocPayloadKeyV1({ docId });
      const sealed = await sealTreecrdtDocPayloadKeyV1({ wrapKey, docId, payloadKey });
      gsSet(sealedKey, base64urlEncode(sealed));
    }

    const sealedB64 = gsGet(sealedKey);
    if (!sealedB64) throw new Error("doc payload key is missing after initialization");
    const sealedBytes = base64urlDecodeSafe(sealedB64);
    if (!sealedBytes) throw new Error("doc payload key blob is not valid base64url");

    const opened = await openTreecrdtDocPayloadKeyV1({ wrapKey, docId, sealed: sealedBytes });
    return base64urlEncode(opened.payloadKey);
  });
}

export async function saveDocPayloadKeyB64(docId: string, payloadKeyB64: string) {
  if (!docId || docId.trim().length === 0) throw new Error("docId must not be empty");
  const payloadKey = base64urlDecodeSafe(payloadKeyB64.trim());
  if (!payloadKey || payloadKey.length !== 32) throw new Error("payload key must be a base64url-encoded 32-byte value");

  const wrapKey = requireDeviceWrapKeyBytes();
  const sealed = await sealTreecrdtDocPayloadKeyV1({ wrapKey, docId, payloadKey });
  gsSet(`${DOC_PAYLOAD_KEY_SEALED_KEY_PREFIX}${docId}`, base64urlEncode(sealed));
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

export function initialRevealIdentity(): boolean {
  if (typeof window === "undefined") return false;
  const param = new URLSearchParams(window.location.search).get("revealIdentity");
  if (param === "0") return false;
  if (param === "1") return true;
  const stored = lsGet(REVEAL_IDENTITY_KEY);
  if (stored === "0") return false;
  if (stored === "1") return true;
  return false;
}

export function persistRevealIdentity(enabled: boolean) {
  if (typeof window === "undefined") return;
  lsSet(REVEAL_IDENTITY_KEY, enabled ? "1" : "0");
}

export type StoredAuthMaterial = {
  issuerPkB64: string | null;
  issuerSkB64: string | null;
  localPkB64: string | null;
  localSkB64: string | null;
  localTokensB64: string[];
};

export async function loadAuthMaterial(docId: string, replicaLabel: string): Promise<StoredAuthMaterial> {
  const wrapKey = requireDeviceWrapKeyBytes();

  const pkKey = `${ISSUER_PK_KEY_PREFIX}${docId}`;
  const legacyIssuerSkKey = `${LEGACY_ISSUER_SK_KEY_PREFIX}${docId}`;
  const sealedIssuerSkKey = `${ISSUER_SK_SEALED_KEY_PREFIX}${docId}`;
  const sealedLocalKey = `${LOCAL_IDENTITY_SEALED_KEY_PREFIX}${docId}:${replicaLabel}`;

  // Migrate legacy issuer secret key (plaintext) -> sealed.
  if (!gsGet(sealedIssuerSkKey)) {
    const legacyIssuerSkB64 = gsGet(legacyIssuerSkKey);
    if (legacyIssuerSkB64) {
      const legacyIssuerSk = base64urlDecodeSafe(legacyIssuerSkB64);
      if (legacyIssuerSk && legacyIssuerSk.length === 32) {
        const sealed = await sealTreecrdtIssuerKeyV1({ wrapKey, docId, issuerSk: legacyIssuerSk });
        gsSet(sealedIssuerSkKey, base64urlEncode(sealed));
      }
      gsDel(legacyIssuerSkKey);
    }
  }

  // Migrate legacy local identity (plaintext) -> sealed.
  if (!lsGet(sealedLocalKey)) {
    const legacyLocalSkB64 = lsGet(`${LEGACY_LOCAL_SK_KEY_PREFIX}${docId}:${replicaLabel}`);
    if (legacyLocalSkB64) {
      const legacyLocalSk = base64urlDecodeSafe(legacyLocalSkB64);
      const legacyTokensRaw = lsGet(`${LEGACY_LOCAL_TOKENS_KEY_PREFIX}${docId}:${replicaLabel}`);
      let legacyTokens: Uint8Array[] = [];
      if (legacyTokensRaw) {
        try {
          const parsed = JSON.parse(legacyTokensRaw) as unknown;
          if (Array.isArray(parsed) && parsed.every((x) => typeof x === "string")) {
            legacyTokens = (parsed as string[]).map((b64) => base64urlDecode(b64));
          }
        } catch {
          // ignore invalid legacy tokens payload
        }
      }

      if (legacyLocalSk && legacyLocalSk.length === 32) {
        const sealed = await sealTreecrdtLocalIdentityV1({
          wrapKey,
          docId,
          replicaLabel,
          localSk: legacyLocalSk,
          localTokens: legacyTokens,
        });
        lsSet(sealedLocalKey, base64urlEncode(sealed));
      }

      lsDel(`${LEGACY_LOCAL_PK_KEY_PREFIX}${docId}:${replicaLabel}`);
      lsDel(`${LEGACY_LOCAL_SK_KEY_PREFIX}${docId}:${replicaLabel}`);
      lsDel(`${LEGACY_LOCAL_TOKENS_KEY_PREFIX}${docId}:${replicaLabel}`);
    }
  }

  // Issuer keys are shared across tabs for the same doc so multiple peers can sync without manually exchanging invites.
  let issuerPkB64 = gsGet(pkKey);
  let issuerSkB64: string | null = null;
  const sealedIssuerSkB64 = gsGet(sealedIssuerSkKey);
  if (sealedIssuerSkB64) {
    const sealedIssuerSkBytes = base64urlDecodeSafe(sealedIssuerSkB64);
    if (!sealedIssuerSkBytes) throw new Error("issuer key blob is not valid base64url");
    const opened = await openTreecrdtIssuerKeyV1({ wrapKey, docId, sealed: sealedIssuerSkBytes });
    issuerSkB64 = base64urlEncode(opened.issuerSk);

    // Keep issuer public key consistent (derived from issuerSk).
    const derivedIssuerPkB64 = base64urlEncode(opened.issuerPk);
    if (!issuerPkB64 || issuerPkB64 !== derivedIssuerPkB64) {
      gsSet(pkKey, derivedIssuerPkB64);
      issuerPkB64 = derivedIssuerPkB64;
    }
  }

  let localPkB64: string | null = null;
  let localSkB64: string | null = null;
  let localTokensB64: string[] = [];
  const localSealedB64 = lsGet(sealedLocalKey);
  if (localSealedB64) {
    const sealedBytes = base64urlDecodeSafe(localSealedB64);
    if (!sealedBytes) throw new Error("local identity blob is not valid base64url");
    const opened = await openTreecrdtLocalIdentityV1({ wrapKey, docId, replicaLabel, sealed: sealedBytes });
    localPkB64 = base64urlEncode(opened.localPk);
    localSkB64 = base64urlEncode(opened.localSk);
    localTokensB64 = opened.localTokens.map((t) => base64urlEncode(t));
  }

  return { issuerPkB64, issuerSkB64, localPkB64, localSkB64, localTokensB64 };
}

export function getSealedIssuerKeyB64(docId: string): string | null {
  return gsGet(`${ISSUER_SK_SEALED_KEY_PREFIX}${docId}`);
}

export function setSealedIssuerKeyB64(docId: string, sealedB64: string) {
  const bytes = base64urlDecodeSafe(sealedB64.trim());
  if (!bytes) throw new Error("issuer key blob must be base64url");
  gsSet(`${ISSUER_SK_SEALED_KEY_PREFIX}${docId}`, base64urlEncode(bytes));
}

export async function saveIssuerKeys(
  docId: string,
  issuerPkB64: string,
  issuerSkB64?: string,
  opts: { forcePk?: boolean } = {}
) {
  const pkKey = `${ISSUER_PK_KEY_PREFIX}${docId}`;
  const skKey = `${ISSUER_SK_SEALED_KEY_PREFIX}${docId}`;

  // Avoid clobbering issuer keys when multiple tabs initialize concurrently.
  if (issuerSkB64 && !gsGet(skKey)) {
    const wrapKey = requireDeviceWrapKeyBytes();
    const issuerSk = base64urlDecode(issuerSkB64);
    const sealed = await sealTreecrdtIssuerKeyV1({ wrapKey, docId, issuerSk });
    gsSet(skKey, base64urlEncode(sealed));
  }
  if (opts.forcePk) gsSet(pkKey, issuerPkB64);
  else if (!gsGet(pkKey)) gsSet(pkKey, issuerPkB64);
}

async function readLocalIdentityOrNull(docId: string, replicaLabel: string): Promise<{
  localSk: Uint8Array;
  localTokens: Uint8Array[];
} | null> {
  const wrapKey = requireDeviceWrapKeyBytes();
  const sealedB64 = lsGet(`${LOCAL_IDENTITY_SEALED_KEY_PREFIX}${docId}:${replicaLabel}`);
  if (!sealedB64) return null;
  const sealed = base64urlDecodeSafe(sealedB64);
  if (!sealed) throw new Error("local identity blob is not valid base64url");
  const opened = await openTreecrdtLocalIdentityV1({ wrapKey, docId, replicaLabel, sealed });
  return { localSk: opened.localSk, localTokens: opened.localTokens };
}

async function writeLocalIdentity(docId: string, replicaLabel: string, localSk: Uint8Array, localTokens: Uint8Array[]) {
  const wrapKey = requireDeviceWrapKeyBytes();
  const sealed = await sealTreecrdtLocalIdentityV1({ wrapKey, docId, replicaLabel, localSk, localTokens });
  lsSet(`${LOCAL_IDENTITY_SEALED_KEY_PREFIX}${docId}:${replicaLabel}`, base64urlEncode(sealed));
}

export async function saveLocalKeys(docId: string, replicaLabel: string, _localPkB64: string, localSkB64: string) {
  const localSk = base64urlDecode(localSkB64);
  const existing = await readLocalIdentityOrNull(docId, replicaLabel);
  const localTokens = existing?.localTokens ?? [];
  await writeLocalIdentity(docId, replicaLabel, localSk, localTokens);
}

export async function saveLocalTokens(docId: string, replicaLabel: string, tokensB64: string[]) {
  const existing = await readLocalIdentityOrNull(docId, replicaLabel);
  if (!existing) throw new Error("local identity is missing; cannot store capability tokens");
  const tokens = tokensB64.map((b64) => base64urlDecode(b64));
  await writeLocalIdentity(docId, replicaLabel, existing.localSk, tokens);
}

export function clearAuthMaterial(docId: string, replicaLabel: string) {
  lsDel(`${LOCAL_IDENTITY_SEALED_KEY_PREFIX}${docId}:${replicaLabel}`);
  // Also clear legacy keys in case the app crashes before migration.
  lsDel(`${LEGACY_LOCAL_PK_KEY_PREFIX}${docId}:${replicaLabel}`);
  lsDel(`${LEGACY_LOCAL_SK_KEY_PREFIX}${docId}:${replicaLabel}`);
  lsDel(`${LEGACY_LOCAL_TOKENS_KEY_PREFIX}${docId}:${replicaLabel}`);
}

export async function generateEd25519KeyPair(): Promise<{ sk: Uint8Array; pk: Uint8Array }> {
  const sk = ed25519Utils.randomSecretKey();
  const pk = await getPublicKey(sk);
  return { sk, pk };
}

export async function deriveEd25519PublicKey(secretKey: Uint8Array): Promise<Uint8Array> {
  return await getPublicKey(secretKey);
}

async function loadOrCreateGlobalIssuerLikeKeyPairBytes(opts: { storageKey: string; docId: string }) {
  const wrapKey = requireDeviceWrapKeyBytes();
  await withGlobalLock(`playground-global-key:${opts.docId}`, async () => {
    if (gsGet(opts.storageKey)) return;
    const { sk } = await generateEd25519KeyPair();
    const sealed = await sealTreecrdtIssuerKeyV1({ wrapKey, docId: opts.docId, issuerSk: sk });
    gsSet(opts.storageKey, base64urlEncode(sealed));
  });

  const sealedB64 = gsGet(opts.storageKey);
  if (!sealedB64) throw new Error("global key is missing after initialization");
  const sealedBytes = base64urlDecodeSafe(sealedB64);
  if (!sealedBytes) throw new Error("global key blob is not valid base64url");

  const opened = await openTreecrdtIssuerKeyV1({ wrapKey, docId: opts.docId, sealed: sealedBytes });
  return { sk: opened.issuerSk, pk: opened.issuerPk };
}

export async function loadOrCreateIdentityKeyPair(): Promise<{ sk: Uint8Array; pk: Uint8Array }> {
  return await loadOrCreateGlobalIssuerLikeKeyPairBytes({
    storageKey: IDENTITY_SK_SEALED_KEY,
    docId: "__treecrdt_playground_identity__",
  });
}

export async function loadOrCreateDeviceSigningKeyPair(): Promise<{ sk: Uint8Array; pk: Uint8Array }> {
  return await loadOrCreateGlobalIssuerLikeKeyPairBytes({
    storageKey: DEVICE_SIGNING_SK_SEALED_KEY,
    docId: "__treecrdt_playground_device_signing__",
  });
}

export async function createLocalIdentityChainV1(opts: {
  docId: string;
  replicaPublicKey: Uint8Array;
}): Promise<TreecrdtIdentityChainV1> {
  if (!opts.docId || opts.docId.trim().length === 0) throw new Error("docId must not be empty");
  if (!(opts.replicaPublicKey instanceof Uint8Array)) throw new Error("replicaPublicKey must be bytes");

  const identity = await loadOrCreateIdentityKeyPair();
  const device = await loadOrCreateDeviceSigningKeyPair();

  const deviceCertBytes = issueDeviceCertV1({
    identityPrivateKey: identity.sk,
    devicePublicKey: device.pk,
  });

  const replicaCertBytes = issueReplicaCertV1({
    devicePrivateKey: device.sk,
    docId: opts.docId,
    replicaPublicKey: opts.replicaPublicKey,
  });

  return {
    identityPublicKey: identity.pk,
    deviceCertBytes,
    replicaCertBytes,
  };
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
  return issueTreecrdtCapabilityTokenV1({
    issuerPrivateKey: opts.issuerPrivateKey,
    subjectPublicKey: opts.subjectPublicKey,
    docId: opts.docId,
    actions: opts.actions,
    rootNodeId: opts.rootNodeId,
    ...(opts.maxDepth !== undefined ? { maxDepth: opts.maxDepth } : {}),
    ...(opts.excludeNodeIds ? { excludeNodeIds: opts.excludeNodeIds } : {}),
  });
}

export type InvitePayloadV1 = {
  v: 1;
  t: "treecrdt.playground.invite";
  docId: string;
  issuerPkB64: string;
  subjectSkB64: string;
  tokenB64: string;
  payloadKeyB64?: string;
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
  if (parsed.payloadKeyB64 !== undefined && typeof parsed.payloadKeyB64 !== "string") {
    throw new Error("invite payloadKeyB64 must be a string if present");
  }
  return parsed as InvitePayloadV1;
}
