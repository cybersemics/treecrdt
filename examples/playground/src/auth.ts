import {
  createTreecrdtPayloadKeyringV1,
  generateTreecrdtDeviceWrapKeyV1,
  generateTreecrdtDocPayloadKeyV1,
  openTreecrdtDocPayloadKeyV1,
  openTreecrdtIssuerKeyV1,
  openTreecrdtLocalIdentityV1,
  rotateTreecrdtPayloadKeyringV1,
  sealTreecrdtDocPayloadKeyV1,
  sealTreecrdtIssuerKeyV1,
  sealTreecrdtLocalIdentityV1,
  type TreecrdtDeviceWrapKeyV1,
  type TreecrdtPayloadKeyringV1,
  upsertTreecrdtPayloadKeyringKeyV1,
} from "@treecrdt/crypto";
import {
  base64urlDecode,
  base64urlEncode,
  getEd25519PublicKey,
  issueDeviceCertV1,
  issueReplicaCertV1,
  issueTreecrdtCapabilityTokenV1,
  randomEd25519SecretKey,
  type TreecrdtIdentityChainV1,
} from "@treecrdt/auth";

import { prefixPlaygroundStorageKey } from "./playground/storage";

const AUTH_ENABLED_KEY = "treecrdt-playground-auth-enabled";
const REVEAL_IDENTITY_KEY = "treecrdt-playground-reveal-identity";
const DEVICE_WRAP_KEY_KEY = "treecrdt-playground-device-wrap-key:v1";

const ISSUER_PK_KEY_PREFIX = "treecrdt-playground-auth-issuer-pk:";
const ISSUER_SK_SEALED_KEY_PREFIX = "treecrdt-playground-auth-issuer-sk-sealed:";
const LOCAL_IDENTITY_SEALED_KEY_PREFIX = "treecrdt-playground-auth-local-identity-sealed:";
const DOC_PAYLOAD_KEY_SEALED_KEY_PREFIX = "treecrdt-playground-e2ee-doc-payload-key-sealed:";
const DOC_PAYLOAD_KEYRING_META_KEY_PREFIX = "treecrdt-playground-e2ee-doc-payload-keyring-meta:";
const DOC_PAYLOAD_KEYRING_SEALED_KEY_PREFIX = "treecrdt-playground-e2ee-doc-payload-keyring-sealed:";
const IDENTITY_SK_SEALED_KEY = "treecrdt-playground-identity-sk-sealed:v1";
const DEVICE_SIGNING_SK_SEALED_KEY = "treecrdt-playground-device-signing-sk-sealed:v1";
const LOCAL_IDENTITY_LABEL_V1 = "replica";

function lsGet(key: string): string | null {
  if (typeof window === "undefined") return null;
  return window.sessionStorage.getItem(prefixPlaygroundStorageKey(key));
}

function lsSet(key: string, val: string) {
  if (typeof window === "undefined") return;
  window.sessionStorage.setItem(prefixPlaygroundStorageKey(key), val);
}

function lsDel(key: string) {
  if (typeof window === "undefined") return;
  window.sessionStorage.removeItem(prefixPlaygroundStorageKey(key));
}

function gsGet(key: string): string | null {
  if (typeof window === "undefined") return null;
  return window.localStorage.getItem(prefixPlaygroundStorageKey(key));
}

function gsSet(key: string, val: string) {
  if (typeof window === "undefined") return;
  window.localStorage.setItem(prefixPlaygroundStorageKey(key), val);
}

function gsDel(key: string) {
  if (typeof window === "undefined") return;
  window.localStorage.removeItem(prefixPlaygroundStorageKey(key));
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
  const lockName = typeof window === "undefined" ? name : prefixPlaygroundStorageKey(name);
  if (locks?.request) return await locks.request(lockName, run);

  // Fallback for browsers without Web Locks API.
  if (typeof window === "undefined") return await run();
  const lockKey = prefixPlaygroundStorageKey(`treecrdt-playground-lock:${name}`);
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

async function requireDeviceWrapKeyBytes(): Promise<TreecrdtDeviceWrapKeyV1> {
  if (typeof window === "undefined") throw new Error("window is undefined");

  return await withGlobalLock("treecrdt-playground-device-wrap-key:v1", async () => {
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
  });
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

type StoredDocPayloadKeyringMetaV1 = {
  v: 1;
  activeKid: string;
  kids: string[];
};

function docPayloadKeyringMetaStorageKey(docId: string): string {
  return `${DOC_PAYLOAD_KEYRING_META_KEY_PREFIX}${docId}`;
}

function docPayloadKeyringEntryStorageKey(docId: string, kid: string): string {
  return `${DOC_PAYLOAD_KEYRING_SEALED_KEY_PREFIX}${docId}:${kid}`;
}

function parseDocPayloadKeyringMeta(raw: string | null): StoredDocPayloadKeyringMetaV1 | null {
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as Partial<StoredDocPayloadKeyringMetaV1>;
    if (parsed.v !== 1) return null;
    if (typeof parsed.activeKid !== "string" || parsed.activeKid.trim().length === 0) return null;
    if (!Array.isArray(parsed.kids) || parsed.kids.some((kid) => typeof kid !== "string" || kid.trim().length === 0)) return null;
    return { v: 1, activeKid: parsed.activeKid, kids: parsed.kids };
  } catch {
    return null;
  }
}

function equalBytes(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i += 1) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

function generateImportedPayloadKeyKid(): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) return `import-${crypto.randomUUID().slice(0, 8)}`;
  return `import-${Date.now().toString(36)}`;
}

function findPayloadKeyKidInKeyring(keyring: TreecrdtPayloadKeyringV1, payloadKey: Uint8Array): string | null {
  for (const [kid, candidate] of Object.entries(keyring.keys)) {
    if (equalBytes(candidate, payloadKey)) return kid;
  }
  return null;
}

async function writeDocPayloadKeyringV1(opts: {
  docId: string;
  wrapKey: TreecrdtDeviceWrapKeyV1;
  keyring: TreecrdtPayloadKeyringV1;
}) {
  const metaKey = docPayloadKeyringMetaStorageKey(opts.docId);
  const prevMeta = parseDocPayloadKeyringMeta(gsGet(metaKey));

  const kids = Object.keys(opts.keyring.keys).sort();
  for (const kid of kids) {
    const payloadKey = opts.keyring.keys[kid];
    if (!payloadKey) continue;
    const sealed = await sealTreecrdtDocPayloadKeyV1({
      wrapKey: opts.wrapKey,
      docId: opts.docId,
      payloadKey,
    });
    gsSet(docPayloadKeyringEntryStorageKey(opts.docId, kid), base64urlEncode(sealed));
  }

  if (prevMeta) {
    const nextKidSet = new Set(kids);
    for (const oldKid of prevMeta.kids) {
      if (!nextKidSet.has(oldKid)) gsDel(docPayloadKeyringEntryStorageKey(opts.docId, oldKid));
    }
  }

  const nextMeta: StoredDocPayloadKeyringMetaV1 = {
    v: 1,
    activeKid: opts.keyring.activeKid,
    kids,
  };
  gsSet(metaKey, JSON.stringify(nextMeta));
}

async function readDocPayloadKeyringV1OrNull(opts: {
  docId: string;
  wrapKey: TreecrdtDeviceWrapKeyV1;
}): Promise<TreecrdtPayloadKeyringV1 | null> {
  const meta = parseDocPayloadKeyringMeta(gsGet(docPayloadKeyringMetaStorageKey(opts.docId)));
  if (meta) {
    const decodedKeys: Record<string, Uint8Array> = {};
    for (const kid of meta.kids) {
      const sealedB64 = gsGet(docPayloadKeyringEntryStorageKey(opts.docId, kid));
      if (!sealedB64) continue;
      const sealedBytes = base64urlDecodeSafe(sealedB64);
      if (!sealedBytes) continue;
      const opened = await openTreecrdtDocPayloadKeyV1({ wrapKey: opts.wrapKey, docId: opts.docId, sealed: sealedBytes });
      decodedKeys[kid] = opened.payloadKey;
    }

    const decodedKids = Object.keys(decodedKeys);
    if (decodedKids.length > 0) {
      const initialKid = decodedKids[0]!;
      let keyring = createTreecrdtPayloadKeyringV1({
        payloadKey: decodedKeys[initialKid]!,
        activeKid: initialKid,
      });
      for (const kid of decodedKids) {
        if (kid === initialKid) continue;
        keyring = upsertTreecrdtPayloadKeyringKeyV1({
          keyring,
          kid,
          payloadKey: decodedKeys[kid]!,
        });
      }
      if (decodedKeys[meta.activeKid]) {
        keyring = upsertTreecrdtPayloadKeyringKeyV1({
          keyring,
          kid: meta.activeKid,
          payloadKey: decodedKeys[meta.activeKid]!,
          makeActive: true,
        });
      }
      return keyring;
    }
  }

  const legacySealedB64 = gsGet(`${DOC_PAYLOAD_KEY_SEALED_KEY_PREFIX}${opts.docId}`);
  if (!legacySealedB64) return null;
  const legacySealedBytes = base64urlDecodeSafe(legacySealedB64);
  if (!legacySealedBytes) throw new Error("legacy doc payload key blob is not valid base64url");
  const opened = await openTreecrdtDocPayloadKeyV1({ wrapKey: opts.wrapKey, docId: opts.docId, sealed: legacySealedBytes });

  return createTreecrdtPayloadKeyringV1({
    payloadKey: opened.payloadKey,
    activeKid: "legacy-k0",
  });
}

async function loadOrCreateDocPayloadKeyringV1Unlocked(opts: {
  docId: string;
  wrapKey: TreecrdtDeviceWrapKeyV1;
}): Promise<TreecrdtPayloadKeyringV1> {
  const existing = await readDocPayloadKeyringV1OrNull(opts);
  if (existing) {
    await writeDocPayloadKeyringV1({ docId: opts.docId, wrapKey: opts.wrapKey, keyring: existing });
    return existing;
  }

  const { payloadKey } = generateTreecrdtDocPayloadKeyV1({ docId: opts.docId });
  const created = createTreecrdtPayloadKeyringV1({ payloadKey });
  await writeDocPayloadKeyringV1({ docId: opts.docId, wrapKey: opts.wrapKey, keyring: created });
  return created;
}

export async function loadOrCreateDocPayloadKeyringV1(docId: string): Promise<TreecrdtPayloadKeyringV1> {
  if (!docId || docId.trim().length === 0) throw new Error("docId must not be empty");

  return await withGlobalLock(`treecrdt-playground-doc-payload-key:${docId}`, async () => {
    const wrapKey = await requireDeviceWrapKeyBytes();
    return await loadOrCreateDocPayloadKeyringV1Unlocked({ docId, wrapKey });
  });
}

export async function loadOrCreateDocPayloadKeyB64(docId: string): Promise<string> {
  const keyring = await loadOrCreateDocPayloadKeyringV1(docId);
  const active = keyring.keys[keyring.activeKid];
  if (!active) throw new Error("doc payload active key is missing");
  return base64urlEncode(active);
}

export async function getDocPayloadActiveKeyInfoB64(docId: string): Promise<{
  payloadKeyB64: string;
  payloadKeyKid: string;
}> {
  const keyring = await loadOrCreateDocPayloadKeyringV1(docId);
  const active = keyring.keys[keyring.activeKid];
  if (!active) throw new Error("doc payload active key is missing");
  return {
    payloadKeyB64: base64urlEncode(active),
    payloadKeyKid: keyring.activeKid,
  };
}

export async function rotateDocPayloadKeyB64(docId: string): Promise<{
  payloadKeyB64: string;
  payloadKeyKid: string;
}> {
  if (!docId || docId.trim().length === 0) throw new Error("docId must not be empty");

  return await withGlobalLock(`treecrdt-playground-doc-payload-key:${docId}`, async () => {
    const wrapKey = await requireDeviceWrapKeyBytes();
    const current = await loadOrCreateDocPayloadKeyringV1Unlocked({ docId, wrapKey });
    const rotated = rotateTreecrdtPayloadKeyringV1({ keyring: current });
    await writeDocPayloadKeyringV1({ docId, wrapKey, keyring: rotated.keyring });
    return {
      payloadKeyB64: base64urlEncode(rotated.rotatedPayloadKey),
      payloadKeyKid: rotated.rotatedKid,
    };
  });
}

export async function saveDocPayloadKeyB64(docId: string, payloadKeyB64: string, payloadKeyKid?: string) {
  if (!docId || docId.trim().length === 0) throw new Error("docId must not be empty");
  const payloadKey = base64urlDecodeSafe(payloadKeyB64.trim());
  if (!payloadKey || payloadKey.length !== 32) throw new Error("payload key must be a base64url-encoded 32-byte value");

  await withGlobalLock(`treecrdt-playground-doc-payload-key:${docId}`, async () => {
    const wrapKey = await requireDeviceWrapKeyBytes();
    let keyring = await loadOrCreateDocPayloadKeyringV1Unlocked({ docId, wrapKey });

    let kid: string;
    if (typeof payloadKeyKid === "string" && payloadKeyKid.trim().length > 0) {
      kid = payloadKeyKid;
    } else {
      const existingKid = findPayloadKeyKidInKeyring(keyring, payloadKey);
      kid = existingKid ?? generateImportedPayloadKeyKid();
    }

    keyring = upsertTreecrdtPayloadKeyringKeyV1({
      keyring,
      kid,
      payloadKey,
      makeActive: true,
    });

    await writeDocPayloadKeyringV1({ docId, wrapKey, keyring });
  });
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

export async function loadAuthMaterial(docId: string): Promise<StoredAuthMaterial> {
  const wrapKey = await requireDeviceWrapKeyBytes();

  const pkKey = `${ISSUER_PK_KEY_PREFIX}${docId}`;
  const sealedIssuerSkKey = `${ISSUER_SK_SEALED_KEY_PREFIX}${docId}`;
  const sealedLocalKey = `${LOCAL_IDENTITY_SEALED_KEY_PREFIX}${docId}`;

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
    const opened = await openTreecrdtLocalIdentityV1({
      wrapKey,
      docId,
      replicaLabel: LOCAL_IDENTITY_LABEL_V1,
      sealed: sealedBytes,
    });
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

export function getSealedIdentityKeyB64(): string | null {
  return gsGet(IDENTITY_SK_SEALED_KEY);
}

export function setSealedIdentityKeyB64(sealedB64: string) {
  const bytes = base64urlDecodeSafe(sealedB64.trim());
  if (!bytes) throw new Error("identity key blob must be base64url");
  gsSet(IDENTITY_SK_SEALED_KEY, base64urlEncode(bytes));
}

export function clearSealedIdentityKey() {
  gsDel(IDENTITY_SK_SEALED_KEY);
}

export function getSealedDeviceSigningKeyB64(): string | null {
  return gsGet(DEVICE_SIGNING_SK_SEALED_KEY);
}

export function setSealedDeviceSigningKeyB64(sealedB64: string) {
  const bytes = base64urlDecodeSafe(sealedB64.trim());
  if (!bytes) throw new Error("device signing key blob must be base64url");
  gsSet(DEVICE_SIGNING_SK_SEALED_KEY, base64urlEncode(bytes));
}

export function clearSealedDeviceSigningKey() {
  gsDel(DEVICE_SIGNING_SK_SEALED_KEY);
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
    const wrapKey = await requireDeviceWrapKeyBytes();
    const issuerSk = base64urlDecode(issuerSkB64);
    const sealed = await sealTreecrdtIssuerKeyV1({ wrapKey, docId, issuerSk });
    gsSet(skKey, base64urlEncode(sealed));
  }
  if (opts.forcePk) gsSet(pkKey, issuerPkB64);
  else if (!gsGet(pkKey)) gsSet(pkKey, issuerPkB64);
}

async function readLocalIdentityOrNull(docId: string): Promise<{
  localSk: Uint8Array;
  localTokens: Uint8Array[];
} | null> {
  const wrapKey = await requireDeviceWrapKeyBytes();
  const sealedB64 = lsGet(`${LOCAL_IDENTITY_SEALED_KEY_PREFIX}${docId}`);
  if (!sealedB64) return null;
  const sealed = base64urlDecodeSafe(sealedB64);
  if (!sealed) throw new Error("local identity blob is not valid base64url");
  const opened = await openTreecrdtLocalIdentityV1({ wrapKey, docId, replicaLabel: LOCAL_IDENTITY_LABEL_V1, sealed });
  return { localSk: opened.localSk, localTokens: opened.localTokens };
}

async function writeLocalIdentity(docId: string, localSk: Uint8Array, localTokens: Uint8Array[]) {
  const wrapKey = await requireDeviceWrapKeyBytes();
  const sealed = await sealTreecrdtLocalIdentityV1({
    wrapKey,
    docId,
    replicaLabel: LOCAL_IDENTITY_LABEL_V1,
    localSk,
    localTokens,
  });
  lsSet(`${LOCAL_IDENTITY_SEALED_KEY_PREFIX}${docId}`, base64urlEncode(sealed));
}

export async function saveLocalKeys(docId: string, localSkB64: string) {
  const localSk = base64urlDecode(localSkB64);
  const existing = await readLocalIdentityOrNull(docId);
  const localTokens = existing?.localTokens ?? [];
  await writeLocalIdentity(docId, localSk, localTokens);
}

export async function saveLocalTokens(docId: string, tokensB64: string[]) {
  const existing = await readLocalIdentityOrNull(docId);
  if (!existing) throw new Error("local identity is missing; cannot store capability tokens");
  const tokens = tokensB64.map((b64) => base64urlDecode(b64));
  await writeLocalIdentity(docId, existing.localSk, tokens);
}

export function clearAuthMaterial(docId: string) {
  lsDel(`${LOCAL_IDENTITY_SEALED_KEY_PREFIX}${docId}`);
}

export async function generateEd25519KeyPair(): Promise<{ sk: Uint8Array; pk: Uint8Array }> {
  const sk = randomEd25519SecretKey();
  const pk = await getEd25519PublicKey(sk);
  return { sk, pk };
}

export async function deriveEd25519PublicKey(secretKey: Uint8Array): Promise<Uint8Array> {
  return await getEd25519PublicKey(secretKey);
}

async function loadOrCreateGlobalIssuerLikeKeyPairBytes(opts: { storageKey: string; docId: string }) {
  const wrapKey = await requireDeviceWrapKeyBytes();
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
  payloadKeyKid?: string;
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
  if (parsed.payloadKeyKid !== undefined && typeof parsed.payloadKeyKid !== "string") {
    throw new Error("invite payloadKeyKid must be a string if present");
  }
  return parsed as InvitePayloadV1;
}
