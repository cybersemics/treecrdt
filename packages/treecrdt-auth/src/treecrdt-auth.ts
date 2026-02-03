import { blake3 } from "@noble/hashes/blake3";
import { utf8ToBytes } from "@noble/hashes/utils";
import { sha512 } from "@noble/hashes/sha512";
import { hashes as ed25519Hashes, sign as ed25519Sign, verify as ed25519Verify } from "@noble/ed25519";

import { decode as cborDecode, encode as cborEncode, rfc8949EncodeOptions } from "cborg";

import type { Operation } from "@treecrdt/interface";
import { bytesToHex, nodeIdToBytes16, replicaIdToBytes, ROOT_NODE_ID_HEX } from "@treecrdt/interface/ids";

import type { Capability, Filter, Hello, HelloAck, OpAuth, SyncAuth } from "@treecrdt/sync";
import { base64urlDecode, base64urlEncode } from "./base64url.js";
import { coseDecodeSign1, coseSign1Ed25519, coseVerifySign1Ed25519, deriveTokenIdV1 } from "./cose.js";
import {
  createTreecrdtIdentityChainCapabilityV1,
  TREECRDT_IDENTITY_CHAIN_CAPABILITY,
  verifyTreecrdtIdentityChainCapabilityV1,
  type TreecrdtIdentityChainV1,
  type VerifiedTreecrdtIdentityChainV1,
} from "./identity.js";

const TREECRDT_DELEGATION_PROOF_HEADER_V1 = "treecrdt.delegation_proof_v1";

let ed25519Ready = false;
function ensureEd25519(): void {
  if (ed25519Ready) return;
  ed25519Hashes.sha512 = sha512;
  ed25519Ready = true;
}

function concatBytes(...parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((acc, p) => acc + p.length, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const p of parts) {
    out.set(p, offset);
    offset += p.length;
  }
  return out;
}

function u8(n: number): Uint8Array {
  if (!Number.isInteger(n) || n < 0 || n > 0xff) throw new Error(`u8 out of range: ${n}`);
  return new Uint8Array([n]);
}

function u32be(n: number): Uint8Array {
  if (!Number.isInteger(n) || n < 0 || n > 0xffff_ffff) throw new Error(`u32 out of range: ${n}`);
  const out = new Uint8Array(4);
  new DataView(out.buffer).setUint32(0, n, false);
  return out;
}

function u64be(n: bigint | number): Uint8Array {
  const v = typeof n === "bigint" ? n : BigInt(n);
  if (v < 0n || v > 0xffff_ffff_ffff_ffffn) throw new Error(`u64 out of range: ${v}`);
  const out = new Uint8Array(8);
  new DataView(out.buffer).setBigUint64(0, v, false);
  return out;
}

const KEY_ID_V1_DOMAIN = utf8ToBytes("treecrdt/keyid/v1");
export function deriveKeyIdV1(pubkey: Uint8Array): Uint8Array {
  return blake3(concatBytes(KEY_ID_V1_DOMAIN, pubkey)).slice(0, 16);
}

function encodeCbor(value: unknown): Uint8Array {
  return cborEncode(value, rfc8949EncodeOptions);
}

export function issueTreecrdtCapabilityTokenV1(opts: {
  issuerPrivateKey: Uint8Array;
  subjectPublicKey: Uint8Array;
  docId: string;
  actions: string[];
  rootNodeId?: string;
  maxDepth?: number;
  excludeNodeIds?: string[];
  exp?: number;
  nbf?: number;
}): Uint8Array {
  if (!opts.docId || opts.docId.trim().length === 0) throw new Error("docId must not be empty");
  if (opts.subjectPublicKey.length !== 32) throw new Error("subjectPublicKey must be 32 bytes");
  if (!Array.isArray(opts.actions) || opts.actions.length === 0) throw new Error("actions must be a non-empty array");

  const cnf = new Map<unknown, unknown>([
    ["pub", opts.subjectPublicKey],
    ["kid", deriveKeyIdV1(opts.subjectPublicKey)],
  ]);

  const resEntries: Array<[unknown, unknown]> = [["doc_id", opts.docId]];
  if (opts.rootNodeId !== undefined) resEntries.push(["root", nodeIdToBytes16(opts.rootNodeId)]);
  if (opts.maxDepth !== undefined) resEntries.push(["max_depth", opts.maxDepth]);
  if (opts.excludeNodeIds && opts.excludeNodeIds.length > 0) {
    resEntries.push(["exclude", opts.excludeNodeIds.map((id) => nodeIdToBytes16(id))]);
  }
  const res = new Map<unknown, unknown>(resEntries);

  const cap = new Map<unknown, unknown>([
    ["res", res],
    ["actions", opts.actions],
  ]);

  const claims = new Map<unknown, unknown>([
    [3, opts.docId], // aud
    [8, cnf], // cnf
    [-1, [cap]], // private claim `caps`
  ]);
  if (opts.exp !== undefined) claims.set(4, opts.exp);
  if (opts.nbf !== undefined) claims.set(5, opts.nbf);

  return coseSign1Ed25519({ payload: encodeCbor(claims), privateKey: opts.issuerPrivateKey });
}

export function issueTreecrdtDelegatedCapabilityTokenV1(opts: {
  delegatorPrivateKey: Uint8Array;
  delegatorProofToken: Uint8Array;
  subjectPublicKey: Uint8Array;
  docId: string;
  actions: string[];
  rootNodeId?: string;
  maxDepth?: number;
  excludeNodeIds?: string[];
  exp?: number;
  nbf?: number;
}): Uint8Array {
  if (!opts.docId || opts.docId.trim().length === 0) throw new Error("docId must not be empty");
  if (opts.delegatorPrivateKey.length !== 32) throw new Error("delegatorPrivateKey must be 32 bytes");
  if (opts.subjectPublicKey.length !== 32) throw new Error("subjectPublicKey must be 32 bytes");
  if (!Array.isArray(opts.actions) || opts.actions.length === 0) throw new Error("actions must be a non-empty array");

  const cnf = new Map<unknown, unknown>([
    ["pub", opts.subjectPublicKey],
    ["kid", deriveKeyIdV1(opts.subjectPublicKey)],
  ]);

  const resEntries: Array<[unknown, unknown]> = [["doc_id", opts.docId]];
  if (opts.rootNodeId !== undefined) resEntries.push(["root", nodeIdToBytes16(opts.rootNodeId)]);
  if (opts.maxDepth !== undefined) resEntries.push(["max_depth", opts.maxDepth]);
  if (opts.excludeNodeIds && opts.excludeNodeIds.length > 0) {
    resEntries.push(["exclude", opts.excludeNodeIds.map((id) => nodeIdToBytes16(id))]);
  }
  const res = new Map<unknown, unknown>(resEntries);

  const cap = new Map<unknown, unknown>([
    ["res", res],
    ["actions", opts.actions],
  ]);

  const claims = new Map<unknown, unknown>([
    [3, opts.docId], // aud
    [8, cnf], // cnf
    [-1, [cap]], // private claim `caps`
  ]);
  if (opts.exp !== undefined) claims.set(4, opts.exp);
  if (opts.nbf !== undefined) claims.set(5, opts.nbf);

  const unprotectedHeader = new Map<unknown, unknown>([[TREECRDT_DELEGATION_PROOF_HEADER_V1, [opts.delegatorProofToken]]]);
  return coseSign1Ed25519({
    payload: encodeCbor(claims),
    privateKey: opts.delegatorPrivateKey,
    unprotectedHeader,
  });
}

export type TreecrdtCapabilityV1 = {
  actions: string[];
  res: {
    docId: string;
    rootNodeId?: string;
    maxDepth?: number;
    excludeNodeIds?: string[];
  };
};

export type TreecrdtCapabilityTokenV1 = {
  tokenId: Uint8Array;
  subjectKeyId: Uint8Array;
  subjectPublicKey: Uint8Array;
  caps: TreecrdtCapabilityV1[];
  exp?: number;
  nbf?: number;
};

const OP_SIG_V1_DOMAIN = utf8ToBytes("treecrdt/op-sig/v1");

export function encodeTreecrdtOpSigInputV1(opts: { docId: string; op: Operation }): Uint8Array {
  const docIdBytes = utf8ToBytes(opts.docId);
  const replicaBytes = replicaIdToBytes(opts.op.meta.id.replica);

  const counter = opts.op.meta.id.counter;
  const lamport = opts.op.meta.lamport;

  let kindTag: number;
  let kindFields: Uint8Array;

  switch (opts.op.kind.type) {
    case "insert": {
      kindTag = 1;
      const parent = nodeIdToBytes16(opts.op.kind.parent);
      const node = nodeIdToBytes16(opts.op.kind.node);
      const orderKey = opts.op.kind.orderKey;
      const orderKeyLen = u32be(orderKey.length);
      const payload = opts.op.kind.payload;
      if (payload) {
        kindFields = concatBytes(parent, node, orderKeyLen, orderKey, u8(1), u32be(payload.length), payload);
      } else {
        kindFields = concatBytes(parent, node, orderKeyLen, orderKey, u8(0));
      }
      break;
    }
    case "move": {
      kindTag = 2;
      const node = nodeIdToBytes16(opts.op.kind.node);
      const newParent = nodeIdToBytes16(opts.op.kind.newParent);
      const orderKey = opts.op.kind.orderKey;
      const orderKeyLen = u32be(orderKey.length);
      kindFields = concatBytes(node, newParent, orderKeyLen, orderKey);
      break;
    }
    case "delete": {
      kindTag = 3;
      const node = nodeIdToBytes16(opts.op.kind.node);
      kindFields = node;
      break;
    }
    case "tombstone": {
      kindTag = 4;
      const node = nodeIdToBytes16(opts.op.kind.node);
      kindFields = node;
      break;
    }
    case "payload": {
      kindTag = 5;
      const node = nodeIdToBytes16(opts.op.kind.node);
      const payload = opts.op.kind.payload;
      if (payload === null) {
        kindFields = concatBytes(node, u8(0));
      } else {
        kindFields = concatBytes(node, u8(1), u32be(payload.length), payload);
      }
      break;
    }
    default: {
      const _exhaustive: never = opts.op.kind;
      throw new Error(`unknown op kind: ${String((_exhaustive as any)?.type)}`);
    }
  }

  return concatBytes(
    OP_SIG_V1_DOMAIN,
    u8(0),
    u32be(docIdBytes.length),
    docIdBytes,
    u32be(replicaBytes.length),
    replicaBytes,
    u64be(counter),
    u64be(lamport),
    u8(kindTag),
    kindFields
  );
}

export async function signTreecrdtOpV1(opts: { docId: string; op: Operation; privateKey: Uint8Array }): Promise<Uint8Array> {
  ensureEd25519();
  const msg = encodeTreecrdtOpSigInputV1({ docId: opts.docId, op: opts.op });
  return ed25519Sign(msg, opts.privateKey);
}

export async function verifyTreecrdtOpV1(opts: {
  docId: string;
  op: Operation;
  signature: Uint8Array;
  publicKey: Uint8Array;
}): Promise<boolean> {
  ensureEd25519();
  const msg = encodeTreecrdtOpSigInputV1({ docId: opts.docId, op: opts.op });
  return ed25519Verify(opts.signature, msg, opts.publicKey);
}

type CapabilityGrant = {
  tokenId: Uint8Array;
  keyId: Uint8Array;
  publicKey: Uint8Array;
  caps: unknown;
  exp?: number;
  nbf?: number;
};

function toNumber(val: unknown, field: string): number | undefined {
  if (val === undefined || val === null) return undefined;
  if (typeof val === "number") return val;
  if (typeof val === "bigint") {
    if (val > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error(`${field} too large`);
    return Number(val);
  }
  throw new Error(`${field} must be a number`);
}

function mapGet(map: Map<unknown, unknown>, key: unknown): unknown {
  return map.has(key) ? map.get(key) : undefined;
}

function getClaim(map: Map<unknown, unknown>, numKey: number, strKey: string): unknown {
  return mapGet(map, numKey) ?? mapGet(map, strKey);
}

function requiredActionsForOp(op: Operation): string[] {
  switch (op.kind.type) {
    case "insert":
      return op.kind.payload ? ["write_structure", "write_payload"] : ["write_structure"];
    case "move":
      return ["write_structure"];
    case "delete":
      return ["delete"];
    case "tombstone":
      return ["tombstone"];
    case "payload":
      return ["write_payload"];
  }
}

type TreecrdtSubtreeScope = {
  root: Uint8Array;
  maxDepth?: number;
  exclude?: Uint8Array[];
};

export type TreecrdtScopeEvaluator = (opts: {
  docId: string;
  node: Uint8Array;
  scope: TreecrdtSubtreeScope;
}) => "allow" | "deny" | "unknown" | Promise<"allow" | "deny" | "unknown">;

function getField(obj: unknown, key: string): unknown {
  if (!obj || typeof obj !== "object") return undefined;
  if (obj instanceof Map) return mapGet(obj, key);
  return (obj as any)[key];
}

type ScopeTri = "allow" | "deny" | "unknown";

function triOr(a: ScopeTri, b: ScopeTri): ScopeTri {
  if (a === "allow" || b === "allow") return "allow";
  if (a === "unknown" || b === "unknown") return "unknown";
  return "deny";
}

function triAnd(a: ScopeTri, b: ScopeTri): ScopeTri {
  if (a === "deny" || b === "deny") return "deny";
  if (a === "unknown" || b === "unknown") return "unknown";
  return "allow";
}

function getRequiredScopeChecks(op: Operation): Array<{ node: Uint8Array; actions: string[] }> {
  const actions = requiredActionsForOp(op);
  switch (op.kind.type) {
    case "insert":
      return [{ node: nodeIdToBytes16(op.kind.parent), actions }];
    case "move":
      // v1: require authorization for both source (node) and destination (new_parent).
      return [
        { node: nodeIdToBytes16(op.kind.node), actions },
        { node: nodeIdToBytes16(op.kind.newParent), actions },
      ];
    case "delete":
    case "tombstone":
    case "payload":
      return [{ node: nodeIdToBytes16(op.kind.node), actions }];
    default: {
      const _exhaustive: never = op.kind;
      throw new Error(`unknown op kind: ${String((_exhaustive as any)?.type)}`);
    }
  }
}

function parseScope(res: unknown): TreecrdtSubtreeScope | undefined {
  if (!res || typeof res !== "object") return undefined;

  const root = (getField(res, "root") ?? getField(res, "root_node_id")) as unknown;
  if (!(root instanceof Uint8Array)) return undefined;

  const maxDepthRaw = getField(res, "max_depth") ?? getField(res, "maxDepth") ?? getField(res, "depth");
  const maxDepth = toNumber(maxDepthRaw, "max_depth");

  const excludeRaw = getField(res, "exclude") ?? getField(res, "exclude_node_ids") ?? getField(res, "excludeNodeIds");
  let exclude: Uint8Array[] | undefined;
  if (excludeRaw !== undefined && excludeRaw !== null) {
    if (!Array.isArray(excludeRaw)) throw new Error("capability res.exclude must be an array");
    exclude = excludeRaw.map((v) => {
      if (!(v instanceof Uint8Array)) throw new Error("capability res.exclude entries must be bytes");
      return v;
    });
  }

  return { root, ...(maxDepth !== undefined ? { maxDepth } : {}), ...(exclude ? { exclude } : {}) };
}

async function capAllowsNode(opts: {
  cap: unknown;
  docId: string;
  node: Uint8Array;
  requiredActions: readonly string[];
  scopeEvaluator?: TreecrdtScopeEvaluator;
}): Promise<ScopeTri> {
  const res = getField(opts.cap, "res");
  const actions = getField(opts.cap, "actions");
  if (!res || typeof res !== "object") return "deny";
  if (!Array.isArray(actions)) return "deny";
  if (getField(res, "doc_id") !== opts.docId) return "deny";

  const actionSet = new Set(actions.map(String));
  // Convenience: writing or mutating implies the ability to read structure/payload
  // for the same scope (apps can still issue explicit read-only tokens).
  if (
    actionSet.has("write_structure") ||
    actionSet.has("write_payload") ||
    actionSet.has("delete") ||
    actionSet.has("tombstone")
  ) {
    actionSet.add("read_structure");
  }
  if (actionSet.has("write_payload")) actionSet.add("read_payload");
  if (!opts.requiredActions.every((a) => actionSet.has(a))) return "deny";

  const scope = parseScope(res);
  if (!scope) {
    // v0 compatibility: if no scope is present, treat as doc-wide allow.
    return "allow";
  }

  const rootHex = bytesToHex(scope.root);
  const nodeHex = bytesToHex(opts.node);

  // Common fast paths that do not require a tree view:
  if (nodeHex === rootHex) return "allow";
  if (rootHex === ROOT_NODE_ID_HEX && !scope.exclude && scope.maxDepth === undefined) return "allow";

  if (!opts.scopeEvaluator) return "unknown";
  return await opts.scopeEvaluator({ docId: opts.docId, node: opts.node, scope });
}

async function capsAllowsNodeAccess(opts: {
  caps: unknown;
  docId: string;
  node: Uint8Array;
  requiredActions: readonly string[];
  scopeEvaluator?: TreecrdtScopeEvaluator;
}): Promise<ScopeTri> {
  if (!Array.isArray(opts.caps)) return "allow"; // v0: treat missing caps as allow-all

  let best: ScopeTri = "deny";
  for (const cap of opts.caps) {
    best = triOr(
      best,
      await capAllowsNode({
        cap,
        docId: opts.docId,
        node: opts.node,
        requiredActions: opts.requiredActions,
        scopeEvaluator: opts.scopeEvaluator,
      })
    );
    if (best === "allow") break;
  }

  return best;
}

async function capsAllowsOp(opts: {
  caps: unknown;
  docId: string;
  op: Operation;
  scopeEvaluator?: TreecrdtScopeEvaluator;
}): Promise<ScopeTri> {
  if (!Array.isArray(opts.caps)) return "allow"; // v0: treat missing caps as allow-all

  const checks = getRequiredScopeChecks(opts.op);
  let overall: ScopeTri = "allow";

  for (const check of checks) {
    let best: ScopeTri = "deny";
    for (const cap of opts.caps) {
      best = triOr(
        best,
        await capAllowsNode({
          cap,
          docId: opts.docId,
          node: check.node,
          requiredActions: check.actions,
          scopeEvaluator: opts.scopeEvaluator,
        })
      );
      if (best === "allow") break;
    }
    overall = triAnd(overall, best);
  }

  return overall;
}

const MAX_DELEGATION_PROOF_CHAIN = 8;

async function parseAndVerifyCapabilityToken(opts: {
  tokenBytes: Uint8Array;
  issuerPublicKeys: Uint8Array[];
  docId: string;
  nowSec: number;
  scopeEvaluator?: TreecrdtScopeEvaluator;
}): Promise<CapabilityGrant> {
  const seenTokenIds = new Set<string>();
  return parseAndVerifyCapabilityTokenDepth({ ...opts, depth: 0, seenTokenIds });
}

async function parseAndVerifyCapabilityTokenDepth(opts: {
  tokenBytes: Uint8Array;
  issuerPublicKeys: Uint8Array[];
  docId: string;
  nowSec: number;
  scopeEvaluator?: TreecrdtScopeEvaluator;
  depth: number;
  seenTokenIds: Set<string>;
}): Promise<CapabilityGrant> {
  if (opts.issuerPublicKeys.length === 0) throw new Error("issuerPublicKeys is empty");
  if (opts.depth > MAX_DELEGATION_PROOF_CHAIN) throw new Error("delegation proof chain is too deep");

  const tokenIdHex = bytesToHex(deriveTokenIdV1(opts.tokenBytes));
  if (opts.seenTokenIds.has(tokenIdHex)) throw new Error("delegation proof cycle detected");
  opts.seenTokenIds.add(tokenIdHex);

  const verifyWithIssuers = async (): Promise<{ payload: Uint8Array } | null> => {
    for (const issuerPk of opts.issuerPublicKeys) {
      try {
        const payload = await coseVerifySign1Ed25519({ bytes: opts.tokenBytes, publicKey: issuerPk });
        return { payload };
      } catch {
        // continue
      }
    }
    return null;
  };

  const issuerVerified = await verifyWithIssuers();
  if (issuerVerified) {
    return parseCapabilityGrantFromClaims({
      tokenBytes: opts.tokenBytes,
      claimsBytes: issuerVerified.payload,
      docId: opts.docId,
      nowSec: opts.nowSec,
    });
  }

  // Delegation: accept tokens signed by the subject key of a proof token.
  // Proof tokens can be issuer-signed or delegated (chained), but must ultimately verify against an issuer key.
  // The delegated token must carry exactly one proof token in its unprotected header.
  const decoded = await coseDecodeSign1(opts.tokenBytes);
  const proofRaw = mapGet(decoded.unprotected, TREECRDT_DELEGATION_PROOF_HEADER_V1);
  if (proofRaw === undefined || proofRaw === null) {
    throw new Error("capability token verification failed: unknown issuer (no delegation proof)");
  }
  const proofTokens: Uint8Array[] = (() => {
    if (proofRaw instanceof Uint8Array) return [proofRaw];
    if (Array.isArray(proofRaw) && proofRaw.every((v) => v instanceof Uint8Array)) return proofRaw as Uint8Array[];
    throw new Error("delegation proof must be a bstr or an array of bstr");
  })();
  if (proofTokens.length !== 1) {
    throw new Error("delegation proof must contain exactly 1 proof token");
  }

  const proofGrant = await parseAndVerifyCapabilityTokenDepth({
    tokenBytes: proofTokens[0]!,
    issuerPublicKeys: opts.issuerPublicKeys,
    docId: opts.docId,
    nowSec: opts.nowSec,
    scopeEvaluator: opts.scopeEvaluator,
    depth: opts.depth + 1,
    seenTokenIds: opts.seenTokenIds,
  });

  const delegatedPayload = await coseVerifySign1Ed25519({ bytes: opts.tokenBytes, publicKey: proofGrant.publicKey });
  const delegatedGrant = parseCapabilityGrantFromClaims({
    tokenBytes: opts.tokenBytes,
    claimsBytes: delegatedPayload,
    docId: opts.docId,
    nowSec: opts.nowSec,
  });

  await assertDelegatedGrantWithinProof({
    docId: opts.docId,
    proof: proofGrant,
    delegated: delegatedGrant,
    scopeEvaluator: opts.scopeEvaluator,
  });

  return delegatedGrant;
}

function parseCapabilityGrantFromClaims(opts: {
  tokenBytes: Uint8Array;
  claimsBytes: Uint8Array;
  docId: string;
  nowSec: number;
}): CapabilityGrant {
  // Decode CWT claims (allow non-string keys).
  const claims = cborDecode(opts.claimsBytes, { useMaps: true }) as unknown;
  if (!(claims instanceof Map)) throw new Error("capability token payload must be a CBOR map");

  const aud = getClaim(claims, 3, "aud");
  if (aud !== undefined) {
    if (typeof aud === "string") {
      if (aud !== opts.docId) throw new Error("capability token audience mismatch");
    } else if (Array.isArray(aud)) {
      if (!aud.some((a) => a === opts.docId)) throw new Error("capability token audience mismatch");
    } else {
      throw new Error("capability token aud claim must be string or array");
    }
  }

  const exp = toNumber(getClaim(claims, 4, "exp"), "exp");
  const nbf = toNumber(getClaim(claims, 5, "nbf"), "nbf");
  if (exp !== undefined && opts.nowSec > exp) throw new Error("capability token expired");
  if (nbf !== undefined && opts.nowSec < nbf) throw new Error("capability token not yet valid");

  const cnf = getClaim(claims, 8, "cnf");
  if (!(cnf instanceof Map)) throw new Error("capability token cnf claim missing or not a map");
  const kid = mapGet(cnf, "kid");
  const pub = mapGet(cnf, "pub");
  if (!(pub instanceof Uint8Array)) throw new Error("capability token cnf.pub missing");
  const derivedKeyId = deriveKeyIdV1(pub);

  let keyId: Uint8Array;
  if (kid === undefined || kid === null) {
    keyId = derivedKeyId;
  } else if (kid instanceof Uint8Array) {
    keyId = kid;
    if (bytesToHex(keyId) !== bytesToHex(derivedKeyId)) throw new Error("capability token cnf.kid does not match pub");
  } else {
    throw new Error("capability token cnf.kid must be bytes");
  }

  const caps = getClaim(claims, -1, "caps");
  return {
    tokenId: deriveTokenIdV1(opts.tokenBytes),
    keyId,
    publicKey: pub,
    caps,
    exp,
    nbf,
  };
}

function expandCapabilityActions(actions: readonly unknown[]): Set<string> {
  const actionSet = new Set(actions.map(String));
  // Keep in sync with capAllowsNode convenience rules.
  if (
    actionSet.has("write_structure") ||
    actionSet.has("write_payload") ||
    actionSet.has("delete") ||
    actionSet.has("tombstone")
  ) {
    actionSet.add("read_structure");
  }
  if (actionSet.has("write_payload")) actionSet.add("read_payload");
  return actionSet;
}

async function assertDelegatedGrantWithinProof(opts: {
  docId: string;
  proof: CapabilityGrant;
  delegated: CapabilityGrant;
  scopeEvaluator?: TreecrdtScopeEvaluator;
}): Promise<void> {
  if (!Array.isArray(opts.proof.caps)) throw new Error("delegation proof token must be a v1 capability token");
  if (!Array.isArray(opts.delegated.caps)) throw new Error("delegated capability token must be a v1 capability token");

  // Time bounds: delegated token cannot be valid outside the proof token window.
  if (opts.proof.exp !== undefined) {
    if (opts.delegated.exp === undefined) throw new Error("delegated token must include exp when proof token has exp");
    if (opts.delegated.exp > opts.proof.exp) throw new Error("delegated token exp exceeds proof token exp");
  }
  if (opts.proof.nbf !== undefined) {
    if (opts.delegated.nbf === undefined) throw new Error("delegated token must include nbf when proof token has nbf");
    if (opts.delegated.nbf < opts.proof.nbf) throw new Error("delegated token nbf precedes proof token nbf");
  }

  for (const delegatedCap of opts.delegated.caps) {
    const delegatedRes = getField(delegatedCap, "res");
    const delegatedActions = getField(delegatedCap, "actions");
    if (!delegatedRes || typeof delegatedRes !== "object") throw new Error("delegated capability missing res");
    if (!Array.isArray(delegatedActions) || delegatedActions.length === 0) {
      throw new Error("delegated capability missing actions");
    }
    if (getField(delegatedRes, "doc_id") !== opts.docId) throw new Error("delegated capability doc_id mismatch");

    const delegatedActionSet = expandCapabilityActions(delegatedActions);
    let matched = false;

    for (const proofCap of opts.proof.caps) {
      const proofRes = getField(proofCap, "res");
      const proofActions = getField(proofCap, "actions");
      if (!proofRes || typeof proofRes !== "object") continue;
      if (!Array.isArray(proofActions) || proofActions.length === 0) continue;
      if (getField(proofRes, "doc_id") !== opts.docId) continue;

      const proofActionSet = expandCapabilityActions(proofActions);
      if (!proofActionSet.has("grant")) continue;
      let ok = true;
      for (const a of delegatedActionSet) {
        if (!proofActionSet.has(a)) {
          ok = false;
          break;
        }
      }
      if (!ok) continue;

      const proofScope = parseScope(proofRes);
      const delegatedScope = parseScope(delegatedRes);
      if (!delegatedScope) throw new Error("delegated capability missing scope");

      const proofDocWide =
        !proofScope ||
        (bytesToHex(proofScope.root) === ROOT_NODE_ID_HEX &&
          proofScope.maxDepth === undefined &&
          (!proofScope.exclude || proofScope.exclude.length === 0));

      if (!proofDocWide) {
        if (!proofScope) throw new Error("proof capability missing scope");
        const proofRootHex = bytesToHex(proofScope.root);
        const delegatedRootHex = bytesToHex(delegatedScope.root);
        if (delegatedRootHex !== proofRootHex) {
          if (proofScope.maxDepth !== undefined) {
            throw new Error("delegated capability root must match proof root when proof uses maxDepth");
          }
          if (!opts.scopeEvaluator) {
            throw new Error("delegated capability root must match proof root (scope evaluator missing)");
          }
          const tri = await opts.scopeEvaluator({ docId: opts.docId, node: delegatedScope.root, scope: proofScope });
          if (tri === "deny") throw new Error("delegated capability root is outside proof scope");
          if (tri === "unknown") throw new Error("cannot validate delegated capability root within proof scope");
        }

        if (proofScope.maxDepth !== undefined) {
          if (delegatedScope.maxDepth === undefined) {
            throw new Error("delegated capability must include maxDepth when proof uses maxDepth");
          }
          if (delegatedScope.maxDepth > proofScope.maxDepth) {
            throw new Error("delegated capability maxDepth exceeds proof maxDepth");
          }
        }

        const proofExclude = new Set((proofScope.exclude ?? []).map((b) => bytesToHex(b)));
        if (proofExclude.size > 0) {
          const delegatedExclude = new Set((delegatedScope.exclude ?? []).map((b) => bytesToHex(b)));
          for (const ex of proofExclude) {
            if (!delegatedExclude.has(ex)) {
              throw new Error("delegated capability must preserve proof exclude list");
            }
          }
        }
      }

      matched = true;
      break;
    }

    if (!matched) {
      throw new Error("delegation proof does not allow delegated capability");
    }
  }
}

export async function describeTreecrdtCapabilityTokenV1(opts: {
  tokenBytes: Uint8Array;
  issuerPublicKeys: Uint8Array[];
  docId: string;
  scopeEvaluator?: TreecrdtScopeEvaluator;
  nowSec?: number;
}): Promise<TreecrdtCapabilityTokenV1> {
  const grant = await parseAndVerifyCapabilityToken({
    tokenBytes: opts.tokenBytes,
    issuerPublicKeys: opts.issuerPublicKeys,
    docId: opts.docId,
    scopeEvaluator: opts.scopeEvaluator,
    nowSec: opts.nowSec ?? Math.floor(Date.now() / 1000),
  });

  const rawCaps = grant.caps;
  const caps: TreecrdtCapabilityV1[] = [];
  if (Array.isArray(rawCaps)) {
    for (const cap of rawCaps) {
      const res = getField(cap, "res");
      const actions = getField(cap, "actions");
      if (!res || typeof res !== "object") continue;
      if (!Array.isArray(actions)) continue;

      const docId = getField(res, "doc_id");
      if (typeof docId !== "string" || docId.trim().length === 0) continue;

      const scope = parseScope(res);
      caps.push({
        actions: actions.map(String),
        res: {
          docId,
          ...(scope?.root ? { rootNodeId: bytesToHex(scope.root) } : {}),
          ...(scope?.maxDepth !== undefined ? { maxDepth: scope.maxDepth } : {}),
          ...(scope?.exclude ? { excludeNodeIds: scope.exclude.map((b) => bytesToHex(b)) } : {}),
        },
      });
    }
  }

  return {
    tokenId: grant.tokenId,
    subjectKeyId: grant.keyId,
    subjectPublicKey: grant.publicKey,
    caps,
    ...(grant.exp !== undefined ? { exp: grant.exp } : {}),
    ...(grant.nbf !== undefined ? { nbf: grant.nbf } : {}),
  };
}

export type TreecrdtCoseCwtAuthOptions = {
  issuerPublicKeys: Uint8Array[];
  localPrivateKey: Uint8Array;
  localPublicKey: Uint8Array;
  localCapabilityTokens?: Uint8Array[];
  localIdentityChain?: TreecrdtIdentityChainV1;
  onPeerIdentityChain?: (chain: VerifiedTreecrdtIdentityChainV1) => void;
  scopeEvaluator?: TreecrdtScopeEvaluator;
  allowUnsigned?: boolean;
  requireProofRef?: boolean;
  now?: () => number;
};

export function createTreecrdtCoseCwtAuth(opts: TreecrdtCoseCwtAuthOptions): SyncAuth<Operation> {
  const now = opts.now ?? (() => Math.floor(Date.now() / 1000));
  const allowUnsigned = opts.allowUnsigned ?? false;
  const requireProofRef = opts.requireProofRef ?? false;

  const localTokens = opts.localCapabilityTokens ?? [];
  const localTokenIds = localTokens.map((t) => deriveTokenIdV1(t));

  const grantsByKeyIdHex = new Map<string, Map<string, CapabilityGrant>>();
  let localTokensRecordedForDoc: string | null = null;

  const recordToken = async (tokenBytes: Uint8Array, docId: string) => {
    const grant = await parseAndVerifyCapabilityToken({
      tokenBytes,
      issuerPublicKeys: opts.issuerPublicKeys,
      docId,
      scopeEvaluator: opts.scopeEvaluator,
      nowSec: now(),
    });
    const keyHex = bytesToHex(grant.keyId);
    const tokenHex = bytesToHex(grant.tokenId);
    let byToken = grantsByKeyIdHex.get(keyHex);
    if (!byToken) {
      byToken = new Map<string, CapabilityGrant>();
      grantsByKeyIdHex.set(keyHex, byToken);
    }
    byToken.set(tokenHex, grant);
  };

  const ensureLocalTokensRecorded = async (docId: string) => {
    if (localTokensRecordedForDoc === docId) return;
    for (const t of localTokens) await recordToken(t, docId);
    localTokensRecordedForDoc = docId;
  };

  const recordCapabilities = async (caps: Capability[], docId: string) => {
    for (const cap of caps) {
      if (cap.name === "auth.capability") {
        const tokenBytes = base64urlDecode(cap.value);
        await recordToken(tokenBytes, docId);
        continue;
      }

      if (cap.name === TREECRDT_IDENTITY_CHAIN_CAPABILITY && opts.onPeerIdentityChain) {
        try {
          const chain = await verifyTreecrdtIdentityChainCapabilityV1({ capability: cap, docId, nowSec: now });
          opts.onPeerIdentityChain(chain);
        } catch {
          // Identity chains are optional and best-effort; ignore invalid entries.
        }
      }
    }
  };

  const helloCaps = (): Capability[] => {
    const caps = localTokens.map((t) => ({ name: "auth.capability", value: base64urlEncode(t) }));
    if (opts.localIdentityChain) {
      caps.push(createTreecrdtIdentityChainCapabilityV1(opts.localIdentityChain));
    }
    return caps;
  };

  const selectGrantForOp = async (opts2: {
    docId: string;
    op: Operation;
    candidates: CapabilityGrant[];
  }): Promise<CapabilityGrant> => {
    const nowSec = now();
    let bestUnknown: CapabilityGrant | null = null;

    for (const grant of opts2.candidates) {
      if (grant.exp !== undefined && nowSec > grant.exp) continue;
      if (grant.nbf !== undefined && nowSec < grant.nbf) continue;

      const scopeRes = await capsAllowsOp({
        caps: grant.caps,
        docId: opts2.docId,
        op: opts2.op,
        scopeEvaluator: opts.scopeEvaluator,
      });
      if (scopeRes === "allow") return grant;
      if (scopeRes === "unknown" && !bestUnknown) bestUnknown = grant;
    }

    if (bestUnknown) return bestUnknown;
    throw new Error("capability does not allow op");
  };

  return {
    helloCapabilities: async (_ctx) => helloCaps(),
    onHello: async (hello: Hello, ctx) => {
      await recordCapabilities(hello.capabilities, ctx.docId);
      // Also advertise local tokens back so the initiator can verify responder ops.
      return helloCaps();
    },
    onHelloAck: async (ack: HelloAck, ctx) => {
      await recordCapabilities(ack.capabilities, ctx.docId);
    },
    authorizeFilter: async (filter: Filter, ctx) => {
      const tokenCaps = ctx.capabilities.filter((c) => c.name === "auth.capability");
      if (tokenCaps.length === 0) throw new Error('missing "auth.capability" token');

      const grants: CapabilityGrant[] = [];
      for (const cap of tokenCaps) {
        const tokenBytes = base64urlDecode(cap.value);
        grants.push(
          await parseAndVerifyCapabilityToken({
            tokenBytes,
            issuerPublicKeys: opts.issuerPublicKeys,
            docId: ctx.docId,
            scopeEvaluator: opts.scopeEvaluator,
            nowSec: now(),
          })
        );
      }

      const requiredActions = ["read_structure"];
      const node =
        "all" in filter
          ? nodeIdToBytes16(ROOT_NODE_ID_HEX)
          : "children" in filter
            ? filter.children.parent
            : (() => {
                throw new Error("unsupported filter");
              })();

      // `filter(all)` is only safe for doc-wide scopes. If a token has any scope restrictions
      // (e.g. `max_depth`/`exclude` or a non-root `root`), allow it to use `children(parent)` instead.
      if ("all" in filter) {
        for (const grant of grants) {
          if (!Array.isArray(grant.caps)) return; // v0 allow-all
          for (const cap of grant.caps) {
            const res = getField(cap, "res");
            if (!res || typeof res !== "object") continue;
            if (getField(res, "doc_id") !== ctx.docId) continue;

            const scope = parseScope(res);
            const isDocWide =
              !scope ||
              (bytesToHex(scope.root) === ROOT_NODE_ID_HEX &&
                scope.maxDepth === undefined &&
                (!scope.exclude || scope.exclude.length === 0));
            if (!isDocWide) continue;

            const tri = await capAllowsNode({
              cap,
              docId: ctx.docId,
              node,
              requiredActions,
              scopeEvaluator: opts.scopeEvaluator,
            });
            if (tri === "allow") return;
          }
        }
        throw new Error("capability does not allow filter");
      }

      let best: ScopeTri = "deny";
      for (const grant of grants) {
        best = triOr(
          best,
          await capsAllowsNodeAccess({
            caps: grant.caps,
            docId: ctx.docId,
            node,
            requiredActions,
            scopeEvaluator: opts.scopeEvaluator,
          })
        );
        if (best === "allow") return;
      }

      if (best === "unknown") {
        throw new Error("missing subtree context to authorize filter");
      }
      throw new Error("capability does not allow filter");
    },
    filterOutgoingOps: async (ops, ctx) => {
      const tokenCaps = ctx.capabilities.filter((c) => c.name === "auth.capability");
      if (tokenCaps.length === 0) return ops.map(() => true);

      const grants: CapabilityGrant[] = [];
      for (const cap of tokenCaps) {
        const tokenBytes = base64urlDecode(cap.value);
        grants.push(
          await parseAndVerifyCapabilityToken({
            tokenBytes,
            issuerPublicKeys: opts.issuerPublicKeys,
            docId: ctx.docId,
            scopeEvaluator: opts.scopeEvaluator,
            nowSec: now(),
          })
        );
      }

      // Fast path: if the peer has any doc-wide read_structure capability, we don't need to filter.
      const requiredStructure = ["read_structure"];
      for (const grant of grants) {
        if (!Array.isArray(grant.caps)) return ops.map(() => true); // v0 allow-all
        for (const cap of grant.caps) {
          const res = getField(cap, "res");
          if (!res || typeof res !== "object") continue;
          if (getField(res, "doc_id") !== ctx.docId) continue;

          const scope = parseScope(res);
          const isDocWide =
            !scope ||
            (bytesToHex(scope.root) === ROOT_NODE_ID_HEX &&
              scope.maxDepth === undefined &&
              (!scope.exclude || scope.exclude.length === 0));
          if (!isDocWide) continue;

          const tri = await capAllowsNode({
            cap,
            docId: ctx.docId,
            node: nodeIdToBytes16(ROOT_NODE_ID_HEX),
            requiredActions: requiredStructure,
            scopeEvaluator: opts.scopeEvaluator,
          });
          if (tri === "allow") return ops.map(() => true);
        }
      }

      const allowNode = async (node: Uint8Array, requiredActions: readonly string[]): Promise<boolean> => {
        let best: ScopeTri = "deny";
        for (const grant of grants) {
          best = triOr(
            best,
            await capsAllowsNodeAccess({
              caps: grant.caps,
              docId: ctx.docId,
              node,
              requiredActions,
              scopeEvaluator: opts.scopeEvaluator,
            })
          );
          if (best === "allow") return true;
        }
        // Fail closed: if scope membership is unknown, do not reveal the op.
        return false;
      };

      const out: boolean[] = [];
      for (const op of ops) {
        // For `children(parent)` we still need to hide ops for nodes outside scope
        // (e.g. excluded private roots) so peers cannot discover them by syncing the parent's children.
        switch (op.kind.type) {
          case "insert":
          case "payload":
          case "move":
          case "delete":
          case "tombstone":
            out.push(await allowNode(nodeIdToBytes16(op.kind.node), requiredStructure));
            break;
          default: {
            const _exhaustive: never = op.kind;
            throw new Error(`unknown op kind: ${String((_exhaustive as any)?.type)}`);
          }
        }
      }

      return out;
    },
    signOps: async (ops, ctx) => {
      await ensureLocalTokensRecorded(ctx.docId);
      const localKeyIdHex = bytesToHex(deriveKeyIdV1(opts.localPublicKey));
      const out: OpAuth[] = [];
      for (const op of ops) {
        const opReplica = replicaIdToBytes(op.meta.id.replica);
        if (bytesToHex(opReplica) !== bytesToHex(opts.localPublicKey)) {
          throw new Error("cannot sign op: op.meta.id.replica does not match localPublicKey");
        }
        let proofRef: Uint8Array | undefined;
        if (localTokenIds.length > 0) {
          const byToken = grantsByKeyIdHex.get(localKeyIdHex);
          if (!byToken || byToken.size === 0) throw new Error("auth enabled but no local capability tokens are recorded");
          const selected = await selectGrantForOp({
            docId: ctx.docId,
            op,
            candidates: Array.from(byToken.values()),
          });
          proofRef = selected.tokenId;
        }
        const sig = await signTreecrdtOpV1({ docId: ctx.docId, op, privateKey: opts.localPrivateKey });
        out.push({ sig, ...(proofRef ? { proofRef } : {}) });
      }
      return out;
    },
    verifyOps: async (ops, auth, ctx) => {
      if (ops.length === 0) return;
      if (!auth) {
        if (allowUnsigned) return;
        throw new Error("missing op auth");
      }

      const dispositions: Array<{ status: "allow" } | { status: "pending_context"; message?: string }> = [];
      for (let i = 0; i < ops.length; i += 1) {
        const op = ops[i]!;
        const a = auth[i]!;
        const replica = replicaIdToBytes(op.meta.id.replica);
        const keyId = deriveKeyIdV1(replica);
        const keyHex = bytesToHex(keyId);
        const byToken = grantsByKeyIdHex.get(keyHex);
        if (!byToken || byToken.size === 0) throw new Error(`unknown author: ${keyHex}`);

        const candidates = Array.from(byToken.values());
        let grant: CapabilityGrant;

        if (requireProofRef) {
          if (!a.proofRef) throw new Error("missing proof_ref");
          const g = byToken.get(bytesToHex(a.proofRef));
          if (!g) throw new Error("proof_ref does not match known token");
          grant = g;
        } else {
          const preferred = a.proofRef ? byToken.get(bytesToHex(a.proofRef)) : undefined;
          const orderedCandidates = preferred
            ? [preferred, ...candidates.filter((c) => bytesToHex(c.tokenId) !== bytesToHex(preferred.tokenId))]
            : candidates;
          grant = await selectGrantForOp({
            docId: ctx.docId,
            op,
            candidates: orderedCandidates,
          });
        }

        if (bytesToHex(grant.publicKey) !== bytesToHex(replica)) {
          throw new Error("author public key does not match op replica_id");
        }

        const nowSec = now();
        if (grant.exp !== undefined && nowSec > grant.exp) throw new Error("capability token expired");
        if (grant.nbf !== undefined && nowSec < grant.nbf) throw new Error("capability token not yet valid");

        const scopeRes = await capsAllowsOp({
          caps: grant.caps,
          docId: ctx.docId,
          op,
          scopeEvaluator: opts.scopeEvaluator,
        });
        if (scopeRes === "deny") throw new Error("capability does not allow op");

        const ok = await verifyTreecrdtOpV1({ docId: ctx.docId, op, signature: a.sig, publicKey: replica });
        if (!ok) throw new Error("invalid op signature");

        if (scopeRes === "unknown") {
          dispositions.push({ status: "pending_context", message: "missing subtree context to authorize op" });
        } else {
          dispositions.push({ status: "allow" });
        }
      }

      if (dispositions.some((d) => d.status !== "allow")) {
        return { dispositions };
      }
    },
  };
}
