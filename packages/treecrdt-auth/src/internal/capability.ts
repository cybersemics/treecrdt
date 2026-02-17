import { blake3 } from "@noble/hashes/blake3";
import { utf8ToBytes } from "@noble/hashes/utils";

import { decode as cborDecode, encode as cborEncode, rfc8949EncodeOptions } from "cborg";

import { bytesToHex, nodeIdToBytes16, ROOT_NODE_ID_HEX } from "@treecrdt/interface/ids";

import { coseDecodeSign1, coseSign1Ed25519, coseVerifySign1Ed25519, deriveTokenIdV1 } from "../cose.js";
import { concatBytes } from "./bytes.js";
import { getClaim, getField, mapGet, toNumber } from "./claims.js";
import { expandCapabilityActions, isDocWideScope, parseScope, type TreecrdtScopeEvaluator } from "./scope.js";

const TREECRDT_DELEGATION_PROOF_HEADER_V1 = "treecrdt.delegation_proof_v1";

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
  resEntries.push(["root", nodeIdToBytes16(opts.rootNodeId ?? ROOT_NODE_ID_HEX)]);
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
  resEntries.push(["root", nodeIdToBytes16(opts.rootNodeId ?? ROOT_NODE_ID_HEX)]);
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
    rootNodeId: string;
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

export type CapabilityGrant = {
  tokenId: Uint8Array;
  keyId: Uint8Array;
  publicKey: Uint8Array;
  caps: unknown[];
  exp?: number;
  nbf?: number;
};

export type TreecrdtCapabilityRevocationCheckContext = {
  tokenId: Uint8Array;
  tokenIdHex: string;
  tokenBytes?: Uint8Array;
  docId: string;
  depth?: number;
};

export type TreecrdtCapabilityRevocationOptions = {
  revokedCapabilityTokenIds?: Uint8Array[];
  isCapabilityTokenRevoked?: (
    ctx: TreecrdtCapabilityRevocationCheckContext
  ) => boolean | Promise<boolean>;
};

const MAX_DELEGATION_PROOF_CHAIN = 8;

export async function parseAndVerifyCapabilityToken(opts: {
  tokenBytes: Uint8Array;
  issuerPublicKeys: Uint8Array[];
  docId: string;
  nowSec: number;
  scopeEvaluator?: TreecrdtScopeEvaluator;
} & TreecrdtCapabilityRevocationOptions): Promise<CapabilityGrant> {
  const seenTokenIds = new Set<string>();
  const revokedTokenIdHexes = opts.revokedCapabilityTokenIds
    ? new Set(opts.revokedCapabilityTokenIds.map((id) => bytesToHex(id)))
    : undefined;
  return parseAndVerifyCapabilityTokenDepth({
    ...opts,
    depth: 0,
    seenTokenIds,
    revokedTokenIdHexes,
  });
}

async function parseAndVerifyCapabilityTokenDepth(opts: {
  tokenBytes: Uint8Array;
  issuerPublicKeys: Uint8Array[];
  docId: string;
  nowSec: number;
  scopeEvaluator?: TreecrdtScopeEvaluator;
  isCapabilityTokenRevoked?: (
    ctx: TreecrdtCapabilityRevocationCheckContext
  ) => boolean | Promise<boolean>;
  depth: number;
  seenTokenIds: Set<string>;
  revokedTokenIdHexes?: Set<string>;
}): Promise<CapabilityGrant> {
  if (opts.issuerPublicKeys.length === 0) throw new Error("issuerPublicKeys is empty");
  if (opts.depth > MAX_DELEGATION_PROOF_CHAIN) throw new Error("delegation proof chain is too deep");

  const tokenId = deriveTokenIdV1(opts.tokenBytes);
  const tokenIdHex = bytesToHex(tokenId);
  if (opts.seenTokenIds.has(tokenIdHex)) throw new Error("delegation proof cycle detected");
  opts.seenTokenIds.add(tokenIdHex);
  if (opts.revokedTokenIdHexes?.has(tokenIdHex)) throw new Error("capability token revoked");
  if (opts.isCapabilityTokenRevoked) {
    const revoked = await opts.isCapabilityTokenRevoked({
      tokenId,
      tokenIdHex,
      tokenBytes: opts.tokenBytes,
      docId: opts.docId,
      depth: opts.depth,
    });
    if (revoked) throw new Error("capability token revoked");
  }

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
      tokenId,
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
    isCapabilityTokenRevoked: opts.isCapabilityTokenRevoked,
    depth: opts.depth + 1,
    seenTokenIds: opts.seenTokenIds,
    revokedTokenIdHexes: opts.revokedTokenIdHexes,
  });

  const delegatedPayload = await coseVerifySign1Ed25519({ bytes: opts.tokenBytes, publicKey: proofGrant.publicKey });
  const delegatedGrant = parseCapabilityGrantFromClaims({
    tokenBytes: opts.tokenBytes,
    tokenId,
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
  tokenId?: Uint8Array;
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
  if (!Array.isArray(caps) || caps.length === 0) {
    throw new Error("capability token caps claim missing or invalid");
  }
  return {
    tokenId: opts.tokenId ?? deriveTokenIdV1(opts.tokenBytes),
    keyId,
    publicKey: pub,
    caps,
    exp,
    nbf,
  };
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

      const proofDocWide = isDocWideScope(proofScope);

      if (!proofDocWide) {
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
} & TreecrdtCapabilityRevocationOptions): Promise<TreecrdtCapabilityTokenV1> {
  const grant = await parseAndVerifyCapabilityToken({
    tokenBytes: opts.tokenBytes,
    issuerPublicKeys: opts.issuerPublicKeys,
    docId: opts.docId,
    scopeEvaluator: opts.scopeEvaluator,
    nowSec: opts.nowSec ?? Math.floor(Date.now() / 1000),
    revokedCapabilityTokenIds: opts.revokedCapabilityTokenIds,
    isCapabilityTokenRevoked: opts.isCapabilityTokenRevoked,
  });
  
  if (!Array.isArray(grant.caps) || grant.caps.length === 0) {
    throw new Error("capability token must be a v1 capability token");
  }

  const caps: TreecrdtCapabilityV1[] = [];
  for (const cap of grant.caps) {
    const res = getField(cap, "res");
    const actions = getField(cap, "actions");
    if (!res || typeof res !== "object") throw new Error("capability missing res");
    if (!Array.isArray(actions) || actions.length === 0) throw new Error("capability missing actions");

    const docId = getField(res, "doc_id");
    if (typeof docId !== "string" || docId.trim().length === 0) throw new Error("capability res.doc_id missing");

    const scope = parseScope(res);
    caps.push({
      actions: actions.map(String),
      res: {
        docId,
        rootNodeId: bytesToHex(scope.root),
        ...(scope.maxDepth !== undefined ? { maxDepth: scope.maxDepth } : {}),
        ...(scope.exclude ? { excludeNodeIds: scope.exclude.map((b) => bytesToHex(b)) } : {}),
      },
    });
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
