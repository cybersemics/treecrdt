import type { Operation } from "@treecrdt/interface";
import { bytesToHex, nodeIdToBytes16, replicaIdToBytes, ROOT_NODE_ID_HEX } from "@treecrdt/interface/ids";

import {
  deriveOpRefV0,
  type Capability,
  type Filter,
  type Hello,
  type HelloAck,
  type OpAuth,
  type OpRef,
  type SyncAuth,
} from "@treecrdt/sync";

import { base64urlDecode, base64urlEncode } from "../base64url.js";
import { deriveTokenIdV1 } from "../cose.js";
import {
  createTreecrdtIdentityChainCapabilityV1,
  TREECRDT_IDENTITY_CHAIN_CAPABILITY,
  verifyTreecrdtIdentityChainCapabilityV1,
  type TreecrdtIdentityChainV1,
  type VerifiedTreecrdtIdentityChainV1,
} from "../identity.js";
import {
  createTreecrdtRevocationCapabilityV1,
  TREECRDT_REVOCATION_CAPABILITY,
  verifyTreecrdtRevocationCapabilityV1,
  verifyTreecrdtRevocationRecordV1,
  type VerifiedTreecrdtRevocationRecordV1,
} from "../revocation.js";
import {
  deriveKeyIdV1,
  parseAndVerifyCapabilityToken,
  type CapabilityGrant,
  type TreecrdtCapabilityRevocationCheckContext,
} from "./capability.js";
import { signTreecrdtOpV1, verifyTreecrdtOpV1 } from "./op-sig.js";
import { getField } from "./claims.js";
import {
  capAllowsNode,
  capsAllowsNodeAccess,
  capsAllowsOp,
  isDocWideScope,
  parseScope,
  triOr,
  type ScopeTri,
  type TreecrdtScopeEvaluator,
} from "./scope.js";

export type TreecrdtCoseCwtAuthOptions = {
  issuerPublicKeys: Uint8Array[];
  localPrivateKey: Uint8Array;
  localPublicKey: Uint8Array;
  localCapabilityTokens?: Uint8Array[];
  localIdentityChain?: TreecrdtIdentityChainV1;
  onPeerIdentityChain?: (chain: VerifiedTreecrdtIdentityChainV1) => void;
  scopeEvaluator?: TreecrdtScopeEvaluator;
  /**
   * Optional persistence layer for op auth (signature + proofRef) for already-applied ops.
   * Needed for peers/servers that restart and must re-serve ops they did not author.
   */
  opAuthStore?: {
    init?: () => Promise<void>;
    storeOpAuth: (entries: Array<{ opRef: OpRef; auth: OpAuth }>) => Promise<void>;
    getOpAuthByOpRefs: (opRefs: OpRef[]) => Promise<Array<OpAuth | null>>;
  };
  localRevocationRecords?: Uint8Array[];
  onPeerRevocationRecord?: (record: VerifiedTreecrdtRevocationRecordV1) => void;
  revokedCapabilityTokenIds?: Uint8Array[];
  /**
   * Optional revocation hook. Runtime checks include operation context, so
   * apps can enforce cutover policies (e.g. revoke from a given counter/lamport).
   */
  isCapabilityTokenRevoked?: (
    ctx: TreecrdtCoseCwtRevocationCheckContext
  ) => boolean | Promise<boolean>;
  allowUnsigned?: boolean;
  requireProofRef?: boolean;
  now?: () => number;
};

export type TreecrdtCoseCwtParseRevocationCheckContext = TreecrdtCapabilityRevocationCheckContext & {
  stage: "parse";
};

export type TreecrdtCoseCwtRuntimeRevocationCheckContext = TreecrdtCapabilityRevocationCheckContext & {
  stage: "runtime";
  purpose: "sign_op" | "verify_op";
  op: Operation;
};

export type TreecrdtCoseCwtRevocationCheckContext =
  | TreecrdtCoseCwtParseRevocationCheckContext
  | TreecrdtCoseCwtRuntimeRevocationCheckContext;

export function createTreecrdtCoseCwtAuth(opts: TreecrdtCoseCwtAuthOptions): SyncAuth<Operation> {
  const now = opts.now ?? (() => Math.floor(Date.now() / 1000));
  const allowUnsigned = opts.allowUnsigned ?? false;
  const requireProofRef = opts.requireProofRef ?? false;

  const localTokens = opts.localCapabilityTokens ?? [];
  const localTokenIds = localTokens.map((t) => deriveTokenIdV1(t));
  const localRevocationRecords = opts.localRevocationRecords ?? [];
  const revokedTokenIdHexes = opts.revokedCapabilityTokenIds
    ? new Set(opts.revokedCapabilityTokenIds.map((id) => bytesToHex(id)))
    : undefined;

  const grantsByKeyIdHex = new Map<string, Map<string, CapabilityGrant>>();
  const opAuthByOpRefHex = new Map<string, OpAuth>();
  const revocationByTokenHex = new Map<
    string,
    {
      record: VerifiedTreecrdtRevocationRecordV1;
      recordBytes: Uint8Array;
    }
  >();
  let localTokensRecordedForDoc: string | null = null;
  let localRevocationsRecordedForDoc: string | null = null;
  let opAuthStoreInitPromise: Promise<void> | null = null;

  const compareBytes = (a: Uint8Array, b: Uint8Array): number => {
    const limit = Math.min(a.length, b.length);
    for (let i = 0; i < limit; i += 1) {
      if (a[i] !== b[i]) return a[i]! - b[i]!;
    }
    return a.length - b.length;
  };

  const applyRevocationRecord = (record: VerifiedTreecrdtRevocationRecordV1, recordBytes: Uint8Array): void => {
    const tokenIdHex = bytesToHex(record.tokenId);
    const existing = revocationByTokenHex.get(tokenIdHex);
    if (!existing) {
      revocationByTokenHex.set(tokenIdHex, { record, recordBytes });
      return;
    }
    if (record.revSeq > existing.record.revSeq) {
      revocationByTokenHex.set(tokenIdHex, { record, recordBytes });
      return;
    }
    if (record.revSeq < existing.record.revSeq) return;

    // Tie-break by lexical record bytes to keep behavior deterministic.
    if (compareBytes(recordBytes, existing.recordBytes) > 0) {
      revocationByTokenHex.set(tokenIdHex, { record, recordBytes });
    }
  };

  const isRevokedByStandardPolicy = (opts2: {
    tokenIdHex: string;
    op?: Operation;
  }): boolean => {
    if (revokedTokenIdHexes?.has(opts2.tokenIdHex)) return true;

    const revocation = revocationByTokenHex.get(opts2.tokenIdHex)?.record;
    if (!revocation) return false;
    if (revocation.mode === "hard") return true;
    if (!opts2.op) return false;

    if (revocation.effectiveFromLamport !== undefined) {
      return opts2.op.meta.lamport >= revocation.effectiveFromLamport;
    }
    if (revocation.effectiveFromCounter === undefined) return false;
    if (revocation.effectiveFromReplica) {
      const opReplicaHex = bytesToHex(replicaIdToBytes(opts2.op.meta.id.replica));
      const targetReplicaHex = bytesToHex(revocation.effectiveFromReplica);
      if (opReplicaHex !== targetReplicaHex) return false;
    }
    return opts2.op.meta.id.counter >= revocation.effectiveFromCounter;
  };

  const parseStageRevocationChecker = async (ctx: TreecrdtCapabilityRevocationCheckContext) => {
    if (isRevokedByStandardPolicy({ tokenIdHex: ctx.tokenIdHex })) return true;
    if (!opts.isCapabilityTokenRevoked) return false;
    return await opts.isCapabilityTokenRevoked({ ...ctx, stage: "parse" });
  };

  const isGrantRevoked = async (opts2: {
    grant: CapabilityGrant;
    docId: string;
    purpose: "sign_op" | "verify_op";
    op: Operation;
  }): Promise<boolean> => {
    const { grant, docId, purpose, op } = opts2;
    const tokenIdHex = bytesToHex(grant.tokenId);
    if (isRevokedByStandardPolicy({ tokenIdHex, op })) return true;
    if (!opts.isCapabilityTokenRevoked) return false;
    return await opts.isCapabilityTokenRevoked({
      stage: "runtime",
      tokenId: grant.tokenId,
      tokenIdHex,
      docId,
      purpose,
      op,
    });
  };

  const recordToken = async (tokenBytes: Uint8Array, docId: string) => {
    const grant = await parseAndVerifyCapabilityToken({
      tokenBytes,
      issuerPublicKeys: opts.issuerPublicKeys,
      docId,
      scopeEvaluator: opts.scopeEvaluator,
      nowSec: now(),
      revokedCapabilityTokenIds: opts.revokedCapabilityTokenIds,
      isCapabilityTokenRevoked: parseStageRevocationChecker,
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
    await ensureLocalRevocationsRecorded(docId);
    for (const t of localTokens) await recordToken(t, docId);
    localTokensRecordedForDoc = docId;
  };

  const ensureLocalRevocationsRecorded = async (docId: string) => {
    if (localRevocationsRecordedForDoc === docId) return;
    for (const recordBytes of localRevocationRecords) {
      const record = await verifyTreecrdtRevocationRecordV1({
        recordBytes,
        issuerPublicKeys: opts.issuerPublicKeys,
        expectedDocId: docId,
        nowSec: now,
      });
      applyRevocationRecord(record, recordBytes);
    }
    localRevocationsRecordedForDoc = docId;
  };

  const ensureOpAuthStoreReady = async () => {
    if (!opts.opAuthStore?.init) return;
    if (opAuthStoreInitPromise) return opAuthStoreInitPromise;
    opAuthStoreInitPromise = opts.opAuthStore.init();
    return opAuthStoreInitPromise;
  };

  const recordCapabilities = async (caps: Capability[], docId: string) => {
    await ensureLocalRevocationsRecorded(docId);

    for (const cap of caps) {
      if (cap.name !== TREECRDT_REVOCATION_CAPABILITY) continue;
      const recordBytes = base64urlDecode(cap.value);
      const record = await verifyTreecrdtRevocationCapabilityV1({
        capability: cap,
        issuerPublicKeys: opts.issuerPublicKeys,
        docId,
        nowSec: now,
      });
      applyRevocationRecord(record, recordBytes);
      opts.onPeerRevocationRecord?.(record);
    }

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

  const helloCaps = async (docId: string): Promise<Capability[]> => {
    await ensureLocalRevocationsRecorded(docId);
    const caps = localTokens.map((t) => ({ name: "auth.capability", value: base64urlEncode(t) }));
    for (const entry of revocationByTokenHex.values()) {
      caps.push(createTreecrdtRevocationCapabilityV1(entry.recordBytes));
    }
    if (opts.localIdentityChain) {
      caps.push(createTreecrdtIdentityChainCapabilityV1(opts.localIdentityChain));
    }
    return caps;
  };

  const selectGrantForOp = async (opts2: {
    docId: string;
    op: Operation;
    candidates: CapabilityGrant[];
    purpose: "sign_op" | "verify_op";
  }): Promise<CapabilityGrant> => {
    const nowSec = now();
    let bestUnknown: CapabilityGrant | null = null;
    let sawRevoked = false;

    for (const grant of opts2.candidates) {
      if (
        await isGrantRevoked({
          grant,
          docId: opts2.docId,
          purpose: opts2.purpose,
          op: opts2.op,
        })
      ) {
        sawRevoked = true;
        continue;
      }
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
    if (sawRevoked) throw new Error("capability token revoked");
    throw new Error("capability does not allow op");
  };

  return {
    helloCapabilities: async (ctx) => helloCaps(ctx.docId),
    onHello: async (hello: Hello, ctx) => {
      await recordCapabilities(hello.capabilities, ctx.docId);
      // Also advertise local tokens back so the initiator can verify responder ops.
      return helloCaps(ctx.docId);
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
            revokedCapabilityTokenIds: opts.revokedCapabilityTokenIds,
            isCapabilityTokenRevoked: parseStageRevocationChecker,
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
          for (const cap of grant.caps) {
            const res = getField(cap, "res");
            if (!res || typeof res !== "object") continue;
            if (getField(res, "doc_id") !== ctx.docId) continue;

            const scope = parseScope(res);
            if (!isDocWideScope(scope)) continue;

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
            revokedCapabilityTokenIds: opts.revokedCapabilityTokenIds,
            isCapabilityTokenRevoked: parseStageRevocationChecker,
          })
        );
      }

      // Fast path: if the peer has any doc-wide read_structure capability, we don't need to filter.
      const requiredStructure = ["read_structure"];
      for (const grant of grants) {
        for (const cap of grant.caps) {
          const res = getField(cap, "res");
          if (!res || typeof res !== "object") continue;
          if (getField(res, "doc_id") !== ctx.docId) continue;

          const scope = parseScope(res);
          if (!isDocWideScope(scope)) continue;

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
      const localReplicaHex = bytesToHex(opts.localPublicKey);
      const localKeyIdHex = bytesToHex(deriveKeyIdV1(opts.localPublicKey));
      const out: OpAuth[] = new Array(ops.length);

      const missingOpRefs: OpRef[] = [];
      const missingIndices: number[] = [];

      for (let i = 0; i < ops.length; i += 1) {
        const op = ops[i]!;
        const opReplica = replicaIdToBytes(op.meta.id.replica);
        const opReplicaHex = bytesToHex(opReplica);
        const opRef = deriveOpRefV0(ctx.docId, { replica: opReplica, counter: op.meta.id.counter });
        const opRefHex = bytesToHex(opRef);

        if (opReplicaHex !== localReplicaHex) {
          const existing = opAuthByOpRefHex.get(opRefHex);
          if (existing) {
            out[i] = existing;
          } else {
            missingOpRefs.push(opRef);
            missingIndices.push(i);
          }
          continue;
        }

        let proofRef: Uint8Array | undefined;
        if (localTokenIds.length > 0) {
          const byToken = grantsByKeyIdHex.get(localKeyIdHex);
          if (!byToken || byToken.size === 0) throw new Error("auth enabled but no local capability tokens are recorded");
          const selected = await selectGrantForOp({
            docId: ctx.docId,
            op,
            candidates: Array.from(byToken.values()),
            purpose: "sign_op",
          });
          proofRef = selected.tokenId;
        }
        const sig = await signTreecrdtOpV1({ docId: ctx.docId, op, privateKey: opts.localPrivateKey });
        const entry: OpAuth = { sig, ...(proofRef ? { proofRef } : {}) };
        opAuthByOpRefHex.set(opRefHex, entry);
        out[i] = entry;
      }

      if (missingOpRefs.length > 0) {
        const store = opts.opAuthStore;
        if (!store) {
          throw new Error("missing op auth for non-local replica; cannot forward unsigned op");
        }
        await ensureOpAuthStoreReady();
        const listed = await store.getOpAuthByOpRefs(missingOpRefs);
        for (let j = 0; j < listed.length; j += 1) {
          const found = listed[j];
          if (!found) {
            throw new Error("missing op auth for non-local replica; cannot forward unsigned op");
          }
          const opRefHex = bytesToHex(missingOpRefs[j]!);
          opAuthByOpRefHex.set(opRefHex, found);
          out[missingIndices[j]!] = found;
        }
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
      const toPersist: Array<{ opRef: OpRef; auth: OpAuth }> = [];
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
          if (
            await isGrantRevoked({
              grant: g,
              docId: ctx.docId,
              purpose: "verify_op",
              op,
            })
          ) {
            throw new Error("capability token revoked");
          }
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
            purpose: "verify_op",
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
        const opRef = deriveOpRefV0(ctx.docId, { replica, counter: op.meta.id.counter });
        opAuthByOpRefHex.set(bytesToHex(opRef), a);
        if (opts.opAuthStore) toPersist.push({ opRef, auth: a });

        if (scopeRes === "unknown") {
          dispositions.push({ status: "pending_context", message: "missing subtree context to authorize op" });
        } else {
          dispositions.push({ status: "allow" });
        }
      }

      if (dispositions.some((d) => d.status !== "allow")) {
        if (opts.opAuthStore && toPersist.length > 0) {
          await ensureOpAuthStoreReady();
          await opts.opAuthStore.storeOpAuth(toPersist);
        }
        return { dispositions };
      }

      if (opts.opAuthStore && toPersist.length > 0) {
        await ensureOpAuthStoreReady();
        await opts.opAuthStore.storeOpAuth(toPersist);
      }
    },
  };
}
