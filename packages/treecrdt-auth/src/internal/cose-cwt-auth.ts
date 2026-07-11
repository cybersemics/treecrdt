import type { Operation } from '@treecrdt/interface';
import {
  bytesToHex,
  nodeIdToBytes16,
  replicaIdToBytes,
  ROOT_NODE_ID_HEX,
} from '@treecrdt/interface/ids';

import {
  AUTH_CAPABILITY_NAME,
  AUTH_REPLAY_CAPABILITY_NAME,
  deriveOpRefV0,
  isAnyAuthCapability,
  isAuthCapability,
  isReplayAuthCapability,
  type Capability,
  type Filter,
  type Hello,
  type HelloAck,
  type OpAuth,
  type OpRef,
  type SyncAuth,
  type SyncCapabilityMaterialStore,
} from '@treecrdt/sync-protocol';

import { base64urlDecode, base64urlEncode } from '../base64url.js';
import { deriveTokenIdV1 } from '../cose.js';
import {
  createTreecrdtIdentityChainCapabilityV1,
  TREECRDT_IDENTITY_CHAIN_CAPABILITY,
  verifyTreecrdtIdentityChainCapabilityV1,
  type TreecrdtIdentityChainV1,
  type VerifiedTreecrdtIdentityChainV1,
} from '../identity.js';
import {
  createTreecrdtRevocationCapabilityV1,
  TREECRDT_REVOCATION_CAPABILITY,
  verifyTreecrdtRevocationCapabilityV1,
  verifyTreecrdtRevocationRecordV1,
  type VerifiedTreecrdtRevocationRecordV1,
} from '../revocation.js';
import {
  deriveKeyIdV1,
  parseAndVerifyCapabilityToken,
  type CapabilityGrant,
  type TreecrdtCapabilityRevocationCheckContext,
} from './capability.js';
import { signTreecrdtOp, verifyTreecrdtOp } from './op-sig.js';
import { getField } from './claims.js';
import {
  capAllowsNode,
  capsAllowsOp,
  isDocWideScope,
  parseScope,
  type TreecrdtScopeEvaluator,
} from './scope.js';

export type TreecrdtCoseCwtAuthOptions = {
  issuerPublicKeys: Uint8Array[];
  localPrivateKey: Uint8Array;
  localPublicKey: Uint8Array;
  localCapabilityTokens?: Uint8Array[];
  capabilityStore?: SyncCapabilityMaterialStore & { init?: () => Promise<void> };
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
    ctx: TreecrdtCoseCwtRevocationCheckContext,
  ) => boolean | Promise<boolean>;
  allowUnsigned?: boolean;
  now?: () => number;
};

export type TreecrdtCoseCwtParseRevocationCheckContext =
  TreecrdtCapabilityRevocationCheckContext & {
    stage: 'parse';
  };

export type TreecrdtCoseCwtRuntimeRevocationCheckContext =
  TreecrdtCapabilityRevocationCheckContext & {
    stage: 'runtime';
    purpose: 'sign_op' | 'verify_op';
    op: Operation;
  };

export type TreecrdtCoseCwtRevocationCheckContext =
  | TreecrdtCoseCwtParseRevocationCheckContext
  | TreecrdtCoseCwtRuntimeRevocationCheckContext;

export function createTreecrdtCoseCwtAuth(opts: TreecrdtCoseCwtAuthOptions): SyncAuth<Operation> {
  const now = opts.now ?? (() => Math.floor(Date.now() / 1000));
  const allowUnsigned = opts.allowUnsigned ?? false;

  const localTokens = opts.localCapabilityTokens ?? [];
  const localTokenIds = localTokens.map((t) => deriveTokenIdV1(t));
  const localTokenIdHexes = new Set(localTokenIds.map((id) => bytesToHex(id)));
  const localRevocationRecords = opts.localRevocationRecords ?? [];
  const revokedTokenIdHexes = opts.revokedCapabilityTokenIds
    ? new Set(opts.revokedCapabilityTokenIds.map((id) => bytesToHex(id)))
    : undefined;

  const grantsByKeyIdHex = new Map<string, Map<string, CapabilityGrant>>();
  const replayAuthCapabilitiesByValue = new Map<string, Capability>();
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
  let capabilityStoreInitPromise: Promise<void> | null = null;
  let capabilityStoreLoadedForDoc: string | null = null;
  let opAuthStoreInitPromise: Promise<void> | null = null;

  const compareBytes = (a: Uint8Array, b: Uint8Array): number => {
    const limit = Math.min(a.length, b.length);
    for (let i = 0; i < limit; i += 1) {
      if (a[i] !== b[i]) return a[i]! - b[i]!;
    }
    return a.length - b.length;
  };

  const applyRevocationRecord = (
    record: VerifiedTreecrdtRevocationRecordV1,
    recordBytes: Uint8Array,
  ): void => {
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

  const isRevokedByStandardPolicy = (opts2: { tokenIdHex: string; op?: Operation }): boolean => {
    if (revokedTokenIdHexes?.has(opts2.tokenIdHex)) return true;

    const revocation = revocationByTokenHex.get(opts2.tokenIdHex)?.record;
    if (!revocation) return false;
    if (revocation.mode === 'hard') return true;
    if (!opts2.op) return false;

    if (revocation.effectiveFromCounter === undefined) return false;
    if (revocation.effectiveFromReplica) {
      const opReplicaHex = bytesToHex(replicaIdToBytes(opts2.op.meta.id.replica));
      const targetReplicaHex = bytesToHex(revocation.effectiveFromReplica);
      if (opReplicaHex !== targetReplicaHex) return false;
    }
    return opts2.op.meta.id.counter >= revocation.effectiveFromCounter;
  };

  const makeAuthCapability = (
    tokenBytes: Uint8Array,
    name: string = AUTH_CAPABILITY_NAME,
  ): Capability => ({
    name,
    value: base64urlEncode(tokenBytes),
  });

  const isLocalToken = (tokenBytes: Uint8Array): boolean =>
    localTokenIdHexes.has(bytesToHex(deriveTokenIdV1(tokenBytes)));

  const rememberReplayAuthCapability = (tokenBytes: Uint8Array) => {
    if (isLocalToken(tokenBytes)) return;
    // Re-advertise non-local tokens in replay mode so later peers can verify proof_ref
    // values without mistaking the token for this peer's own live auth grant.
    const cap = makeAuthCapability(tokenBytes, AUTH_REPLAY_CAPABILITY_NAME);
    replayAuthCapabilitiesByValue.set(cap.value, cap);
  };

  const parseStageRevocationChecker = async (ctx: TreecrdtCapabilityRevocationCheckContext) => {
    if (isRevokedByStandardPolicy({ tokenIdHex: ctx.tokenIdHex })) return true;
    if (!opts.isCapabilityTokenRevoked) return false;
    return await opts.isCapabilityTokenRevoked({ ...ctx, stage: 'parse' });
  };

  const isGrantRevoked = async (opts2: {
    grant: CapabilityGrant;
    docId: string;
    purpose: 'sign_op' | 'verify_op';
    op: Operation;
  }): Promise<boolean> => {
    const { grant, docId, purpose, op } = opts2;
    const tokenIdHex = bytesToHex(grant.tokenId);
    if (isRevokedByStandardPolicy({ tokenIdHex, op })) return true;
    if (!opts.isCapabilityTokenRevoked) return false;
    return await opts.isCapabilityTokenRevoked({
      stage: 'runtime',
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

  const ensureCapabilityStoreReady = async () => {
    if (!opts.capabilityStore?.init) return;
    if (capabilityStoreInitPromise) return capabilityStoreInitPromise;
    capabilityStoreInitPromise = opts.capabilityStore.init();
    return capabilityStoreInitPromise;
  };

  const persistTrustedCapability = async (cap: Capability) => {
    if (!opts.capabilityStore || !isAnyAuthCapability(cap)) return;
    await ensureCapabilityStoreReady();
    await opts.capabilityStore.storeCapabilities([cap]);
  };

  const ensureCapabilityStoreRecorded = async (docId: string) => {
    if (!opts.capabilityStore || capabilityStoreLoadedForDoc === docId) return;
    await ensureCapabilityStoreReady();
    const stored = await opts.capabilityStore.listCapabilities();
    for (const cap of stored) {
      if (!isAnyAuthCapability(cap)) continue;
      const tokenBytes = base64urlDecode(cap.value);
      try {
        await recordToken(tokenBytes, docId);
        if (!isLocalToken(tokenBytes)) rememberReplayAuthCapability(tokenBytes);
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        // Locally cached capability material can legitimately go stale after a
        // hard revoke or access replacement. Ignore those entries here so a
        // peer does not poison its own hello path on restart.
        if (message.includes('capability token revoked')) continue;
        if (!message.includes('unknown issuer')) throw err;
      }
    }
    capabilityStoreLoadedForDoc = docId;
  };

  const ensureLocalTokensRecorded = async (docId: string) => {
    if (localTokensRecordedForDoc === docId) return;
    await ensureCapabilityStoreRecorded(docId);
    await ensureLocalRevocationsRecorded(docId);
    for (const t of localTokens) {
      const cap = makeAuthCapability(t);
      await recordToken(t, docId);
      await persistTrustedCapability(cap);
    }
    localTokensRecordedForDoc = docId;
  };

  const parsePeerCapabilityGrants = async (
    tokenCaps: readonly Capability[],
    docId: string,
  ): Promise<CapabilityGrant[]> => {
    const grants: CapabilityGrant[] = [];
    for (const cap of tokenCaps) {
      const tokenBytes = base64urlDecode(cap.value);
      try {
        grants.push(
          await parseAndVerifyCapabilityToken({
            tokenBytes,
            issuerPublicKeys: opts.issuerPublicKeys,
            docId,
            nowSec: now(),
            revokedCapabilityTokenIds: opts.revokedCapabilityTokenIds,
            isCapabilityTokenRevoked: parseStageRevocationChecker,
          }),
        );
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        if (!message.includes('unknown issuer')) throw err;
      }
    }
    return grants;
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
    await ensureCapabilityStoreRecorded(docId);
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
      if (isAnyAuthCapability(cap)) {
        // Persist both live and replayed auth material so durable peers can verify
        // historical ops after restart, even when the original writer is offline.
        const tokenBytes = base64urlDecode(cap.value);
        try {
          await recordToken(tokenBytes, docId);
          if (!isLocalToken(tokenBytes)) rememberReplayAuthCapability(tokenBytes);
          await persistTrustedCapability(
            isLocalToken(tokenBytes)
              ? makeAuthCapability(tokenBytes, AUTH_CAPABILITY_NAME)
              : makeAuthCapability(tokenBytes, AUTH_REPLAY_CAPABILITY_NAME),
          );
        } catch (err) {
          const message = err instanceof Error ? err.message : String(err);
          // Replay-only capability material is best-effort cache state. If a
          // peer still advertises a revoked replay token after an access
          // replacement, ignore it rather than failing the whole handshake.
          if (isReplayAuthCapability(cap) && message.includes('capability token revoked')) continue;
          if (!message.includes('unknown issuer')) throw err;
          // Peers and relay servers may advertise capability tokens that are
          // irrelevant for this replica's trust roots. Ignore them here and
          // fail later only if an op actually requires a missing proof.
        }
        continue;
      }

      if (cap.name === TREECRDT_IDENTITY_CHAIN_CAPABILITY && opts.onPeerIdentityChain) {
        try {
          const chain = await verifyTreecrdtIdentityChainCapabilityV1({
            capability: cap,
            docId,
            nowSec: now,
          });
          opts.onPeerIdentityChain(chain);
        } catch {
          // Identity chains are optional and best-effort; ignore invalid entries.
        }
      }
    }
  };

  const helloCaps = async (docId: string): Promise<Capability[]> => {
    await ensureCapabilityStoreRecorded(docId);
    await ensureLocalRevocationsRecorded(docId);
    await ensureLocalTokensRecorded(docId);
    // Advertise local tokens as live auth, and cached third-party tokens as replay-only
    // proof material for downstream verification.
    const caps = localTokens.map((t) => makeAuthCapability(t, AUTH_CAPABILITY_NAME));
    caps.push(...replayAuthCapabilitiesByValue.values());
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
    purpose: 'sign_op' | 'verify_op';
  }): Promise<CapabilityGrant> => {
    const nowSec = now();
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

      if (
        capsAllowsOp({
          caps: grant.caps,
          docId: opts2.docId,
          op: opts2.op,
        })
      ) {
        return grant;
      }
    }

    if (sawRevoked) throw new Error('capability token revoked');
    throw new Error('capability does not allow op');
  };

  const grantsAllowDocWideRead = async (
    grants: readonly CapabilityGrant[],
    docId: string,
    requiredActions: readonly string[],
  ): Promise<boolean> => {
    for (const grant of grants) {
      for (const cap of grant.caps) {
        const res = getField(cap, 'res');
        if (!res || typeof res !== 'object') continue;
        if (getField(res, 'doc_id') !== docId) continue;
        if (!isDocWideScope(parseScope(res))) continue;

        const tri = await capAllowsNode({
          cap,
          docId,
          node: nodeIdToBytes16(ROOT_NODE_ID_HEX),
          requiredActions,
        });
        if (tri === 'allow') return true;
      }
    }
    return false;
  };

  const containsPayloadState = (op: Operation): boolean => {
    switch (op.kind.type) {
      case 'insert':
        return op.kind.payload !== undefined;
      case 'payload':
        return true;
      case 'move':
      case 'delete':
      case 'tombstone':
        return false;
      default: {
        const _exhaustive: never = op.kind;
        throw new Error(`unknown op kind: ${String((_exhaustive as any)?.type)}`);
      }
    }
  };

  const opAuthMatches = async (docId: string, op: Operation, auth: OpAuth): Promise<boolean> => {
    if (
      !(auth?.sig instanceof Uint8Array) ||
      auth.sig.length !== 64 ||
      !(auth?.proofRef instanceof Uint8Array) ||
      auth.proofRef.length !== 16
    ) {
      return false;
    }

    const publicKey = replicaIdToBytes(op.meta.id.replica);
    try {
      return await verifyTreecrdtOp({
        docId,
        op,
        proofRef: auth.proofRef,
        signature: auth.sig,
        publicKey,
      });
    } catch {
      return false;
    }
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
      // Only a peer's live auth capability can authorize reads. Replay capabilities
      // help verify old ops, but they do not grant the advertising peer new access.
      const tokenCaps = ctx.capabilities.filter(isAuthCapability);
      if (tokenCaps.length === 0) throw new Error(`missing "${AUTH_CAPABILITY_NAME}" token`);

      const grants = await parsePeerCapabilityGrants(tokenCaps, ctx.docId);
      if (grants.length === 0) throw new Error(`no trusted "${AUTH_CAPABILITY_NAME}" token`);

      if (!('all' in filter) && !('children' in filter)) throw new Error('unsupported filter');

      // The current wire format reconciles historical operations. Current materialized ancestry
      // cannot prove where an operation's node lived when that operation was authored: a node may
      // leave an excluded subtree and later re-enter readable state. Until filtered reads carry an
      // authenticated historical ancestry witness or use redacted snapshot records, every
      // operation-log filter requires state-independent, document-wide structure access.
      if (await grantsAllowDocWideRead(grants, ctx.docId, ['read_structure'])) return;
      throw new Error('capability does not allow filter');
    },
    filterOutgoingOps: async (ops, ctx) => {
      const tokenCaps = ctx.capabilities.filter(isAuthCapability);
      // A replay capability can verify historical op proofs, but only a live
      // capability proves that this peer may read the outgoing operations.
      if (tokenCaps.length === 0) throw new Error(`missing "${AUTH_CAPABILITY_NAME}" token`);

      const grants = await parsePeerCapabilityGrants(tokenCaps, ctx.docId);
      if (grants.length === 0) return ops.map(() => false);

      if (!(await grantsAllowDocWideRead(grants, ctx.docId, ['read_structure']))) {
        throw new Error('capability does not allow operation-log projection');
      }

      // Payload updates (including clears) and payload-bearing inserts cannot be omitted without
      // changing the selected op set and cannot be redacted in Sync v0. Reject the entire
      // projection unless payload state is also readable document-wide.
      if (
        ops.some(containsPayloadState) &&
        !(await grantsAllowDocWideRead(grants, ctx.docId, ['read_payload']))
      ) {
        throw new Error('operation-log projection requires read_payload for payload state');
      }

      return ops.map(() => true);
    },
    signOps: async (ops, ctx) => {
      await ensureLocalTokensRecorded(ctx.docId);
      const localReplicaHex = bytesToHex(opts.localPublicKey);
      const localKeyIdHex = bytesToHex(deriveKeyIdV1(opts.localPublicKey));
      const out: OpAuth[] = new Array(ops.length);

      const opRefs = ops.map((op) => {
        const replica = replicaIdToBytes(op.meta.id.replica);
        return deriveOpRefV0(ctx.docId, { replica, counter: op.meta.id.counter });
      });
      const unresolved: number[] = [];

      for (let i = 0; i < ops.length; i += 1) {
        const opRefHex = bytesToHex(opRefs[i]!);
        const existing = opAuthByOpRefHex.get(opRefHex);
        if (existing && (await opAuthMatches(ctx.docId, ops[i]!, existing))) {
          out[i] = existing;
        } else {
          if (existing) opAuthByOpRefHex.delete(opRefHex);
          unresolved.push(i);
        }
      }

      if (opts.opAuthStore && unresolved.length > 0) {
        await ensureOpAuthStoreReady();
        const listed = await opts.opAuthStore.getOpAuthByOpRefs(
          unresolved.map((index) => opRefs[index]!),
        );
        if (listed.length !== unresolved.length) {
          throw new Error('op auth store returned misaligned entries');
        }
        for (let j = 0; j < listed.length; j += 1) {
          const index = unresolved[j]!;
          const found = listed[j];
          if (found && (await opAuthMatches(ctx.docId, ops[index]!, found))) {
            out[index] = found;
            opAuthByOpRefHex.set(bytesToHex(opRefs[index]!), found);
          }
        }
      }

      for (let i = 0; i < ops.length; i += 1) {
        const op = ops[i]!;
        const opReplica = replicaIdToBytes(op.meta.id.replica);
        const opReplicaHex = bytesToHex(opReplica);
        const opRefHex = bytesToHex(opRefs[i]!);

        if (out[i]) continue;

        if (ctx.purpose !== 'local_write') {
          throw new Error('missing exact retained op auth; outbound sync cannot mint auth');
        }
        if (opReplicaHex !== localReplicaHex) {
          throw new Error('missing op auth or exact proof is invalid for non-local replica');
        }

        const byToken = grantsByKeyIdHex.get(localKeyIdHex);
        const currentLocalGrants = byToken
          ? Array.from(byToken.entries())
              .filter(([tokenIdHex]) => localTokenIdHexes.has(tokenIdHex))
              .map(([, grant]) => grant)
          : [];
        if (currentLocalGrants.length === 0) {
          throw new Error('auth enabled but no local capability tokens are recorded');
        }
        const selected = await selectGrantForOp({
          docId: ctx.docId,
          op,
          candidates: currentLocalGrants,
          purpose: 'sign_op',
        });
        const proofRef = selected.tokenId;
        const sig = await signTreecrdtOp({
          docId: ctx.docId,
          op,
          proofRef,
          privateKey: opts.localPrivateKey,
        });
        const entry: OpAuth = { sig, proofRef };
        opAuthByOpRefHex.set(opRefHex, entry);
        out[i] = entry;
      }

      return out;
    },
    verifyOps: async (ops, auth, ctx) => {
      if (ops.length === 0) return;
      await ensureCapabilityStoreRecorded(ctx.docId);
      if (!auth) {
        if (allowUnsigned) return;
        throw new Error('missing op auth');
      }
      if (auth.length !== ops.length) {
        throw new Error(`op auth length ${auth.length} does not match ops length ${ops.length}`);
      }

      const toPersist: Array<{ opRef: OpRef; auth: OpAuth }> = [];
      for (let i = 0; i < ops.length; i += 1) {
        const op = ops[i]!;
        const a = auth[i]!;
        if (
          !(a?.sig instanceof Uint8Array) ||
          a.sig.length !== 64 ||
          !(a?.proofRef instanceof Uint8Array) ||
          a.proofRef.length !== 16
        ) {
          throw new Error('op auth requires a 64-byte sig and 16-byte proof_ref');
        }
        const replica = replicaIdToBytes(op.meta.id.replica);
        const keyId = deriveKeyIdV1(replica);
        const keyHex = bytesToHex(keyId);
        const byToken = grantsByKeyIdHex.get(keyHex);
        if (!byToken || byToken.size === 0) throw new Error(`unknown author: ${keyHex}`);

        const grant = byToken.get(bytesToHex(a.proofRef));
        if (!grant) throw new Error('proof_ref does not match known token');
        if (
          await isGrantRevoked({
            grant,
            docId: ctx.docId,
            purpose: 'verify_op',
            op,
          })
        ) {
          throw new Error('capability token revoked');
        }

        if (bytesToHex(grant.publicKey) !== bytesToHex(replica)) {
          throw new Error('author public key does not match op replica_id');
        }

        const nowSec = now();
        if (grant.exp !== undefined && nowSec > grant.exp)
          throw new Error('capability token expired');
        if (grant.nbf !== undefined && nowSec < grant.nbf)
          throw new Error('capability token not yet valid');

        if (!capsAllowsOp({ caps: grant.caps, docId: ctx.docId, op })) {
          throw new Error('capability does not allow op');
        }

        const ok = await verifyTreecrdtOp({
          docId: ctx.docId,
          op,
          proofRef: a.proofRef,
          signature: a.sig,
          publicKey: replica,
        });
        if (!ok) throw new Error('invalid op signature');
        const opRef = deriveOpRefV0(ctx.docId, { replica, counter: op.meta.id.counter });
        opAuthByOpRefHex.set(bytesToHex(opRef), a);
        if (opts.opAuthStore) toPersist.push({ opRef, auth: a });
      }

      if (opts.opAuthStore && toPersist.length > 0) {
        await ensureOpAuthStoreReady();
        await opts.opAuthStore.storeOpAuth(toPersist);
      }
    },
  };
}
