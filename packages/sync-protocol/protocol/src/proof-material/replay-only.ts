import type { Operation } from '@justtemporary/interface';
import { replicaIdToBytes } from '@justtemporary/interface/ids';

import { isAnyAuthCapability } from '../auth-capabilities.js';
import type { SyncAuth } from '../auth.js';
import { deriveOpRefV0 } from '../opref.js';
import type { Capability, OpAuth, OpRef } from '../types.js';
import type { SyncAuthMaterialStore } from './types.js';

export function createReplayOnlySyncAuth(opts: {
  docId: string;
  authMaterialStore: Pick<SyncAuthMaterialStore<Operation>, 'opAuth' | 'capabilities'>;
}): SyncAuth<Operation> {
  const opRefForOp = (op: Operation): OpRef =>
    deriveOpRefV0(opts.docId, {
      replica: replicaIdToBytes(op.meta.id.replica),
      counter: op.meta.id.counter,
    });

  const listAuthCapabilities = async (): Promise<Capability[]> => {
    const caps = (await opts.authMaterialStore.capabilities?.listCapabilities()) ?? [];
    return caps.filter(isAnyAuthCapability);
  };

  const storeAuthCapabilities = async (caps: readonly Capability[]): Promise<void> => {
    if (!opts.authMaterialStore.capabilities) return;
    const filtered = caps.filter(isAnyAuthCapability);
    if (filtered.length === 0) return;
    await opts.authMaterialStore.capabilities.storeCapabilities(filtered);
  };

  return {
    helloCapabilities: async () => await listAuthCapabilities(),

    onHello: async (hello) => {
      await storeAuthCapabilities(hello.capabilities);
      return await listAuthCapabilities();
    },

    onHelloAck: async (ack) => {
      await storeAuthCapabilities(ack.capabilities);
    },

    verifyOps: async () => {
      // This adapter is intentionally replay-only. It persists auth metadata
      // for already-accepted ops, but leaves real verification to the auth
      // policy layer (for example @justtemporary/auth COSE+CWT auth).
    },

    onVerifiedOps: async (ops, auth) => {
      if (ops.length === 0 || auth.length === 0) return;
      await opts.authMaterialStore.opAuth.storeOpAuth(
        ops.map((op, index) => ({ opRef: opRefForOp(op), auth: auth[index]! })),
      );
    },

    signOps: async (ops) => {
      if (ops.length === 0) return [];
      const opRefs = ops.map(opRefForOp);
      const found = await opts.authMaterialStore.opAuth.getOpAuthByOpRefs(opRefs);
      const missing = found.findIndex((entry) => !entry);
      if (missing !== -1) {
        throw new Error('missing op auth for non-local replica; cannot forward unsigned op');
      }
      return found as OpAuth[];
    },
  };
}
