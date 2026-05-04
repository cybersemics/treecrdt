import React, { useEffect, useMemo, useRef, useState } from "react";
import type { Operation } from "@treecrdt/interface";
import type { LocalWriteOptions } from "@treecrdt/interface/engine";
import { bytesToHex } from "@treecrdt/interface/ids";
import { base64urlDecode, type TreecrdtAuthSession } from "@treecrdt/auth";
import type { SyncAuth } from "@treecrdt/sync-protocol";
import type { TreecrdtClient } from "@treecrdt/wa-sqlite/client";

import { createLocalIdentityChainV1, type StoredAuthMaterial } from "../../auth";
import { hexToBytes16 } from "../../sync-v0";

type UsePlaygroundAuthSessionOptions = {
  authEnabled: boolean;
  client: TreecrdtClient | null;
  docId: string;
  authMaterial: StoredAuthMaterial;
  hardRevokedTokenIds: string[];
  revealIdentity: boolean;
  onPeerIdentityChain: (chain: {
    identityPublicKey: Uint8Array;
    devicePublicKey: Uint8Array;
    replicaPublicKey: Uint8Array;
  }) => void;
  setAuthError: React.Dispatch<React.SetStateAction<string | null>>;
};

export type PlaygroundAuthSessionState = {
  syncAuth: SyncAuth<Operation> | null;
  replica: Uint8Array | null;
  selfPeerId: string | null;
  getLocalWriteOptions: () => LocalWriteOptions | undefined;
  clearAuthSession: () => void;
  resetLocalIdentityChain: () => void;
};

export function usePlaygroundAuthSession(
  opts: UsePlaygroundAuthSessionOptions,
): PlaygroundAuthSessionState {
  const {
    authEnabled,
    client,
    docId,
    authMaterial,
    hardRevokedTokenIds,
    revealIdentity,
    onPeerIdentityChain,
    setAuthError,
  } = opts;
  const [syncAuth, setSyncAuth] = useState<SyncAuth<Operation> | null>(null);
  const localAuthSessionRef = useRef<TreecrdtAuthSession | null>(null);
  const localIdentityChainPromiseRef = useRef<
    Promise<Awaited<ReturnType<typeof createLocalIdentityChainV1>> | null> | null
  >(null);

  useEffect(() => {
    // Local identity chains are doc-bound (replica cert includes `docId`) and depend on the current replica key.
    localIdentityChainPromiseRef.current = null;
  }, [docId, authMaterial.localPkB64, revealIdentity]);

  const getLocalIdentityChain = React.useCallback(async () => {
    if (!revealIdentity) return null;
    const pkB64 = authMaterial.localPkB64;
    if (!pkB64) return null;

    if (!localIdentityChainPromiseRef.current) {
      const replicaPk = base64urlDecode(pkB64);
      localIdentityChainPromiseRef.current = createLocalIdentityChainV1({
        docId,
        replicaPublicKey: replicaPk,
      }).catch((err) => {
        console.error("Failed to create identity chain", err);
        return null;
      });
    }

    return await localIdentityChainPromiseRef.current;
  }, [authMaterial.localPkB64, docId, revealIdentity]);

  const hardRevokedTokenIdBytes = useMemo(
    () => hardRevokedTokenIds.map((hex) => hexToBytes16(hex)),
    [hardRevokedTokenIds],
  );

  const resetLocalIdentityChain = React.useCallback(() => {
    localIdentityChainPromiseRef.current = null;
  }, []);

  const clearAuthSession = React.useCallback(() => {
    localAuthSessionRef.current = null;
    setSyncAuth(null);
  }, []);

  useEffect(() => {
    let cancelled = false;
    setSyncAuth(null);

    if (!authEnabled || !client) {
      localAuthSessionRef.current = null;
      return () => {
        cancelled = true;
      };
    }

    const { issuerPkB64, localSkB64, localPkB64 } = authMaterial;
    const localTokensB64 = authMaterial.localTokensB64;

    if (!issuerPkB64 || !localSkB64 || !localPkB64 || localTokensB64.length === 0) {
      localAuthSessionRef.current = null;
      return () => {
        cancelled = true;
      };
    }

    void (async () => {
      try {
        const issuerPk = base64urlDecode(issuerPkB64);
        const localSk = base64urlDecode(localSkB64);
        const localPk = base64urlDecode(localPkB64);
        const localTokens = localTokensB64.map((t) => base64urlDecode(t));

        const authSession = await client.auth.createSession({
          docId,
          trust: { issuerPublicKeys: [issuerPk] },
          local: {
            privateKey: localSk,
            publicKey: localPk,
            capabilityTokens: localTokens,
          },
          revokedCapabilityTokenIds: hardRevokedTokenIdBytes,
          requireProofRef: true,
          identity: {
            onPeer: onPeerIdentityChain,
            local: getLocalIdentityChain,
          },
        });
        if (cancelled) return;

        const preparedAuth = authSession.syncAuth;
        localAuthSessionRef.current = authSession;

        await authSession.ready;
        if (cancelled) return;
        setSyncAuth(preparedAuth);
      } catch (err) {
        if (cancelled) return;
        localAuthSessionRef.current = null;
        setSyncAuth(null);
        setAuthError(err instanceof Error ? err.message : String(err));
      }
    })();

    return () => {
      cancelled = true;
      if (localAuthSessionRef.current) localAuthSessionRef.current = null;
    };
  }, [
    authEnabled,
    client,
    docId,
    authMaterial.issuerPkB64,
    authMaterial.localSkB64,
    authMaterial.localPkB64,
    authMaterial.localTokensB64,
    hardRevokedTokenIdBytes,
    getLocalIdentityChain,
    onPeerIdentityChain,
    setAuthError,
  ]);

  const replica = useMemo(
    () => (authMaterial.localPkB64 ? base64urlDecode(authMaterial.localPkB64) : null),
    [authMaterial.localPkB64],
  );
  const selfPeerId = useMemo(() => (replica ? bytesToHex(replica) : null), [replica]);

  const getLocalWriteOptions = React.useCallback((): LocalWriteOptions | undefined => {
    if (!authEnabled) return;
    const session = localAuthSessionRef.current;
    if (!session) throw new Error("auth is enabled but not configured");
    return { authSession: session };
  }, [authEnabled]);

  return {
    syncAuth,
    replica,
    selfPeerId,
    getLocalWriteOptions,
    clearAuthSession,
    resetLocalIdentityChain,
  };
}
