import React, { useEffect, useMemo, useRef, useState } from "react";
import { bytesToHex } from "@treecrdt/interface/ids";
import type { Operation } from "@treecrdt/interface";
import {
  base64urlDecode,
  base64urlEncode,
  createTreecrdtCoseCwtAuth,
  createTreecrdtIdentityChainCapabilityV1,
  createTreecrdtSqliteSubtreeScopeEvaluator,
  describeTreecrdtCapabilityTokenV1,
  deriveKeyIdV1,
  deriveTokenIdV1,
  issueTreecrdtDelegatedCapabilityTokenV1,
  type TreecrdtCapabilityTokenV1,
} from "@treecrdt/auth";
import {
  createCapabilityMaterialStore,
  createOpAuthStore,
  createPendingOpsStore,
} from "@treecrdt/sync-sqlite";
import type { SyncAuth } from "@treecrdt/sync";
import type { TreecrdtClient } from "@treecrdt/wa-sqlite/client";

import {
  clearAuthMaterial,
  createLocalIdentityChainV1,
  createCapabilityTokenV1,
  decodeInvitePayload,
  encodeInvitePayload,
  generateEd25519KeyPair,
  deriveEd25519PublicKey,
  initialAuthEnabled,
  initialRevealIdentity,
  loadOrCreateDocPayloadKeyB64,
  loadAuthMaterial,
  persistRevealIdentity,
  persistAuthEnabled,
  saveIssuerKeys,
  saveLocalKeys,
  saveLocalTokens,
  saveDocPayloadKeyB64,
  type StoredAuthMaterial,
} from "../../auth";
import { hexToBytes16, type AuthGrantMessageV1 } from "../../sync-v0";
import { ROOT_ID } from "../constants";
import { applySyncSettingsToUrl, loadPrivateRoots, persistPrivateRoots } from "../persist";
import { prefixPlaygroundStorageKey } from "../storage";
import type { InviteActions } from "../invite";
import type { ToastState } from "../components/PlaygroundToast";
import type { SyncTransportMode } from "../types";

function computeInviteExcludeNodeIds(privateRoots: Set<string>, inviteRoot: string): string[] {
  return Array.from(privateRoots).filter((id) => id !== inviteRoot && id !== ROOT_ID && /^[0-9a-f]{32}$/i.test(id));
}

function hexToBytes32(hex: string): Uint8Array {
  const clean = hex.trim().toLowerCase().replace(/^0x/, "");
  if (!/^[0-9a-f]{64}$/.test(clean)) throw new Error("expected 64 hex chars");
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) {
    out[i] = Number.parseInt(clean.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

function parseReplicaPublicKeyInput(input: string): Uint8Array {
  const raw = input.trim();
  if (!raw) throw new Error("replica public key is required");
  if (/^(0x)?[0-9a-f]{64}$/i.test(raw)) return hexToBytes32(raw);
  try {
    const bytes = base64urlDecode(raw);
    if (bytes.length !== 32) throw new Error("replica public key must be 32 bytes");
    return bytes;
  } catch {
    throw new Error("replica public key must be 64 hex chars (or base64url-encoded 32 bytes)");
  }
}

function normalizeTokenIdHex(input: string): string | null {
  const clean = input.trim().toLowerCase().replace(/^0x/, "");
  if (!/^[0-9a-f]{32}$/.test(clean)) return null;
  return clean;
}

function extractInviteB64FromLink(link: string): string | null {
  if (!link) return null;
  try {
    const url = new URL(link);
    const invite = new URLSearchParams(url.hash.slice(1)).get("invite");
    return typeof invite === "string" && invite.length > 0 ? invite : null;
  } catch {
    return null;
  }
}

export type IssuedGrantRecord = {
  recipientPkHex: string;
  tokenIdHex: string;
  rootNodeId: string;
  actions: string[];
  maxDepth?: number;
  excludeCount: number;
  ts: number;
};

const ALLOWED_GRANT_ACTIONS = new Set([
  "write_structure",
  "write_payload",
  "delete",
  "tombstone",
  "grant",
  "read_structure",
  "read_payload",
]);

function normalizeGrantActions(input: string[]): string[] {
  const out: string[] = [];
  for (const raw of input) {
    const action = String(raw).trim();
    if (!ALLOWED_GRANT_ACTIONS.has(action)) continue;
    if (out.includes(action)) continue;
    out.push(action);
  }
  return out;
}

function expandInternalCompatActions(input: string[]): string[] {
  const out = normalizeGrantActions(input);
  const hasAnyAction = out.length > 0;
  if (hasAnyAction) {
    // Playground permission model: read is always included whenever any capability is issued.
    if (!out.includes("read_structure")) out.push("read_structure");
    if (!out.includes("read_payload")) out.push("read_payload");
  }
  if (out.includes("delete") && !out.includes("tombstone")) out.push("tombstone");
  return out;
}

function issuedGrantsStorageKey(docId: string): string {
  return prefixPlaygroundStorageKey(`treecrdt-playground-issued-grants:${docId}`);
}

function loadIssuedGrantRecords(docId: string): IssuedGrantRecord[] {
  if (typeof window === "undefined") return [];
  try {
    const raw = window.localStorage.getItem(issuedGrantsStorageKey(docId));
    if (!raw) return [];
    const parsed = JSON.parse(raw) as unknown;
    if (!Array.isArray(parsed)) return [];
    const out: IssuedGrantRecord[] = [];
    for (const item of parsed) {
      if (!item || typeof item !== "object") continue;
      const row = item as Partial<IssuedGrantRecord>;
      const recipientPkHex = typeof row.recipientPkHex === "string" ? row.recipientPkHex.toLowerCase() : "";
      const tokenIdHex = typeof row.tokenIdHex === "string" ? row.tokenIdHex.toLowerCase() : "";
      const rootNodeId = typeof row.rootNodeId === "string" ? row.rootNodeId.toLowerCase() : "";
      const actions = Array.isArray(row.actions)
        ? Array.from(new Set(row.actions.filter((v): v is string => typeof v === "string")))
        : [];
      const maxDepth =
        typeof row.maxDepth === "number" && Number.isInteger(row.maxDepth) && row.maxDepth >= 0 ? row.maxDepth : undefined;
      const excludeCount =
        typeof row.excludeCount === "number" && Number.isInteger(row.excludeCount) && row.excludeCount >= 0 ? row.excludeCount : 0;
      const ts = typeof row.ts === "number" && Number.isFinite(row.ts) ? row.ts : 0;
      if (!/^[0-9a-f]{64}$/.test(recipientPkHex)) continue;
      if (!/^[0-9a-f]{32}$/.test(tokenIdHex)) continue;
      if (!/^[0-9a-f]{32}$/.test(rootNodeId)) continue;
      if (ts <= 0) continue;
      out.push({ recipientPkHex, tokenIdHex, rootNodeId, actions, maxDepth, excludeCount, ts });
    }
    out.sort((a, b) => b.ts - a.ts);
    return out.slice(0, 256);
  } catch {
    return [];
  }
}

function persistIssuedGrantRecords(docId: string, rows: IssuedGrantRecord[]) {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(issuedGrantsStorageKey(docId), JSON.stringify(rows.slice(0, 256)));
  } catch {
    // ignore storage failures
  }
}

async function copyToClipboard(text: string): Promise<void> {
  if (typeof navigator === "undefined" || typeof navigator.clipboard?.writeText !== "function") {
    throw new Error("clipboard API is not available");
  }
  await navigator.clipboard.writeText(text);
}

export type PlaygroundAuthApi = {
  authEnabled: boolean;
  setAuthEnabled: React.Dispatch<React.SetStateAction<boolean>>;
  revealIdentity: boolean;
  setRevealIdentity: React.Dispatch<React.SetStateAction<boolean>>;
  showAuthPanel: boolean;
  setShowAuthPanel: React.Dispatch<React.SetStateAction<boolean>>;
  showShareDialog: boolean;
  setShowShareDialog: React.Dispatch<React.SetStateAction<boolean>>;
  showAuthAdvanced: boolean;
  setShowAuthAdvanced: React.Dispatch<React.SetStateAction<boolean>>;
  authInfo: string | null;
  authError: string | null;
  setAuthError: React.Dispatch<React.SetStateAction<string | null>>;
  authBusy: boolean;
  toast: ToastState | null;
  setToast: React.Dispatch<React.SetStateAction<ToastState | null>>;

  wrapKeyImportText: string;
  setWrapKeyImportText: React.Dispatch<React.SetStateAction<string>>;
  issuerKeyBlobImportText: string;
  setIssuerKeyBlobImportText: React.Dispatch<React.SetStateAction<string>>;
  identityKeyBlobImportText: string;
  setIdentityKeyBlobImportText: React.Dispatch<React.SetStateAction<string>>;
  deviceSigningKeyBlobImportText: string;
  setDeviceSigningKeyBlobImportText: React.Dispatch<React.SetStateAction<string>>;

  authMaterial: StoredAuthMaterial;
  syncAuth: SyncAuth<Operation> | null;
  refreshAuthMaterial: () => Promise<StoredAuthMaterial>;
  localIdentityChainPromiseRef: React.MutableRefObject<
    Promise<Awaited<ReturnType<typeof createLocalIdentityChainV1>> | null> | null
  >;

  replica: Uint8Array | null;
  selfPeerId: string | null;

  viewRootId: string;
  authCanSyncAll: boolean;
  canWriteStructure: boolean;
  canWritePayload: boolean;
  canDelete: boolean;
  isScopedAccess: boolean;

  authCanIssue: boolean;
  authCanDelegate: boolean;
  authIssuerPkHex: string | null;
  authLocalKeyIdHex: string | null;
  authLocalTokenIdHex: string | null;
  authTokenCount: number;
  authTokenScope: TreecrdtCapabilityTokenV1["caps"][number]["res"] | null;
  authTokenActions: TreecrdtCapabilityTokenV1["caps"][number]["actions"] | null;
  authNeedsInvite: boolean;
  hardRevokedTokenIds: string[];
  toggleHardRevokedTokenId: (tokenIdHex: string) => void;

  pendingOps: Array<{ id: string; kind: string; message?: string }>;
  refreshPendingOps: () => Promise<void>;

  privateRoots: Set<string>;
  privateRootsCount: number;
  togglePrivateRoot: (id: string) => void;

  inviteRoot: string;
  inviteActions: InviteActions;
  setInviteActions: React.Dispatch<React.SetStateAction<InviteActions>>;
  inviteAllowGrant: boolean;
  setInviteAllowGrant: React.Dispatch<React.SetStateAction<boolean>>;
  inviteLink: string;
  generateInviteLink: (opts?: { rootNodeId?: string; copyToClipboard?: boolean }) => Promise<void>;

  issuedGrantRecords: IssuedGrantRecord[];
  grantSubtreeToReplicaPubkey: (
    sendGrant: (msg: AuthGrantMessageV1) => boolean,
    opts?: { recipientKey?: string; rootNodeId?: string; actions?: string[]; supersedesTokenIds?: string[] }
  ) => Promise<boolean>;

  resetAuth: () => void;
  openMintingPeerTab: () => void;
  openNewIsolatedPeerTab: (opts: { autoInvite: boolean; rootNodeId?: string }) => Promise<void>;
  openShareForNode: (nodeId: string) => void;

  verifyLocalOps: (ops: Operation[]) => Promise<void>;
  copyToClipboard: (text: string) => Promise<void>;
  onAuthGrantMessage: (grant: AuthGrantMessageV1) => void;
};

export type UsePlaygroundAuthOptions = {
  docId: string;
  joinMode: boolean;
  client: TreecrdtClient | null;
  syncServerUrl: string;
  syncTransportMode: SyncTransportMode;
  onPeerIdentityChain: (chain: {
    identityPublicKey: Uint8Array;
    devicePublicKey: Uint8Array;
    replicaPublicKey: Uint8Array;
  }) => void;
  /**
   * App-owned doc payload key refresher (used by invite/grant import flows).
   * This keeps crypto state (decrypt/encrypt) in App while auth UI is extracted.
   */
  refreshDocPayloadKey: () => Promise<Uint8Array | null>;
};

export function usePlaygroundAuth(opts: UsePlaygroundAuthOptions): PlaygroundAuthApi {
  const { docId, joinMode, client, syncServerUrl, syncTransportMode, onPeerIdentityChain, refreshDocPayloadKey } = opts;

  const [authEnabled, setAuthEnabled] = useState(() => initialAuthEnabled());
  const [revealIdentity, setRevealIdentity] = useState(() => initialRevealIdentity());
  const [showAuthPanel, setShowAuthPanel] = useState(() => {
    if (typeof window === "undefined") return false;
    if (!joinMode) return false;
    return !new URLSearchParams(window.location.hash.slice(1)).has("invite");
  });
  const [showShareDialog, setShowShareDialog] = useState(false);
  const [showAuthAdvanced, setShowAuthAdvanced] = useState(false);
  const [authInfo, setAuthInfo] = useState<string | null>(null);
  const [authError, setAuthError] = useState<string | null>(null);
  const [authBusy, setAuthBusy] = useState(false);
  const [toast, setToast] = useState<ToastState | null>(null);

  const [wrapKeyImportText, setWrapKeyImportText] = useState("");
  const [issuerKeyBlobImportText, setIssuerKeyBlobImportText] = useState("");
  const [identityKeyBlobImportText, setIdentityKeyBlobImportText] = useState("");
  const [deviceSigningKeyBlobImportText, setDeviceSigningKeyBlobImportText] = useState("");

  const [authMaterial, setAuthMaterial] = useState<StoredAuthMaterial>(() => ({
    issuerPkB64: null,
    issuerSkB64: null,
    localPkB64: null,
    localSkB64: null,
    localTokensB64: [],
  }));

  const [syncAuth, setSyncAuth] = useState<SyncAuth<Operation> | null>(null);
  const localAuthRef = useRef<SyncAuth<Operation> | null>(null);
  const localIdentityChainPromiseRef = useRef<
    Promise<Awaited<ReturnType<typeof createLocalIdentityChainV1>> | null> | null
  >(null);

  const [authToken, setAuthToken] = useState<TreecrdtCapabilityTokenV1 | null>(null);
  const [hardRevokedTokenIds, setHardRevokedTokenIds] = useState<string[]>([]);

  const [inviteRoot, setInviteRoot] = useState(ROOT_ID);
  const [inviteActions, setInviteActions] = useState<InviteActions>({
    write_structure: true,
    write_payload: true,
    delete: false,
  });
  const [inviteAllowGrant, setInviteAllowGrant] = useState(true);
  const [inviteLink, setInviteLink] = useState<string>("");
  const inviteLinkConfigKeyRef = useRef<string | null>(null);
  const [issuedGrantRecords, setIssuedGrantRecords] = useState<IssuedGrantRecord[]>(() => loadIssuedGrantRecords(docId));
  const [pendingOps, setPendingOps] = useState<Array<{ id: string; kind: string; message?: string }>>([]);

  const [privateRoots, setPrivateRoots] = useState<Set<string>>(() => loadPrivateRoots(docId));
  const privateRootsCount = useMemo(
    () => Array.from(privateRoots).filter((id) => id !== ROOT_ID).length,
    [privateRoots]
  );

  useEffect(() => {
    if (typeof window === "undefined") return;
    if (!toast) return;
    const id = window.setTimeout(() => setToast(null), toast.durationMs ?? 10_000);
    return () => window.clearTimeout(id);
  }, [toast]);

  useEffect(() => {
    setPrivateRoots(loadPrivateRoots(docId));
  }, [docId]);

  useEffect(() => {
    setIssuedGrantRecords(loadIssuedGrantRecords(docId));
  }, [docId]);

  const rememberIssuedGrantRecord = React.useCallback(
    (opts2: {
      recipientPk: Uint8Array;
      tokenBytes: Uint8Array;
      rootNodeId: string;
      actions: string[];
      maxDepth?: number;
      excludeNodeIds: string[];
    }) => {
      const { recipientPk, tokenBytes, rootNodeId, actions, maxDepth, excludeNodeIds } = opts2;
      const recipientPkHex = bytesToHex(recipientPk).toLowerCase();
      const tokenIdHex = bytesToHex(deriveTokenIdV1(tokenBytes)).toLowerCase();
      setIssuedGrantRecords((prev) => {
        const next: IssuedGrantRecord[] = [
          {
            recipientPkHex,
            tokenIdHex,
            rootNodeId: rootNodeId.toLowerCase(),
            actions: [...actions],
            ...(maxDepth !== undefined ? { maxDepth } : {}),
            excludeCount: excludeNodeIds.length,
            ts: Date.now(),
          },
          ...prev.filter((r) => r.tokenIdHex !== tokenIdHex),
        ].slice(0, 256);
        persistIssuedGrantRecords(docId, next);
        return next;
      });
    },
    [docId]
  );

  useEffect(() => {
    // Local identity chains are doc-bound (replica cert includes `docId`) and depend on the current replica key.
    localIdentityChainPromiseRef.current = null;
  }, [docId, authMaterial.localPkB64, revealIdentity]);

  const togglePrivateRoot = (id: string) => {
    if (id === ROOT_ID) return;
    setPrivateRoots((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      persistPrivateRoots(docId, next);
      return next;
    });
  };

  const rememberScopedPrivateRootsFromToken = React.useCallback(
    async (issuerPkB64: string, tokenB64: string) => {
      try {
        const issuerPk = base64urlDecode(issuerPkB64);
        const tokenBytes = base64urlDecode(tokenB64);
        const described = await describeTreecrdtCapabilityTokenV1({ tokenBytes, issuerPublicKeys: [issuerPk], docId });
        const roots = new Set<string>();
        for (const cap of described.caps) {
          const root = cap.res.rootNodeId?.toLowerCase();
          if (!root) continue;
          if (root === ROOT_ID) continue;
          if (!/^[0-9a-f]{32}$/.test(root)) continue;
          roots.add(root);
        }
        if (roots.size === 0) return;
        setPrivateRoots((prev) => {
          let changed = false;
          const next = new Set(prev);
          for (const root of roots) {
            if (!next.has(root)) {
              next.add(root);
              changed = true;
            }
          }
          if (!changed) return prev;
          persistPrivateRoots(docId, next);
          return next;
        });
      } catch {
        // Best-effort: tokens may be invalid or unverifiable at this moment.
      }
    },
    [docId]
  );

  const refreshAuthMaterial = React.useCallback(async () => {
    const next = await loadAuthMaterial(docId);
    setAuthMaterial(next);
    return next;
  }, [docId]);

  useEffect(() => {
    persistAuthEnabled(authEnabled);
  }, [authEnabled]);

  useEffect(() => {
    persistRevealIdentity(revealIdentity);
  }, [revealIdentity]);

  useEffect(() => {
    if (!authEnabled) setPendingOps([]);
  }, [authEnabled]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const url = new URL(window.location.href);
    if (url.searchParams.get("fresh") !== "1") return;
    url.searchParams.delete("fresh");
    window.history.replaceState({}, "", url);
    clearAuthMaterial(docId);
    void refreshAuthMaterial().catch((err) => setAuthError(err instanceof Error ? err.message : String(err)));
  }, [docId, refreshAuthMaterial]);

  const getLocalIdentityChain = React.useCallback(async () => {
    if (!revealIdentity) return null;
    const pkB64 = authMaterial.localPkB64;
    if (!pkB64) return null;

    if (!localIdentityChainPromiseRef.current) {
      const replicaPk = base64urlDecode(pkB64);
      localIdentityChainPromiseRef.current = createLocalIdentityChainV1({ docId, replicaPublicKey: replicaPk }).catch(
        (err) => {
          console.error("Failed to create identity chain", err);
          return null;
        }
      );
    }

    return await localIdentityChainPromiseRef.current;
  }, [authMaterial.localPkB64, docId, revealIdentity]);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const next = await loadAuthMaterial(docId);
        if (cancelled) return;
        setAuthMaterial(next);
      } catch (err) {
        if (cancelled) return;
        setAuthError(err instanceof Error ? err.message : String(err));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [docId]);

  useEffect(() => {
    let cancelled = false;
    setSyncAuth(null);

    if (!authEnabled || !client) {
      localAuthRef.current = null;
      return () => {
        cancelled = true;
      };
    }

    if (
      !authMaterial.issuerPkB64 ||
      !authMaterial.localSkB64 ||
      !authMaterial.localPkB64 ||
      authMaterial.localTokensB64.length === 0
    ) {
      localAuthRef.current = null;
      return () => {
        cancelled = true;
      };
    }

    try {
      const issuerPk = base64urlDecode(authMaterial.issuerPkB64);
      const localSk = base64urlDecode(authMaterial.localSkB64);
      const localPk = base64urlDecode(authMaterial.localPkB64);
      const localTokens = authMaterial.localTokensB64.map((t) => base64urlDecode(t));
      const scopeEvaluator = createTreecrdtSqliteSubtreeScopeEvaluator(client.runner);
      const opAuthStore = createOpAuthStore({ runner: client.runner, docId });
      const capabilityStore = createCapabilityMaterialStore({ runner: client.runner, docId });

      const baseAuth = createTreecrdtCoseCwtAuth({
        issuerPublicKeys: [issuerPk],
        localPrivateKey: localSk,
        localPublicKey: localPk,
        localCapabilityTokens: localTokens,
        capabilityStore,
        revokedCapabilityTokenIds: hardRevokedTokenIdBytes,
        requireProofRef: true,
        scopeEvaluator,
        opAuthStore,
        onPeerIdentityChain,
      });

      const preparedAuth: SyncAuth<Operation> = {
        ...baseAuth,
        helloCapabilities: async (ctx) => {
          const caps = (await baseAuth.helloCapabilities?.(ctx)) ?? [];
          try {
            const chain = await getLocalIdentityChain();
            if (chain) caps.push(createTreecrdtIdentityChainCapabilityV1(chain));
          } catch {
            // Identity chains are optional and best-effort.
          }
          return caps;
        },
        onHello: async (hello, ctx) => {
          const ackCaps = (await baseAuth.onHello?.(hello, ctx)) ?? [];
          try {
            const chain = await getLocalIdentityChain();
            if (chain) ackCaps.push(createTreecrdtIdentityChainCapabilityV1(chain));
          } catch {
            // Identity chains are optional and best-effort.
          }
          return ackCaps;
        },
      };

      localAuthRef.current = preparedAuth;

      void (async () => {
        try {
          await preparedAuth.helloCapabilities?.({ docId });
          if (cancelled) return;
          setSyncAuth(preparedAuth);
        } catch (err) {
          if (cancelled) return;
          if (localAuthRef.current === preparedAuth) localAuthRef.current = null;
          setAuthError(err instanceof Error ? err.message : String(err));
        }
      })();
    } catch (err) {
      localAuthRef.current = null;
      setSyncAuth(null);
      setAuthError(err instanceof Error ? err.message : String(err));
    }

    return () => {
      cancelled = true;
      if (localAuthRef.current) localAuthRef.current = null;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [
    authEnabled,
    client,
    docId,
    authMaterial.issuerPkB64,
    authMaterial.localSkB64,
    authMaterial.localPkB64,
    authMaterial.localTokensB64.join(","),
    hardRevokedTokenIds.join(","),
    getLocalIdentityChain,
    onPeerIdentityChain,
  ]);

  const replica = useMemo(() => (authMaterial.localPkB64 ? base64urlDecode(authMaterial.localPkB64) : null), [
    authMaterial.localPkB64,
  ]);
  const selfPeerId = useMemo(() => (replica ? bytesToHex(replica) : null), [replica]);

  const verifyLocalOps = React.useCallback(
    async (ops: Operation[]) => {
      if (!authEnabled) return;
      const auth = localAuthRef.current;
      if (!auth?.signOps || !auth.verifyOps) throw new Error("auth is enabled but not configured");
      const ctx = { docId, purpose: "reconcile" as const, filterId: "__local__" };
      const authEntries = await auth.signOps(ops, ctx);
      const res = await auth.verifyOps(ops, authEntries, ctx);
      const dispositions = (res as any)?.dispositions as Array<{ status: string; message?: string }> | undefined;
      const rejected = dispositions?.find((d) => d.status !== "allow");
      if (rejected?.status === "pending_context") {
        throw new Error(rejected.message ?? "missing subtree context to authorize op");
      }
    },
    [authEnabled, docId]
  );

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      if (!authEnabled) {
        setAuthToken(null);
        return;
      }
      if (!authMaterial.issuerPkB64 || authMaterial.localTokensB64.length === 0) {
        setAuthToken(null);
        return;
      }
      try {
        const issuerPk = base64urlDecode(authMaterial.issuerPkB64);
        const tokenBytes = base64urlDecode(authMaterial.localTokensB64[0]!);
        const described = await describeTreecrdtCapabilityTokenV1({
          tokenBytes,
          issuerPublicKeys: [issuerPk],
          docId,
        });
        if (cancelled) return;
        setAuthToken(described);
      } catch {
        if (cancelled) return;
        setAuthToken(null);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [authEnabled, authMaterial.issuerPkB64, authMaterial.localTokensB64.join(","), docId]);

  const viewRootId = useMemo(() => {
    const raw = authEnabled ? authToken?.caps?.[0]?.res.rootNodeId : null;
    if (!raw || typeof raw !== "string") return ROOT_ID;
    const clean = raw.toLowerCase();
    if (!/^[0-9a-f]{32}$/.test(clean)) return ROOT_ID;
    return clean;
  }, [authEnabled, authToken]);

  const authCanSyncAll = useMemo(() => {
    if (!authEnabled) return true;
    if (!authToken) return false;
    if (authToken.caps.length === 0) return true;
    return authToken.caps.some((cap) => {
      const root = cap.res.rootNodeId?.toLowerCase();
      const excludeCount = cap.res.excludeNodeIds?.length ?? 0;
      return root === ROOT_ID && cap.res.maxDepth === undefined && excludeCount === 0;
    });
  }, [authEnabled, authToken]);

  const authActionSet = useMemo(() => {
    const set = new Set<string>();
    if (!authEnabled) return set;
    if (!authToken) return set;
    for (const cap of authToken.caps) {
      for (const action of cap.actions ?? []) set.add(String(action));
    }
    return set;
  }, [authEnabled, authToken]);

  const canWriteStructure = !authEnabled || authActionSet.has("write_structure");
  const canWritePayload = !authEnabled || authActionSet.has("write_payload");
  const canDelete = !authEnabled || authActionSet.has("delete");
  const isScopedAccess = authEnabled && viewRootId !== ROOT_ID;

  const authCanIssue = Boolean(authMaterial.issuerSkB64);
  const authCanDelegate =
    authEnabled &&
    !authCanIssue &&
    Boolean(authMaterial.localSkB64) &&
    authMaterial.localTokensB64.length > 0 &&
    authActionSet.has("grant");
  const authIssuerPkHex = authMaterial.issuerPkB64 ? bytesToHex(base64urlDecode(authMaterial.issuerPkB64)) : null;
  const authLocalKeyIdHex = authMaterial.localPkB64
    ? bytesToHex(deriveKeyIdV1(base64urlDecode(authMaterial.localPkB64)))
    : null;
  const authLocalTokenIdHex =
    authMaterial.localTokensB64.length > 0
      ? bytesToHex(deriveTokenIdV1(base64urlDecode(authMaterial.localTokensB64[0]!)))
      : null;
  const authTokenCount = authMaterial.localTokensB64.length;
  const authTokenScope = authToken?.caps?.[0]?.res ?? null;
  const authTokenActions = authToken?.caps?.[0]?.actions ?? null;
  const authNeedsInvite = Boolean(authEnabled && joinMode && authTokenCount === 0);

  useEffect(() => {
    setHardRevokedTokenIds([]);
  }, [docId]);

  const toggleHardRevokedTokenId = React.useCallback((tokenIdHex: string) => {
    const normalized = normalizeTokenIdHex(tokenIdHex);
    if (!normalized) return;
    setHardRevokedTokenIds((prev) => {
      if (prev.includes(normalized)) return prev.filter((v) => v !== normalized);
      return [...prev, normalized];
    });
  }, []);

  const hardRevokedTokenIdBytes = useMemo(() => hardRevokedTokenIds.map((hex) => hexToBytes16(hex)), [hardRevokedTokenIds]);

  const importInvitePayload = React.useCallback(
    async (inviteB64: string, opts2: { clearHash?: boolean } = {}) => {
      const payload = decodeInvitePayload(inviteB64);
      if (payload.docId !== docId) {
        throw new Error(`invite doc mismatch: got ${payload.docId}, expected ${docId}`);
      }

      if (payload.payloadKeyB64) {
        await saveDocPayloadKeyB64(docId, payload.payloadKeyB64);
        await refreshDocPayloadKey();
      }

      await saveIssuerKeys(docId, payload.issuerPkB64);

      const localSk = base64urlDecode(payload.subjectSkB64);
      await deriveEd25519PublicKey(localSk);
      await saveLocalKeys(docId, payload.subjectSkB64);
      await saveLocalTokens(docId, [payload.tokenB64]);
      await rememberScopedPrivateRootsFromToken(payload.issuerPkB64, payload.tokenB64);

      if (opts2.clearHash && typeof window !== "undefined") {
        const url = new URL(window.location.href);
        url.hash = "";
        window.history.replaceState({}, "", url);
      }

      setAuthEnabled(true);
      await refreshAuthMaterial();
    },
    [docId, refreshAuthMaterial, refreshDocPayloadKey, rememberScopedPrivateRootsFromToken]
  );

  useEffect(() => {
    if (typeof window === "undefined") return;
    const inviteB64 = new URLSearchParams(window.location.hash.slice(1)).get("invite");
    if (!inviteB64) return;

    void (async () => {
      try {
        await importInvitePayload(inviteB64, { clearHash: true });
      } catch (err) {
        console.error("Failed to import invite", err);
        setAuthError(err instanceof Error ? err.message : String(err));
      }
    })();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [docId]);

  useEffect(() => {
    let cancelled = false;
    void (async () => {
      try {
        const current = await loadAuthMaterial(docId);
        let { issuerSkB64, localPkB64, localSkB64, localTokensB64 } = current;

        const ensureIssuerKeys = async (): Promise<Pick<StoredAuthMaterial, "issuerPkB64" | "issuerSkB64">> => {
          const run = async (): Promise<Pick<StoredAuthMaterial, "issuerPkB64" | "issuerSkB64">> => {
            let { issuerPkB64, issuerSkB64 } = await loadAuthMaterial(docId);

            if (!issuerPkB64 && !issuerSkB64) {
              if (authEnabled && !joinMode) {
                const { sk, pk } = await generateEd25519KeyPair();
                await saveIssuerKeys(docId, base64urlEncode(pk), base64urlEncode(sk));
              }
            }

            // Reload in case another tab raced us.
            ({ issuerPkB64, issuerSkB64 } = await loadAuthMaterial(docId));

            if (issuerSkB64) {
              // Treat issuer secret key as authoritative and force-sync the public key to match it.
              const issuerSk = base64urlDecode(issuerSkB64);
              const issuerPk = await deriveEd25519PublicKey(issuerSk);
              const issuerPkB64 = base64urlEncode(issuerPk);
              await saveIssuerKeys(docId, issuerPkB64, issuerSkB64, { forcePk: true });
            }

            const final = await loadAuthMaterial(docId);
            return { issuerPkB64: final.issuerPkB64, issuerSkB64: final.issuerSkB64 };
          };

          const locks = typeof navigator === "undefined" ? null : (navigator as any).locks;
          if (locks?.request) {
            return await locks.request(prefixPlaygroundStorageKey(`treecrdt-playground-issuer:${docId}`), run);
          }

          // Fallback for browsers without Web Locks API.
          if (typeof window === "undefined") return await run();
          const lockKey = prefixPlaygroundStorageKey(`treecrdt-playground-issuer-lock:${docId}`);
          const lockId =
            typeof crypto !== "undefined" && "randomUUID" in crypto ? crypto.randomUUID() : `${Math.random()}`;
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
        };

        if (authEnabled) {
          const ensured = await ensureIssuerKeys();
          issuerSkB64 = ensured.issuerSkB64;
        }

        const canIssue = Boolean(issuerSkB64);

        if (!localPkB64 && !localSkB64) {
          if (authEnabled && localTokensB64.length > 0) {
            throw new Error("auth enabled but local keys are missing; re-import an invite link or reset auth");
          }
          const { sk, pk } = await generateEd25519KeyPair();
          localPkB64 = base64urlEncode(pk);
          localSkB64 = base64urlEncode(sk);
          await saveLocalKeys(docId, localSkB64);
        } else if (!localPkB64 && localSkB64) {
          const localSk = base64urlDecode(localSkB64);
          const localPk = await deriveEd25519PublicKey(localSk);
          localPkB64 = base64urlEncode(localPk);
          await saveLocalKeys(docId, localSkB64);
        } else if (localPkB64 && !localSkB64) {
          if (authEnabled) {
            throw new Error("auth enabled but local private key is missing; import an invite link or reset auth");
          }
        }

        if (authEnabled && localTokensB64.length === 0) {
          if (!canIssue || !issuerSkB64) {
            // In join-only mode we intentionally start without any capability tokens.
            // The user must import an invite/grant from another peer before sync is enabled.
            if (!joinMode) {
              throw new Error("auth enabled but no capability token; import an invite link");
            }
          } else {
            if (!localPkB64) throw new Error("auth enabled but local public key is missing");

            const issuerSk = base64urlDecode(issuerSkB64);
            const subjectPk = base64urlDecode(localPkB64);
            const tokenBytes = createCapabilityTokenV1({
              issuerPrivateKey: issuerSk,
              subjectPublicKey: subjectPk,
              docId,
              rootNodeId: ROOT_ID,
              actions: ["write_structure", "write_payload", "delete", "tombstone"],
            });
            localTokensB64 = [base64urlEncode(tokenBytes)];
            await saveLocalTokens(docId, localTokensB64);
          }
        }

        const next = await loadAuthMaterial(docId);
        if (cancelled) return;
        setAuthMaterial(next);
        setAuthError(null);
      } catch (err) {
        if (cancelled) return;
        localAuthRef.current = null;
        setAuthError(authEnabled ? (err instanceof Error ? err.message : String(err)) : null);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [authEnabled, docId, joinMode]);

  const resetAuth = () => {
    clearAuthMaterial(docId);
    setInviteLink("");
    inviteLinkConfigKeyRef.current = null;
    setAuthEnabled(false);
    setAuthError(null);
    void refreshAuthMaterial().catch((err) => setAuthError(err instanceof Error ? err.message : String(err)));
  };

  const openMintingPeerTab = () => {
    if (typeof window === "undefined") return;
    const url = new URL(window.location.href);
    url.searchParams.set("doc", docId);
    applySyncSettingsToUrl(url, syncServerUrl, syncTransportMode);
    url.searchParams.delete("join");
    url.searchParams.delete("fresh");
    url.searchParams.set("auth", "1");
    url.hash = "";
    window.open(url.toString(), "_blank", "noopener,noreferrer");
  };

  const readInviteConfig = (rootNodeId: string) => {
    let actions = Object.entries(inviteActions)
      .filter(([, enabled]) => enabled)
      .map(([name]) => name);
    if (inviteAllowGrant && !actions.includes("grant")) actions.push("grant");
    actions = expandInternalCompatActions(actions);
    if (actions.length === 0) actions = ["read_structure", "read_payload"];

    const excludeNodeIds = computeInviteExcludeNodeIds(privateRoots, rootNodeId);
    return { actions, maxDepth: undefined, excludeNodeIds };
  };

  const inviteConfigCacheKey = (rootNodeId: string): string => {
    const { actions, maxDepth, excludeNodeIds } = readInviteConfig(rootNodeId);
    return JSON.stringify({
      docId,
      rootNodeId: rootNodeId.toLowerCase(),
      actions: [...actions].sort(),
      maxDepth: maxDepth ?? null,
      excludeNodeIds: [...excludeNodeIds].sort(),
    });
  };

  const buildInviteB64 = async (opts2: { rootNodeId?: string } = {}): Promise<string> => {
    let issuerSkB64 = authMaterial.issuerSkB64;
    let issuerPkB64 = authMaterial.issuerPkB64;
    let refreshedMaterial: StoredAuthMaterial | null = null;

    // If the UI state is stale, re-read from storage once before giving up.
    if (!issuerPkB64) {
      const latest = await loadAuthMaterial(docId);
      issuerSkB64 = latest.issuerSkB64;
      issuerPkB64 = latest.issuerPkB64;
      refreshedMaterial = latest;
      setAuthMaterial(latest);
    }

    // Best-effort bootstrap so "New device" can mint an invite even if the background auth init hasn't completed yet.
    if (!issuerSkB64 && !issuerPkB64) {
      if (joinMode) {
        throw new Error("This is an isolated device and can’t mint invites. Open a minting peer tab and share from there.");
      }
      const { sk, pk } = await generateEd25519KeyPair();
      await saveIssuerKeys(docId, base64urlEncode(pk), base64urlEncode(sk));
      const latest = await loadAuthMaterial(docId);
      issuerSkB64 = latest.issuerSkB64;
      issuerPkB64 = latest.issuerPkB64;
      setAuthMaterial(latest);
    }
    if (!issuerSkB64 && issuerPkB64) {
      const latest = refreshedMaterial ?? authMaterial;
      const localSkB64 = latest.localSkB64;
      const proofTokenB64 = latest.localTokensB64[0] ?? null;
      if (!localSkB64 || !proofTokenB64) {
        throw new Error("This tab is verify-only and has no local keys/tokens yet; import an invite link first.");
      }

      const issuerPk = base64urlDecode(issuerPkB64);
      const proofTokenBytes = base64urlDecode(proofTokenB64);
      const scopeEvaluator = client ? createTreecrdtSqliteSubtreeScopeEvaluator(client.runner) : undefined;
      const proofDesc = await describeTreecrdtCapabilityTokenV1({
        tokenBytes: proofTokenBytes,
        issuerPublicKeys: [issuerPk],
        docId,
        scopeEvaluator,
      });
      const proofActions = new Set(proofDesc.caps.flatMap((c) => c.actions ?? []));
      if (!proofActions.has("grant")) {
        throw new Error("This tab is verify-only and cannot mint invites (missing grant permission).");
      }

      const rootNodeId = opts2.rootNodeId ?? inviteRoot;
      const { actions, maxDepth, excludeNodeIds } = readInviteConfig(rootNodeId);

      const proofScope = proofDesc.caps?.[0]?.res ?? null;
      const proofRootId = (proofScope?.rootNodeId ?? ROOT_ID).toLowerCase();
      const proofDocWide =
        proofRootId === ROOT_ID && proofScope?.maxDepth === undefined && (proofScope?.excludeNodeIds?.length ?? 0) === 0;
      if (!proofDocWide && rootNodeId.toLowerCase() !== proofRootId) {
        if (proofScope?.maxDepth !== undefined) {
          throw new Error("This tab can only mint delegated invites for its current subtree scope (maxDepth).");
        }
        if (!scopeEvaluator) {
          throw new Error("This tab can only mint delegated invites for its current subtree scope.");
        }
        const tri = await scopeEvaluator({
          docId,
          node: hexToBytes16(rootNodeId),
          scope: {
            root: hexToBytes16(proofRootId),
            ...(proofScope?.excludeNodeIds?.length
              ? { exclude: proofScope.excludeNodeIds.map((id) => hexToBytes16(id)) }
              : {}),
          },
        });
        if (tri === "deny") throw new Error("This tab can only mint delegated invites within its granted subtree scope.");
        if (tri === "unknown") throw new Error("Missing subtree context to mint delegated invites for that node.");
      }

      const { sk: subjectSk, pk: subjectPk } = await generateEd25519KeyPair();
      const tokenBytes = issueTreecrdtDelegatedCapabilityTokenV1({
        delegatorPrivateKey: base64urlDecode(localSkB64),
        delegatorProofToken: proofTokenBytes,
        subjectPublicKey: subjectPk,
        docId,
        rootNodeId,
        actions,
        ...(maxDepth !== undefined ? { maxDepth } : {}),
        ...(excludeNodeIds.length > 0 ? { excludeNodeIds } : {}),
      });

      rememberIssuedGrantRecord({
        recipientPk: subjectPk,
        tokenBytes,
        rootNodeId,
        actions,
        ...(maxDepth !== undefined ? { maxDepth } : {}),
        excludeNodeIds,
      });

      return encodeInvitePayload({
        v: 1,
        t: "treecrdt.playground.invite",
        docId,
        issuerPkB64,
        subjectSkB64: base64urlEncode(subjectSk),
        tokenB64: base64urlEncode(tokenBytes),
        payloadKeyB64: await loadOrCreateDocPayloadKeyB64(docId),
      });
    }
    if (!issuerSkB64 || !issuerPkB64) {
      throw new Error("issuer private key is not available in this tab (cannot mint invites)");
    }

    const rootNodeId = opts2.rootNodeId ?? inviteRoot;
    const { actions, maxDepth, excludeNodeIds } = readInviteConfig(rootNodeId);

    const issuerSk = base64urlDecode(issuerSkB64);
    const { sk: subjectSk, pk: subjectPk } = await generateEd25519KeyPair();
    const tokenBytes = createCapabilityTokenV1({
      issuerPrivateKey: issuerSk,
      subjectPublicKey: subjectPk,
      docId,
      rootNodeId,
      actions,
      ...(maxDepth !== undefined ? { maxDepth } : {}),
      ...(excludeNodeIds.length > 0 ? { excludeNodeIds } : {}),
    });

    rememberIssuedGrantRecord({
      recipientPk: subjectPk,
      tokenBytes,
      rootNodeId,
      actions,
      ...(maxDepth !== undefined ? { maxDepth } : {}),
      excludeNodeIds,
    });

    return encodeInvitePayload({
      v: 1,
      t: "treecrdt.playground.invite",
      docId,
      issuerPkB64,
      subjectSkB64: base64urlEncode(subjectSk),
      tokenB64: base64urlEncode(tokenBytes),
      payloadKeyB64: await loadOrCreateDocPayloadKeyB64(docId),
    });
  };

  const generateInviteLink = async (opts2: { rootNodeId?: string; copyToClipboard?: boolean } = {}) => {
    if (typeof window === "undefined") return;
    setAuthBusy(true);
    setAuthError(null);
    setAuthInfo(null);
    try {
      const rootNodeId = opts2.rootNodeId ?? inviteRoot;
      const configKey = inviteConfigCacheKey(rootNodeId);

      if (inviteLink && inviteLinkConfigKeyRef.current === configKey) {
        if (opts2.copyToClipboard) {
          await copyToClipboard(inviteLink);
          setAuthInfo("Invite link copied to clipboard.");
        }
        return;
      }

      const inviteB64 = await buildInviteB64({ rootNodeId });

      const url = new URL(window.location.href);
      url.searchParams.set("doc", docId);
      applySyncSettingsToUrl(url, syncServerUrl, syncTransportMode);
      url.searchParams.delete("replica");
      url.searchParams.delete("fresh");
      url.searchParams.set("join", "1");
      url.searchParams.set("profile", makeNewProfileId());
      url.searchParams.set("auth", "1");
      url.hash = `invite=${inviteB64}`;
      const link = url.toString();
      setInviteLink(link);
      inviteLinkConfigKeyRef.current = configKey;
      if (opts2.copyToClipboard) {
        await copyToClipboard(link);
        setAuthInfo("Invite link copied to clipboard.");
      }
    } catch (err) {
      setAuthError(err instanceof Error ? err.message : String(err));
    } finally {
      setAuthBusy(false);
    }
  };

  const refreshPendingOps = async () => {
    if (!client) return;
    setAuthBusy(true);
    setAuthError(null);
    try {
      const store = createPendingOpsStore({ runner: client.runner, docId });
      await store.init();
      const listed = await store.listPendingOps();
      setPendingOps(
        listed.map((p) => ({
          id: `${bytesToHex(p.op.meta.id.replica)}:${p.op.meta.id.counter}`,
          kind: p.op.kind.type,
          ...(p.message ? { message: p.message } : {}),
        }))
      );
    } catch (err) {
      setAuthError(err instanceof Error ? err.message : String(err));
    } finally {
      setAuthBusy(false);
    }
  };

  const onAuthGrantMessage = React.useCallback(
    (grant: AuthGrantMessageV1) => {
      const issuerPkB64 = grant.issuer_pk_b64;
      const tokenB64 = grant.token_b64;
      const payloadKeyB64 = typeof grant.payload_key_b64 === "string" ? grant.payload_key_b64 : null;
      const supersededTokenIds = Array.isArray(grant.supersedes_token_ids_hex)
        ? grant.supersedes_token_ids_hex
            .map((id) => normalizeTokenIdHex(id))
            .filter((id): id is string => typeof id === "string")
        : [];

      void (async () => {
        setAuthBusy(true);
        setAuthError(null);
        setAuthInfo(null);
        try {
          await saveIssuerKeys(docId, issuerPkB64);

          if (payloadKeyB64) {
            await saveDocPayloadKeyB64(docId, payloadKeyB64);
            await refreshDocPayloadKey();
          }

          const current = await loadAuthMaterial(docId);
          if (!current.localPkB64 || !current.localSkB64) {
            throw new Error("received grant but local keys are missing; import an invite link first");
          }

          const merged = new Set<string>(current.localTokensB64);
          if (supersededTokenIds.length > 0) {
            const superseded = new Set(supersededTokenIds);
            for (const existingTokenB64 of Array.from(merged.values())) {
              try {
                const existingTokenIdHex = bytesToHex(deriveTokenIdV1(base64urlDecode(existingTokenB64)));
                if (superseded.has(existingTokenIdHex)) merged.delete(existingTokenB64);
              } catch {
                // ignore malformed token bytes
              }
            }
          }
          merged.add(tokenB64);
          await saveLocalTokens(docId, Array.from(merged));
          await rememberScopedPrivateRootsFromToken(issuerPkB64, tokenB64);
          await refreshAuthMaterial();
          setAuthInfo(
            supersededTokenIds.length > 0
              ? "Access updated. Click Sync to refresh with your latest capability."
              : "Access grant received. Click Sync to fetch newly authorized ops."
          );
          setToast({
            kind: "success",
            title: supersededTokenIds.length > 0 ? "Access updated" : "Access granted",
            message:
              supersededTokenIds.length > 0
                ? "Sync to refresh using your latest capability."
                : "Sync to fetch newly authorized ops.",
            actions: ["sync", "details"],
          });
        } catch (err) {
          setAuthError(err instanceof Error ? err.message : String(err));
        } finally {
          setAuthBusy(false);
        }
      })();
    },
    [docId, refreshAuthMaterial, refreshDocPayloadKey, rememberScopedPrivateRootsFromToken]
  );

  const grantSubtreeToReplicaPubkey = async (
    sendGrant: (msg: AuthGrantMessageV1) => boolean,
    opts2?: { recipientKey?: string; rootNodeId?: string; actions?: string[]; supersedesTokenIds?: string[] }
  ) => {
    if (!authEnabled) return false;
    if (typeof window === "undefined") return false;

    setAuthBusy(true);
    setAuthError(null);
    setAuthInfo(null);

    try {
      if (!selfPeerId) throw new Error("local replica public key is not ready yet");

      const issuerPkB64 = authMaterial.issuerPkB64;
      const issuerSkB64 = authMaterial.issuerSkB64;
      if (!issuerPkB64) throw new Error("issuer public key is missing; import an invite link first");

      const recipientInput = opts2?.recipientKey;
      if (!recipientInput || recipientInput.trim().length === 0) {
        throw new Error("recipient public key is required");
      }
      const subjectPk = parseReplicaPublicKeyInput(recipientInput);
      const rootNodeId = opts2?.rootNodeId ?? inviteRoot;
      const supersedesTokenIds = Array.isArray(opts2?.supersedesTokenIds)
        ? opts2.supersedesTokenIds
            .map((id) => normalizeTokenIdHex(id))
            .filter((id): id is string => typeof id === "string")
        : [];
      const inviteConfig = readInviteConfig(rootNodeId);
      const actions =
        Array.isArray(opts2?.actions) && opts2.actions.length > 0
          ? expandInternalCompatActions(opts2.actions)
          : inviteConfig.actions;
      if (actions.length === 0) throw new Error("select at least one capability action");
      const { maxDepth, excludeNodeIds } = inviteConfig;

      let tokenBytes: Uint8Array;
      if (issuerSkB64) {
        const issuerSk = base64urlDecode(issuerSkB64);
        tokenBytes = createCapabilityTokenV1({
          issuerPrivateKey: issuerSk,
          subjectPublicKey: subjectPk,
          docId,
          rootNodeId,
          actions,
          ...(maxDepth !== undefined ? { maxDepth } : {}),
          ...(excludeNodeIds.length > 0 ? { excludeNodeIds } : {}),
        });
      } else {
        const localSkB64 = authMaterial.localSkB64;
        const proofTokenB64 = authMaterial.localTokensB64[0] ?? null;
        if (!localSkB64 || !proofTokenB64) {
          throw new Error("cannot delegate grants without local keys/tokens; import an invite link first");
        }

        const issuerPk = base64urlDecode(issuerPkB64);
        const proofTokenBytes = base64urlDecode(proofTokenB64);
        const scopeEvaluator = client ? createTreecrdtSqliteSubtreeScopeEvaluator(client.runner) : undefined;
        const proofDesc = await describeTreecrdtCapabilityTokenV1({
          tokenBytes: proofTokenBytes,
          issuerPublicKeys: [issuerPk],
          docId,
          scopeEvaluator,
        });
        const proofActions = new Set(proofDesc.caps.flatMap((c) => c.actions ?? []));
        if (!proofActions.has("grant")) {
          throw new Error("this tab cannot delegate grants (missing grant permission)");
        }

        const proofScope = proofDesc.caps?.[0]?.res ?? null;
        const proofRootId = (proofScope?.rootNodeId ?? ROOT_ID).toLowerCase();
        const proofDocWide =
          proofRootId === ROOT_ID && proofScope?.maxDepth === undefined && (proofScope?.excludeNodeIds?.length ?? 0) === 0;
        if (!proofDocWide && rootNodeId.toLowerCase() !== proofRootId) {
          if (proofScope?.maxDepth !== undefined) {
            throw new Error("this tab can only delegate grants for its current subtree scope (maxDepth)");
          }
          if (!scopeEvaluator) {
            throw new Error("this tab can only delegate grants for its current subtree scope");
          }
          const tri = await scopeEvaluator({
            docId,
            node: hexToBytes16(rootNodeId),
            scope: {
              root: hexToBytes16(proofRootId),
              ...(proofScope?.excludeNodeIds?.length
                ? { exclude: proofScope.excludeNodeIds.map((id) => hexToBytes16(id)) }
                : {}),
            },
          });
          if (tri === "deny") throw new Error("this tab can only delegate grants within its granted subtree scope");
          if (tri === "unknown") throw new Error("missing subtree context to delegate grants for that node");
        }

        tokenBytes = issueTreecrdtDelegatedCapabilityTokenV1({
          delegatorPrivateKey: base64urlDecode(localSkB64),
          delegatorProofToken: proofTokenBytes,
          subjectPublicKey: subjectPk,
          docId,
          rootNodeId,
          actions,
          ...(maxDepth !== undefined ? { maxDepth } : {}),
          ...(excludeNodeIds.length > 0 ? { excludeNodeIds } : {}),
        });
      }

      const msg: AuthGrantMessageV1 = {
        t: "auth_grant_v1",
        doc_id: docId,
        to_replica_pk_hex: bytesToHex(subjectPk),
        issuer_pk_b64: issuerPkB64,
        token_b64: base64urlEncode(tokenBytes),
        ...(supersedesTokenIds.length > 0 ? { supersedes_token_ids_hex: supersedesTokenIds } : {}),
        payload_key_b64: await loadOrCreateDocPayloadKeyB64(docId),
        from_peer_id: selfPeerId,
        ts: Date.now(),
      };

      if (!sendGrant(msg)) throw new Error("sync channel is not ready yet");
      rememberIssuedGrantRecord({
        recipientPk: subjectPk,
        tokenBytes,
        rootNodeId,
        actions,
        ...(maxDepth !== undefined ? { maxDepth } : {}),
        excludeNodeIds,
      });
      setAuthInfo("Grant sent. The recipient should sync again to receive newly authorized ops.");
      return true;
    } catch (err) {
      setAuthError(err instanceof Error ? err.message : String(err));
      return false;
    } finally {
      setAuthBusy(false);
    }
  };

  const makeNewProfileId = () => `profile-${crypto.randomUUID().slice(0, 8)}`;

  const openNewIsolatedPeerTab = async (opts2: { autoInvite: boolean; rootNodeId?: string }) => {
    if (typeof window === "undefined") return;

    const url = new URL(window.location.href);
    url.searchParams.set("doc", docId);
    applySyncSettingsToUrl(url, syncServerUrl, syncTransportMode);
    url.searchParams.delete("replica");
    url.searchParams.set("profile", makeNewProfileId());
    url.searchParams.set("join", "1");
    url.searchParams.set("auth", "1");
    url.searchParams.delete("autosync");
    url.hash = "";

    if (opts2.autoInvite) {
      try {
        // Auto-invite makes the common "simulate another device" flow 1 click.
        const rootNodeId = opts2.rootNodeId ?? ROOT_ID;
        const configKey = inviteConfigCacheKey(rootNodeId);
        url.searchParams.set("autosync", "1");
        let inviteB64: string | null = null;
        if (inviteLink && inviteLinkConfigKeyRef.current === configKey) {
          inviteB64 = extractInviteB64FromLink(inviteLink);
        }
        if (!inviteB64) {
          inviteB64 = await buildInviteB64({ rootNodeId });
        }
        url.hash = `invite=${inviteB64}`;
      } catch (err) {
        // Fall back to a blank join-only tab and show the reason on the current tab.
        setAuthError(err instanceof Error ? err.message : String(err));
        setShowAuthPanel(true);
      }
    }

    window.open(url.toString(), "_blank", "noopener,noreferrer");
  };

  const openShareForNode = (nodeId: string) => {
    setInviteRoot(nodeId);
    setShowShareDialog(true);
    void generateInviteLink({ rootNodeId: nodeId });
  };

  return {
    authEnabled,
    setAuthEnabled,
    revealIdentity,
    setRevealIdentity,
    showAuthPanel,
    setShowAuthPanel,
    showShareDialog,
    setShowShareDialog,
    showAuthAdvanced,
    setShowAuthAdvanced,
    authInfo,
    authError,
    setAuthError,
    authBusy,
    toast,
    setToast,
    wrapKeyImportText,
    setWrapKeyImportText,
    issuerKeyBlobImportText,
    setIssuerKeyBlobImportText,
    identityKeyBlobImportText,
    setIdentityKeyBlobImportText,
    deviceSigningKeyBlobImportText,
    setDeviceSigningKeyBlobImportText,
    authMaterial,
    syncAuth,
    refreshAuthMaterial,
    localIdentityChainPromiseRef,
    replica,
    selfPeerId,
    viewRootId,
    authCanSyncAll,
    canWriteStructure,
    canWritePayload,
    canDelete,
    isScopedAccess,
    authCanIssue,
    authCanDelegate,
    authIssuerPkHex,
    authLocalKeyIdHex,
    authLocalTokenIdHex,
    authTokenCount,
    authTokenScope,
    authTokenActions,
    authNeedsInvite,
    hardRevokedTokenIds,
    toggleHardRevokedTokenId,
    pendingOps,
    refreshPendingOps,
    privateRoots,
    privateRootsCount,
    togglePrivateRoot,
    inviteRoot,
    inviteActions,
    setInviteActions,
    inviteAllowGrant,
    setInviteAllowGrant,
    inviteLink,
    generateInviteLink,
    issuedGrantRecords,
    grantSubtreeToReplicaPubkey,
    resetAuth,
    openMintingPeerTab,
    openNewIsolatedPeerTab,
    openShareForNode,
    verifyLocalOps,
    copyToClipboard,
    onAuthGrantMessage,
  };
}
