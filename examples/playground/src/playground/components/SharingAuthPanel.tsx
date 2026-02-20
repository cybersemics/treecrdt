import React from "react";
import { MdContentCopy } from "react-icons/md";

import { ROOT_ID } from "../constants";
import {
  getDeviceWrapKeyB64,
  getSealedDeviceSigningKeyB64,
  getSealedIdentityKeyB64,
  getSealedIssuerKeyB64,
  importDeviceWrapKeyB64,
  setSealedDeviceSigningKeyB64,
  setSealedIdentityKeyB64,
  setSealedIssuerKeyB64,
} from "../../auth";

type AuthTokenScope = {
  docId: string;
  rootNodeId?: string;
  maxDepth?: number;
  excludeNodeIds?: string[];
};

type PendingOpEntry = { id: string; kind: string; message?: string };

export type SharingAuthPanelProps = {
  docId: string;
  authEnabled: boolean;
  authBusy: boolean;
  authNeedsInvite: boolean;
  authError: string | null;
  authInfo: string | null;
  setAuthError: React.Dispatch<React.SetStateAction<string | null>>;

  authCanIssue: boolean;
  authCanDelegate: boolean;
  authIssuerPkHex: string | null;
  authLocalKeyIdHex: string | null;
  authLocalTokenIdHex: string | null;
  authTokenCount: number;
  authTokenScope: AuthTokenScope | null;
  authTokenActions: string[] | null;
  authScopeSummary: string;
  authScopeTitle: string;
  authSummaryBadges: string[];

  selfPeerId: string | null;
  copyToClipboard: (text: string) => Promise<void>;
  openMintingPeerTab: () => void;

  resetAuth: () => void;
  setAuthEnabled: React.Dispatch<React.SetStateAction<boolean>>;

  showAuthAdvanced: boolean;
  setShowAuthAdvanced: React.Dispatch<React.SetStateAction<boolean>>;

  wrapKeyImportText: string;
  setWrapKeyImportText: React.Dispatch<React.SetStateAction<string>>;
  issuerKeyBlobImportText: string;
  setIssuerKeyBlobImportText: React.Dispatch<React.SetStateAction<string>>;
  identityKeyBlobImportText: string;
  setIdentityKeyBlobImportText: React.Dispatch<React.SetStateAction<string>>;
  deviceSigningKeyBlobImportText: string;
  setDeviceSigningKeyBlobImportText: React.Dispatch<React.SetStateAction<string>>;

  localIdentityChainPromiseRef: React.MutableRefObject<unknown | null>;

  client: unknown | null;
  pendingOps: PendingOpEntry[];
  refreshPendingOps: () => Promise<void>;

  revealIdentity: boolean;
  setRevealIdentity: React.Dispatch<React.SetStateAction<boolean>>;

  refreshAuthMaterial: () => Promise<unknown>;
};

export function SharingAuthPanel(props: SharingAuthPanelProps) {
  const {
    docId,
    authEnabled,
    setAuthEnabled,
    authBusy,
    resetAuth,
    authNeedsInvite,
    authError,
    authInfo,
    setAuthError,
    authCanIssue,
    authCanDelegate,
    authIssuerPkHex,
    authLocalKeyIdHex,
    authLocalTokenIdHex,
    authTokenCount,
    authTokenScope,
    authTokenActions,
    authScopeSummary,
    authScopeTitle,
    authSummaryBadges,
    selfPeerId,
    openMintingPeerTab,
    revealIdentity,
    setRevealIdentity,
    showAuthAdvanced,
    setShowAuthAdvanced,
    copyToClipboard,
    refreshAuthMaterial,
    refreshPendingOps,
    client,
    pendingOps,
    wrapKeyImportText,
    setWrapKeyImportText,
    issuerKeyBlobImportText,
    setIssuerKeyBlobImportText,
    identityKeyBlobImportText,
    setIdentityKeyBlobImportText,
    deviceSigningKeyBlobImportText,
    setDeviceSigningKeyBlobImportText,
    localIdentityChainPromiseRef,
  } = props;

  const deviceWrapKeyB64 = getDeviceWrapKeyB64();
  const sealedIssuerKeyB64 = getSealedIssuerKeyB64(docId);
  const sealedIdentityKeyB64 = getSealedIdentityKeyB64();
  const sealedDeviceSigningKeyB64 = getSealedDeviceSigningKeyB64();
  const userFacingAuthTokenActions = authTokenActions?.filter((name) => name !== "tombstone") ?? null;

  return (
    <div id="playground-auth-panel" className="mb-3 rounded-xl border border-slate-800/80 bg-slate-950/40 p-3 text-xs text-slate-300">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Sharing & Auth</div>
          <div className="mt-1 text-[11px] text-slate-400">
            {authEnabled ? "Enabled (ops must be signed and authorized)" : "Disabled (no signature/ACL checks)"}
          </div>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <button
            className={`rounded-lg border px-3 py-1.5 text-xs font-semibold transition ${
              authEnabled
                ? "border-emerald-400/60 bg-emerald-500/10 text-emerald-50 hover:border-accent"
                : "border-slate-700 bg-slate-800/70 text-slate-200 hover:border-accent hover:text-white"
            }`}
            type="button"
            onClick={() => setAuthEnabled((v) => !v)}
            disabled={authBusy}
          >
            {authEnabled ? "Disable" : "Enable"}
          </button>
          <button
            className="rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
            type="button"
            onClick={resetAuth}
            disabled={authBusy}
            title="Clears this tab's auth keys/tokens"
          >
            Reset
          </button>
          <button
            className="rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
            type="button"
            onClick={() => setShowAuthAdvanced((v) => !v)}
            disabled={authBusy}
            aria-expanded={showAuthAdvanced}
            title={showAuthAdvanced ? "Hide advanced" : "Show advanced"}
          >
            {showAuthAdvanced ? "Hide advanced" : "Advanced"}
          </button>
        </div>
      </div>

      {!showAuthAdvanced && (
        <div className="mt-2 text-[11px] text-slate-500">
          Join with an invite URL in the browser address bar (`...#invite=...`). Member capabilities are managed from
          private nodes via the people icon.
        </div>
      )}

      {authNeedsInvite && (
        <div className="mt-3 rounded-lg border border-sky-400/40 bg-sky-500/10 px-3 py-2 text-xs text-sky-50">
          <div className="font-semibold">Join-only tab</div>
          <div className="mt-1 text-sky-100/90">
            This isolated tab has no local minting keys. Open an invite link, or ask a minting peer for a grant.
          </div>
          <div className="mt-2 flex flex-wrap items-center gap-2">
            <span className="min-w-0 max-w-full truncate font-mono text-[11px] text-sky-50" title={selfPeerId ?? ""}>
              {selfPeerId ?? "(pubkey initializing)"}
            </span>
            <button
              className="flex items-center gap-2 rounded-lg border border-sky-400/40 bg-sky-500/10 px-3 py-1.5 text-xs font-semibold text-sky-50 transition hover:border-sky-300/60 disabled:opacity-50"
              type="button"
              onClick={() =>
                void (selfPeerId ? copyToClipboard(selfPeerId) : Promise.resolve()).catch((err) =>
                  setAuthError(err instanceof Error ? err.message : String(err))
                )
              }
              disabled={!selfPeerId}
              title="Copy public key"
            >
              <MdContentCopy className="text-[14px]" />
              Copy pubkey
            </button>
            <button
              className="rounded-lg border border-sky-400/40 bg-sky-500/10 px-3 py-1.5 text-xs font-semibold text-sky-50 transition hover:border-sky-300/60 disabled:opacity-50"
              type="button"
              onClick={openMintingPeerTab}
              disabled={typeof window === "undefined"}
              title="Open a minting peer (same storage, no join-only mode)"
            >
              Open minting peer
            </button>
          </div>
        </div>
      )}

      {authInfo && (
        <div className="mt-3 rounded-lg border border-emerald-400/40 bg-emerald-500/10 px-3 py-2 text-xs text-emerald-50">
          {authInfo}
        </div>
      )}

      {authError && !authNeedsInvite && (
        <div className="mt-3 rounded-lg border border-rose-400/50 bg-rose-500/10 px-3 py-2 text-xs text-rose-50">
          {authError}
        </div>
      )}

      {showAuthAdvanced && (
        <>
          <div className="mt-3 rounded-lg border border-slate-800/80 bg-slate-950/30 p-3">
            <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Access</div>
            <div className="mt-1 text-[11px] text-slate-500">
              {authCanIssue
                ? "Minting: can mint invites/grants."
                : authCanDelegate
                  ? "Delegate-only: can mint invites/grants within your scope."
                  : "Verify-only (open invite URL to join)."}
            </div>
            <div className="mt-2 flex flex-wrap gap-2 text-[11px]">
              <span
                className={`rounded-full border px-2 py-0.5 font-semibold ${
                  authEnabled
                    ? authCanIssue
                      ? "border-emerald-400/60 bg-emerald-500/10 text-emerald-50"
                      : "border-slate-700 bg-slate-900/60 text-slate-200"
                    : "border-slate-800/70 bg-slate-900/60 text-slate-400"
                }`}
              >
                issuer {authCanIssue ? "minting" : "verify-only"}
              </span>
              <span
                data-testid="auth-token-count"
                className="rounded-full border border-slate-700 bg-slate-900/60 px-2 py-0.5 font-semibold text-slate-200"
              >
                tokens {authTokenCount}
              </span>
              <span
                data-testid="auth-scope"
                className="rounded-full border border-slate-700 bg-slate-900/60 px-2 py-0.5 font-semibold text-slate-200"
                title={authScopeTitle}
              >
                scope {authScopeSummary}
              </span>
              {authSummaryBadges.map((name) => (
                <span
                  key={name}
                  className="rounded-full border border-slate-700 bg-slate-900/60 px-2 py-0.5 font-semibold text-slate-200"
                >
                  {name}
                </span>
              ))}
            </div>
            <div className="mt-2 text-[11px] text-slate-500">
              Member capability grants/revocations are managed from private nodes via the people icon.
            </div>
          </div>

          <div className="mt-3 grid grid-cols-1 gap-2 md:grid-cols-3">
            <div className="rounded-lg border border-slate-800/80 bg-slate-950/30 px-3 py-2">
              <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Issuer</div>
              <div className="mt-1 font-mono text-slate-200">{authIssuerPkHex ? `${authIssuerPkHex.slice(0, 16)}…` : "-"}</div>
              <div className="mt-1 text-[11px] text-slate-500">{authCanIssue ? "can mint invites" : "verify-only"}</div>
            </div>
            <div className="rounded-lg border border-slate-800/80 bg-slate-950/30 px-3 py-2">
              <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Local key_id</div>
              <div className="mt-1 font-mono text-slate-200">{authLocalKeyIdHex ? `${authLocalKeyIdHex.slice(0, 16)}…` : "-"}</div>
            </div>
            <div className="rounded-lg border border-slate-800/80 bg-slate-950/30 px-3 py-2">
              <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Token id</div>
              <div className="mt-1 font-mono text-slate-200">{authLocalTokenIdHex ? `${authLocalTokenIdHex.slice(0, 16)}…` : "-"}</div>
              <div className="mt-1 text-[11px] text-slate-500">{authTokenCount > 0 ? `${authTokenCount} token(s)` : "-"}</div>
              <div className="mt-1 text-[11px] text-slate-500">
                {authTokenScope
                  ? (() => {
                      const rootId = authTokenScope.rootNodeId ?? ROOT_ID;
                      return `scope=${rootId === ROOT_ID ? "doc-wide" : `${rootId.slice(0, 8)}…`}${
                        authTokenScope.maxDepth !== undefined ? ` depth≤${authTokenScope.maxDepth}` : ""
                      }${
                        authTokenScope.excludeNodeIds && authTokenScope.excludeNodeIds.length > 0
                          ? ` exclude=${authTokenScope.excludeNodeIds.length}`
                          : ""
                      }`;
                    })()
                  : "-"}
              </div>
              {userFacingAuthTokenActions && userFacingAuthTokenActions.length > 0 && (
                <div className="mt-1 text-[11px] text-slate-500" title={userFacingAuthTokenActions.join(", ")}>
                  {userFacingAuthTokenActions.join(", ")}
                </div>
              )}
            </div>
          </div>

          <div className="mt-3 grid grid-cols-1 gap-2 md:grid-cols-2">
            <div className="rounded-lg border border-slate-800/80 bg-slate-950/30 p-3">
              <div className="flex items-center justify-between gap-2">
                <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Device wrap key</div>
                <button
                  className="flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                  type="button"
                  onClick={() =>
                    void copyToClipboard(deviceWrapKeyB64 ?? "").catch((err) =>
                      setAuthError(err instanceof Error ? err.message : String(err))
                    )
                  }
                  disabled={authBusy || !deviceWrapKeyB64}
                  title="Copy device wrap key"
                >
                  <MdContentCopy className="text-[16px]" />
                  Copy
                </button>
              </div>
              <div className="mt-1 font-mono text-slate-200" title={deviceWrapKeyB64 ?? ""}>
                {deviceWrapKeyB64 ? `${deviceWrapKeyB64.slice(0, 24)}…` : "(initializing)"}
              </div>
              <div className="mt-2 flex flex-col gap-2 sm:flex-row sm:items-center">
                <input
                  type="text"
                  value={wrapKeyImportText}
                  onChange={(e) => setWrapKeyImportText(e.target.value)}
                  placeholder="Paste base64url wrap key"
                  className="w-full rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                  disabled={authBusy}
                />
                <button
                  className="rounded-lg bg-accent px-4 py-2 text-sm font-semibold text-white shadow-lg shadow-accent/30 transition hover:bg-accent/90 disabled:opacity-50"
                  type="button"
                  onClick={() => {
                    try {
                      importDeviceWrapKeyB64(wrapKeyImportText);
                      setWrapKeyImportText("");
                      void refreshAuthMaterial().catch((err) =>
                        setAuthError(err instanceof Error ? err.message : String(err))
                      );
                    } catch (err) {
                      setAuthError(err instanceof Error ? err.message : String(err));
                    }
                  }}
                  disabled={authBusy || wrapKeyImportText.trim().length === 0}
                  title="Import device wrap key"
                >
                  Import
                </button>
              </div>
              <div className="mt-1 text-[11px] text-slate-500">
                Back up this key (e.g. Supabase). Needed to decrypt doc key blobs.
              </div>
            </div>

            <div className="rounded-lg border border-slate-800/80 bg-slate-950/30 p-3">
              <div className="flex items-center justify-between gap-2">
                <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Issuer key blob</div>
                <button
                  className="flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                  type="button"
                  onClick={() =>
                    void copyToClipboard(sealedIssuerKeyB64 ?? "").catch((err) =>
                      setAuthError(err instanceof Error ? err.message : String(err))
                    )
                  }
                  disabled={authBusy || !sealedIssuerKeyB64}
                  title="Copy sealed issuer key blob (base64url)"
                >
                  <MdContentCopy className="text-[16px]" />
                  Copy
                </button>
              </div>
              <div className="mt-1 font-mono text-slate-200" title={sealedIssuerKeyB64 ?? ""}>
                {sealedIssuerKeyB64 ? `${sealedIssuerKeyB64.slice(0, 24)}…` : "-"}
              </div>
              <div className="mt-2 flex flex-col gap-2 sm:flex-row sm:items-center">
                <input
                  type="text"
                  value={issuerKeyBlobImportText}
                  onChange={(e) => setIssuerKeyBlobImportText(e.target.value)}
                  placeholder="Paste sealed issuer key blob (base64url)"
                  className="w-full rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                  disabled={authBusy}
                />
                <button
                  className="rounded-lg bg-accent px-4 py-2 text-sm font-semibold text-white shadow-lg shadow-accent/30 transition hover:bg-accent/90 disabled:opacity-50"
                  type="button"
                  onClick={() => {
                    try {
                      setSealedIssuerKeyB64(docId, issuerKeyBlobImportText);
                      setIssuerKeyBlobImportText("");
                      void refreshAuthMaterial().catch((err) =>
                        setAuthError(err instanceof Error ? err.message : String(err))
                      );
                    } catch (err) {
                      setAuthError(err instanceof Error ? err.message : String(err));
                    }
                  }}
                  disabled={authBusy || issuerKeyBlobImportText.trim().length === 0}
                  title="Import sealed issuer key blob"
                >
                  Import
                </button>
              </div>
              <div className="mt-1 text-[11px] text-slate-500">Encrypted at rest. Bound to this `docId` via AAD.</div>
            </div>
          </div>

          <div className="mt-3 grid grid-cols-1 gap-2 md:grid-cols-2">
            <div className="rounded-lg border border-slate-800/80 bg-slate-950/30 p-3">
              <div className="flex items-center justify-between gap-2">
                <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Identity key blob</div>
                <button
                  className="flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                  type="button"
                  onClick={() =>
                    void copyToClipboard(sealedIdentityKeyB64 ?? "").catch((err) =>
                      setAuthError(err instanceof Error ? err.message : String(err))
                    )
                  }
                  disabled={authBusy || !sealedIdentityKeyB64}
                  title="Copy sealed identity key blob (base64url)"
                >
                  <MdContentCopy className="text-[16px]" />
                  Copy
                </button>
              </div>
              <div className="mt-1 font-mono text-slate-200" title={sealedIdentityKeyB64 ?? ""}>
                {sealedIdentityKeyB64 ? `${sealedIdentityKeyB64.slice(0, 24)}…` : "-"}
              </div>
              <div className="mt-2 flex flex-col gap-2 sm:flex-row sm:items-center">
                <input
                  type="text"
                  value={identityKeyBlobImportText}
                  onChange={(e) => setIdentityKeyBlobImportText(e.target.value)}
                  placeholder="Paste sealed identity key blob (base64url)"
                  className="w-full rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                  disabled={authBusy}
                />
                <button
                  className="rounded-lg bg-accent px-4 py-2 text-sm font-semibold text-white shadow-lg shadow-accent/30 transition hover:bg-accent/90 disabled:opacity-50"
                  type="button"
                  onClick={() => {
                    try {
                      setSealedIdentityKeyB64(identityKeyBlobImportText);
                      setIdentityKeyBlobImportText("");
                      localIdentityChainPromiseRef.current = null;
                    } catch (err) {
                      setAuthError(err instanceof Error ? err.message : String(err));
                    }
                  }}
                  disabled={authBusy || identityKeyBlobImportText.trim().length === 0}
                  title="Import sealed identity key blob"
                >
                  Import
                </button>
              </div>
              <div className="mt-1 text-[11px] text-slate-500">Encrypted at rest. Requires the device wrap key to open.</div>
            </div>

            <div className="rounded-lg border border-slate-800/80 bg-slate-950/30 p-3">
              <div className="flex items-center justify-between gap-2">
                <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Device signing key blob</div>
                <button
                  className="flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                  type="button"
                  onClick={() =>
                    void copyToClipboard(sealedDeviceSigningKeyB64 ?? "").catch((err) =>
                      setAuthError(err instanceof Error ? err.message : String(err))
                    )
                  }
                  disabled={authBusy || !sealedDeviceSigningKeyB64}
                  title="Copy sealed device signing key blob (base64url)"
                >
                  <MdContentCopy className="text-[16px]" />
                  Copy
                </button>
              </div>
              <div className="mt-1 font-mono text-slate-200" title={sealedDeviceSigningKeyB64 ?? ""}>
                {sealedDeviceSigningKeyB64 ? `${sealedDeviceSigningKeyB64.slice(0, 24)}…` : "-"}
              </div>
              <div className="mt-2 flex flex-col gap-2 sm:flex-row sm:items-center">
                <input
                  type="text"
                  value={deviceSigningKeyBlobImportText}
                  onChange={(e) => setDeviceSigningKeyBlobImportText(e.target.value)}
                  placeholder="Paste sealed device signing key blob (base64url)"
                  className="w-full rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                  disabled={authBusy}
                />
                <button
                  className="rounded-lg bg-accent px-4 py-2 text-sm font-semibold text-white shadow-lg shadow-accent/30 transition hover:bg-accent/90 disabled:opacity-50"
                  type="button"
                  onClick={() => {
                    try {
                      setSealedDeviceSigningKeyB64(deviceSigningKeyBlobImportText);
                      setDeviceSigningKeyBlobImportText("");
                      localIdentityChainPromiseRef.current = null;
                    } catch (err) {
                      setAuthError(err instanceof Error ? err.message : String(err));
                    }
                  }}
                  disabled={authBusy || deviceSigningKeyBlobImportText.trim().length === 0}
                  title="Import sealed device signing key blob"
                >
                  Import
                </button>
              </div>
              <div className="mt-1 text-[11px] text-slate-500">Encrypted at rest. Requires the device wrap key to open.</div>
            </div>
          </div>

          <div className="mt-3 rounded-lg border border-slate-800/80 bg-slate-950/30 p-3">
            <div className="flex items-center justify-between gap-2">
              <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Pending ops</div>
              <button
                className="rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                type="button"
                onClick={() => void refreshPendingOps()}
                disabled={!authEnabled || authBusy || !client}
                title="Fetch pending ops stored due to missing ancestry context"
              >
                Refresh
              </button>
            </div>
            <div className="mt-2 text-[11px] text-slate-400">{pendingOps.length} pending</div>
            {pendingOps.length > 0 && (
              <div className="mt-2 max-h-28 overflow-auto pr-1">
                {pendingOps.map((p) => (
                  <div key={p.id} className="flex items-center justify-between gap-2 py-1">
                    <span className="font-mono text-[11px] text-slate-200">
                      {p.id} <span className="text-slate-500">{p.kind}</span>
                    </span>
                    <span className="text-[10px] text-slate-500">{p.message ?? ""}</span>
                  </div>
                ))}
              </div>
            )}
          </div>

          <div className="mt-3 rounded-lg border border-slate-800/80 bg-slate-950/30 p-3">
            <div className="flex items-center justify-between gap-2">
              <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Identity</div>
              <button
                className={`rounded-lg border px-3 py-1.5 text-xs font-semibold transition disabled:opacity-50 ${
                  revealIdentity
                    ? "border-amber-400/70 bg-amber-500/10 text-amber-100 hover:border-amber-300"
                    : "border-slate-700 bg-slate-800/70 text-slate-200 hover:border-accent hover:text-white"
                }`}
                type="button"
                onClick={() => setRevealIdentity((v) => !v)}
                disabled={authBusy}
                title={
                  revealIdentity
                    ? "Stop advertising an identity chain (unlinkable by default)"
                    : "Advertise identity chain (identity→device→replica)"
                }
              >
                {revealIdentity ? "Revealing" : "Private"}
              </button>
            </div>
            <div className="mt-1 text-[11px] text-slate-500">
              When enabled, this tab advertises an identity chain so peers can attribute signatures. This is linkable
              across documents; keep disabled for unlinkable-by-default privacy.
            </div>
          </div>
        </>
      )}
    </div>
  );
}
