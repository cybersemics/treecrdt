import type { Dispatch, SetStateAction } from "react";
import { MdContentCopy, MdLockOutline } from "react-icons/md";

import { ROOT_ID } from "../constants";
import type { InviteActions, InvitePreset } from "../invite";

import { InvitePermissionsEditor } from "./InvitePermissionsEditor";

export type { InvitePreset } from "../invite";

export type ShareSubtreeDialogProps = {
  open: boolean;
  onClose: () => void;

  inviteRoot: string;
  nodeLabelForId: (id: string) => string;

  joinMode: boolean;
  authEnabled: boolean;
  authBusy: boolean;
  authCanIssue: boolean;
  authCanDelegate: boolean;
  authScopeTitle: string;
  authScopeSummary: string;
  inviteExcludeNodeIds: string[];

  onEnableAuth: () => void;
  openMintingPeerTab: () => void;

  authInfo: string | null;
  authError: string | null;
  setAuthError: Dispatch<SetStateAction<string | null>>;

  invitePreset: InvitePreset;
  applyInvitePreset: (preset: InvitePreset) => void;
  inviteActions: InviteActions;
  setInviteActions: Dispatch<SetStateAction<InviteActions>>;
  inviteAllowGrant: boolean;
  setInviteAllowGrant: Dispatch<SetStateAction<boolean>>;

  openNewIsolatedPeerTab: (opts: { autoInvite: boolean; rootNodeId?: string }) => Promise<void>;
  generateInviteLink: (opts: { rootNodeId?: string; copyToClipboard?: boolean }) => Promise<void>;
  inviteLink: string;
  copyToClipboard: (text: string) => Promise<void>;

  grantRecipientKey: string;
  setGrantRecipientKey: Dispatch<SetStateAction<string>>;
  grantSubtreeToReplicaPubkey: () => Promise<void>;

  selfPeerId: string | null;
};

export function ShareSubtreeDialog(props: ShareSubtreeDialogProps) {
  const {
    open,
    onClose,
    inviteRoot,
    nodeLabelForId,
    joinMode,
    authEnabled,
    authBusy,
    authCanIssue,
    authCanDelegate,
    authScopeTitle,
    authScopeSummary,
    inviteExcludeNodeIds,
    onEnableAuth,
    openMintingPeerTab,
    authInfo,
    authError,
    setAuthError,
    invitePreset,
    applyInvitePreset,
    inviteActions,
    setInviteActions,
    inviteAllowGrant,
    setInviteAllowGrant,
    openNewIsolatedPeerTab,
    generateInviteLink,
    inviteLink,
    copyToClipboard,
    grantRecipientKey,
    setGrantRecipientKey,
    grantSubtreeToReplicaPubkey,
    selfPeerId,
  } = props;

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4" onClick={onClose}>
      <div
        className="w-full max-w-xl rounded-2xl border border-slate-800/80 bg-slate-950/70 p-5 shadow-2xl shadow-black/40 backdrop-blur"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Share subtree</div>
            <div className="mt-1 text-sm font-semibold text-slate-100">
              {inviteRoot === ROOT_ID ? "Root (whole document)" : nodeLabelForId(inviteRoot)}
            </div>
            <div className="mt-1 font-mono text-[11px] text-slate-500">{inviteRoot}</div>
          </div>
          <button
            className="rounded-lg border border-slate-700 bg-slate-900/60 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white"
            type="button"
            onClick={onClose}
          >
            Close
          </button>
        </div>

        <div className="mt-3 flex flex-wrap gap-2 text-[11px] text-slate-400">
          <span className="rounded-full border border-slate-800/70 bg-slate-900/60 px-2 py-0.5 font-semibold">
            mode {joinMode ? "isolated" : "shared"}
          </span>
          <span className="rounded-full border border-slate-800/70 bg-slate-900/60 px-2 py-0.5 font-semibold">
            auth {authEnabled ? "on" : "off"}
          </span>
          {authEnabled && (
            <span
              className="rounded-full border border-slate-800/70 bg-slate-900/60 px-2 py-0.5 font-semibold"
              title={authScopeTitle}
            >
              scope {authScopeSummary}
            </span>
          )}
          {inviteExcludeNodeIds.length > 0 && (
            <span className="rounded-full border border-amber-400/40 bg-amber-500/10 px-2 py-0.5 font-semibold text-amber-100">
              excludes {inviteExcludeNodeIds.length} private root{inviteExcludeNodeIds.length === 1 ? "" : "s"}
            </span>
          )}
        </div>

        {!authEnabled && (
          <div className="mt-3 rounded-lg border border-amber-400/40 bg-amber-500/10 px-3 py-2 text-xs text-amber-50">
            <div className="font-semibold">Enable Auth to share</div>
            <div className="mt-1 text-amber-100/90">
              Sharing uses signed capability tokens (invite links / grants). Enable Auth to mint invites.
            </div>
            <button
              className="mt-2 rounded-lg border border-amber-400/40 bg-amber-500/10 px-3 py-1.5 text-xs font-semibold text-amber-50 transition hover:border-amber-300/60"
              type="button"
              onClick={onEnableAuth}
              disabled={authBusy}
            >
              Enable Auth
            </button>
          </div>
        )}

        {authEnabled && !(authCanIssue || authCanDelegate) && (
          <div className="mt-3 rounded-lg border border-sky-400/40 bg-sky-500/10 px-3 py-2 text-xs text-sky-50">
            <div className="font-semibold">Verify-only tab</div>
            <div className="mt-1 text-sky-100/90">
              This tab can’t mint invites/grants. Open a minting peer (same storage) or import a grant with share
              permission.
            </div>
            <button
              className="mt-2 rounded-lg border border-sky-400/40 bg-sky-500/10 px-3 py-1.5 text-xs font-semibold text-sky-50 transition hover:border-sky-300/60"
              type="button"
              onClick={openMintingPeerTab}
              disabled={typeof window === "undefined"}
            >
              Open minting peer
            </button>
          </div>
        )}

        {authInfo && (
          <div className="mt-3 rounded-lg border border-emerald-400/40 bg-emerald-500/10 px-3 py-2 text-xs text-emerald-50">
            {authInfo}
          </div>
        )}

        {authError && (
          <div className="mt-3 rounded-lg border border-rose-400/50 bg-rose-500/10 px-3 py-2 text-xs text-rose-50">
            {authError}
          </div>
        )}

        {authEnabled && (
          <div className="mt-3">
            <InvitePermissionsEditor
              busy={authBusy}
              invitePreset={invitePreset}
              inviteActions={inviteActions}
              setInviteActions={setInviteActions}
              applyInvitePreset={applyInvitePreset}
              inviteAllowGrant={inviteAllowGrant}
              setInviteAllowGrant={setInviteAllowGrant}
            />
          </div>
        )}

        <div className="mt-3 rounded-lg border border-slate-800/80 bg-slate-950/30 p-3">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <div>
              <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Invite link</div>
              <div className="mt-1 text-[11px] text-slate-500">
                Copies a subtree-scoped invite link (includes E2EE doc payload key).
              </div>
            </div>
            <div className="flex items-center gap-2">
              <button
                className="flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                type="button"
                onClick={() => void openNewIsolatedPeerTab({ autoInvite: true, rootNodeId: inviteRoot })}
                disabled={authBusy || !authEnabled || !(authCanIssue || authCanDelegate)}
                title={
                  authCanIssue || authCanDelegate
                    ? "Open an isolated device tab and auto-import the invite"
                    : "This tab can’t mint invites (verify-only)"
                }
              >
                <MdLockOutline className="text-[16px]" />
                Open device
              </button>
              <button
                className="flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
                type="button"
                onClick={() => void generateInviteLink({ rootNodeId: inviteRoot, copyToClipboard: true })}
                disabled={authBusy || !authEnabled || !(authCanIssue || authCanDelegate)}
                title={
                  !authEnabled
                    ? "Enable Auth to mint invites"
                    : authCanIssue || authCanDelegate
                      ? "Copy invite"
                      : "Verify-only tab"
                }
              >
                <MdContentCopy className="text-[14px]" />
                Copy invite
              </button>
            </div>
          </div>
          <textarea
            className="mt-2 w-full rounded-lg border border-slate-800 bg-slate-950/60 px-3 py-2 font-mono text-[11px] text-slate-200 outline-none focus:border-accent focus:ring-2 focus:ring-accent/40"
            rows={3}
            value={inviteLink || ""}
            readOnly
            placeholder="Invite link will appear here…"
          />
          <div className="mt-2 flex flex-wrap items-center justify-between gap-2 text-[11px] text-slate-500">
            <span>Tip: The recipient can paste it into Auth → Import invite.</span>
            <button
              className="flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
              type="button"
              onClick={() =>
                void (inviteLink ? copyToClipboard(inviteLink) : Promise.resolve()).catch((err) =>
                  setAuthError(err instanceof Error ? err.message : String(err))
                )
              }
              disabled={!inviteLink || authBusy}
              title="Copy invite link text"
            >
              <MdContentCopy className="text-[14px]" />
              Copy text
            </button>
          </div>
        </div>

        {authEnabled && (authCanIssue || authCanDelegate) && (
          <div className="mt-3 rounded-lg border border-slate-800/80 bg-slate-950/30 p-3">
            <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">
              {authCanIssue ? "Grant to pubkey" : "Delegated grant to pubkey"}
            </div>
            <div className="mt-1 text-[11px] text-slate-500">
              Send a capability token to another peer (they should resync to fetch newly authorized ops).
            </div>
            <div className="mt-2 flex flex-wrap items-end gap-2">
              <label className="flex-1 space-y-2 text-sm text-slate-200">
                <span className="text-[11px] text-slate-400">Recipient public key (hex or base64url)</span>
                <input
                  value={grantRecipientKey}
                  onChange={(e) => setGrantRecipientKey(e.target.value)}
                  placeholder="e.g. c7df…bf32"
                  className="w-full rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
                  disabled={authBusy}
                />
              </label>
              <button
                className="rounded-lg bg-accent px-4 py-2 text-sm font-semibold text-white shadow-lg shadow-accent/30 transition hover:bg-accent/90 disabled:opacity-50"
                type="button"
                onClick={() => void grantSubtreeToReplicaPubkey()}
                disabled={authBusy || grantRecipientKey.trim().length === 0}
              >
                Grant
              </button>
            </div>
          </div>
        )}

        <div className="mt-3 flex flex-wrap items-center justify-between gap-2 text-[11px] text-slate-400">
          <div>
            Your pubkey: <span className="font-mono text-slate-200">{selfPeerId ?? "-"}</span>
          </div>
          <button
            className="flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
            type="button"
            onClick={() =>
              void (selfPeerId ? copyToClipboard(selfPeerId) : Promise.resolve()).catch((err) =>
                setAuthError(err instanceof Error ? err.message : String(err))
              )
            }
            disabled={!selfPeerId}
            title="Copy your public key"
          >
            <MdContentCopy className="text-[14px]" />
            Copy my pubkey
          </button>
        </div>
      </div>
    </div>
  );
}
