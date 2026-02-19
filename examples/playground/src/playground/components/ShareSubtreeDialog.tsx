import type { Dispatch, SetStateAction } from "react";
import { MdContentCopy, MdLockOutline } from "react-icons/md";

import { ROOT_ID } from "../constants";
import type { InviteActions } from "../invite";

import { InvitePermissionsEditor } from "./InvitePermissionsEditor";

export type ShareSubtreeDialogProps = {
  open: boolean;
  onClose: () => void;
  busy: boolean;

  inviteRoot: string;
  nodeLabelForId: (id: string) => string;

  authEnabled: boolean;
  authBusy: boolean;
  authCanIssue: boolean;
  authCanDelegate: boolean;

  onEnableAuth: () => void;
  openMintingPeerTab: () => void;

  authInfo: string | null;
  authError: string | null;

  inviteActions: InviteActions;
  setInviteActions: Dispatch<SetStateAction<InviteActions>>;
  inviteAllowGrant: boolean;
  setInviteAllowGrant: Dispatch<SetStateAction<boolean>>;

  openNewIsolatedPeerTab: (opts: { autoInvite: boolean; rootNodeId?: string }) => Promise<void>;
  generateInviteLink: (opts: { rootNodeId?: string; copyToClipboard?: boolean }) => Promise<void>;
  inviteLink: string;
};

export function ShareSubtreeDialog(props: ShareSubtreeDialogProps) {
  const {
    open,
    onClose,
    busy,
    inviteRoot,
    nodeLabelForId,
    authEnabled,
    authBusy,
    authCanIssue,
    authCanDelegate,
    onEnableAuth,
    openMintingPeerTab,
    authInfo,
    authError,
    inviteActions,
    setInviteActions,
    inviteAllowGrant,
    setInviteAllowGrant,
    openNewIsolatedPeerTab,
    generateInviteLink,
    inviteLink,
  } = props;

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4" onClick={onClose}>
      <div
        className="max-h-[calc(100vh-2rem)] w-full max-w-xl overflow-y-auto rounded-2xl border border-slate-800/80 bg-slate-950/70 p-5 shadow-2xl shadow-black/40 backdrop-blur"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-400">Share subtree</div>
            <div className="mt-1 text-sm font-semibold text-slate-100">
              {inviteRoot === ROOT_ID ? "Root (whole document)" : nodeLabelForId(inviteRoot)}
            </div>
            <div className="mt-1 font-mono text-[11px] text-slate-500">
              {inviteRoot.length > 24 ? `${inviteRoot.slice(0, 12)}…${inviteRoot.slice(-8)}` : inviteRoot}
            </div>
          </div>
          <button
            className="rounded-lg border border-slate-700 bg-slate-900/60 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white"
            type="button"
            onClick={onClose}
          >
            Close
          </button>
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
              disabled={authBusy || busy}
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
              disabled={typeof window === "undefined" || busy}
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
          <div className="mt-3 flex items-center">
            <InvitePermissionsEditor
              busy={authBusy}
              inviteActions={inviteActions}
              setInviteActions={setInviteActions}
              inviteAllowGrant={inviteAllowGrant}
              setInviteAllowGrant={setInviteAllowGrant}
            />
          </div>
        )}

        <div className="mt-3">
          <div className="flex flex-wrap items-center gap-2">
            <button
              className="flex items-center gap-2 rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-1.5 text-xs font-semibold text-slate-200 transition hover:border-accent hover:text-white disabled:opacity-50"
              type="button"
              onClick={() => void openNewIsolatedPeerTab({ autoInvite: true, rootNodeId: inviteRoot })}
              disabled={busy || authBusy || !authEnabled || !(authCanIssue || authCanDelegate)}
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
              disabled={busy || authBusy || !authEnabled || !(authCanIssue || authCanDelegate)}
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
          <textarea
            className="mt-2 w-full rounded-lg border border-slate-800 bg-slate-950/60 px-3 py-2 font-mono text-[11px] text-slate-200 outline-none focus:border-accent focus:ring-2 focus:ring-accent/40"
            rows={3}
            value={inviteLink || ""}
            readOnly
            placeholder="Invite link will appear here…"
          />
        </div>
      </div>
    </div>
  );
}
