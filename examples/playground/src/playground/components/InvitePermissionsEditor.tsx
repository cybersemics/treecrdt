import React, { useMemo } from "react";
import { MdCheck } from "react-icons/md";

import {
  CAPABILITY_ACTION_ORDER,
  CAPABILITY_META,
  type CapabilityAction,
} from "../capabilities";
import type { InviteActions } from "../invite";

export function InvitePermissionsEditor({
  busy,
  inviteActions,
  setInviteActions,
  inviteAllowGrant,
  setInviteAllowGrant,
}: {
  busy: boolean;
  inviteActions: InviteActions;
  setInviteActions: React.Dispatch<React.SetStateAction<InviteActions>>;
  inviteAllowGrant: boolean;
  setInviteAllowGrant: React.Dispatch<React.SetStateAction<boolean>>;
}) {
  const selectedActions = useMemo(() => {
    const out: CapabilityAction[] = ["read"];
    if (inviteActions.write_structure) out.push("write_structure");
    if (inviteActions.write_payload) out.push("write_payload");
    if (inviteActions.delete) out.push("delete");
    if (inviteAllowGrant) out.push("grant");
    return out;
  }, [inviteActions, inviteAllowGrant]);

  const toggleCapability = (action: CapabilityAction) => {
    if (action === "read") return;
    if (action === "grant") {
      setInviteAllowGrant((v) => !v);
      return;
    }
    setInviteActions((prev) => ({ ...prev, [action]: !prev[action] }));
  };

  return (
    <div className="space-y-1.5">
      <div className="text-[11px] text-slate-500">Read is always included.</div>
      <div className="flex flex-wrap gap-1">
        {CAPABILITY_ACTION_ORDER.map((action) => {
          const enabled = selectedActions.includes(action);
          const Icon = CAPABILITY_META[action].icon;
          const readLocked = action === "read";
          return (
            <button
              key={action}
              type="button"
              className={`relative flex h-7 w-7 items-center justify-center rounded-md border text-slate-100 transition ${
                enabled
                  ? "border-emerald-400/70 bg-emerald-500/10"
                  : "border-slate-700 bg-slate-800/60 hover:border-accent"
              }`}
              title={readLocked ? "Read (always included)" : CAPABILITY_META[action].label}
              aria-label={CAPABILITY_META[action].label}
              aria-pressed={enabled}
              onClick={() => toggleCapability(action)}
              disabled={busy || readLocked}
            >
              <Icon className="text-[14px]" />
              {enabled && <MdCheck className="absolute -right-1 -top-1 text-[12px] text-emerald-300" />}
            </button>
          );
        })}
      </div>
    </div>
  );
}
