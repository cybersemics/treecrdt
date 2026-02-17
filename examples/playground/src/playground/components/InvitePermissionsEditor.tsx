import React, { useMemo } from "react";
import { MdAccountTree, MdCheck, MdDeleteOutline, MdEdit, MdShare } from "react-icons/md";

import type { InviteActions, InvitePreset } from "../invite";

type InviteCapability = keyof InviteActions | "grant";

const PRESET_ORDER: InvitePreset[] = ["read", "read_write", "admin"];

const PRESET_LABEL: Record<Exclude<InvitePreset, "custom">, string> = {
  read: "Read",
  read_write: "Read + Write",
  admin: "Admin",
};

const CAPABILITY_ORDER: InviteCapability[] = ["write_structure", "write_payload", "delete", "grant"];

const CAPABILITY_META: Record<
  InviteCapability,
  { label: string; icon: React.ComponentType<{ className?: string }> }
> = {
  write_structure: { label: "Write structure", icon: MdAccountTree },
  write_payload: { label: "Write payload", icon: MdEdit },
  delete: { label: "Delete", icon: MdDeleteOutline },
  grant: { label: "Grant (reshare)", icon: MdShare },
};

function presetToActions(preset: InvitePreset, custom: InviteActions): InviteActions {
  if (preset === "read") return { write_structure: false, write_payload: false, delete: false };
  if (preset === "read_write") return { write_structure: true, write_payload: true, delete: false };
  if (preset === "admin") return { write_structure: true, write_payload: true, delete: true };
  return custom;
}

function inferPreset(actions: InviteActions): InvitePreset {
  const { write_structure, write_payload, delete: canDelete } = actions;
  if (!write_structure && !write_payload && !canDelete) return "read";
  if (write_structure && write_payload && !canDelete) return "read_write";
  if (write_structure && write_payload && canDelete) return "admin";
  return "custom";
}

export function InvitePermissionsEditor({
  busy,
  invitePreset,
  inviteActions,
  setInviteActions,
  applyInvitePreset,
  inviteAllowGrant,
  setInviteAllowGrant,
  showPresets = true,
  showContainer = true,
  showHeader = true,
  showCapabilitiesLabel = true,
  showHint = true,
}: {
  busy: boolean;
  invitePreset: InvitePreset;
  inviteActions: InviteActions;
  setInviteActions: React.Dispatch<React.SetStateAction<InviteActions>>;
  applyInvitePreset: (preset: InvitePreset) => void;
  inviteAllowGrant: boolean;
  setInviteAllowGrant: React.Dispatch<React.SetStateAction<boolean>>;
  showPresets?: boolean;
  showContainer?: boolean;
  showHeader?: boolean;
  showCapabilitiesLabel?: boolean;
  showHint?: boolean;
}) {
  const effective = useMemo(() => presetToActions(invitePreset, inviteActions), [invitePreset, inviteActions]);

  const setNextActions = (next: InviteActions) => {
    const preset = inferPreset(next);
    if (preset === "custom") {
      applyInvitePreset("custom");
      setInviteActions(next);
      return;
    }
    applyInvitePreset(preset);
  };

  const toggleCapability = (name: InviteCapability) => {
    if (name === "grant") {
      setInviteAllowGrant((v) => !v);
      return;
    }
    setNextActions({ ...effective, [name]: !effective[name] });
  };

  const content = (
    <>
      {showHeader && (
        <div className="flex flex-wrap items-center justify-between gap-2">
          <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Permissions</div>
          <div className="text-[11px] text-slate-500">Read always included</div>
        </div>
      )}

      {showPresets && (
        <div className={`${showHeader ? "mt-2" : ""} flex flex-wrap gap-1.5`}>
          {PRESET_ORDER.map((preset) => {
            if (preset === "custom") return null;
            const selected = invitePreset === preset;
            return (
              <button
                key={preset}
                type="button"
                className={`h-8 rounded-lg border px-3 text-[11px] font-semibold transition ${
                  selected
                    ? "border-accent bg-accent/20 text-white shadow-sm shadow-accent/20"
                    : "border-slate-700 bg-slate-800/60 text-slate-200 hover:border-accent hover:text-white"
                }`}
                onClick={() => applyInvitePreset(preset)}
                disabled={busy}
              >
                {PRESET_LABEL[preset]}
              </button>
            );
          })}
          {invitePreset === "custom" && (
            <span className="inline-flex h-8 items-center rounded-lg border border-slate-700 bg-slate-900/60 px-3 text-[11px] font-semibold text-slate-300">
              Custom
            </span>
          )}
        </div>
      )}

      <div className={`${showHeader || showPresets ? "mt-2" : ""}`}>
        {showCapabilitiesLabel && <div className="text-[10px] uppercase tracking-wide text-slate-500">Capabilities</div>}
        <div className={`${showCapabilitiesLabel ? "mt-1" : ""} flex flex-wrap gap-1`}>
          {CAPABILITY_ORDER.map((name) => {
            const enabled = name === "grant" ? inviteAllowGrant : Boolean(effective[name]);
            const Icon = CAPABILITY_META[name].icon;
            return (
              <button
                key={name}
                type="button"
                className={`relative flex h-8 w-8 items-center justify-center rounded-md border text-slate-100 transition ${
                  enabled ? "border-emerald-400/70 bg-emerald-500/10" : "border-slate-700 bg-slate-800/60 hover:border-accent"
                }`}
                title={CAPABILITY_META[name].label}
                aria-label={CAPABILITY_META[name].label}
                aria-pressed={enabled}
                onClick={() => toggleCapability(name)}
                disabled={busy}
              >
                <Icon className="text-[15px]" />
                {enabled && <MdCheck className="absolute -right-1 -top-1 text-[12px] text-emerald-300" />}
              </button>
            );
          })}
        </div>
      </div>

      {showHint && (
        <div className="mt-2 text-[11px] text-slate-500">
          Delete uses defensive delete; tombstone handling is internal to token encoding.
        </div>
      )}
    </>
  );

  if (!showContainer) return content;

  return <div className="rounded-lg border border-slate-800/80 bg-slate-950/30 p-3">{content}</div>;
}
