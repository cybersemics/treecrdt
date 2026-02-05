import React, { useEffect, useMemo, useRef, useState } from "react";

import type { InviteActions, InvitePreset } from "../invite";

function presetToActions(preset: InvitePreset, custom: InviteActions): InviteActions {
  if (preset === "read") return { write_structure: false, write_payload: false, delete: false, tombstone: false };
  if (preset === "read_write") return { write_structure: true, write_payload: true, delete: false, tombstone: false };
  if (preset === "admin") return { write_structure: true, write_payload: true, delete: true, tombstone: true };
  return custom;
}

function inferPreset(actions: InviteActions): InvitePreset {
  const { write_structure, write_payload, delete: canDelete, tombstone } = actions;
  if (!write_structure && !write_payload && !canDelete && !tombstone) return "read";
  if (write_structure && write_payload && !canDelete && !tombstone) return "read_write";
  if (write_structure && write_payload && canDelete && tombstone) return "admin";
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
  showAdvancedByDefault,
}: {
  busy: boolean;
  invitePreset: InvitePreset;
  inviteActions: InviteActions;
  setInviteActions: React.Dispatch<React.SetStateAction<InviteActions>>;
  applyInvitePreset: (preset: InvitePreset) => void;
  inviteAllowGrant: boolean;
  setInviteAllowGrant: React.Dispatch<React.SetStateAction<boolean>>;
  showAdvancedByDefault?: boolean;
}) {
  const effective = useMemo(() => presetToActions(invitePreset, inviteActions), [invitePreset, inviteActions]);
  const writeEnabled = effective.write_structure || effective.write_payload;
  const writeMasterRef = useRef<HTMLInputElement>(null);
  const [showAdvanced, setShowAdvanced] = useState(Boolean(showAdvancedByDefault));

  useEffect(() => {
    const el = writeMasterRef.current;
    if (!el) return;
    el.indeterminate = writeEnabled && !(effective.write_structure && effective.write_payload);
  }, [writeEnabled, effective.write_structure, effective.write_payload]);

  const setNextActions = (next: InviteActions) => {
    const preset = inferPreset(next);
    if (preset === "custom") {
      applyInvitePreset("custom");
      setInviteActions(next);
      return;
    }
    applyInvitePreset(preset);
  };

  const toggleWriteMaster = (checked: boolean) => {
    setNextActions({
      ...effective,
      write_structure: checked,
      write_payload: checked,
    });
  };

  const toggleAction = (name: keyof InviteActions, checked: boolean) => {
    setNextActions({ ...effective, [name]: checked });
  };

  return (
    <div className="rounded-lg border border-slate-800/80 bg-slate-950/30 p-3">
      <div className="text-[10px] font-semibold uppercase tracking-wide text-slate-500">Permissions</div>

      <div className="mt-2 grid gap-2 md:grid-cols-2">
        <label className="flex items-start gap-2 text-sm text-slate-200">
          <input type="checkbox" checked disabled />
          <span className="leading-tight">
            <span className="font-semibold">Read</span>
            <span className="mt-0.5 block text-[11px] text-slate-500">Always included (structure + values).</span>
          </span>
        </label>

        <label className="flex items-start gap-2 text-sm text-slate-200">
          <input
            ref={writeMasterRef}
            type="checkbox"
            checked={writeEnabled}
            onChange={(e) => toggleWriteMaster(e.target.checked)}
            disabled={busy}
          />
          <span className="leading-tight">
            <span className="font-semibold">Write</span>
            <span className="mt-0.5 block text-[11px] text-slate-500">Enable edits. Fine-tune below.</span>
          </span>
        </label>
      </div>

      {writeEnabled ? (
        <div className="mt-2 grid gap-2 border-t border-slate-800/70 pt-2 md:grid-cols-2">
          <label className="flex items-start gap-2 text-sm text-slate-200">
            <input
              type="checkbox"
              checked={effective.write_structure}
              onChange={(e) => toggleAction("write_structure", e.target.checked)}
              disabled={busy}
            />
            <span className="leading-tight">
              <span className="font-semibold">Edit tree</span>{" "}
              <span className="font-mono text-[11px] text-slate-500">write_structure</span>
              <span className="mt-0.5 block text-[11px] text-slate-500">Create/move nodes (tree shape).</span>
            </span>
          </label>

          <label className="flex items-start gap-2 text-sm text-slate-200">
            <input
              type="checkbox"
              checked={effective.write_payload}
              onChange={(e) => toggleAction("write_payload", e.target.checked)}
              disabled={busy}
            />
            <span className="leading-tight">
              <span className="font-semibold">Edit values</span>{" "}
              <span className="font-mono text-[11px] text-slate-500">write_payload</span>
              <span className="mt-0.5 block text-[11px] text-slate-500">Edit node values (payload bytes).</span>
            </span>
          </label>
        </div>
      ) : null}

      <div className="mt-2 flex flex-wrap items-center justify-between gap-2 border-t border-slate-800/70 pt-2">
        <label className="flex items-center gap-2 text-sm text-slate-200">
          <input
            type="checkbox"
            checked={inviteAllowGrant}
            onChange={(e) => setInviteAllowGrant(e.target.checked)}
            disabled={busy}
          />
          <span className="text-[13px]">
            Allow resharing <span className="text-[11px] text-slate-500">(grant)</span>
          </span>
        </label>

        <button
          type="button"
          className="text-xs font-semibold text-slate-300 transition hover:text-white"
          onClick={() => setShowAdvanced((v) => !v)}
          aria-expanded={showAdvanced}
        >
          {showAdvanced ? "Hide extra actions" : "Extra actions"}
        </button>
      </div>

      {showAdvanced ? (
        <div className="mt-2 grid gap-2 md:grid-cols-2">
          <label className="flex items-start gap-2 text-sm text-slate-200">
            <input
              type="checkbox"
              checked={effective.delete}
              onChange={(e) => toggleAction("delete", e.target.checked)}
              disabled={busy}
            />
            <span className="leading-tight">
              <span className="font-semibold">Delete nodes</span>{" "}
              <span className="font-mono text-[11px] text-slate-500">delete</span>
              <span className="mt-0.5 block text-[11px] text-slate-500">Remove nodes (soft delete).</span>
            </span>
          </label>

          <label className="flex items-start gap-2 text-sm text-slate-200">
            <input
              type="checkbox"
              checked={effective.tombstone}
              onChange={(e) => toggleAction("tombstone", e.target.checked)}
              disabled={busy}
            />
            <span className="leading-tight">
              <span className="font-semibold">Tombstone nodes</span>{" "}
              <span className="font-mono text-[11px] text-slate-500">tombstone</span>
              <span className="mt-0.5 block text-[11px] text-slate-500">Permanently hide nodes.</span>
            </span>
          </label>
        </div>
      ) : null}

      <div className="mt-2 text-[11px] text-slate-500">
        Tip: “Structure” = insert/move/delete; “payload” = the node’s value/content bytes.
      </div>
    </div>
  );
}
