import React from "react";

export const ParentPicker = React.memo(function ParentPicker({
  nodeList,
  value,
  onChange,
  disabled,
}: {
  nodeList: Array<{ id: string; label: string; depth: number }>;
  value: string;
  onChange: (next: string) => void;
  disabled: boolean;
}) {
  return (
    <label className="min-w-0 w-full space-y-2 text-sm text-slate-200 md:w-52">
      <span>Parent</span>
      <select
        className="min-w-0 w-full rounded-lg border border-slate-700 bg-slate-800/70 px-3 py-2 text-sm text-white outline-none focus:border-accent focus:ring-2 focus:ring-accent/50"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        disabled={disabled}
      >
        {nodeList.map(({ id, label, depth }) => (
          <option key={id} value={id}>
            {"".padStart(depth * 2, " ")}
            {label}
          </option>
        ))}
      </select>
    </label>
  );
});
