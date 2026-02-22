import type { ComponentType } from "react";
import { MdAccountTree, MdDeleteOutline, MdEdit, MdShare, MdVisibility } from "react-icons/md";

export type CapabilityAction = "read" | "write_structure" | "write_payload" | "delete" | "grant";

export const CAPABILITY_ACTION_ORDER: CapabilityAction[] = [
  "read",
  "write_structure",
  "write_payload",
  "delete",
  "grant",
];

export const DEFAULT_MEMBER_CAPABILITY_ACTIONS: CapabilityAction[] = [
  "read",
  "write_structure",
  "write_payload",
  "grant",
];

export const CAPABILITY_META: Record<
  CapabilityAction,
  { label: string; icon: ComponentType<{ className?: string }> }
> = {
  read: { label: "Read", icon: MdVisibility },
  write_structure: { label: "Write structure", icon: MdAccountTree },
  write_payload: { label: "Write payload", icon: MdEdit },
  delete: { label: "Delete", icon: MdDeleteOutline },
  grant: { label: "Grant", icon: MdShare },
};

export function normalizeCapabilityActions(actions: readonly CapabilityAction[]): CapabilityAction[] {
  const set = new Set(actions);
  const hasNonRead = CAPABILITY_ACTION_ORDER.some((action) => action !== "read" && set.has(action));

  // Read is a prerequisite for all other capabilities in the playground UI model.
  if (hasNonRead) set.add("read");
  if (!set.has("read")) {
    for (const action of CAPABILITY_ACTION_ORDER) {
      if (action === "read") continue;
      set.delete(action);
    }
  }

  return CAPABILITY_ACTION_ORDER.filter((name) => set.has(name));
}

export function capabilityActionsFromGrantActions(actions: readonly string[]): CapabilityAction[] {
  const actionSet = new Set(actions.map((action) => String(action)));
  const out: CapabilityAction[] = [];
  if (actionSet.has("read_structure") || actionSet.has("read_payload")) out.push("read");
  if (actionSet.has("write_structure")) out.push("write_structure");
  if (actionSet.has("write_payload")) out.push("write_payload");
  if (actionSet.has("delete")) out.push("delete");
  if (actionSet.has("grant")) out.push("grant");
  return normalizeCapabilityActions(out);
}

export function grantActionsFromCapabilityActions(actions: readonly CapabilityAction[]): string[] {
  const normalized = normalizeCapabilityActions(actions);
  if (!normalized.includes("read")) return [];

  const out = new Set<string>(["read_structure", "read_payload"]);
  if (normalized.includes("write_structure")) out.add("write_structure");
  if (normalized.includes("write_payload")) out.add("write_payload");
  if (normalized.includes("delete")) out.add("delete");
  if (normalized.includes("grant")) out.add("grant");
  return Array.from(out.values());
}

export function toggleCapabilityAction(
  actions: readonly CapabilityAction[],
  action: CapabilityAction
): CapabilityAction[] {
  const set = new Set(actions);
  if (action === "read") {
    if (set.has("read")) return [];
    set.add("read");
    return normalizeCapabilityActions(Array.from(set.values()));
  }
  if (set.has(action)) set.delete(action);
  else set.add(action);
  return normalizeCapabilityActions(Array.from(set.values()));
}
