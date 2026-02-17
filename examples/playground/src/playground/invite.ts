export type InvitePreset = "read" | "read_write" | "admin" | "custom";

export type InviteActions = {
  write_structure: boolean;
  write_payload: boolean;
  delete: boolean;
};
