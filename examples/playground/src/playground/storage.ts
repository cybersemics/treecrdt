export function getPlaygroundProfileId(): string | null {
  if (typeof window === "undefined") return null;
  const raw = new URLSearchParams(window.location.search).get("profile");
  const trimmed = raw?.trim();
  if (!trimmed) return null;
  const safe = trimmed.replace(/[^a-zA-Z0-9._-]/g, "");
  return safe.length > 0 ? safe : null;
}

export function prefixPlaygroundStorageKey(key: string): string {
  const profile = getPlaygroundProfileId();
  if (!profile) return key;
  return `treecrdt-playground-profile:${profile}:${key}`;
}

