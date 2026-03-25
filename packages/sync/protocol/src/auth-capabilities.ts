import type { Capability } from "./types.js";

// The live auth token a peer is presenting as its own session authority.
export const AUTH_CAPABILITY_NAME = "auth.capability";
// Forwarded capability material used to verify proof_ref values on replayed ops.
// This is cache/verification data, not an active auth grant for the advertising peer.
export const AUTH_REPLAY_CAPABILITY_NAME = "auth.capability.replay";

export function isAuthCapability(cap: Capability): boolean {
  return cap.name === AUTH_CAPABILITY_NAME;
}

export function isReplayAuthCapability(cap: Capability): boolean {
  return cap.name === AUTH_REPLAY_CAPABILITY_NAME;
}

export function isAnyAuthCapability(cap: Capability): boolean {
  return isAuthCapability(cap) || isReplayAuthCapability(cap);
}
