import type { Capability } from "./types.js";

export const AUTH_CAPABILITY_NAME = "auth.capability";
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
