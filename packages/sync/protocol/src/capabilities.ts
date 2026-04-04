import type { Capability } from './types.js';

export const DIRECT_SEND_SMALL_SCOPE_SUPPORT_CAPABILITY =
  'treecrdt.sync.direct_send_small_scope.v1';
export const DIRECT_SEND_SMALL_SCOPE_REQUEST_CAPABILITY =
  'treecrdt.sync.direct_send_small_scope.request.v1';
export const DIRECT_SEND_SMALL_SCOPE_FILTER_CAPABILITY =
  'treecrdt.sync.direct_send_small_scope.filter.v1';
export const DIRECT_SEND_EMPTY_RECEIVER_SUPPORT_CAPABILITY =
  'treecrdt.sync.direct_send_empty_receiver.v1';
export const DIRECT_SEND_EMPTY_RECEIVER_FILTER_CAPABILITY =
  'treecrdt.sync.direct_send_empty_receiver.filter.v1';
export const DIRECT_SEND_EMPTY_RECEIVER_MAX_OPS_PER_BATCH = 5_000;
export const SERVER_INSTANCE_CAPABILITY_NAME = 'treecrdt.sync.server.instance.v1';

function pushCapabilityIfMissing(capabilities: Capability[], name: string, value: string): void {
  if (capabilities.some((capability) => capability.name === name && capability.value === value)) {
    return;
  }
  capabilities.push({ name, value });
}

export function buildInitiatorHelloCapabilities(
  baseCapabilities: readonly Capability[],
  opts: { filterId: string; localHasOps: boolean },
): Capability[] {
  const capabilities = [...baseCapabilities];
  pushCapabilityIfMissing(capabilities, DIRECT_SEND_SMALL_SCOPE_SUPPORT_CAPABILITY, '1');
  pushCapabilityIfMissing(capabilities, DIRECT_SEND_EMPTY_RECEIVER_SUPPORT_CAPABILITY, '1');
  if (!opts.localHasOps) {
    pushCapabilityIfMissing(
      capabilities,
      DIRECT_SEND_SMALL_SCOPE_REQUEST_CAPABILITY,
      opts.filterId,
    );
  }
  return capabilities;
}

export function peerSupportsDirectSendSmallScope(capabilities: readonly Capability[]): boolean {
  return capabilities.some(
    (capability) =>
      capability.name === DIRECT_SEND_SMALL_SCOPE_SUPPORT_CAPABILITY && capability.value === '1',
  );
}

export function peerSelectedDirectSendFilter(
  capabilities: readonly Capability[],
  filterId: string,
): boolean {
  return capabilities.some(
    (capability) =>
      capability.name === DIRECT_SEND_SMALL_SCOPE_FILTER_CAPABILITY &&
      capability.value === filterId,
  );
}

export function peerRequestedDirectSendFilter(
  capabilities: readonly Capability[],
  filterId: string,
): boolean {
  return capabilities.some(
    (capability) =>
      capability.name === DIRECT_SEND_SMALL_SCOPE_REQUEST_CAPABILITY &&
      capability.value === filterId,
  );
}

export function peerSupportsDirectSendEmptyReceiver(capabilities: readonly Capability[]): boolean {
  return capabilities.some(
    (capability) =>
      capability.name === DIRECT_SEND_EMPTY_RECEIVER_SUPPORT_CAPABILITY && capability.value === '1',
  );
}

export function peerSelectedDirectSendEmptyReceiverFilter(
  capabilities: readonly Capability[],
  filterId: string,
): boolean {
  return capabilities.some(
    (capability) =>
      capability.name === DIRECT_SEND_EMPTY_RECEIVER_FILTER_CAPABILITY &&
      capability.value === filterId,
  );
}

export function capabilitySetFingerprint(capabilities: readonly Capability[]): string {
  return capabilities
    .map((capability) => `${capability.name}\u0000${capability.value}`)
    .sort()
    .join('\u0001');
}
