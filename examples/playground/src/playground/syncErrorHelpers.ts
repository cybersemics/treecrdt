type InboundSyncFailureLike = {
  peerId: string;
  error: unknown;
};

function inboundSyncFailures(error: unknown): readonly InboundSyncFailureLike[] | undefined {
  if (!(error instanceof Error) || error.name !== 'InboundSyncAggregateError') return undefined;
  const failures = (error as Error & { failures?: unknown }).failures;
  if (!Array.isArray(failures)) return undefined;

  return failures.filter(
    (failure): failure is InboundSyncFailureLike =>
      typeof failure === 'object' &&
      failure !== null &&
      typeof (failure as { peerId?: unknown }).peerId === 'string' &&
      'error' in failure,
  );
}

function syncErrorCauses(error: unknown): readonly unknown[] {
  if (!(error instanceof Error)) return [error];
  const errors = (error as Error & { errors?: unknown }).errors;
  return Array.isArray(errors) ? errors : [error];
}

export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

export function isCapabilityRevokedError(error: unknown): boolean {
  return syncErrorCauses(error).some((cause) =>
    /capability token revoked/i.test(errorMessage(cause)),
  );
}

export function formatSyncError(error: unknown): string {
  const causes = syncErrorCauses(error);
  if (causes.some(isCapabilityRevokedError)) {
    return 'Access revoked for this capability. Import/update access, then sync again.';
  }
  if (causes.some((cause) => /unknown author:/i.test(errorMessage(cause)))) {
    return 'This document contains ops from an author whose capability token is not available here yet. Sync from a peer that has the full author history, or try a fresh doc.';
  }

  const messages = Array.from(new Set(causes.map(errorMessage)));
  if (messages.length === 1) return messages[0]!;
  return `${errorMessage(error)}: ${messages.join('; ')}`;
}

export function inboundSyncPeerIdsToDrop(
  error: unknown,
  fallbackPeerIds: readonly string[] = [],
): readonly string[] {
  const failures = inboundSyncFailures(error);
  if (!failures) {
    return isCapabilityRevokedError(error) ? [] : Array.from(new Set(fallbackPeerIds));
  }

  const peerIds = new Set<string>();
  for (const failure of failures) {
    if (!isCapabilityRevokedError(failure.error)) peerIds.add(failure.peerId);
  }
  return Array.from(peerIds);
}
