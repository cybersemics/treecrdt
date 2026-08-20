import {
  createStringStoreRouteCache,
  isDiscoveryBootstrapUrl,
  normalizeDirectSyncWebSocketUrl,
  type DiscoveryRouteCache,
} from '@treecrdt/discovery';

import {
  PLAYGROUND_PEER_TIMEOUT_MS,
  PLAYGROUND_REMOTE_SYNC_TIMEOUT_MS,
  PLAYGROUND_SYNC_MAX_CODEWORDS,
  PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
} from './constants';

const REMOTE_SYNC_CODEWORDS_PER_MESSAGE = 512;

export function isCurrentSyncGeneration(
  activeGeneration: number | null,
  expectedGeneration: number,
): boolean {
  return activeGeneration === expectedGeneration;
}

export function isCurrentConnection<T>(
  connections: ReadonlyMap<string, T>,
  peerId: string,
  expectedConnection: T,
): boolean {
  return connections.has(peerId) && connections.get(peerId) === expectedConnection;
}

export function areCurrentConnections<T>(
  connections: ReadonlyMap<string, T>,
  expectedConnections: ReadonlyMap<string, T>,
): boolean {
  for (const [peerId, connection] of expectedConnections) {
    if (!isCurrentConnection(connections, peerId, connection)) return false;
  }
  return true;
}

export function deleteCurrentConnection<T>(
  connections: Map<string, T>,
  peerId: string,
  expectedConnection: T,
): boolean {
  if (!isCurrentConnection(connections, peerId, expectedConnection)) return false;
  return connections.delete(peerId);
}

export function runConnectionCleanup(layers: {
  deleteCurrent: () => void;
  unregisterInbound: () => void;
  unsetOutbound: () => void;
  detachPeer: () => void;
}): void {
  try {
    layers.deleteCurrent();
  } finally {
    try {
      layers.unregisterInbound();
    } finally {
      try {
        layers.unsetOutbound();
      } finally {
        layers.detachPeer();
      }
    }
  }
}

export function normalizeSyncServerUrl(raw: string, docId: string): URL {
  return normalizeDirectSyncWebSocketUrl(raw, docId);
}

let browserDiscoveryRouteCache: DiscoveryRouteCache | null | undefined;

export function getBrowserDiscoveryRouteCache(): DiscoveryRouteCache | undefined {
  if (typeof window === 'undefined' || typeof window.localStorage === 'undefined') return undefined;
  if (browserDiscoveryRouteCache === undefined) {
    browserDiscoveryRouteCache = createStringStoreRouteCache(
      window.localStorage,
      'treecrdt.playground.discovery.',
    );
  }
  return browserDiscoveryRouteCache ?? undefined;
}

export function previewDiscoveryHost(raw: string): string {
  let input = raw.trim();
  if (input.length === 0) throw new Error('Sync server URL is empty');
  if (!/^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(input)) input = `https://${input}`;
  const url = new URL(input);
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error('Discovery endpoint must use http:// or https://');
  }
  return url.host;
}

export function isTransientRemoteConnectError(message: string | null): boolean {
  if (!message) return false;
  return (
    message === 'Failed to fetch' ||
    message === 'Load failed' ||
    message === 'Network request failed' ||
    message.startsWith('Remote sync socket error (')
  );
}

export function formatRemoteRouteDetail(
  host: string,
  opts: {
    bootstrapHost?: string;
  } = {},
): string {
  const base = `Connected to ${host}`;
  if (!opts.bootstrapHost || opts.bootstrapHost === host) return base;
  return `${base} via ${opts.bootstrapHost}`;
}

export function formatRemoteConnectDetail(
  verb: string,
  host: string,
  bootstrapHost?: string,
): string {
  if (!bootstrapHost || bootstrapHost === host) {
    return `${verb} ${host}...`;
  }
  return `${verb} ${host} via ${bootstrapHost}...`;
}

export function formatRemoteErrorDetail(
  kind: 'disconnected' | 'could_not_connect' | 'connection_error' | 'could_not_reach',
  host: string,
  bootstrapHost?: string,
): string {
  const base =
    kind === 'disconnected'
      ? `Disconnected from ${host}`
      : kind === 'could_not_connect'
        ? `Could not connect to ${host}`
        : kind === 'connection_error'
          ? `Connection error talking to ${host}`
          : `Could not reach ${host}`;
  if (!bootstrapHost || bootstrapHost === host) return base;
  return `${base} via ${bootstrapHost}`;
}

export function isRemotePeerId(peerId: string): boolean {
  return peerId.startsWith('remote:');
}

export function syncOnceOptionsForPeer(peerId: string, localCodewordsPerMessage: number) {
  return {
    maxCodewords: PLAYGROUND_SYNC_MAX_CODEWORDS,
    maxOpsPerBatch: PLAYGROUND_SYNC_MAX_OPS_PER_BATCH,
    codewordsPerMessage: isRemotePeerId(peerId)
      ? REMOTE_SYNC_CODEWORDS_PER_MESSAGE
      : localCodewordsPerMessage,
  };
}

export function syncTimeoutMsForPeer(
  peerId: string,
  opts: { autoSync?: boolean; multipleTargets?: boolean } = {},
) {
  if (isRemotePeerId(peerId)) return PLAYGROUND_REMOTE_SYNC_TIMEOUT_MS;
  if (opts.autoSync) return PLAYGROUND_PEER_TIMEOUT_MS;
  return opts.multipleTargets ? 8_000 : 15_000;
}

export { isDiscoveryBootstrapUrl };
export {
  errorMessage,
  formatSyncError,
  inboundSyncPeerIdsToDrop,
  isCapabilityRevokedError,
} from './syncErrorHelpers';
