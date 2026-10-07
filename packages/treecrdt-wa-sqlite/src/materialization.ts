import type { MaterializationEvent } from '@treecrdt/interface/engine';
import {
  addMaterializationWriteId,
  createMaterializationDispatcher,
} from '@treecrdt/interface/engine';
import type {
  ClientMaterializationDispatcher,
  ClientMaterializationDispatcherOptions,
  CrossTabMaterializationMessage,
  CrossTabMaterializationScope,
} from './types.js';

const CROSS_TAB_MATERIALIZED_MESSAGE = 'treecrdt-materialized-v1';

/** Client-side materialization fan-out with optional BroadcastChannel cross-tab delivery. */
export function createClientMaterializationDispatcher(
  opts: ClientMaterializationDispatcherOptions = {},
): ClientMaterializationDispatcher {
  const dispatcher = createMaterializationDispatcher();
  const clientId = randomClientId();
  let channel: BroadcastChannel | null = null;
  let scope: CrossTabMaterializationScope | null = null;

  const close = () => {
    channel?.close();
    channel = null;
    scope = null;
  };

  const eventForPeers = (event: MaterializationEvent): MaterializationEvent => {
    return {
      ...event,
      changes: event.changes.map((change) => {
        if (!change.source?.writeIds) return change;
        const { writeIds: _writeIds, ...source } = change.source;
        if (Object.keys(source).length > 0) return { ...change, source };
        const { source: _source, ...nextChange } = change;
        return nextChange;
      }),
    };
  };

  const broadcast = (event: MaterializationEvent) => {
    if (!channel || !scope || event.changes.length === 0) return;
    channel.postMessage({
      type: CROSS_TAB_MATERIALIZED_MESSAGE,
      sourceId: clientId,
      docId: scope.docId,
      filename: scope.filename,
      event: eventForPeers(event),
    } satisfies CrossTabMaterializationMessage);
  };

  const emitEvent = (event: MaterializationEvent) => {
    if (event.changes.length === 0) return;
    dispatcher.emitEvent(event);
    opts.broadcast?.(eventForPeers(event));
    broadcast(event);
  };

  const emitOutcome: ClientMaterializationDispatcher['emitOutcome'] = (outcome, writeId) => {
    if (outcome.changes.length === 0) return;
    emitEvent(addMaterializationWriteId(outcome, writeId));
  };

  const enableCrossTab = (nextScope: CrossTabMaterializationScope) => {
    if (typeof BroadcastChannel === 'undefined') return;
    close();
    scope = nextScope;
    channel = new BroadcastChannel(materializationChannelName(nextScope));
    channel.onmessage = (ev: MessageEvent<CrossTabMaterializationMessage>) => {
      const msg = ev.data;
      if (!msg || msg.type !== CROSS_TAB_MATERIALIZED_MESSAGE) return;
      if (msg.sourceId === clientId) return;
      if (msg.docId !== nextScope.docId || msg.filename !== nextScope.filename) return;
      dispatcher.emitEvent(msg.event);
    };
  };

  return {
    emitEvent,
    emitOutcome,
    emitIncomingEvent: dispatcher.emitEvent,
    onMaterialized: dispatcher.onMaterialized,
    enableCrossTab,
    close,
  };
}

function materializationChannelName(scope: CrossTabMaterializationScope): string {
  return `${CROSS_TAB_MATERIALIZED_MESSAGE}:${scope.docId}:${scope.filename}`;
}

function randomClientId(): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID();
  }
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}
