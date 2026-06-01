import type { SyncOnceOptions, SyncSubscribeOptions } from '@treecrdt/sync-protocol';

export const DEFAULT_MAX_OPS_PER_BATCH = 500;

/** Default batching limits for one-shot `syncOnce` and live `subscribe`. */
export const DEFAULT_SYNC_ONCE: Required<
  Pick<SyncOnceOptions, 'maxCodewords' | 'maxOpsPerBatch' | 'codewordsPerMessage'>
> = {
  maxCodewords: 2_000_000,
  maxOpsPerBatch: DEFAULT_MAX_OPS_PER_BATCH,
  codewordsPerMessage: 2_048,
};

export const DEFAULT_LIVE_SUBSCRIBE: Required<
  Pick<
    SyncSubscribeOptions,
    'maxCodewords' | 'maxOpsPerBatch' | 'codewordsPerMessage' | 'intervalMs' | 'immediate'
  >
> = {
  maxCodewords: 2_000_000,
  maxOpsPerBatch: DEFAULT_MAX_OPS_PER_BATCH,
  codewordsPerMessage: 1_024,
  intervalMs: 0,
  immediate: true,
};
