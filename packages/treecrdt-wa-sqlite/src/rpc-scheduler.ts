import type { RpcCall, RpcCallOptions } from './types.js';

export type RpcSchedulePriority = NonNullable<RpcCallOptions['priority']> | 'normal';

type ScheduledJob = {
  priority: RpcSchedulePriority;
  run: () => Promise<unknown>;
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
};

// Bound foreground bypasses so a steady read stream cannot starve sync forever.
const MAX_FOREGROUND_BURST = 8;

/** Serializes RPC work while allowing reads to bypass only explicitly-background work. */
export function createRpcScheduler() {
  const queue: ScheduledJob[] = [];
  let running = false;
  let foregroundBurst = 0;

  const nextJobIndex = (): number => {
    // A normal call is an ordering barrier: a later read must not jump an earlier local write.
    const normalBarrier = queue.findIndex((job) => job.priority === 'normal');
    const foreground = queue.findIndex(
      (job, index) =>
        job.priority === 'foreground' && (normalBarrier === -1 || index < normalBarrier),
    );
    if (
      foreground !== -1 &&
      (foregroundBurst < MAX_FOREGROUND_BURST || queue[0]?.priority === 'foreground')
    ) {
      return foreground;
    }
    return 0;
  };

  const drain = () => {
    if (running || queue.length === 0) return;
    const [job] = queue.splice(nextJobIndex(), 1);
    if (!job) return;
    running = true;
    foregroundBurst = job.priority === 'foreground' ? foregroundBurst + 1 : 0;
    void Promise.resolve()
      .then(job.run)
      .then(job.resolve, job.reject)
      .finally(() => {
        running = false;
        drain();
      });
  };

  return <T>(priority: RpcSchedulePriority, run: () => Promise<T>): Promise<T> =>
    new Promise<T>((resolve, reject) => {
      queue.push({
        priority,
        run,
        resolve: resolve as (value: unknown) => void,
        reject,
      });
      drain();
    });
}

/** Applies priority scheduling before a dedicated worker request is posted. */
export function createPrioritizedRpcCall(runRaw: RpcCall): RpcCall {
  const schedule = createRpcScheduler();

  return (method, params, options) =>
    schedule(options?.priority ?? 'normal', () => runRaw(method, params));
}
