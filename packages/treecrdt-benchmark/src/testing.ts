import type { Operation } from '@treecrdt/interface';

// Benchmark backends can expose these optional helpers to speed up fixture setup
// without putting bench-specific APIs on their main runtime entrypoints.
export type BenchmarkFixtureHelpers = {
  resetForTests: () => Promise<void>;
  resetDocForTests: (docId: string) => Promise<void>;
  cloneDocForTests: (sourceDocId: string, targetDocId: string) => Promise<void>;
  cloneMaterializedDocForTests: (sourceDocId: string, targetDocId: string) => Promise<void>;
  primeDocForTests: (docId: string, ops: Operation[]) => Promise<void>;
  primeBalancedFanoutDocForTests: (
    docId: string,
    size: number,
    fanout: number,
    payloadBytes: number,
    replicaLabel: string,
  ) => Promise<void>;
};

export type BenchmarkFixtureFactory<TBackend> = {
  open: (docId: string) => Promise<TBackend>;
} & Partial<BenchmarkFixtureHelpers>;
