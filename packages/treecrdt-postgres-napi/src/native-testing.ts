import { loadNative, type NativeOp } from './native.js';

export type NativeTestingFactory = {
  resetForTests(): void;
  resetDocForTests(docId: string): void;
  cloneDocForTests(sourceDocId: string, targetDocId: string): void;
  cloneMaterializedDocForTests(sourceDocId: string, targetDocId: string): void;
  primeDocForTests(docId: string, ops: NativeOp[]): void;
  primeBalancedFanoutDocForTests(
    docId: string,
    size: number,
    fanout: number,
    payloadBytes: number,
    replicaLabel: string,
  ): void;
};

type NativeTestingExports = {
  PgTestingFactory: new (url: string) => NativeTestingFactory;
};

export function loadTestingNative(): NativeTestingExports {
  return loadNative() as unknown as NativeTestingExports;
}
