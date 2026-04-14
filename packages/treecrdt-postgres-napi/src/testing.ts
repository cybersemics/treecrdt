import type { BenchmarkFixtureHelpers } from '@treecrdt/benchmark/testing';
import type { Operation } from '@treecrdt/interface';
import { nodeIdToBytes16, replicaIdToBytes } from '@treecrdt/interface/ids';

import { createPostgresNapiAdapterFactory, type PostgresNapiAdapterFactory } from './adapter.js';
import { createTreecrdtPostgresClient } from './client.js';
import { operationToNativeWithSerializers } from './codec.js';
import {
  createPostgresNapiSyncBackendFactory,
  type PostgresNapiSyncBackendFactory,
} from './index.js';
import { loadTestingNative, type NativeTestingFactory } from './native-testing.js';

export { createTreecrdtPostgresClient } from './client.js';

type PostgresNapiFixtureHelpers = Pick<
  BenchmarkFixtureHelpers,
  | 'resetForTests'
  | 'resetDocForTests'
  | 'cloneDocForTests'
  | 'cloneMaterializedDocForTests'
  | 'primeDocForTests'
  | 'primeBalancedFanoutDocForTests'
>;

type PostgresNapiSyncFixtureHelpers = Pick<
  BenchmarkFixtureHelpers,
  'resetForTests' | 'resetDocForTests' | 'cloneDocForTests' | 'primeBalancedFanoutDocForTests'
>;

export type PostgresNapiTestAdapterFactory = PostgresNapiAdapterFactory &
  PostgresNapiFixtureHelpers;

export type PostgresNapiTestSyncBackendFactory = PostgresNapiSyncBackendFactory &
  PostgresNapiSyncFixtureHelpers;

function ensureNonEmptyString(name: string, value: string): void {
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`${name} must be a non-empty string`);
  }
}

function ensurePositiveInteger(name: string, value: number): void {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer, got ${String(value)}`);
  }
}

function ensureNonNegativeInteger(name: string, value: number): void {
  if (!Number.isInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer, got ${String(value)}`);
  }
}

function opToNative(op: Operation) {
  return operationToNativeWithSerializers(op, nodeIdToBytes16, replicaIdToBytes);
}

function createTestingFactory(url: string): NativeTestingFactory {
  const native = loadTestingNative();
  return new native.PgTestingFactory(url);
}

export function createPostgresNapiTestAdapterFactory(url: string): PostgresNapiTestAdapterFactory {
  ensureNonEmptyString('url', url);
  const base = createPostgresNapiAdapterFactory(url);
  const factory = createTestingFactory(url);

  return {
    ...base,
    resetForTests: async () => factory.resetForTests(),
    resetDocForTests: async (docId: string) => {
      ensureNonEmptyString('docId', docId);
      factory.resetDocForTests(docId);
    },
    cloneDocForTests: async (sourceDocId: string, targetDocId: string) => {
      ensureNonEmptyString('sourceDocId', sourceDocId);
      ensureNonEmptyString('targetDocId', targetDocId);
      factory.cloneDocForTests(sourceDocId, targetDocId);
    },
    cloneMaterializedDocForTests: async (sourceDocId: string, targetDocId: string) => {
      ensureNonEmptyString('sourceDocId', sourceDocId);
      ensureNonEmptyString('targetDocId', targetDocId);
      factory.cloneMaterializedDocForTests(sourceDocId, targetDocId);
    },
    primeDocForTests: async (docId: string, ops: Operation[]) => {
      ensureNonEmptyString('docId', docId);
      factory.primeDocForTests(docId, ops.map(opToNative));
    },
    primeBalancedFanoutDocForTests: async (
      docId: string,
      size: number,
      fanout: number,
      payloadBytes: number,
      replicaLabel: string,
    ) => {
      ensureNonEmptyString('docId', docId);
      ensureNonEmptyString('replicaLabel', replicaLabel);
      ensurePositiveInteger('size', size);
      ensurePositiveInteger('fanout', fanout);
      ensureNonNegativeInteger('payloadBytes', payloadBytes);
      factory.primeBalancedFanoutDocForTests(docId, size, fanout, payloadBytes, replicaLabel);
    },
  };
}

export function createPostgresNapiTestSyncBackendFactory(
  url: string,
): PostgresNapiTestSyncBackendFactory {
  ensureNonEmptyString('url', url);
  const base = createPostgresNapiSyncBackendFactory(url);
  const factory = createTestingFactory(url);

  return {
    ...base,
    resetForTests: async () => factory.resetForTests(),
    resetDocForTests: async (docId: string) => {
      ensureNonEmptyString('docId', docId);
      factory.resetDocForTests(docId);
    },
    cloneDocForTests: async (sourceDocId: string, targetDocId: string) => {
      ensureNonEmptyString('sourceDocId', sourceDocId);
      ensureNonEmptyString('targetDocId', targetDocId);
      factory.cloneDocForTests(sourceDocId, targetDocId);
    },
    primeBalancedFanoutDocForTests: async (
      docId: string,
      size: number,
      fanout: number,
      payloadBytes: number,
      replicaLabel: string,
    ) => {
      ensureNonEmptyString('docId', docId);
      ensureNonEmptyString('replicaLabel', replicaLabel);
      ensurePositiveInteger('size', size);
      ensurePositiveInteger('fanout', fanout);
      ensureNonNegativeInteger('payloadBytes', payloadBytes);
      factory.primeBalancedFanoutDocForTests(docId, size, fanout, payloadBytes, replicaLabel);
    },
  };
}
