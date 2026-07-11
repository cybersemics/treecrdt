import { isDeepStrictEqual } from 'node:util';

import type { Operation } from '@treecrdt/interface';
import { type LocalWriteAuthProof, type LocalWriteAuthSession } from '@treecrdt/interface/engine';

const POSTGRES_LOCAL_AUTH_MAX_ATTEMPTS = 4;

type OptimisticLocalProposal<T> = {
  operation: Operation;
  /** `null` is the only retryable conflict signal; thrown failures are terminal. */
  commit: (proof: LocalWriteAuthProof) => { operation: Operation; value: T } | null;
};

type OptimisticLocalWriteOptions<T> = {
  authSession: LocalWriteAuthSession;
  prepare: () => OptimisticLocalProposal<T> | null;
  onCommitted: (value: T) => void;
  maxAttempts?: number;
};

function assertExactAuthOperation(expected: Operation, actual: readonly Operation[]): void {
  let unchanged = false;
  try {
    unchanged = actual.length === 1 && isDeepStrictEqual(actual[0], expected);
  } catch {
    // Treat hostile proxies and other comparison failures as mutation.
  }
  if (!unchanged) {
    throw new Error('treecrdt: local authorization mutated the proposed operation');
  }
}

/**
 * Authorize and optimistically commit one exact PostgreSQL-backed local operation.
 *
 * Only explicit `null` prepare/commit results are retried. Policy, verifier, database, codec, proof,
 * and listener failures remain terminal, so callers cannot accidentally remint after a real error.
 */
export async function commitOptimisticAuthorizedLocalWrite<T>(
  options: OptimisticLocalWriteOptions<T>,
): Promise<Operation> {
  const maxAttempts = options.maxAttempts ?? POSTGRES_LOCAL_AUTH_MAX_ATTEMPTS;
  const waitForConflict = (attempt: number) =>
    attempt < maxAttempts
      ? new Promise<void>((resolve) => setTimeout(resolve, attempt - 1))
      : Promise.resolve();
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const proposal = options.prepare();
    if (!proposal) {
      await waitForConflict(attempt);
      continue;
    }

    // Keep the Rust-backed proposal private. Auth hooks receive an isolated clone while another
    // clone remains untouched for exact structural and typed-array byte comparisons.
    const expectedOperation = structuredClone(proposal.operation);
    const authOperations = [structuredClone(expectedOperation)];
    const proofs = await options.authSession.authorizeLocalOps(authOperations);
    assertExactAuthOperation(expectedOperation, authOperations);
    if (!Array.isArray(proofs) || proofs.length !== 1) {
      throw new Error('authorizeLocalOps must return exactly one proof for one operation');
    }
    const proof = proofs[0];
    if (!proof) {
      throw new Error('authorizeLocalOps returned an invalid operation proof');
    }
    const { sig, proofRef } = proof;
    if (
      !(sig instanceof Uint8Array) ||
      sig.length !== 64 ||
      !(proofRef instanceof Uint8Array) ||
      proofRef.length !== 16
    ) {
      throw new Error('authorizeLocalOps returned an invalid operation proof');
    }
    const proofSnapshot = {
      sig: Uint8Array.from(sig),
      proofRef: Uint8Array.from(proofRef),
    };
    const committed = proposal.commit(proofSnapshot);
    if (!committed) {
      await waitForConflict(attempt);
      continue;
    }

    options.onCommitted(committed.value);
    return committed.operation;
  }

  throw new Error(
    `treecrdt: local authorization could not commit after ${maxAttempts} attempts because PostgreSQL state kept changing`,
  );
}
