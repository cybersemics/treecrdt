import type { PayloadRecord } from "./types";

export function compareOpMeta(
  a: Pick<PayloadRecord, "lamport" | "replica" | "counter">,
  b: Pick<PayloadRecord, "lamport" | "replica" | "counter">
): number {
  return a.lamport - b.lamport || a.replica.localeCompare(b.replica) || a.counter - b.counter;
}

