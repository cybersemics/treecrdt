import { createTreecrdtClient } from "@treecrdt/wa-sqlite/client";
import { detectOpfsSupport, opfsStorageExists } from "@treecrdt/wa-sqlite/opfs";
import { makeOp, nodeIdFromInt } from "@treecrdt/benchmark";

function orderKeyFromPosition(position: number): Uint8Array {
  if (!Number.isInteger(position) || position < 0) throw new Error(`invalid position: ${position}`);
  const n = position + 1;
  if (n > 0xffff) throw new Error(`position too large for u16 order key: ${position}`);
  const bytes = new Uint8Array(2);
  new DataView(bytes.buffer).setUint16(0, n, false);
  return bytes;
}

function replicaFromLabel(label: string): Uint8Array {
  const encoded = new TextEncoder().encode(label);
  if (encoded.length === 0) throw new Error("replica label must not be empty");
  const out = new Uint8Array(32);
  for (let i = 0; i < out.length; i += 1) out[i] = encoded[i % encoded.length]!;
  return out;
}

export async function runDropStorageE2E(): Promise<
  { ok: true } | { ok: false; error: string }
> {
  const support = detectOpfsSupport();
  if (!support.available) {
    return { ok: false, error: `OPFS unavailable: ${support.reason ?? "unknown"}` };
  }

  const baseUrl =
    typeof window !== "undefined"
      ? new URL(".", window.location.href).href
      : undefined;

  const filename = `/drop-test-${crypto.randomUUID()}.db`;
  const client = await createTreecrdtClient({
    storage: "opfs",
    filename,
    preferWorker: true,
    baseUrl,
  });

  try {
    const root = "0".repeat(32);
    const replica = replicaFromLabel("drop-test");
    const op = makeOp(replica, 1, 1, {
      type: "insert",
      parent: root,
      node: nodeIdFromInt(1),
      orderKey: orderKeyFromPosition(0),
    });
    await client.ops.append(op);

    const existsBefore = await opfsStorageExists(filename);
    if (!existsBefore) {
      return { ok: false, error: "OPFS storage should exist after append" };
    }

    await client.drop();
    const existsAfter = await opfsStorageExists(filename);
    if (existsAfter) {
      return { ok: false, error: "OPFS storage should be fully deleted after drop" };
    }

    return { ok: true };
  } finally {
    await client.drop().catch(() => {});
  }
}

declare global {
  interface Window {
    __runDropStorageE2E?: typeof runDropStorageE2E;
  }
}

if (typeof window !== "undefined") {
  window.__runDropStorageE2E = runDropStorageE2E;
}
