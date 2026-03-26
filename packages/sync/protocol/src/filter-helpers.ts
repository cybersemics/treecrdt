import { bytesToHex } from "@treecrdt/interface/ids";

import type { OpRef } from "./types.js";

/**
 * `children(parent)` scoped sync needs the scope root node's own latest payload-writer opRef in
 * addition to child-scope refs. That payload op may only be discoverable via the root node itself
 * when the root's parent is outside scope, so append it once and dedupe by opRef bytes.
 */
export async function loadScopedChildrenOpRefs(opts: {
  listChildRefs: () => Promise<OpRef[]>;
  loadScopeRootPayloadWriter: () => Promise<OpRef | null>;
}): Promise<OpRef[]> {
  const refs = await opts.listChildRefs();
  const payloadWriter = await opts.loadScopeRootPayloadWriter();
  if (!payloadWriter) return refs;

  const seen = new Set(refs.map(bytesToHex));
  const payloadWriterHex = bytesToHex(payloadWriter);
  if (seen.has(payloadWriterHex)) return refs;
  return [...refs, payloadWriter];
}
