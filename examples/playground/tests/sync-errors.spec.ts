import { expect, test } from "@playwright/test";
import { InboundSyncAggregateError } from "@treecrdt/sync";

import {
  formatSyncError,
  inboundSyncPeerIdsToDrop,
  isCapabilityRevokedError,
} from "../src/playground/syncErrorHelpers.js";

function inboundAggregate(
  failures: readonly { peerId: string; error: unknown }[],
): InboundSyncAggregateError {
  return new InboundSyncAggregateError(
    failures.map((failure) => ({ ...failure, filter: { all: {} } })),
  );
}

test("drops only non-revoked peers from an inbound aggregate", () => {
  const error = inboundAggregate([
    { peerId: "peer-offline", error: new Error("transport closed") },
    { peerId: "peer-revoked", error: new Error("capability token revoked") },
    { peerId: "peer-offline", error: new Error("retry also failed") },
  ]);

  expect(inboundSyncPeerIdsToDrop(error)).toEqual(["peer-offline"]);
  expect(isCapabilityRevokedError(error)).toBe(true);
  expect(formatSyncError(error)).toBe(
    "Access revoked for this capability. Import/update access, then sync again.",
  );
});

test("preserves the underlying single-target aggregate error", () => {
  const error = inboundAggregate([{ peerId: "peer-a", error: new Error("transport closed") }]);

  expect(formatSyncError(error)).toBe("transport closed");
  expect(inboundSyncPeerIdsToDrop(error)).toEqual(["peer-a"]);
});

test("applies fallback peer handling only to non-revoked direct errors", () => {
  expect(inboundSyncPeerIdsToDrop(new Error("offline"), ["peer-a"])).toEqual(["peer-a"]);
  expect(inboundSyncPeerIdsToDrop(new Error("capability token revoked"), ["peer-a"])).toEqual([]);
});
