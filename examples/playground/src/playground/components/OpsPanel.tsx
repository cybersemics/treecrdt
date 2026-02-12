import React from "react";
import type { Operation } from "@treecrdt/interface";
import { bytesToHex } from "@treecrdt/interface/ids";
import { deriveKeyIdV1 } from "@treecrdt/auth";
import type { Virtualizer } from "@tanstack/react-virtual";

import { renderKind } from "../ops";

type IdentityInfo = { identityPk: Uint8Array; devicePk: Uint8Array };

export function OpsPanel({
  ops,
  headLamport,
  authEnabled,
  localReplicaHex,
  getIdentityByReplicaHex,
  opsParentRef,
  opsVirtualizer,
}: {
  ops: Operation[];
  headLamport: number;
  authEnabled: boolean;
  localReplicaHex: string | null;
  getIdentityByReplicaHex: (replicaHex: string) => IdentityInfo | undefined;
  opsParentRef: React.RefObject<HTMLDivElement>;
  opsVirtualizer: Virtualizer<HTMLDivElement, Element>;
}) {
  return (
    <aside className="space-y-3 rounded-2xl bg-slate-900/60 p-5 shadow-lg shadow-black/20 ring-1 ring-slate-800/60">
      <div className="text-sm font-semibold uppercase tracking-wide text-slate-400">Operations</div>
      <div className="flex items-center justify-between text-xs text-slate-400">
        <span>Ops: {ops.length}</span>
        <span>Head lamport: {headLamport}</span>
      </div>
      <div className="rounded-xl border border-slate-800 bg-slate-950/60 p-3 shadow-inner shadow-black/30">
        <div ref={opsParentRef} className="max-h-[520px] overflow-auto pr-2 text-xs">
          {ops.length === 0 && <div className="text-slate-500">No operations yet.</div>}
          {ops.length > 0 && (
            <div style={{ height: `${opsVirtualizer.getTotalSize()}px`, position: "relative" }} className="w-full">
              {opsVirtualizer.getVirtualItems().map((item) => {
                const op = ops[item.index];
                if (!op) return null;
                const signerHex = bytesToHex(op.meta.id.replica);
                const signerShort =
                  signerHex.length > 24 ? `${signerHex.slice(0, 12)}…${signerHex.slice(-8)}` : signerHex;
                const signerKeyIdHex = bytesToHex(deriveKeyIdV1(op.meta.id.replica));
                const signerKeyIdShort =
                  signerKeyIdHex.length > 16
                    ? `${signerKeyIdHex.slice(0, 8)}…${signerKeyIdHex.slice(-4)}`
                    : signerKeyIdHex;
                const identity = getIdentityByReplicaHex(signerHex);
                const identityKeyIdHex = identity ? bytesToHex(deriveKeyIdV1(identity.identityPk)) : null;
                const identityKeyIdShort =
                  identityKeyIdHex && identityKeyIdHex.length > 16
                    ? `${identityKeyIdHex.slice(0, 8)}…${identityKeyIdHex.slice(-4)}`
                    : identityKeyIdHex;
                const identityPkHex = identity ? bytesToHex(identity.identityPk) : null;
                const isLocalSigner = localReplicaHex ? signerHex === localReplicaHex : false;

                return (
                  <div
                    key={item.key}
                    data-index={item.index}
                    ref={opsVirtualizer.measureElement}
                    className="absolute left-0 top-0 w-full"
                    style={{ transform: `translateY(${item.start}px)` }}
                  >
                    <div className="mb-2 rounded-lg border border-slate-800/80 bg-slate-900/60 px-3 py-2 text-slate-100">
                      <div className="flex items-center justify-between">
                        <span className="font-semibold text-accent">{op.kind.type}</span>
                        <div className="flex items-center gap-2">
                          {authEnabled ? (
                            <span
                              className="rounded-full border border-emerald-400/40 bg-emerald-500/10 px-2 py-0.5 text-[10px] font-semibold text-emerald-100"
                              title={
                                isLocalSigner
                                  ? "Auth enabled: this op will be signed when syncing"
                                  : "Auth enabled: this op was verified before apply"
                              }
                            >
                              signed
                            </span>
                          ) : (
                            <span
                              className="rounded-full border border-slate-700 bg-slate-800/60 px-2 py-0.5 text-[10px] font-semibold text-slate-300"
                              title="Auth disabled: ops are not required to carry signatures/capabilities"
                            >
                              unsigned
                            </span>
                          )}
                          <span className="font-mono text-slate-400">lamport {op.meta.lamport}</span>
                        </div>
                      </div>
                      <div className="mt-1 text-slate-300">{renderKind(op.kind)}</div>
                      <div className="mt-1 flex flex-wrap items-center justify-between gap-2 text-[10px] text-slate-500">
                        <span className="font-mono">counter {op.meta.id.counter}</span>
                        <span className="font-mono" title={signerHex}>
                          signer {signerShort}
                          {isLocalSigner ? " (local)" : ""}
                        </span>
                      </div>
                      <div className="mt-0.5 text-[10px] text-slate-500">
                        <span className="font-mono" title={signerKeyIdHex}>
                          keyId {signerKeyIdShort}
                        </span>
                      </div>
                      {identity && identityKeyIdHex && (
                        <div className="mt-0.5 text-[10px] text-slate-500">
                          <span className="font-mono" title={identityPkHex ?? ""}>
                            identity {identityKeyIdShort}
                          </span>
                        </div>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </div>
    </aside>
  );
}
