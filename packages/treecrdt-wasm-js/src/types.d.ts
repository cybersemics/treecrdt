declare module "../pkg/treecrdt_wasm.js" {
  export class WasmTree {
    constructor(replica_hex: string);
    appendOp(op_json: string): void;
    opsSince(lamport: bigint): any;
  }
}
