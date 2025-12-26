import init, { RibltDecoder16, RibltEncoder16 } from "../pkg-web/treecrdt_riblt_wasm.js";

export type { RibltCodeword16 } from "./types.js";

await init();

export { RibltDecoder16, RibltEncoder16 };

