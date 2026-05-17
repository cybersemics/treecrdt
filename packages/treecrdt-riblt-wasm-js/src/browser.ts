import init, { RibltDecoder16, RibltEncoder16 } from '../pkg-web/treecrdt_riblt_wasm.js';
import wasmUrl from '../pkg-web/treecrdt_riblt_wasm_bg.wasm?url';

export type { RibltCodeword16 } from './types.js';

await init({ module_or_path: wasmUrl });

export { RibltDecoder16, RibltEncoder16 };
