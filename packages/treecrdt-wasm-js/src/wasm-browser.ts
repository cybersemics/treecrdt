import type { InitInput } from '../pkg-web/treecrdt_wasm.js';

let ready: Promise<typeof import('../pkg-web/treecrdt_wasm.js')> | undefined;

/** All browser entry points share one instance, including concurrent first loads. */
export function loadWasm(input?: InitInput) {
  return (ready ??= import('../pkg-web/treecrdt_wasm.js')
    .then(async (wasm) => {
      await wasm.default(input === undefined ? undefined : { module_or_path: input });
      return wasm;
    })
    .catch((error) => {
      ready = undefined;
      throw error;
    }));
}
