import { createVersionVectorCodecLoader } from './codec.js';

export type {
  VersionVector,
  VersionVectorCodec,
  VersionVectorEntry,
  VersionVectorRange,
} from './codec.js';

export const loadVersionVectorCodec = createVersionVectorCodecLoader(async () => {
  const wasm = await import('../pkg-web/treecrdt_wasm.js');
  await wasm.default();
  return wasm;
});
