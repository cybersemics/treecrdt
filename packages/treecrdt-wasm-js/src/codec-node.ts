import { createVersionVectorCodec } from './codec.js';

export type { VersionVector, VersionVectorEntry, VersionVectorRange } from './codec.js';
export { VersionVectorCodecError } from './codec.js';

export const { encodeVersionVectorV0, decodeVersionVectorV0 } = createVersionVectorCodec(
  () => import('../pkg/treecrdt_wasm.js'),
);
