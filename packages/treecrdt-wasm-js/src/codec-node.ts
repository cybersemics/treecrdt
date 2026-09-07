import { createVersionVectorCodecLoader } from './codec.js';

export type {
  VersionVector,
  VersionVectorCodec,
  VersionVectorEntry,
  VersionVectorRange,
} from './codec.js';
export { VersionVectorCodecError } from './codec.js';

export const loadVersionVectorCodec = createVersionVectorCodecLoader(
  () => import('../pkg/treecrdt_wasm.js'),
);
