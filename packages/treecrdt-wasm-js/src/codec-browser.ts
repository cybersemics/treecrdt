import { createVersionVectorCodecLoader } from './codec.js';
import { loadWasm } from './wasm-browser.js';

export type {
  VersionVector,
  VersionVectorCodec,
  VersionVectorEntry,
  VersionVectorRange,
} from './codec.js';

export const loadVersionVectorCodec = createVersionVectorCodecLoader(loadWasm);
