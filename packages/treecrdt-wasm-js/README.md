# @treecrdt/wasm

## Version-vector codec

The portable `./codec` entry point calls the canonical Rust version-vector codec in Node and browsers.

```ts
import { encodeVersionVectorV0, decodeVersionVectorV0 } from '@treecrdt/wasm/codec';

const bytes = await encodeVersionVectorV0({ entries: [] });
const vector = await decodeVersionVectorV0(bytes);
```

Replica IDs are `Uint8Array` values; frontiers and range bounds are `bigint`. Inputs must already
be canonical: encoding rejects unsorted entries and non-normalized ranges. The
[format contract](https://github.com/cybersemics/treecrdt/blob/main/docs/version-vector-v0.md) describes the bytes and validation rules.

WASM initializes on the first codec call and is reused. Inputs are copied before initialization;
a failed load can be retried. Invalid input rejects with `VersionVectorCodecError`; loading failures
and unexpected runtime failures propagate separately. Browser bundlers must serve the generated WASM
asset; Vite handles its URL.

The package root remains the Node-only tree-engine adapter. Browser consumers should import `./codec`.
