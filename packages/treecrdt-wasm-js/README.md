# @treecrdt/wasm

## Version-vector codec

The portable `./codec` entry point calls the canonical Rust version-vector codec in Node and browsers.

```ts
import { loadVersionVectorCodec } from '@treecrdt/wasm/codec';

async function example() {
  const codec = await loadVersionVectorCodec();
  const bytes = codec.encodeVersionVectorV0({ entries: [] });
  return codec.decodeVersionVectorV0(bytes);
}
```

The typed API uses `Uint8Array` replica IDs and `bigint` counters; decoding always returns those types.
Rust also accepts safe integer numbers and compatible byte sequences when encoding. It rejects
malformed ranges, unsorted entries, and non-normalized ranges. The
[format contract](https://github.com/cybersemics/treecrdt/blob/main/docs/version-vector-v0.md) describes the bytes and validation rules.

Importing this entry point does not load WASM. The loader initializes it on demand and caches the ready
codec; a failed load can be retried. Encoding and decoding are synchronous after loading, with no
top-level await required. Invalid input throws `VersionVectorCodecError`; loading failures and
unexpected runtime failures propagate separately. Browser bundlers must serve the generated WASM asset;
Vite handles its URL.

The package root remains the Node-only tree-engine adapter. Browser consumers should import `./codec`.
