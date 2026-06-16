# @treecrdt/content

Typed content helpers for TreeCRDT payload bytes.

TreeCRDT storage still sees ordinary `Uint8Array` payloads. This package defines a small app-layer protocol for interpreting those bytes as user-facing content.

```ts
import { decodeContent, encodeImageFileContent, encodeTextContent } from "@treecrdt/content";

const textPayload = encodeTextContent("hello");
const imagePayload = await encodeImageFileContent(file);

const content = decodeContent(imagePayload);
if (content.kind === "image") {
  console.log(content.mime, content.size);
}
```

V1 keeps text payloads as raw UTF-8 for compatibility. Image payloads use a compact binary envelope with JSON metadata followed by raw image bytes.

Future versions can add chunked or external blob content by extending this app-layer protocol without changing TreeCRDT core payload semantics.
