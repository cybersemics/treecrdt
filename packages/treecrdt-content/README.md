# @treecrdt/content

A small app-layer codec for interpreting TreeCRDT payload bytes as text or inline images.
TreeCRDT storage remains media-agnostic and continues to store ordinary `Uint8Array` payloads.

```ts
import { decodeContent, encodeImageContent, encodeTextContent } from '@treecrdt/content';

const textPayload = encodeTextContent('hello');
const imagePayload = encodeImageContent({
  mime: 'image/png',
  bytes: imageBytes,
});

const content = decodeContent(imagePayload);
```

Text uses raw UTF-8 as the canonical zero-envelope fast path. Images use a versioned binary envelope containing small
JSON metadata (including the exact image byte length) followed by the original image bytes.
Recognized envelopes fail closed when corrupt or unsupported; byte-size limits and browser
object-URL lifecycles remain application policy.
