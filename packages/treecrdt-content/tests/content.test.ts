import { describe, expect, test } from "vitest";

import {
  TreecrdtContentObjectUrlCache,
  decodeContent,
  encodeImageContent,
  encodeTextContent,
  validateImageContentFile,
} from "../src/index";

describe("@treecrdt/content", () => {
  test("keeps raw UTF-8 text payloads as the simple text protocol", () => {
    const bytes = encodeTextContent("hello image world");

    expect(decodeContent(bytes)).toMatchObject({
      kind: "text",
      text: "hello image world",
    });
  });

  test("roundtrips binary image content envelopes", () => {
    const imageBytes = new Uint8Array([1, 2, 3, 4]);
    const encoded = encodeImageContent({
      mime: "image/png",
      name: "pixel.png",
      bytes: imageBytes,
    });

    const decoded = decodeContent(encoded);
    expect(decoded).toMatchObject({
      kind: "image",
      mime: "image/png",
      name: "pixel.png",
      size: imageBytes.byteLength,
    });
    expect(decoded.kind === "image" ? Array.from(decoded.bytes) : []).toEqual([1, 2, 3, 4]);
  });

  test("rejects SVG and unsupported image MIME types", () => {
    expect(() =>
      encodeImageContent({
        mime: "image/svg+xml",
        bytes: new Uint8Array([1]),
      }),
    ).toThrow(/Unsupported image content MIME type/);

    const file = new File([new Uint8Array([1])], "vector.svg", { type: "image/svg+xml" });
    expect(() => validateImageContentFile(file)).toThrow(/Unsupported image type/);
  });

  test("falls back to text for corrupt image envelopes", () => {
    const corrupt = new Uint8Array([0x89, 0x54, 0x43, 0x52, 0x49, 0x4d, 0x47, 0x01, 0, 0, 0, 3, 1, 2, 3]);

    expect(decodeContent(corrupt).kind).toBe("text");
  });

  test("revokes stale object URLs", () => {
    const revoked: string[] = [];
    let nextUrl = 0;
    const cache = new TreecrdtContentObjectUrlCache({
      createObjectURL: () => `blob:test-${++nextUrl}`,
      revokeObjectURL: (url) => revoked.push(url),
    });

    cache.set("node-a", { kind: "image", mime: "image/png", size: 1, bytes: new Uint8Array([1]) });
    cache.set("node-a", { kind: "image", mime: "image/png", size: 1, bytes: new Uint8Array([2]) });
    cache.clear();

    expect(revoked).toEqual(["blob:test-1", "blob:test-2"]);
  });
});
