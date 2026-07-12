export const SUPPORTED_IMAGE_MIME_TYPES = [
  'image/png',
  'image/jpeg',
  'image/webp',
  'image/gif',
] as const;

export type SupportedImageMime = (typeof SUPPORTED_IMAGE_MIME_TYPES)[number];

export type TreecrdtContent =
  | { kind: 'empty' }
  | { kind: 'text'; text: string; bytes: Uint8Array }
  | {
      kind: 'image';
      mime: SupportedImageMime;
      name?: string;
      size: number;
      bytes: Uint8Array;
    };

export type TreecrdtImageContentInput = {
  mime: string;
  name?: string;
  bytes: Uint8Array;
};

const CONTENT_MAGIC = new Uint8Array([0x89, 0x54, 0x43, 0x52, 0x43, 0x4e, 0x54]);
const CONTENT_VERSION = 1;
const METADATA_LENGTH_BYTES = 4;
const MAX_METADATA_BYTES = 16 * 1024;
const HEADER_BYTES = CONTENT_MAGIC.byteLength + 1 + METADATA_LENGTH_BYTES;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();
const metadataDecoder = new TextDecoder('utf-8', { fatal: true });

export function isSupportedImageMime(mime: string): mime is SupportedImageMime {
  return (SUPPORTED_IMAGE_MIME_TYPES as readonly string[]).includes(mime);
}

/** Text uses raw UTF-8 as the canonical zero-envelope representation. */
export function encodeTextContent(text: string): Uint8Array {
  return textEncoder.encode(text);
}

export function encodeImageContent(input: TreecrdtImageContentInput): Uint8Array {
  if (!isSupportedImageMime(input.mime)) {
    throw new Error(`Unsupported image content MIME type: ${input.mime || '(empty)'}`);
  }

  const name = input.name?.trim();
  const metadata = {
    kind: 'image',
    mime: input.mime,
    size: input.bytes.byteLength,
    ...(name ? { name } : {}),
  };
  const metadataBytes = textEncoder.encode(JSON.stringify(metadata));
  if (metadataBytes.byteLength > MAX_METADATA_BYTES) {
    throw new Error('Image content metadata is too large');
  }

  const output = new Uint8Array(HEADER_BYTES + metadataBytes.byteLength + input.bytes.byteLength);
  output.set(CONTENT_MAGIC);
  output[CONTENT_MAGIC.byteLength] = CONTENT_VERSION;
  new DataView(
    output.buffer,
    output.byteOffset + CONTENT_MAGIC.byteLength + 1,
    METADATA_LENGTH_BYTES,
  ).setUint32(0, metadataBytes.byteLength, false);
  output.set(metadataBytes, HEADER_BYTES);
  output.set(input.bytes, HEADER_BYTES + metadataBytes.byteLength);
  return output;
}

/**
 * Decodes the TreeCRDT app-layer content protocol.
 *
 * Bytes without the binary envelope are interpreted as canonical UTF-8 text. Once
 * the envelope magic is present, malformed or unsupported data throws instead
 * of being silently reclassified as text.
 */
export function decodeContent(bytes: Uint8Array | null): TreecrdtContent {
  if (bytes === null) return { kind: 'empty' };
  if (!hasContentMagic(bytes)) {
    return { kind: 'text', text: textDecoder.decode(bytes), bytes };
  }
  return decodeEnvelope(bytes);
}

function decodeEnvelope(bytes: Uint8Array): TreecrdtContent {
  if (bytes.byteLength < HEADER_BYTES) throw new Error('Truncated TreeCRDT content envelope');

  const version = bytes[CONTENT_MAGIC.byteLength];
  if (version !== CONTENT_VERSION) {
    throw new Error(`Unsupported TreeCRDT content envelope version: ${String(version)}`);
  }

  const metadataLength = new DataView(
    bytes.buffer,
    bytes.byteOffset + CONTENT_MAGIC.byteLength + 1,
    METADATA_LENGTH_BYTES,
  ).getUint32(0, false);
  if (metadataLength === 0 || metadataLength > MAX_METADATA_BYTES) {
    throw new Error('Invalid TreeCRDT content metadata length');
  }

  const payloadOffset = HEADER_BYTES + metadataLength;
  if (payloadOffset > bytes.byteLength) throw new Error('Truncated TreeCRDT content metadata');

  let decoded: unknown;
  try {
    decoded = JSON.parse(metadataDecoder.decode(bytes.subarray(HEADER_BYTES, payloadOffset)));
  } catch {
    throw new Error('Invalid TreeCRDT content metadata');
  }
  if (typeof decoded !== 'object' || decoded === null || Array.isArray(decoded)) {
    throw new Error('Invalid TreeCRDT content metadata');
  }

  const metadata = decoded as Record<string, unknown>;
  if (metadata.kind !== 'image') throw new Error('Unsupported TreeCRDT content kind');
  if (typeof metadata.mime !== 'string' || !isSupportedImageMime(metadata.mime)) {
    throw new Error('Unsupported image content MIME type');
  }
  if (metadata.name !== undefined && typeof metadata.name !== 'string') {
    throw new Error('Invalid image content name');
  }

  if (
    typeof metadata.size !== 'number' ||
    !Number.isSafeInteger(metadata.size) ||
    metadata.size < 0
  ) {
    throw new Error('Invalid image content size');
  }
  const payloadLength = bytes.byteLength - payloadOffset;
  if (metadata.size !== payloadLength) {
    throw new Error('TreeCRDT image content size mismatch');
  }

  const imageBytes = bytes.subarray(payloadOffset);
  const name = metadata.name?.trim();
  return {
    kind: 'image',
    mime: metadata.mime,
    ...(name ? { name } : {}),
    size: metadata.size,
    bytes: imageBytes,
  };
}

function hasContentMagic(bytes: Uint8Array): boolean {
  if (bytes.byteLength < CONTENT_MAGIC.byteLength) return false;
  for (let i = 0; i < CONTENT_MAGIC.byteLength; i += 1) {
    if (bytes[i] !== CONTENT_MAGIC[i]) return false;
  }
  return true;
}
