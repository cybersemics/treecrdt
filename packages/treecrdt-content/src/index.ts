export const TREECRDT_CONTENT_IMAGE_MAX_BYTES = 5 * 1024 * 1024;

const IMAGE_CONTENT_MAGIC = new Uint8Array([0x89, 0x54, 0x43, 0x52, 0x49, 0x4d, 0x47, 0x01]);
const IMAGE_CONTENT_METADATA_BYTES = 4;
const MAX_IMAGE_CONTENT_METADATA_BYTES = 16 * 1024;

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export const SUPPORTED_IMAGE_MIME_TYPES = [
  'image/png',
  'image/jpeg',
  'image/webp',
  'image/gif',
] as const;

export type SupportedImageMime = (typeof SUPPORTED_IMAGE_MIME_TYPES)[number];

export type TreecrdtTextContent = {
  kind: 'text';
  text: string;
  bytes: Uint8Array;
};

export type TreecrdtImageContent = {
  kind: 'image';
  mime: SupportedImageMime;
  name?: string;
  size: number;
  bytes: Uint8Array;
};

export type TreecrdtContent = { kind: 'empty' } | TreecrdtTextContent | TreecrdtImageContent;

export type TreecrdtImageContentInput = {
  mime: string;
  name?: string;
  bytes: Uint8Array;
};

export type TreecrdtImageContentMetadata = {
  kind: 'image';
  mime: SupportedImageMime;
  name?: string;
  size: number;
};

export type TreecrdtContentObjectUrlFactory = {
  createObjectURL: (blob: Blob) => string;
  revokeObjectURL: (url: string) => void;
};

export class TreecrdtContentObjectUrlCache {
  private readonly urls = new Map<string, string>();

  constructor(private readonly factory: TreecrdtContentObjectUrlFactory) {}

  set(key: string, image: TreecrdtImageContent): string {
    this.revoke(key);
    const bytes = new Uint8Array(image.bytes.byteLength);
    bytes.set(image.bytes);
    const url = this.factory.createObjectURL(
      new Blob([bytes.buffer as ArrayBuffer], { type: image.mime }),
    );
    this.urls.set(key, url);
    return url;
  }

  revoke(key: string): void {
    const current = this.urls.get(key);
    if (!current) return;
    this.factory.revokeObjectURL(current);
    this.urls.delete(key);
  }

  clear(): void {
    for (const url of this.urls.values()) this.factory.revokeObjectURL(url);
    this.urls.clear();
  }
}

export function browserContentObjectUrlFactory(): TreecrdtContentObjectUrlFactory | null {
  if (typeof URL === 'undefined') return null;
  if (typeof URL.createObjectURL !== 'function' || typeof URL.revokeObjectURL !== 'function')
    return null;
  return {
    createObjectURL: (blob) => URL.createObjectURL(blob),
    revokeObjectURL: (url) => URL.revokeObjectURL(url),
  };
}

export function isSupportedImageMime(mime: string): mime is SupportedImageMime {
  return (SUPPORTED_IMAGE_MIME_TYPES as readonly string[]).includes(mime);
}

export function formatContentBytes(bytes: number): string {
  if (!Number.isFinite(bytes) || bytes < 0) return '0 B';
  if (bytes < 1024) return `${bytes} B`;
  const units = ['KiB', 'MiB', 'GiB'] as const;
  let value = bytes / 1024;
  for (let i = 0; i < units.length; i += 1) {
    const unit = units[i]!;
    if (value < 1024 || i === units.length - 1) {
      const decimals = value >= 10 ? 1 : 2;
      return `${value.toFixed(decimals)} ${unit}`;
    }
    value /= 1024;
  }
  return `${bytes} B`;
}

export function validateImageContentFile(
  file: File,
  maxBytes = TREECRDT_CONTENT_IMAGE_MAX_BYTES,
): void {
  if (!isSupportedImageMime(file.type)) {
    throw new Error('Unsupported image type. Use PNG, JPEG, WebP, or GIF.');
  }
  if (file.size > maxBytes) {
    throw new Error(`Image is too large. The upload limit is ${formatContentBytes(maxBytes)}.`);
  }
}

export function encodeTextContent(text: string): Uint8Array {
  // V1 text content is intentionally raw UTF-8 so existing payloads remain readable.
  return textEncoder.encode(text);
}

export async function encodeImageFileContent(file: File): Promise<Uint8Array> {
  validateImageContentFile(file);
  return encodeImageContent({
    mime: file.type,
    name: file.name,
    bytes: new Uint8Array(await file.arrayBuffer()),
  });
}

export function encodeImageContent(input: TreecrdtImageContentInput): Uint8Array {
  if (!isSupportedImageMime(input.mime)) {
    throw new Error(`Unsupported image content MIME type: ${input.mime || '(empty)'}`);
  }

  const metadata: TreecrdtImageContentMetadata = {
    kind: 'image',
    mime: input.mime,
    size: input.bytes.byteLength,
  };
  const name = input.name?.trim();
  if (name) metadata.name = name;

  const metadataBytes = textEncoder.encode(JSON.stringify(metadata));
  if (metadataBytes.byteLength > MAX_IMAGE_CONTENT_METADATA_BYTES) {
    throw new Error('Image content metadata is too large.');
  }

  const headerLength = IMAGE_CONTENT_MAGIC.byteLength + IMAGE_CONTENT_METADATA_BYTES;
  // Image content uses a compact envelope so callers can distinguish it from legacy text bytes.
  const out = new Uint8Array(headerLength + metadataBytes.byteLength + input.bytes.byteLength);
  out.set(IMAGE_CONTENT_MAGIC, 0);
  new DataView(
    out.buffer,
    out.byteOffset + IMAGE_CONTENT_MAGIC.byteLength,
    IMAGE_CONTENT_METADATA_BYTES,
  ).setUint32(0, metadataBytes.byteLength, false);
  out.set(metadataBytes, headerLength);
  out.set(input.bytes, headerLength + metadataBytes.byteLength);
  return out;
}

export function decodeContent(bytes: Uint8Array | null): TreecrdtContent {
  if (bytes === null) return { kind: 'empty' };

  const image = decodeImageEnvelope(bytes);
  if (image) return image;

  return { kind: 'text', text: textDecoder.decode(bytes), bytes };
}

function decodeImageEnvelope(bytes: Uint8Array): TreecrdtImageContent | null {
  if (!hasImageContentMagic(bytes)) return null;

  const metadataOffset = IMAGE_CONTENT_MAGIC.byteLength;
  const payloadOffsetBase = metadataOffset + IMAGE_CONTENT_METADATA_BYTES;
  if (bytes.byteLength < payloadOffsetBase) return null;

  const metadataLength = new DataView(
    bytes.buffer,
    bytes.byteOffset + metadataOffset,
    IMAGE_CONTENT_METADATA_BYTES,
  ).getUint32(0, false);
  if (metadataLength <= 0 || metadataLength > MAX_IMAGE_CONTENT_METADATA_BYTES) return null;
  const payloadOffset = payloadOffsetBase + metadataLength;
  if (payloadOffset > bytes.byteLength) return null;

  try {
    const metadataRaw = textDecoder.decode(bytes.slice(payloadOffsetBase, payloadOffset));
    const metadata = JSON.parse(metadataRaw) as Partial<TreecrdtImageContentMetadata>;
    if (metadata.kind !== 'image') return null;
    if (typeof metadata.mime !== 'string' || !isSupportedImageMime(metadata.mime)) return null;
    const size = metadata.size;
    if (typeof size !== 'number' || !Number.isInteger(size) || size < 0) return null;
    const imageBytes = bytes.slice(payloadOffset);
    if (size !== imageBytes.byteLength) return null;
    const decoded: TreecrdtImageContent = {
      kind: 'image',
      mime: metadata.mime,
      size,
      bytes: imageBytes,
    };
    if (typeof metadata.name === 'string' && metadata.name.trim().length > 0) {
      decoded.name = metadata.name.trim();
    }
    return decoded;
  } catch {
    return null;
  }
}

function hasImageContentMagic(bytes: Uint8Array): boolean {
  if (bytes.byteLength < IMAGE_CONTENT_MAGIC.byteLength) return false;
  for (let i = 0; i < IMAGE_CONTENT_MAGIC.byteLength; i += 1) {
    if (bytes[i] !== IMAGE_CONTENT_MAGIC[i]) return false;
  }
  return true;
}
