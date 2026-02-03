import { decode, encode, rfc8949EncodeOptions } from "cborg";

import { blake3 } from "@noble/hashes/blake3";
import { sha512 } from "@noble/hashes/sha512";
import { utf8ToBytes } from "@noble/hashes/utils";
import { hashes as ed25519Hashes, sign as ed25519Sign, verify as ed25519Verify } from "@noble/ed25519";

const COSE_EDDSA_ALG = -8;
const COSE_SIGNATURE1 = "Signature1";

let ed25519Ready = false;
function ensureEd25519(): void {
  if (ed25519Ready) return;
  ed25519Hashes.sha512 = sha512;
  ed25519Ready = true;
}

export type CoseSign1 = {
  protected: Uint8Array;
  unprotected: Map<unknown, unknown>;
  payload: Uint8Array;
  signature: Uint8Array;
};

function decodeCbor(bytes: Uint8Array): unknown {
  return decode(bytes, { useMaps: true });
}

function encodeCbor(value: unknown): Uint8Array {
  return encode(value, rfc8949EncodeOptions);
}

function sigStructure(protectedHeader: Uint8Array, payload: Uint8Array): Uint8Array {
  return encodeCbor([COSE_SIGNATURE1, protectedHeader, new Uint8Array(0), payload]);
}

export function coseSign1Ed25519(opts: {
  payload: Uint8Array;
  privateKey: Uint8Array;
  protectedHeader?: Map<unknown, unknown>;
  unprotectedHeader?: Map<unknown, unknown>;
}): Uint8Array {
  ensureEd25519();

  const protectedHeader = opts.protectedHeader ?? new Map<unknown, unknown>([[1, COSE_EDDSA_ALG]]);
  const unprotectedHeader = opts.unprotectedHeader ?? new Map<unknown, unknown>();
  const protectedBytes = encodeCbor(protectedHeader);
  const toSign = sigStructure(protectedBytes, opts.payload);

  const signature = ed25519Sign(toSign, opts.privateKey);
  return encodeCbor([protectedBytes, unprotectedHeader, opts.payload, signature]);
}

export async function coseDecodeSign1(bytes: Uint8Array): Promise<CoseSign1> {
  const value = decodeCbor(bytes);
  if (!Array.isArray(value) || value.length !== 4) throw new Error("COSE_Sign1 must be a 4-item array");

  const [protectedHeader, unprotectedHeader, payload, signature] = value as unknown[];
  if (!(protectedHeader instanceof Uint8Array)) throw new Error("COSE_Sign1[0] (protected) must be bstr");
  if (!(unprotectedHeader instanceof Map)) throw new Error("COSE_Sign1[1] (unprotected) must be map");
  if (!(payload instanceof Uint8Array)) throw new Error("COSE_Sign1[2] (payload) must be bstr");
  if (!(signature instanceof Uint8Array)) throw new Error("COSE_Sign1[3] (signature) must be bstr");

  return { protected: protectedHeader, unprotected: unprotectedHeader, payload, signature };
}

export async function coseVerifySign1Ed25519(opts: { bytes: Uint8Array; publicKey: Uint8Array }): Promise<Uint8Array> {
  ensureEd25519();

  const decoded = await coseDecodeSign1(opts.bytes);
  const toVerify = sigStructure(decoded.protected, decoded.payload);
  const ok = await ed25519Verify(decoded.signature, toVerify, opts.publicKey);
  if (!ok) throw new Error("COSE_Sign1 signature verification failed");
  return decoded.payload;
}

const TOKEN_ID_DOMAIN = utf8ToBytes("treecrdt/tokenid/v1");

export function deriveTokenIdV1(tokenBytes: Uint8Array): Uint8Array {
  const out = new Uint8Array(TOKEN_ID_DOMAIN.length + tokenBytes.length);
  out.set(TOKEN_ID_DOMAIN, 0);
  out.set(tokenBytes, TOKEN_ID_DOMAIN.length);
  return blake3(out).slice(0, 16);
}

