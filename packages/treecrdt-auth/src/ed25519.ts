import { hashes as ed25519Hashes, getPublicKey as getPublicKeyImpl, sign as signImpl, utils, verify as verifyImpl } from "@noble/ed25519";
import { sha512 } from "@noble/hashes/sha512";

let ed25519Ready = false;

export function ensureEd25519(): void {
  if (ed25519Ready) return;
  ed25519Hashes.sha512 = sha512;
  ed25519Ready = true;
}

export function randomEd25519SecretKey(): Uint8Array {
  ensureEd25519();
  return utils.randomSecretKey();
}

export async function getEd25519PublicKey(privateKey: Uint8Array): Promise<Uint8Array> {
  ensureEd25519();
  return await getPublicKeyImpl(privateKey);
}

export function signEd25519(message: Uint8Array, privateKey: Uint8Array): Uint8Array {
  ensureEd25519();
  return signImpl(message, privateKey);
}

export async function verifyEd25519(signature: Uint8Array, message: Uint8Array, publicKey: Uint8Array): Promise<boolean> {
  ensureEd25519();
  return await verifyImpl(signature, message, publicKey);
}
