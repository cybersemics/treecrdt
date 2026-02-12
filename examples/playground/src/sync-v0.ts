import { nodeIdToBytes16 } from "@treecrdt/interface/ids";

export type AuthGrantMessageV1 = {
  t: "auth_grant_v1";
  doc_id: string;
  to_replica_pk_hex: string;
  issuer_pk_b64: string;
  token_b64: string;
  payload_key_b64?: string;
  from_peer_id: string;
  ts: number;
};

export function hexToBytes16(hex: string): Uint8Array {
  return nodeIdToBytes16(hex);
}
