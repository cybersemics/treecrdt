import { expect, test } from "vitest";

import { bytesToHex } from "@treecrdt/interface/ids";

import { hashes as ed25519Hashes, getPublicKey, utils as ed25519Utils } from "@noble/ed25519";
import { sha512 } from "@noble/hashes/sha512";

import {
  issueDeviceCertV1,
  issueReplicaCertV1,
  verifyDeviceCertV1,
  verifyReplicaCertV1,
  verifyReplicaChainV1,
} from "../dist/identity.js";

ed25519Hashes.sha512 = sha512;

test("identity v1: device + replica cert chain verifies", async () => {
  const now = 1_700_000_000;
  const docId = "doc-identity-1";

  const identitySk = ed25519Utils.randomSecretKey();
  const identityPk = await getPublicKey(identitySk);

  const deviceSk = ed25519Utils.randomSecretKey();
  const devicePk = await getPublicKey(deviceSk);

  const replicaSk = ed25519Utils.randomSecretKey();
  const replicaPk = await getPublicKey(replicaSk);

  const deviceCert = issueDeviceCertV1({
    identityPrivateKey: identitySk,
    devicePublicKey: devicePk,
    iat: now,
    exp: now + 60,
  });
  const replicaCert = issueReplicaCertV1({
    devicePrivateKey: deviceSk,
    docId,
    replicaPublicKey: replicaPk,
    iat: now,
    exp: now + 60,
  });

  const chain = await verifyReplicaChainV1({
    identityPublicKey: identityPk,
    deviceCertBytes: deviceCert,
    replicaCertBytes: replicaCert,
    expectedDocId: docId,
    expectedReplicaPublicKey: replicaPk,
    nowSec: () => now,
  });

  expect(bytesToHex(chain.devicePublicKey)).toBe(bytesToHex(devicePk));
  expect(bytesToHex(chain.replicaPublicKey)).toBe(bytesToHex(replicaPk));
});

test("identity v1: device cert expiry is enforced when nowSec provided", async () => {
  const now = 1_700_000_000;
  const identitySk = ed25519Utils.randomSecretKey();
  const identityPk = await getPublicKey(identitySk);
  const deviceSk = ed25519Utils.randomSecretKey();
  const devicePk = await getPublicKey(deviceSk);

  const cert = issueDeviceCertV1({
    identityPrivateKey: identitySk,
    devicePublicKey: devicePk,
    exp: now - 1,
  });

  await expect(
    verifyDeviceCertV1({ certBytes: cert, identityPublicKey: identityPk, nowSec: () => now })
  ).rejects.toThrow(/expired/i);
});

test("identity v1: replica cert doc mismatch throws when expectedDocId provided", async () => {
  const now = 1_700_000_000;
  const deviceSk = ed25519Utils.randomSecretKey();
  const devicePk = await getPublicKey(deviceSk);
  const replicaSk = ed25519Utils.randomSecretKey();
  const replicaPk = await getPublicKey(replicaSk);

  const cert = issueReplicaCertV1({
    devicePrivateKey: deviceSk,
    docId: "doc-a",
    replicaPublicKey: replicaPk,
    exp: now + 60,
  });

  await expect(
    verifyReplicaCertV1({
      certBytes: cert,
      devicePublicKey: devicePk,
      expectedDocId: "doc-b",
      nowSec: () => now,
    })
  ).rejects.toThrow(/doc_id mismatch/i);
});

