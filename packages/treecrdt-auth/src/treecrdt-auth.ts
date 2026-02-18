export {
  deriveKeyIdV1,
  describeTreecrdtCapabilityTokenV1,
  issueTreecrdtCapabilityTokenV1,
  issueTreecrdtDelegatedCapabilityTokenV1,
} from "./internal/capability.js";
export type {
  TreecrdtCapabilityTokenV1,
  TreecrdtCapabilityV1,
  TreecrdtCapabilityRevocationCheckContext,
  TreecrdtCapabilityRevocationOptions,
} from "./internal/capability.js";

export { encodeTreecrdtOpSigInputV1, signTreecrdtOpV1, verifyTreecrdtOpV1 } from "./internal/op-sig.js";

export type { TreecrdtScopeEvaluator } from "./internal/scope.js";

export {
  TREECRDT_REVOCATION_CAPABILITY,
  createTreecrdtRevocationCapabilityV1,
  issueTreecrdtRevocationRecordV1,
  verifyTreecrdtRevocationCapabilityV1,
  verifyTreecrdtRevocationRecordV1,
} from "./revocation.js";
export type {
  TreecrdtRevocationModeV1,
  TreecrdtRevocationRecordV1,
  VerifiedTreecrdtRevocationRecordV1,
} from "./revocation.js";

export type {
  TreecrdtCoseCwtAuthOptions,
  TreecrdtCoseCwtParseRevocationCheckContext,
  TreecrdtCoseCwtRuntimeRevocationCheckContext,
  TreecrdtCoseCwtRevocationCheckContext,
} from "./internal/cose-cwt-auth.js";
export { createTreecrdtCoseCwtAuth } from "./internal/cose-cwt-auth.js";
