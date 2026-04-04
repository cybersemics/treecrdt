export type DiscoveryPrincipalKind = 'user' | 'device' | 'workspace' | 'team' | 'service';

export type DiscoveryPrincipal = {
  kind: DiscoveryPrincipalKind;
  id: string;
};

export type DocAccessSummary = {
  canRead: boolean;
  canWriteStructure: boolean;
  canWritePayload: boolean;
  canDelete: boolean;
  canManageAccess?: boolean;
};

export type DocDiscoveryMetadata = {
  title?: string;
  snippet?: string;
  archived?: boolean;
  deleted?: boolean;
  createdAt?: string;
  updatedAt?: string;
  lastActivityAt?: string;
};

export type DiscoveryTopology = 'relay' | 'regional-relay' | 'peer-mesh' | 'hybrid';

export type DiscoveryAttachmentProtocol = 'websocket' | 'https' | 'webrtc-signal';

export type DiscoveryAttachment = {
  protocol: DiscoveryAttachmentProtocol;
  url: string;
  role?: 'preferred' | 'durable' | 'relay' | 'bootstrap' | 'signal';
  label?: string;
};

/**
 * Attachment plan returned by the discovery/bootstrap control plane.
 *
 * The plan is intentionally provider-neutral. A simple hosted sync service can
 * return one websocket attachment, while more advanced providers may return a
 * hybrid or peer-mesh bootstrap plan later.
 */
export type DocAttachmentPlan = {
  topology: DiscoveryTopology;
  attachments: DiscoveryAttachment[];
  cacheTtlMs?: number;
  routeVersion?: string;
};

export type CreateDocRequest = {
  creator: DiscoveryPrincipal;
  docId?: string;
  metadata?: DocDiscoveryMetadata;
  placementHint?: string;
};

export type CreateDocResponse = {
  docId: string;
  plan: DocAttachmentPlan;
  metadata?: DocDiscoveryMetadata;
  access: DocAccessSummary;
};

export type ResolveDocRequest = {
  docId: string;
  principal?: DiscoveryPrincipal;
};

export type ResolveDocResponse = {
  docId: string;
  plan: DocAttachmentPlan;
  metadata?: DocDiscoveryMetadata;
  access?: DocAccessSummary;
};

export type DocListingEntry = {
  docId: string;
  access: DocAccessSummary;
  metadata?: DocDiscoveryMetadata;
  discoveredVia?: 'owner' | 'invite' | 'grant' | 'workspace' | 'catalog';
  lastOpenedAt?: string;
  /**
   * Optional eager attachment plan to avoid a second lookup when the UI
   * immediately opens a doc from the listing.
   */
  plan?: DocAttachmentPlan;
};

export type ListAccessibleDocsRequest = {
  principal: DiscoveryPrincipal;
  cursor?: string;
  limit?: number;
  includePlans?: boolean;
};

export type ListAccessibleDocsResponse = {
  items: DocListingEntry[];
  nextCursor?: string;
};

/**
 * Control-plane contract for document creation, bootstrap routing, and listing.
 *
 * This service is intentionally separate from the sync protocol hot path:
 * clients should resolve once, cache the returned attachment plan, and then
 * connect directly to the chosen data-plane endpoint for live traffic.
 */
export interface DocDiscoveryService {
  createDoc(request: CreateDocRequest): Promise<CreateDocResponse>;
  resolveDoc(request: ResolveDocRequest): Promise<ResolveDocResponse>;
  listAccessibleDocs(request: ListAccessibleDocsRequest): Promise<ListAccessibleDocsResponse>;
}
