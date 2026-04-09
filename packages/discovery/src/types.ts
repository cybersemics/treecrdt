export type DiscoveryTopology = 'relay';

export type DiscoveryAttachmentProtocol = 'websocket' | 'https';

export type DiscoveryAttachment = {
  protocol: DiscoveryAttachmentProtocol;
  url: string;
  role?: 'preferred' | 'bootstrap';
};

/**
 * Attachment plan returned by the bootstrap layer.
 */
export type DocAttachmentPlan = {
  topology: DiscoveryTopology;
  attachments: DiscoveryAttachment[];
  cacheTtlMs?: number;
};

export type ResolveDocRequest = {
  docId: string;
};

export type ResolveDocResponse = {
  docId: string;
  plan: DocAttachmentPlan;
};

/**
 * Connect-time bootstrap contract for document routing and attachment planning.
 *
 * This service is intentionally separate from the sync protocol hot path:
 * clients should resolve once, cache the returned attachment plan, and then
 * connect directly to the chosen data-plane endpoint for live traffic.
 */
export interface DocDiscoveryService {
  resolveDoc(request: ResolveDocRequest): Promise<ResolveDocResponse>;
}
