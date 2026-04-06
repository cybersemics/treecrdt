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
