export function createPostgresNapiSyncBackendFactory() {
  return {
    open() {
      throw new Error('readiness tests do not open documents');
    },
  };
}
