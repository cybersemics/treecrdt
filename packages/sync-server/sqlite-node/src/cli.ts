import path from "node:path";

import { startSyncServer } from "./server.js";

async function main() {
  const host = process.env.HOST ?? "0.0.0.0";
  const port = Number(process.env.PORT ?? "8787");
  const dbDir = path.resolve(process.env.TREECRDT_DB_DIR ?? path.join(process.cwd(), "data"));
  const idleCloseMs = Number(process.env.TREECRDT_IDLE_CLOSE_MS ?? "30000");
  const maxPayloadBytes = Number(process.env.TREECRDT_MAX_PAYLOAD_BYTES ?? String(10 * 1024 * 1024));

  if (!Number.isFinite(port) || port <= 0) throw new Error(`invalid PORT: ${process.env.PORT}`);
  if (!Number.isFinite(idleCloseMs) || idleCloseMs < 0) throw new Error(`invalid TREECRDT_IDLE_CLOSE_MS`);
  if (!Number.isFinite(maxPayloadBytes) || maxPayloadBytes <= 0) {
    throw new Error(`invalid TREECRDT_MAX_PAYLOAD_BYTES`);
  }

  const handle = await startSyncServer({ host, port, dbDir, idleCloseMs, maxPayloadBytes });
  console.log(`TreeCRDT sync server listening on http://${handle.host}:${handle.port}`);
  console.log(`- health: http://${handle.host}:${handle.port}/health`);
  console.log(`- ws: ws://${handle.host}:${handle.port}/sync?docId=YOUR_DOC_ID`);
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});

