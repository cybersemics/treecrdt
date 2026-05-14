import http from 'node:http';

import { expect, test } from 'vitest';

import { startDiscoveryServer } from '../dist/index.js';

async function httpRequest(
  url: string,
  opts: { method?: string; headers?: Record<string, string> } = {},
): Promise<{ status: number; headers: http.IncomingHttpHeaders; body: string }> {
  return await new Promise((resolve, reject) => {
    const req = http.request(
      url,
      {
        method: opts.method ?? 'GET',
        headers: opts.headers,
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
        res.on('end', () => {
          resolve({
            status: res.statusCode ?? 0,
            headers: res.headers,
            body: Buffer.concat(chunks).toString('utf8'),
          });
        });
      },
    );
    req.once('error', reject);
    req.end();
  });
}

test('resolve-doc returns a relay attachment plan', async () => {
  const server = await startDiscoveryServer({
    host: '127.0.0.1',
    port: 0,
    publicHttpBaseUrl: 'https://bootstrap.example.com',
    publicWebSocketBaseUrl: 'wss://eu.sync.example.com',
    cacheTtlMs: 42_000,
  });

  try {
    const response = await httpRequest(
      `http://${server.host}:${server.port}/resolve-doc?docId=abc123`,
    );
    expect(response.status).toBe(200);
    expect(JSON.parse(response.body)).toEqual({
      docId: 'abc123',
      plan: {
        topology: 'relay',
        attachments: [
          {
            protocol: 'websocket',
            role: 'preferred',
            url: 'wss://eu.sync.example.com/sync',
          },
          {
            protocol: 'https',
            role: 'bootstrap',
            url: 'https://bootstrap.example.com',
          },
        ],
        cacheTtlMs: 42_000,
      },
    });
  } finally {
    await server.close();
  }
});

test('resolve-doc derives public urls from forwarded headers', async () => {
  const server = await startDiscoveryServer({
    host: '127.0.0.1',
    port: 0,
  });

  try {
    const response = await httpRequest(
      `http://${server.host}:${server.port}/resolve-doc?docId=abc123`,
      {
        headers: {
          host: 'internal.local',
          'x-forwarded-host': 'sync.emhub.net',
          'x-forwarded-proto': 'https',
        },
      },
    );
    expect(response.status).toBe(200);
    expect(JSON.parse(response.body)).toEqual({
      docId: 'abc123',
      plan: {
        topology: 'relay',
        attachments: [
          {
            protocol: 'websocket',
            role: 'preferred',
            url: 'wss://sync.emhub.net/sync',
          },
          {
            protocol: 'https',
            role: 'bootstrap',
            url: 'https://sync.emhub.net',
          },
        ],
        cacheTtlMs: 3_600_000,
      },
    });
  } finally {
    await server.close();
  }
});

test('status endpoint reports configured bootstrap settings', async () => {
  const server = await startDiscoveryServer({
    host: '127.0.0.1',
    port: 0,
    resolveDocPath: '/bootstrap',
    publicHttpBaseUrl: 'https://bootstrap.example.com',
    publicWebSocketBaseUrl: 'wss://us.sync.example.com',
  });

  try {
    const response = await httpRequest(`http://${server.host}:${server.port}/status`);
    expect(response.status).toBe(200);
    expect(JSON.parse(response.body)).toMatchObject({
      ok: true,
      service: '@justtemporary/discovery-server-node',
      resolveDocPath: '/bootstrap',
      publicHttpBaseUrl: 'https://bootstrap.example.com',
      publicWebSocketBaseUrl: 'wss://us.sync.example.com',
    });
  } finally {
    await server.close();
  }
});
