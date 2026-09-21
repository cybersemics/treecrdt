import { afterEach, expect, test, vi } from 'vitest';

import { opfsFilenameForDocId } from '../src/opfs.js';
import { resolveBrowserEnvironment } from '../src/runtime/resolve.js';

vi.mock('../src/opfs.js', async (importOriginal) => {
  const original = await importOriginal<typeof import('../src/opfs.js')>();
  return {
    ...original,
    detectOpfsSupport: vi.fn(() => ({ available: true })),
  };
});

afterEach(() => {
  vi.unstubAllGlobals();
});

test.each([
  ['derived-filename', '/treecrdt-derived-filename.CKkA3FFh7ePDFYNk9n1Ktw.db'],
  ['My Document/α', '/treecrdt-my-document.cGyKjTFebsO7Cd3umUvwiA.db'],
  ['!!!', '/treecrdt-doc.6ExTjn_iUHMO9i3iIMQN-g.db'],
])('derives a readable OPFS filename with a truncated SHA-256 hash', (docId, filename) => {
  expect(opfsFilenameForDocId(docId)).toBe(filename);
});

test('persistent storage derives its filename from docId by default', () => {
  const docId = 'derived-filename';
  expect(resolveBrowserEnvironment({ docId, persistent: true }).filename).toBe(
    opfsFilenameForDocId(docId),
  );
});

test('persistent storage accepts an explicit filename', () => {
  expect(
    resolveBrowserEnvironment({
      docId: 'explicit-filename',
      persistent: true,
      filename: '/existing.db',
    }).filename,
  ).toBe('/existing.db');
});

test.each([false, true])(
  'crossTab selects the shared-worker runtime (persistent: %s)',
  (persistent) => {
    vi.stubGlobal('SharedWorker', class {});
    expect(
      resolveBrowserEnvironment({ docId: 'cross-tab', persistent, crossTab: true }).runtime,
    ).toBe('shared-worker');
  },
);

test('crossTab requires a boolean and SharedWorker support', () => {
  expect(() =>
    resolveBrowserEnvironment({ docId: 'cross-tab', crossTab: 'yes' as unknown as boolean }),
  ).toThrow(/crossTab must be a boolean/);
  expect(() => resolveBrowserEnvironment({ docId: 'cross-tab', crossTab: true })).toThrow(
    /requires SharedWorker support/,
  );
});
