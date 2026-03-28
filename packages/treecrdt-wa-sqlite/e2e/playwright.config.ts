import { defineConfig, devices } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  fullyParallel: false,
  workers: 1,
  retries: 0,
  use: {
    trace: 'retain-on-failure',
  },
  projects: [
    {
      name: 'chromium-dev',
      use: { ...devices['Desktop Chrome'], baseURL: 'http://localhost:4166' },
    },
    {
      name: 'chromium-basepath-preview',
      testMatch: ['**/basepath.spec.ts'],
      use: { ...devices['Desktop Chrome'], baseURL: 'http://localhost:4172' },
    },
  ],
  webServer: [
    {
      command: 'pnpm run dev -- --host --port 4166',
      url: 'http://localhost:4166',
      reuseExistingServer: !process.env.CI,
    },
    {
      command: 'pnpm run build:app:path && pnpm run preview:path',
      url: 'http://localhost:4172/base-path/',
      reuseExistingServer: false,
      timeout: 180_000,
    },
  ],
});
