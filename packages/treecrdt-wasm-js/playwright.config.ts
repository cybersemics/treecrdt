import { defineConfig } from '@playwright/test';

export default defineConfig({
  testDir: './tests',
  testMatch: 'browser.spec.ts',
  use: { browserName: 'chromium' },
  projects: [
    { name: 'production', use: { baseURL: 'http://127.0.0.1:4295/memory/' } },
    { name: 'development', use: { baseURL: 'http://127.0.0.1:4296/memory/' } },
  ],
  webServer: [
    {
      command:
        'pnpm exec vite build --config tests/browser/vite.config.ts && pnpm exec vite preview --config tests/browser/vite.config.ts --host 127.0.0.1 --port 4295 --strictPort',
      url: 'http://127.0.0.1:4295/memory/',
      reuseExistingServer: false,
      timeout: 30000,
    },
    {
      command:
        'pnpm exec vite --config tests/browser/vite.config.ts --host 127.0.0.1 --port 4296 --strictPort',
      url: 'http://127.0.0.1:4296/memory/',
      reuseExistingServer: false,
      timeout: 30000,
    },
  ],
});
