import { defineConfig, devices } from "@playwright/test";

export default defineConfig({
  testDir: "./tests",
  fullyParallel: false,
  workers: 1,
  retries: 0,
  use: {
    trace: "retain-on-failure",
  },
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"], baseURL: "http://localhost:5193" },
    },
  ],
  webServer: [
    {
      command: "pnpm run dev -- --host --port 5193 --strictPort",
      url: "http://localhost:5193",
      reuseExistingServer: !process.env.CI,
    },
  ],
});

