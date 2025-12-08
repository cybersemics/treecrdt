# TreeCRDT wa-sqlite demo

This package hosts the React demo and Playwright e2e tests for the TreeCRDT wa-sqlite build. The library code lives in `@treecrdt/wa-sqlite`; this package depends on it and exercises it in the browser.

## Scripts
- `pnpm run dev` – start Vite dev server on port 4166.
- `pnpm run build` – rebuild wa-sqlite artifacts and Vite build the demo.
- `pnpm run test:e2e` – rebuild wa-sqlite artifacts and run Playwright tests.
