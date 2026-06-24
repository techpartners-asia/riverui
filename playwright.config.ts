import { defineConfig, devices } from "@playwright/test";

// End-to-end smoke tests. These exist to catch the class of bug where a
// user-visible action silently no-ops because of a contract mismatch
// between backend and frontend (e.g. metadata key naming). Backend unit
// tests and frontend unit tests cannot catch that class; only a real
// browser running the real bundle can.
//
// Prerequisites:
//   1. Postgres at $DATABASE_URL (or postgres://localhost/river_test).
//   2. riverui server listening on $RIVERUI_URL (default http://localhost:8080).
//      Start with: cd /Users/.../riverui && npm run dev
//   3. The river OSS repo checked out at $RIVER_REPO (default ../river)
//      so the seed binary at cmd/seed-failed-workflow can be invoked.
//
// Run: npm run test:e2e
export default defineConfig({
  // Sequential — these tests share a database and seed real workflow rows.
  fullyParallel: false,
  projects: [
    {
      name: "chromium",
      use: { ...devices["Desktop Chrome"] },
    },
  ],
  reporter: [["list"]],
  testDir: "./tests/e2e",
  use: {
    baseURL: process.env.RIVERUI_URL ?? "http://localhost:8080",
    trace: "retain-on-failure",
  },
  workers: 1,
});
