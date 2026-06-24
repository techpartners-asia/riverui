import { expect, test } from "@playwright/test";
import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const HERE = dirname(fileURLToPath(import.meta.url));
const RIVER_REPO = process.env.RIVER_REPO ?? resolve(HERE, "../../../river");
const SEED_PKG = "./cmd/seed-failed-workflow";

const seedFailedWorkflow = (): string => {
  if (!existsSync(`${RIVER_REPO}/cmd/seed-failed-workflow/main.go`)) {
    throw new Error(
      `seed binary not found at ${RIVER_REPO}/cmd/seed-failed-workflow. ` +
        `Set RIVER_REPO env var to the OSS river checkout.`,
    );
  }
  const stdout = execFileSync("go", ["run", SEED_PKG], {
    cwd: RIVER_REPO,
    encoding: "utf8",
    env: {
      ...process.env,
      DATABASE_URL:
        process.env.DATABASE_URL ?? "postgres://localhost/river_test",
    },
  });
  // Seeder prints: "seeded workflow failed-demo-<unix-ts> with ..."
  const match = stdout.match(/seeded workflow (\S+)/);
  if (!match) throw new Error(`could not parse workflow ID from: ${stdout}`);
  return match[1];
};

test.describe("workflow retry", () => {
  test("clicking Retry fires POST and resets failed tasks", async ({
    page,
  }) => {
    const workflowID = seedFailedWorkflow();

    // Listen for the retry POST BEFORE clicking, so a silent no-op is caught
    // by the timeout on waitForRequest rather than being interpreted as
    // "test passed because nothing went wrong."
    const retryRequest = page.waitForRequest(
      (req) =>
        req.url().endsWith(`/api/pro/workflows/${workflowID}/retry`) &&
        req.method() === "POST",
      { timeout: 10_000 },
    );
    const retryResponse = page.waitForResponse(
      (resp) =>
        resp.url().endsWith(`/api/pro/workflows/${workflowID}/retry`) &&
        resp.request().method() === "POST",
      { timeout: 10_000 },
    );

    await page.goto(`/workflows/${workflowID}`);

    // Wait for the page to actually load the workflow (Retry button only
    // enables once tasks render and a workflowID is extractable).
    const retryButton = page.getByRole("button", { name: /^Retry$/ });
    await expect(retryButton).toBeEnabled({ timeout: 10_000 });

    await retryButton.click();
    await page.locator("#retry-mode-failed-downstream").check();
    await page.getByRole("button", { name: /Re-run jobs/ }).click();

    // The two assertions that would have caught the original bug: a real
    // network request must fire, and it must succeed.
    const req = await retryRequest;
    const resp = await retryResponse;
    expect(resp.status()).toBe(200);

    // Body sanity-check: server reports >=1 retried_jobs.
    const body = await resp.json();
    expect(Array.isArray(body.retried_jobs)).toBe(true);
    expect(body.retried_jobs.length).toBeGreaterThan(0);

    // Sanity-check the request body included the workflow ID via the URL,
    // not as a payload — pin the URL contract here so a future refactor
    // that changes the path also breaks this test.
    expect(req.url()).toContain(`/api/pro/workflows/${workflowID}/retry`);
  });
});
