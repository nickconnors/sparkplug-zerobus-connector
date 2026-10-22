import { test, expect } from "@playwright/test";

const E2E = process.env.E2E === "1";
const CONNECTOR_BASE_URL = process.env.CONNECTOR_BASE_URL ?? "http://localhost:8001";

test.describe("React WebUI smoke", () => {
  test.beforeEach(() => {
    test.skip(!E2E, "Set E2E=1 and run the connector locally to enable");
  });

  test("connector loads React UI", async ({ page }) => {
    await page.goto(CONNECTOR_BASE_URL, { waitUntil: "domcontentloaded" });
    await expect(page.getByRole("heading", { name: "Sparkplug Zerobus Connector" })).toBeVisible();
  });
});
