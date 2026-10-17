import { test, expect } from "@playwright/test";

test("bundled graph renders and supports selection, direction and zoom without external requests", async ({ page }) => {
  const errors = [];
  const external = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.route("**/*", (route) => {
    const url = new URL(route.request().url());
    if (url.hostname !== "127.0.0.1") {
      external.push(url.href);
      return route.abort();
    }
    return route.continue();
  });
  await page.goto("/frontend/tests/index.html");
  const graph = page.locator(".pytred-canvas");
  await expect(graph.locator("canvas").first()).toBeVisible();
  const picker = page.getByLabel("Inspect node");
  await picker.selectOption("first");
  await expect(page.locator(".pytred-details")).toContainText("doubled");
  await picker.selectOption("combined");
  await expect(page.locator(".pytred-details")).toContainText("Join: left on id");
  await expect(page.locator(".pytred-details")).toContainText('<img src=x onerror="window.injected=true">');
  await expect(page.locator(".pytred-details img")).toHaveCount(0);
  const vertical = await graph.screenshot();
  await page.getByLabel("Graph direction").selectOption("LR");
  const horizontal = await graph.screenshot();
  expect(horizontal.equals(vertical)).toBe(false);
  await graph.hover();
  await page.mouse.wheel(0, -250);
  await expect.poll(async () => (await graph.screenshot()).equals(horizontal)).toBe(false);
  await page.getByRole("button", { name: "Fit", exact: true }).click();
  await page.getByRole("button", { name: "Reset", exact: true }).click();
  await expect(picker).toHaveValue("");
  await expect(page.locator(".pytred-details")).toContainText("Scroll to zoom");
  await page.screenshot({ path: "test-results/widget.png", fullPage: true });
  expect(external).toEqual([]);
  expect(errors).toEqual([]);
});

test("views remain independent and release resources on disposal", async ({ page }) => {
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto("/frontend/tests/index.html");
  await page.waitForFunction(() => window.first);
  await page.evaluate(() => { window.second = window.mountFixture(); });
  await page.getByLabel("Inspect node").first().selectOption("first");
  await expect(page.getByLabel("Inspect node").nth(1)).toHaveValue("");
  await page.evaluate(() => {
    window.first.update("elements", [{ data: { id: "single", label: "独立した処理", kind: "process" } }]);
  });
  await expect(page.getByLabel("Inspect node").first().locator("option")).toHaveCount(2);
  await expect(page.getByLabel("Inspect node").nth(1).locator("option")).toHaveCount(7);
  await page.evaluate(() => window.first.cleanup());
  await expect(page.locator(".pytred-dataflow")).toHaveCount(1);
  expect(await page.evaluate(() => window.first.listenerCount())).toBe(0);
  await page.setViewportSize({ width: 700, height: 900 });
  await page.getByLabel("Inspect node").selectOption("combined");
  await expect(page.locator(".pytred-details")).toContainText("combined");
  expect(errors).toEqual([]);
});
