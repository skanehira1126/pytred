import { test, expect } from "@playwright/test";

// 配布するJSバンドルをブラウザで読み、描画と基本操作が動くことを確認する。
// Python/Jupyterとの通信はharness.jsの簡易モデルで代用しているため、
// 実ノートブックとの接続やPython側のグラフ生成はこのテストの対象外。
test("bundled graph renders and supports selection, direction and zoom without external requests", async ({ page }) => {
  const errors = [];
  const external = [];
  page.on("pageerror", (error) => errors.push(error.message));
  // CDNなどへの通信を遮断・記録し、ローカルの同梱資産だけで表示できるか調べる。
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
  // ノードを選ぶと、その処理名と結合条件が詳細欄に表示される。
  await picker.selectOption("first");
  await expect(page.locator(".pytred-details")).toContainText("doubled");
  await picker.selectOption("combined");
  await expect(page.locator(".pytred-details")).toContainText("Join: left on id");
  // 説明文中のHTMLを文字列として表示し、実際のimg要素を生成しない。
  await expect(page.locator(".pytred-details")).toContainText('<img src=x onerror="window.injected=true">');
  await expect(page.locator(".pytred-details img")).toHaveCount(0);
  // 方向変更とホイール操作で描画が変わることを画像の差分で確認する。
  // 正解画像との比較ではなく、厳密な配置座標やズーム倍率は検証しない。
  const vertical = await graph.screenshot();
  await page.getByLabel("Graph direction").selectOption("LR");
  const horizontal = await graph.screenshot();
  expect(horizontal.equals(vertical)).toBe(false);
  await graph.hover();
  await page.mouse.wheel(0, -250);
  await expect.poll(async () => (await graph.screenshot()).equals(horizontal)).toBe(false);
  // Fitを操作でき、Resetで選択と詳細欄が初期状態に戻ることを確認する。
  // Fit後のグラフ全体の収まり具合は、このテストでは数値検証していない。
  await page.getByRole("button", { name: "Fit", exact: true }).click();
  await page.getByRole("button", { name: "Reset", exact: true }).click();
  await expect(picker).toHaveValue("");
  await expect(page.locator(".pytred-details")).toContainText("Scroll to zoom");
  // 目視確認用の画像を保存する。画像の保存自体は合否判定ではない。
  await page.screenshot({ path: "test-results/widget.png", fullPage: true });
  expect(external).toEqual([]);
  expect(errors).toEqual([]);
});

// 同一ページの2つの表示で状態が混ざらず、片方を破棄しても他方を操作できる。
// 後始末はDOMとモデル購読の解除で確認し、メモリリーク全般を測定するものではない。
test("views remain independent and release resources on disposal", async ({ page }) => {
  const errors = [];
  page.on("pageerror", (error) => errors.push(error.message));
  await page.goto("/frontend/tests/index.html");
  await page.waitForFunction(() => window.first);
  await page.evaluate(() => { window.second = window.mountFixture(); });
  // 1つ目の選択が、2つ目の選択状態へ伝わらない。
  await page.getByLabel("Inspect node").first().selectOption("first");
  await expect(page.getByLabel("Inspect node").nth(1)).toHaveValue("");
  // Python側からのデータ更新を模擬する。選択肢には未選択用の1項目があるため、
  // 更新後の1ノードは2項目、未更新の6ノードは7項目になる。
  await page.evaluate(() => {
    window.first.update("elements", [{ data: { id: "single", label: "独立した処理", kind: "process" } }]);
  });
  await expect(page.getByLabel("Inspect node").first().locator("option")).toHaveCount(2);
  await expect(page.getByLabel("Inspect node").nth(1).locator("option")).toHaveCount(7);
  // 1つ目のDOMとイベント購読が消え、リサイズ後も2つ目が動くことを確認する。
  await page.evaluate(() => window.first.cleanup());
  await expect(page.locator(".pytred-dataflow")).toHaveCount(1);
  expect(await page.evaluate(() => window.first.listenerCount())).toBe(0);
  await page.setViewportSize({ width: 700, height: 900 });
  await page.getByLabel("Inspect node").selectOption("combined");
  await expect(page.locator(".pytred-details")).toContainText("combined");
  expect(errors).toEqual([]);
});
