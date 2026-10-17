import widget from "/src/pytred/static/widget.js";

// 分岐→合流とroot→outputを持つ固定グラフ。結合情報・日本語・HTML風の説明を
// 含め、表示と操作を確認する。実データの処理やPython側の依存解析は行わない。
const elements = [
  { data: { id: "source", label: "source", kind: "input", order: -1 } },
  { data: { id: "first", label: "doubled", kind: "process", order: 0 } },
  { data: { id: "second", label: "offset", kind: "process", order: 0 } },
  { data: { id: "combined", label: "combined", kind: "process", order: 10,
    join: "left", keys: ["id"], description: '<img src=x onerror="window.injected=true"> & 日本語' } },
  { data: { id: "root", label: "root_df", kind: "root" } },
  { data: { id: "output", label: "Output", kind: "output" } },
  { data: { id: "e1", source: "source", target: "first", kind: "dependency", label: "" } },
  { data: { id: "e2", source: "source", target: "second", kind: "dependency", label: "" } },
  { data: { id: "e3", source: "first", target: "combined", kind: "dependency", label: "" } },
  { data: { id: "e4", source: "second", target: "combined", kind: "dependency", label: "" } },
  { data: { id: "e5", source: "combined", target: "output", kind: "join", label: "left: id" } },
  { data: { id: "e6", source: "root", target: "output", kind: "root", label: "" } },
];

// anywidgetのモデルから値を読む・変更を購読する部分だけを簡易実装する。
// 描画処理はモックせず、配布用バンドルのCytoscape.jsとDagreをそのまま使う。
// 各イベントの購読者が1つという現在のwidget実装に必要な範囲に限定している。
window.mountFixture = () => {
  const el = document.createElement("div");
  document.body.append(el);
  const listeners = new Map();
  const state = { elements: structuredClone(elements), direction: "TB" };
  const model = {
    get: (key) => state[key],
    on: (key, fn) => listeners.set(key, fn),
    off: (key) => listeners.delete(key),
  };
  const cleanup = widget.render({ model, el });
  return {
    cleanup,
    // cleanupがモデルの購読を解除したことを、テストから観測する。
    listenerCount: () => listeners.size,
    // 値の変更とchangeイベントを模擬し、描画側の更新処理を呼び出す。
    update(key, value) { state[key] = value; listeners.get(`change:${key}`)?.(); },
  };
};
window.first = window.mountFixture();
