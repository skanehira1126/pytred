import widget from "/src/pytred/static/widget.js";

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

// Implement only the AFM model event boundary; drawing uses the real bundled libraries.
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
    listenerCount: () => listeners.size,
    update(key, value) { state[key] = value; listeners.get(`change:${key}`)?.(); },
  };
};
window.first = window.mountFixture();
