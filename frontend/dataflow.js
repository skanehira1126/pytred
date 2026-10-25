import cytoscape from "cytoscape";
import dagre from "@dagrejs/dagre";
import hljs from "highlight.js/lib/core";
import python from "highlight.js/lib/languages/python";

hljs.registerLanguage("python", python);

function showCode(element, data) {
  if (data.implementation) {
    // Only the highlighter output is HTML; it escapes the source text itself.
    element.innerHTML = hljs.highlight(data.implementation, { language: "python" }).value;
  } else {
    element.textContent = `Source unavailable for ${data.label}.${data.description ? `\n\n${data.description}` : ""}`;
  }
}

// Dependencies use Dagre; the accumulated base follows the actual join sequence.
function arrange(cy, direction) {
  const preparation = cy.nodes().filter(node => !["root", "output", "join-step"].includes(node.data("kind")));
  const graph = new dagre.graphlib.Graph({ multigraph: true });
  graph.setGraph({ rankdir: "TB", nodesep: 60, ranksep: 80, marginx: 24, marginy: 24 });
  graph.setDefaultEdgeLabel(() => ({}));
  // Reserve the rendered node dimensions after rotating the vertical layout.
  preparation.forEach(node => graph.setNode(node.id(), direction === "LR"
    ? { width: 190, height: 64 } : { width: 64, height: 190 }));
  cy.edges('[kind = "dependency"]').forEach(edge => {
    graph.setEdge(edge.source().id(), edge.target().id(), {}, edge.id());
  });
  dagre.layout(graph);
  const positions = new Map();
  preparation.forEach(node => {
    const point = graph.node(node.id());
    positions.set(node.id(), { x: point.x + (direction === "LR" ? 240 : 140), y: point.y });
  });
  const baseline = Math.max(120, ...Array.from(positions.values(), point => point.y + (direction === "LR" ? 160 : 260)));
  positions.set("root", { x: 110, y: baseline });
  let x = 110;
  cy.nodes('[kind = "join-step"]').sort((a, b) => a.data("sequence") - b.data("sequence")).forEach(node => {
    const table = node.incomers('edge[kind = "join"]').source();
    x = Math.max(x + (direction === "LR" ? 260 : 140), positions.get(table.id()).x);
    positions.set(node.id(), { x, y: baseline });
  });
  positions.set("output", { x: x + (direction === "LR" ? 260 : 140), y: baseline });
  cy.nodes().positions(node => {
    const point = positions.get(node.id());
    return direction === "LR" ? point : { x: baseline - point.y + 110, y: point.x };
  });
  cy.fit(undefined, 40);
}

function render({ elements, direction: initialDirection }, el) {
  const root = document.createElement("section");
  root.className = "pytred-dataflow";
  // Pipeline names and metadata are always inserted as text.
  root.innerHTML = `
    <div class="pytred-toolbar">
      <strong>pytred <span>/ Pipeline viewer</span></strong>
      <label>Direction <select class="pytred-direction" aria-label="Graph direction">
        <option value="TB">Top to bottom</option><option value="LR">Left to right</option>
      </select></label>
      <button type="button" class="pytred-fit">Fit</button>
      <button type="button" class="pytred-reset">Reset</button>
    </div>
    <div class="pytred-legend"><span class="base-key">━━ Base / join sequence</span> · Gray: dependency · <span class="join-key">┄ Joined table</span> · Orange: missing input</div>
    <div class="pytred-workspace">
    <div class="pytred-graph">
      <div class="pytred-canvas" role="img" aria-label="Pipeline dependencies. Use the node selector below for details."></div>
      <div class="pytred-code-preview" hidden>
        <strong class="pytred-code-title"></strong>
        <pre></pre>
        <span>Click the node to inspect the full implementation.</span>
      </div>
    </div>
    <div class="pytred-inspector">
      <h2 class="pytred-selected-title">Inspect pipeline</h2>
      <label>Inspect <select class="pytred-node-select" aria-label="Inspect node"></select></label>
      <p class="pytred-details" aria-live="polite"></p>
      <h3>Source</h3>
      <pre class="pytred-implementation" tabindex="0" aria-label="Selected node implementation" hidden></pre>
    </div>
    </div>`;
  el.append(root);
  const canvas = root.querySelector(".pytred-canvas");
  const direction = root.querySelector(".pytred-direction");
  const picker = root.querySelector(".pytred-node-select");
  const title = root.querySelector(".pytred-selected-title");
  const details = root.querySelector(".pytred-details");
  const implementation = root.querySelector(".pytred-implementation");
  const preview = root.querySelector(".pytred-code-preview");
  const previewTitle = preview.querySelector("strong");
  const previewCode = preview.querySelector("pre");
  const cy = cytoscape({
    container: canvas,
    elements: [],
    layout: { name: "preset" },
    selectionType: "single",
    boxSelectionEnabled: false,
    minZoom: 0.08,
    maxZoom: 3,
    style: [
      { selector: "node", style: {
        "label": "data(label)", "shape": "round-rectangle", "width": 190, "height": 64,
        "background-color": "#edf0f4", "border-width": 1.5, "border-color": "#94a3b8",
        "color": "#172033", "font-size": 13, "text-valign": "center", "text-halign": "center",
        "text-wrap": "ellipsis", "text-max-width": 170,
      } },
      { selector: 'node[kind = "input"], node[kind = "root"]', style: {
        "background-color": "#e0f2fe", "border-color": "#0284c7",
      } },
      { selector: 'node[kind = "output"]', style: {
        "background-color": "#dcfce7", "border-color": "#16a34a", "border-width": 3,
      } },
      { selector: 'node[kind = "missing"]', style: {
        "background-color": "#fff7ed", "border-color": "#ea580c", "border-style": "dashed",
      } },
      { selector: "edge", style: {
        "curve-style": "bezier", "target-arrow-shape": "triangle", "width": 1.8,
        "line-color": "#94a3b8", "target-arrow-color": "#94a3b8", "label": "data(label)",
        "font-size": 11, "color": "#334155", "text-wrap": "wrap", "text-max-width": 160,
        "text-background-color": "#ffffff", "text-background-opacity": 0.95, "text-background-padding": 3,
      } },
      { selector: 'edge[kind = "join"]', style: {
        "line-style": "dashed", "line-color": "#16a34a", "target-arrow-color": "#16a34a",
      } },
      { selector: 'node[kind = "join-step"]', style: {
        "background-color": "#e0f2fe", "border-color": "#3b82f6", "border-width": 2,
        "label": node => `${node.data("label")}\n${node.data("keys").length ? `on: ${node.data("keys").join(", ")}` : "cross product"}`,
        "text-wrap": "wrap",
      } },
      { selector: 'edge[kind = "base"]', style: {
        "width": 3, "line-color": "#2563eb", "target-arrow-color": "#2563eb",
      } },
      { selector: ".pytred-dim", style: { "opacity": 0.18 } },
      { selector: "node:selected", style: { "border-width": 3, "border-color": "#7c3aed" } },
    ],
  });

  function inspectNode() {
    preview.hidden = true;
    implementation.hidden = true;
    implementation.textContent = "";
    const selected = cy.nodes(":selected").first();
    cy.elements().removeClass("pytred-dim");
    if (selected.empty()) {
      title.textContent = "Inspect pipeline";
      picker.value = "";
      details.textContent = "Scroll to zoom, drag to pan or move nodes. Select a node to highlight its upstream and downstream dependencies.";
      return;
    }
    picker.value = selected.id();
    const related = selected.union(selected.predecessors()).union(selected.successors());
    cy.elements().difference(related).addClass("pytred-dim");
    const data = selected.data();
    title.textContent = data.label;
    const lines = [`Type: ${data.kind}`];
    if (data.table) lines.push(`Right table: ${data.table}`);
    if (data.kind === "output") lines.push("Joined base, followed by post_step and final filters.");
    if (data.order !== undefined && data.order >= 0) lines.push(`Execution order: ${data.order}`);
    if (data.optional) lines.push("Optional step: skipped when a required input is missing.");
    if (data.join) lines.push(`Join: ${data.join}${data.keys.length ? ` on ${data.keys.join(", ")}` : ""}`);
    if (data.kind === "missing") lines.push("This dependency has not been supplied.");
    if (data.description) lines.push(data.description);
    details.textContent = lines.join("\n");
    if (data.kind === "process") {
      showCode(implementation, data);
      implementation.hidden = false;
    }
  }

  function update() {
    cy.elements().remove();
    cy.add(elements);
    direction.value = initialDirection;
    picker.replaceChildren(new Option("All nodes", ""));
    cy.nodes().forEach((node) => picker.add(new Option(node.data("label"), node.id())));
    arrange(cy, direction.value);
    inspectNode();
  }
  cy.on("select unselect", "node", inspectNode);
  cy.on("mouseover", 'node[kind = "process"]', (event) => {
    const data = event.target.data();
    previewTitle.textContent = data.label;
    showCode(previewCode, data);
    preview.hidden = false;
    const position = event.target.renderedPosition();
    const width = preview.offsetWidth;
    const height = preview.offsetHeight;
    const left = position.x + 20 + width < canvas.clientWidth
      ? position.x + 20 : position.x - width - 20;
    preview.style.left = `${Math.max(8, Math.min(left, canvas.clientWidth - width - 8))}px`;
    preview.style.top = `${Math.max(8, Math.min(position.y + 16, canvas.clientHeight - height - 8))}px`;
  });
  cy.on("mouseout", "node", () => { preview.hidden = true; });
  cy.on("pan zoom grab", () => { preview.hidden = true; });
  root.addEventListener("keydown", (event) => {
    if (event.key === "Escape") preview.hidden = true;
  });
  cy.on("tap", (event) => { if (event.target === cy) cy.nodes().unselect(); });
  picker.addEventListener("change", () => {
    const id = picker.value;
    cy.nodes().unselect();
    if (id) cy.getElementById(id).select();
  });
  direction.addEventListener("change", () => arrange(cy, direction.value));
  root.querySelector(".pytred-fit").addEventListener("click", () => cy.fit(undefined, 32));
  root.querySelector(".pytred-reset").addEventListener("click", () => {
    cy.nodes().unselect();
    arrange(cy, direction.value);
  });
  const observer = new ResizeObserver(() => cy.resize());
  observer.observe(canvas);
  // Finish Cytoscape's initial layout before placing and fitting our two flows.
  cy.ready(() => requestAnimationFrame(update));

  window.addEventListener("pagehide", () => {
    observer.disconnect();
    cy.destroy();
    root.remove();
  }, { once: true });
}

// Each HTML output owns an isolated iframe; no Python model or comm is retained.
render(JSON.parse(document.getElementById("pytred-data").textContent), document.body);
