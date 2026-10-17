import cytoscape from "cytoscape";
import dagre from "@dagrejs/dagre";

// Dagre owns placement; execution-order numbers never become coordinates.
function arrange(cy, direction) {
  const graph = new dagre.graphlib.Graph({ multigraph: true });
  graph.setGraph({ rankdir: direction, nodesep: 40, ranksep: 90, marginx: 24, marginy: 24 });
  graph.setDefaultEdgeLabel(() => ({}));
  cy.nodes().forEach((node) => {
    graph.setNode(node.id(), { width: node.outerWidth(), height: node.outerHeight() });
  });
  cy.edges().forEach((edge) => {
    graph.setEdge(edge.source().id(), edge.target().id(), {}, edge.id());
  });
  dagre.layout(graph);
  cy.nodes().positions((node) => graph.node(node.id()));
  cy.fit(undefined, 32);
}

function render({ model, el }) {
  const root = document.createElement("section");
  root.className = "pytred-dataflow";
  // Only fixed markup goes through innerHTML. Pipeline names and docs use textContent.
  root.innerHTML = `
    <div class="pytred-toolbar">
      <strong>Pipeline</strong>
      <label>Direction <select class="pytred-direction" aria-label="Graph direction">
        <option value="TB">Top to bottom</option><option value="LR">Left to right</option>
      </select></label>
      <button type="button" class="pytred-fit">Fit</button>
      <button type="button" class="pytred-reset">Reset</button>
    </div>
    <div class="pytred-legend">Blue: input · Gray: step · Green: output · Dashed: join · Orange: missing input</div>
    <div class="pytred-canvas" role="img" aria-label="Pipeline dependencies. Use the node selector below for details."></div>
    <div class="pytred-inspector">
      <label>Inspect <select class="pytred-node-select" aria-label="Inspect node"></select></label>
      <p class="pytred-details" aria-live="polite"></p>
    </div>`;
  el.append(root);
  const canvas = root.querySelector(".pytred-canvas");
  const direction = root.querySelector(".pytred-direction");
  const picker = root.querySelector(".pytred-node-select");
  const details = root.querySelector(".pytred-details");
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
      { selector: ".pytred-dim", style: { "opacity": 0.18 } },
      { selector: "node:selected", style: { "border-width": 3, "border-color": "#7c3aed" } },
    ],
  });

  function inspectNode() {
    const selected = cy.nodes(":selected").first();
    cy.elements().removeClass("pytred-dim");
    if (selected.empty()) {
      picker.value = "";
      details.textContent = "Scroll to zoom, drag to pan or move nodes. Select a node to highlight its upstream and downstream dependencies.";
      return;
    }
    picker.value = selected.id();
    const related = selected.union(selected.predecessors()).union(selected.successors());
    cy.elements().difference(related).addClass("pytred-dim");
    const data = selected.data();
    const lines = [data.label, `Type: ${data.kind}`];
    if (data.order !== undefined && data.order >= 0) lines.push(`Execution order: ${data.order}`);
    if (data.optional) lines.push("Optional step: skipped when a required input is missing.");
    if (data.join) lines.push(`Join: ${data.join}${data.keys.length ? ` on ${data.keys.join(", ")}` : ""}`);
    if (data.kind === "missing") lines.push("This dependency has not been supplied.");
    if (data.description) lines.push(data.description);
    details.textContent = lines.join("\n");
  }

  function update() {
    cy.elements().remove();
    cy.add(model.get("elements"));
    direction.value = model.get("direction");
    picker.replaceChildren(new Option("All nodes", ""));
    cy.nodes().forEach((node) => picker.add(new Option(node.data("label"), node.id())));
    arrange(cy, direction.value);
    inspectNode();
  }
  function changeDirection() {
    direction.value = model.get("direction");
    arrange(cy, direction.value);
  }
  cy.on("select unselect", "node", inspectNode);
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
  model.on("change:elements", update);
  model.on("change:direction", changeDirection);
  const observer = new ResizeObserver(() => cy.resize());
  observer.observe(canvas);
  update();

  return () => {
    model.off("change:elements", update);
    model.off("change:direction", changeDirection);
    observer.disconnect();
    cy.destroy();
    root.remove();
  };
}

export default { render };
