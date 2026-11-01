"""Interactive notebook views of DataHub definitions, without executing tables."""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from html import escape
import inspect
import json
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING
from typing import Literal

from pytred._types import Root
from pytred.helpers.decorator import get_metadata


if TYPE_CHECKING:
    from pytred.data_hub import DataHub


@dataclass(frozen=True)
class DataflowView:
    """An HTML snapshot with browser-only interaction and no live Python model."""

    html: str = field(repr=False)

    def _repr_html_(self):
        return self.html


def _method_details(method) -> dict:
    try:
        implementation = dedent(inspect.getsource(inspect.unwrap(method))).strip()
    except (OSError, TypeError, ValueError):
        # Dynamic functions and some notebook cells have no retrievable source.
        implementation = ""
    return {"description": inspect.getdoc(method) or "", "implementation": implementation}


def _process_node_data(datahub: DataHub, name: str, order: int, node_id: str) -> dict:
    method = getattr(datahub, name)
    return {
        "id": node_id,
        "label": name,
        "kind": "process",
        "order": order,
        "join": get_metadata(method, "join") or "",
        "keys": list(get_metadata(method, "keys") or ()),
        "optional": bool(get_metadata(method, "is_optional")),
        **_method_details(method),
    }


def _raw_input_node(datahub: DataHub, name: str, index: int) -> dict:
    definition = datahub.input_tables.get(name)
    supplied = datahub.table_order.get(name) == -1
    table = datahub.tables[name] if supplied else None
    marker = definition["marker"] if definition else None
    metadata = marker.metadata if marker else None
    kind = "input" if supplied or definition else "missing"
    if isinstance(marker, Root):
        kind = "root"
    return {
        "id": "root" if kind == "root" else f"input-{index}",
        "label": name,
        "kind": kind,
        "order": -1,
        "supplied": supplied,
        "join": table.join if table and table.join else "",
        "keys": list(table.keys) if table and table.keys else [],
        "optional": False,
        "description": metadata.description or "" if metadata else "",
        "metadata": metadata.model_dump(mode="json") if metadata else None,
        "implementation": "",
        "validators": [],
    }


def _input_flow(datahub: DataHub, input_names: list[str]) -> tuple[list[dict], list[dict], dict]:
    """Build hook dependencies from raw inputs and index the tables available after prepare."""
    raw: dict[str, dict] = {}

    def raw_input(name: str) -> dict:
        if name not in raw:
            raw[name] = _raw_input_node(datahub, name, len(raw))
        return raw[name]

    for name in [*input_names, *datahub.input_tables]:
        raw_input(name)

    for name in getattr(datahub, "validate_functions", ()):
        parameters = list(inspect.signature(getattr(datahub, name)).parameters)
        for argument in parameters:
            raw_input(argument)["validators"].append({"name": name, "inputs": parameters})

    hooks = []
    edges = []
    prepared = {}
    for index, (target, name) in enumerate(getattr(datahub, "prepare_functions", {}).items()):
        original = raw_input(target)
        original["stage"] = "raw"
        method = getattr(datahub, name)
        hook_id = f"prepare-{index}"
        hooks.append(
            {
                "id": hook_id,
                "label": name,
                "kind": "prepare",
                "target_table": target,
                **_method_details(method),
            }
        )
        for argument in inspect.signature(method).parameters:
            edges.append(
                {
                    "source": raw_input(argument)["id"],
                    "target": hook_id,
                    "kind": "dependency",
                    "label": "",
                }
            )
        prepared[target] = {
            "id": f"prepared-{index}",
            "label": target,
            "kind": "prepared",
            "join": original["join"],
            "keys": original["keys"],
            "optional": False,
        }
        original.update(join="", keys=[])
        edges.append(
            {
                "source": hook_id,
                "target": prepared[target]["id"],
                "kind": "dependency",
                "label": "",
            }
        )

    return [*raw.values(), *hooks, *prepared.values()], edges, {**raw, **prepared}


def _build_join_path(table_nodes: list[dict], previous: str) -> tuple[list[dict], list[dict], str]:
    """Link the effective root and joined tables in execution order."""
    nodes = []
    edges = []
    sequence = 0
    for table in table_nodes:
        join = table["join"]
        if not join:
            continue
        sequence += 1
        join_id = f"join-{sequence}"
        name = table["label"]
        nodes.append(
            {
                "id": join_id,
                "label": f"{sequence:02d}  {join.upper()} JOIN",
                "kind": "join-step",
                "join": join,
                "keys": table["keys"].copy(),
                "table": name,
                "optional": table["optional"],
                "sequence": sequence,
                "description": "Join the accumulated base table with this table.",
            }
        )
        edges.append({"source": previous, "target": join_id, "kind": "base", "label": ""})
        edges.append({"source": table["id"], "target": join_id, "kind": "join", "label": ""})
        previous = join_id
    return nodes, edges, previous


def _build_dependencies(
    tables: list[tuple[int, str, list[str]]], identifiers: dict[str, str]
) -> tuple[list[dict], list[dict]]:
    missing_nodes: list[dict] = []
    edges = []
    for _, name, arguments in tables:
        for argument in arguments:
            if argument not in identifiers:
                node_id = f"missing-{len(missing_nodes)}"
                identifiers[argument] = node_id
                missing_nodes.append({"id": node_id, "label": argument, "kind": "missing"})
            edges.append(
                {
                    "source": identifiers[argument],
                    "target": identifiers[name],
                    "kind": "dependency",
                    "label": "",
                }
            )
    return missing_nodes, edges


def collect_dataflow_elements(datahub: DataHub) -> list[dict]:
    """Describe declared dependencies and joins as JSON-compatible graph elements.

    Node IDs are independent of user-provided names. Missing arguments are shown
    explicitly. Execution order is metadata, not a graph rank or an edge length.
    """
    tables = list(
        datahub.collect_table_and_arguments(datahub.table_order, include_input_table=True)
    )
    nodes, edges, effective = _input_flow(
        datahub, [name for order, name, _ in tables if order == -1]
    )
    root = next((node for node in nodes if node["kind"] == "root"), None)
    if root is None:
        nodes.append({"id": "root", "label": "root_df", "kind": "root"})
        base = "root"
    else:
        base = effective[root["label"]]["id"]

    processes = [(order, name, args) for order, name, args in tables if order >= 0]
    process_nodes = [
        _process_node_data(datahub, name, order, f"table-{index}")
        for index, (order, name, _) in enumerate(processes)
    ]
    nodes.extend(process_nodes)
    join_nodes, join_edges, last_base = _build_join_path(
        [*effective.values(), *process_nodes], base
    )
    identifiers = {name: node["id"] for name, node in effective.items()}
    identifiers.update({node["label"]: node["id"] for node in process_nodes})
    missing_nodes, dependencies = _build_dependencies(processes, identifiers)
    nodes.extend(join_nodes)
    nodes.extend(missing_nodes)
    edges.extend(join_edges)
    nodes.append({"id": "output", "label": "Output", "kind": "output"})
    edges.extend(dependencies)
    edges.append({"source": last_base, "target": "output", "kind": "base", "label": ""})
    for index, edge in enumerate(edges):
        edge["id"] = f"edge-{index}"
    return [{"data": item} for item in nodes + edges]


def visualize_datahub(datahub: DataHub, *, direction: Literal["TB", "LR"] = "LR") -> DataflowView:
    """Create a displayable HTML snapshot without executing the DataHub.

    Requires trusted HTML output with JavaScript and iframe support. The graph
    libraries are bundled; no widget manager, comm, or CDN is used.
    """
    return DataflowView(render_datahub_html(datahub, direction=direction))


def render_datahub_html(datahub: DataHub, *, direction: Literal["TB", "LR"] = "LR") -> str:
    """Embed graph data and bundled assets in an isolated HTML document."""
    if direction not in ("TB", "LR"):
        raise ValueError("direction must be 'TB' or 'LR'")
    assets = Path(__file__).parents[1] / "static"
    script = (assets / "dataflow.js").read_text(encoding="utf-8")
    style = (assets / "dataflow.css").read_text(encoding="utf-8")
    # JSON is script text, not HTML: escape '<' so names/docstrings cannot close
    # the data script. The entire document is then escaped for the srcdoc attribute.
    payload = json.dumps(
        {"elements": collect_dataflow_elements(datahub), "direction": direction}
    ).replace("<", "\\u003c")
    document = (
        '<!doctype html><html><head><meta charset="utf-8">'
        f"<style>body {{ margin: 0; }}{style}</style></head><body>"
        f'<script type="application/json" id="pytred-data">{payload}</script>'
        f"<script>{script}</script></body></html>"
    )
    names = ", ".join(escape(name) for name in datahub.table_order)
    return (
        f"<div><strong>{escape(type(datahub).__name__)}</strong>: {names}"
        '<iframe title="DataHub pipeline graph" sandbox="allow-scripts" '
        'style="width:100%;height:760px;border:0" '
        f'srcdoc="{escape(document, quote=True)}"></iframe></div>'
    )
