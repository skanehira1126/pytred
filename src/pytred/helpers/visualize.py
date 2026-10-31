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

from pytred.helpers.decorator import get_metadata


if TYPE_CHECKING:
    from pytred.data_hub import DataHub


@dataclass(frozen=True)
class DataflowView:
    """An HTML snapshot with browser-only interaction and no live Python model."""

    html: str = field(repr=False)

    def _repr_html_(self):
        return self.html


def _table_node_data(datahub: DataHub, name: str, order: int, node_id: str) -> dict:
    if order == -1:
        table = datahub.tables[name]
        definition = datahub.input_tables.get(name)
        description = ""
        metadata = None
        if definition is not None and definition["marker"].metadata is not None:
            declared = definition["marker"].metadata
            description = declared.description or ""
            metadata = declared.model_dump(mode="json")

        return {
            "id": node_id,
            "label": name,
            "kind": "input",
            "order": order,
            "join": table.join or "",
            "keys": list(table.keys or ()),
            "description": description,
            "optional": False,
            "implementation": "",
            "metadata": metadata,
        }

    function = getattr(datahub, name)
    try:
        implementation = dedent(inspect.getsource(inspect.unwrap(function))).strip()
    except (OSError, TypeError, ValueError):
        # Dynamic functions and some notebook cells have no retrievable source.
        implementation = ""
    return {
        "id": node_id,
        "label": name,
        "kind": "process",
        "order": order,
        "join": get_metadata(function, "join") or "",
        "keys": list(get_metadata(function, "keys") or ()),
        "description": inspect.getdoc(function) or "",
        "optional": bool(get_metadata(function, "is_optional")),
        "implementation": implementation,
    }


def _build_join_path(table_nodes: list[dict]) -> tuple[list[dict], list[dict], str]:
    """Interleave tables with their join steps and return the last base node's ID."""
    nodes = []
    edges = []
    previous = "root"
    sequence = 0
    for table in table_nodes:
        nodes.append(table)
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
    missing_nodes = []
    edges = []
    for _, name, arguments in tables:
        for argument in arguments:
            if argument not in identifiers:
                node_id = f"table-{len(identifiers)}"
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
    identifiers = {name: f"table-{index}" for index, (_, name, _) in enumerate(tables)}
    table_nodes = [
        _table_node_data(datahub, name, order, identifiers[name]) for order, name, _ in tables
    ]
    nodes, edges, last_base = _build_join_path(table_nodes)
    missing_nodes, dependencies = _build_dependencies(tables, identifiers)
    nodes.extend(missing_nodes)
    nodes.append({"id": "root", "label": "root_df", "kind": "root"})
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
