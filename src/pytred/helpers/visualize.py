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


def collect_dataflow_elements(datahub: DataHub) -> list[dict]:
    """Describe declared dependencies and joins as JSON-compatible graph elements.

    Node IDs are independent of user-provided names. Missing arguments are shown
    explicitly. Execution order is metadata, not a graph rank or an edge length.
    """
    tables = list(
        datahub.collect_table_and_arguments(datahub.table_order, include_input_table=True)
    )
    identifiers = {name: f"table-{index}" for index, (_, name, _) in enumerate(tables)}
    nodes = []
    edges = []
    previous = "root"
    join_index = 0
    for order, name, _ in tables:
        implementation = ""
        if order == -1:
            table = datahub.tables[name]
            join, keys = table.join, table.keys
            description = ""
            optional = False
        else:
            function = getattr(datahub, name)
            join = get_metadata(function, "join")
            keys = get_metadata(function, "keys")
            description = inspect.getdoc(function) or ""
            optional = bool(get_metadata(function, "is_optional"))
            try:
                implementation = dedent(inspect.getsource(inspect.unwrap(function))).strip()
            except (OSError, TypeError, ValueError):
                # Dynamic functions and some notebook cells have no retrievable source.
                pass
        nodes.append(
            {
                "data": {
                    "id": identifiers[name],
                    "label": name,
                    "kind": "input" if order == -1 else "process",
                    "order": order,
                    "join": join or "",
                    "keys": list(keys) if keys is not None else [],
                    "description": description,
                    "optional": optional,
                    "implementation": implementation,
                }
            }
        )
        if join is not None:
            join_index += 1
            join_id = f"join-{join_index}"
            nodes.append(
                {
                    "data": {
                        "id": join_id,
                        "label": f"{join_index:02d}  {join.upper()} JOIN",
                        "kind": "join-step",
                        "join": join,
                        "keys": list(keys) if keys is not None else [],
                        "table": name,
                        "optional": optional,
                        "sequence": join_index,
                        "description": "Join the accumulated base table with this table.",
                    }
                }
            )
            edges.append(
                {"data": {"source": previous, "target": join_id, "kind": "base", "label": ""}}
            )
            edges.append(
                {
                    "data": {
                        "source": identifiers[name],
                        "target": join_id,
                        "kind": "join",
                        "label": "",
                    }
                }
            )
            previous = join_id

    for _, name, arguments in tables:
        for argument in arguments:
            if argument not in identifiers:
                identifiers[argument] = f"table-{len(identifiers)}"
                nodes.append(
                    {
                        "data": {
                            "id": identifiers[argument],
                            "label": argument,
                            "kind": "missing",
                        }
                    }
                )
            edges.append(
                {
                    "data": {
                        "source": identifiers[argument],
                        "target": identifiers[name],
                        "kind": "dependency",
                        "label": "",
                    }
                }
            )

    nodes.extend(
        [
            {"data": {"id": "root", "label": "root_df", "kind": "root"}},
            {"data": {"id": "output", "label": "Output", "kind": "output"}},
        ]
    )
    edges.append({"data": {"source": previous, "target": "output", "kind": "base", "label": ""}})
    for index, edge in enumerate(edges):
        edge["data"]["id"] = f"edge-{index}"
    return nodes + edges


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
