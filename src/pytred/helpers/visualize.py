"""Interactive notebook views of DataHub definitions, without executing tables."""

from __future__ import annotations

from html import escape
import inspect
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Literal

import anywidget
import traitlets

from pytred.helpers.decorator import get_metadata


if TYPE_CHECKING:
    from pytred.data_hub import DataHub


class DataflowWidget(anywidget.AnyWidget):
    """A snapshot of a pipeline displayed with bundled Cytoscape.js and Dagre.

    Pan or zoom the graph, select a node to inspect its metadata and dependencies,
    and use Fit or Reset to restore the view. Display a new widget after changing
    the pipeline definition. Rendering never executes the pipeline.
    """

    # anywidgetは_esmのJSをブラウザで読み込み、表示時にrender({model, el})を呼ぶ。
    # ここでは同梱済みファイルの本文を渡す。_cssは表示に使うスタイルシート。
    _esm = (Path(__file__).parents[1] / "static" / "widget.js").read_text(encoding="utf-8")
    _css = (Path(__file__).parents[1] / "static" / "widget.css").read_text(encoding="utf-8")

    # traitletsで型検証・変更通知を持つ属性を定義する。sync=TrueはWidget側が
    # PythonとJSの共有対象として扱う印で、JSではmodel.get("elements")等で読む。
    # elementsはノードと矢印の辞書のリスト。DataHub自体を監視する属性ではない。
    elements = traitlets.List(traitlets.Dict()).tag(sync=True)
    # 配置方向はTB（上→下）かLR（左→右）のみ許可し、初期値はTBとする。
    direction = traitlets.Enum(["TB", "LR"], default_value="TB").tag(sync=True)

    def __repr__(self):
        return f"DataflowWidget({sum('source' not in e['data'] for e in self.elements)} nodes)"


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
    for order, name, _ in tables:
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
                }
            }
        )
        if join is not None:
            edges.append(
                {
                    "data": {
                        "source": identifiers[name],
                        "target": "output",
                        "kind": "join",
                        "label": join + (": " + ", ".join(keys) if keys else ""),
                    }
                }
            )

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
    edges.append({"data": {"source": "root", "target": "output", "kind": "root", "label": ""}})
    for index, edge in enumerate(edges):
        edge["data"]["id"] = f"edge-{index}"
    return nodes + edges


def visualize_datahub(
    datahub: DataHub, *, direction: Literal["TB", "LR"] = "TB"
) -> DataflowWidget:
    """Create an interactive widget without running the DataHub.

    Requires a notebook frontend with Jupyter Widgets support. The graph libraries
    are bundled with pytred; rendering does not fetch them from a CDN.
    """
    return DataflowWidget(elements=collect_dataflow_elements(datahub), direction=direction)


def render_datahub_html(datahub: DataHub) -> str:
    """Provide a static summary for HTML-only frontends, without JavaScript."""
    names = ", ".join(escape(name) for name in datahub.table_order)
    return (
        f"<div><strong>{escape(type(datahub).__name__)}</strong>: {names}"
        "<p>Interactive graph requires a notebook frontend with Jupyter Widgets support.</p>"
        "</div>"
    )
