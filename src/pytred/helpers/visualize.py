from __future__ import annotations

import pathlib
import subprocess
import sys
import tempfile
from typing import TYPE_CHECKING
from typing import Literal

from pytred.data_node import EmptyDataNode
from pytred.helpers.dataflow import DataEdge
from pytred.helpers.dataflow import DataflowGraph
from pytred.helpers.dataflow import DataflowNode
from pytred.helpers.decorator import get_metadata
from pytred.helpers.md_tabulator import MarkdownTableTabulator


if TYPE_CHECKING:
    from pytred.data_hub import DataHub


TEMPLATE = """## {datahub_name}
{datahub_description}

### {datahub_name} detail
{detail}

### Dataflow image
```mermaid
{mermaid}
```
"""


def report_datahub(datahub_class: type[DataHub], *tables: EmptyDataNode) -> str:
    """
    Make report of datahub with markdown format.
    This report has contents below.
    1. docstrings of datahub class
    2. image of dataflow
    3. tables about each tables in datahub

    Parameters
    -----
    datahub_class: DataHub class
        make report of this datahub
    *tables: EmptyDataNode
        EmptyDataNode used in input DataHub class

    Returns
    -------
    str
        report of this datahub
    """

    template_variables = {
        "datahub_name": datahub_class.__name__,
        "datahub_description": trim(datahub_class.__doc__).replace("\n", "  \n"),
    }

    # make graph
    graph, dataflow_nodes = make_dataflow_graph_from_datahub(datahub_class, *tables)
    template_variables["mermaid"] = str(graph)

    # make table detail
    md_tabulator = MarkdownTableTabulator(mode="row")
    for node in dataflow_nodes:
        params = {
            "order": node.level,
            "name": node.name,
        }
        if node.level == -1:
            params["table_type"] = "input"
            params["description"] = ""
        else:
            params["table_type"] = "function"
            params["description"] = trim(getattr(datahub_class, node.name).__doc__).replace(
                "\n", "<br>"
            )

        if node.join is None:
            params["join"] = ""
            params["keys"] = ""
        else:
            params["join"] = node.join
            params["keys"] = ", ".join(node.keys) if node.keys is not None else ""

        md_tabulator.add_rows(params)

    template_variables["detail"] = md_tabulator.build(index=False)

    return TEMPLATE.format(**template_variables)


def make_dataflow_graph_from_datahub(
    datahub_class: type[DataHub],
    *tables: EmptyDataNode,
    output: str | pathlib.Path | None = None,
    direction: Literal["TD", "LR"] = "TD",
) -> tuple[DataflowGraph, list[DataflowNode]]:
    """
    Visualize the hierarchical structure of data preprocessing nodes from DataHub.

    Parameters
    ----------
    datahub: DataHub class
        visualizing dataflow of this DataHub
    output: str or pathlib.Path, optional
        file path of dataflow image
    direction: {'TD', 'LR'}, default 'TD'
        graph direction
    *tables: EmptyDataNode
        DataNode used in input DataHub class

    """
    nodes = search_tables(datahub_class, *tables)
    graph = get_dataflow_graph(nodes, direction)

    if output is not None:
        with tempfile.TemporaryDirectory() as tmpdir:
            file_path = pathlib.Path(tmpdir, "temp.mmd")
            with file_path.open("w") as f:
                f.write(str(graph))

            subprocess.run(["mmdc", "-i", file_path.as_posix(), "-o", str(output)])

    return graph, nodes


def trim(docstring: str | None) -> str:
    """
    ref.: https://peps.python.org/pep-0257/
    """
    if not docstring:
        return ""
    # Convert tabs to spaces (following the normal Python rules)
    # and split into a list of lines:
    lines = docstring.expandtabs().splitlines()
    # Determine minimum indentation (first line doesn't count):
    indent = sys.maxsize
    for line in lines[1:]:
        stripped = line.lstrip()
        if stripped:
            indent = min(indent, len(line) - len(stripped))
    # Remove indentation (first line is special):
    trimmed = [lines[0].strip()]
    if indent < sys.maxsize:
        for line in lines[1:]:
            trimmed.append(line[indent:].rstrip())
    # Strip off trailing and leading blank lines:
    while trimmed and not trimmed[-1]:
        trimmed.pop()
    while trimmed and not trimmed[0]:
        trimmed.pop(0)
    # Return a single string:
    return "\n".join(trimmed)


def collect_dataflow_nodes(
    datahub: type[DataHub] | DataHub,
    table_order: dict[str, int],
    *input_tables: EmptyDataNode,
) -> list[DataflowNode]:
    """Build display nodes from execution metadata without executing any tables."""
    nodes = [
        DataflowNode(table.name, keys=table.keys, join=table.join, level=-1, shape="[()]")
        for table in input_tables
    ]
    for order, name, arguments in datahub.collect_table_and_arguments(
        table_order, include_input_table=True
    ):
        if order == -1:
            join = None
            keys = None
            shape = "[()]"
        else:
            function = getattr(datahub, name)
            join = get_metadata(function, "join")
            keys = get_metadata(function, "keys")
            shape = "[]" if join is None else "([])"
        node = DataflowNode(name, keys=keys, join=join, level=order, shape=shape)
        for argument in arguments:
            for parent in nodes:
                if parent.name == argument:
                    parent.add_child(node)
        nodes.append(node)
    return nodes


def search_tables(datahub_class: type[DataHub], *tables: EmptyDataNode) -> list[DataflowNode]:
    """Collect display nodes from a DataHub class and its declared inputs."""
    if datahub_class.registerd_tables_order is None:
        raise ValueError("Functions as table are not found.")
    return collect_dataflow_nodes(datahub_class, datahub_class.registerd_tables_order, *tables)


def get_dataflow_graph(
    nodes: list[DataflowNode], direction: Literal["TD", "LR"] = "TD"
) -> DataflowGraph:
    """
    Get the hierarchical structure of data preprocessing nodes.

    Given a list of ProcessingNode objects, which each have a name, level, and children,
    this function creates a directed graph that visualizes the dependencies between the nodes.
    Nodes at the same level are grouped together, and dependencies are shown with edges.

    Parameters
    ----------
    nodes: list of DataflowNode
        A list of dataflow nodes, each with a name, level, and children.
    direction: {'TD', 'LR'}, default 'TD'
        graph direction

    Returns
    -------
    DataflowGraph
        A DataflowGraph object that can be rendered to visualize the tree structure.
    """

    graph = DataflowGraph(graph_direction=direction)

    max_level = max([node.level for node in nodes])

    for _node in sorted(nodes, key=lambda x: x.level):
        graph.add_node(_node)

        # add edge to children
        for _child_node in _node.children:
            level_diff = abs(_node.level - _child_node.level)
            link_type = "-" * level_diff + "->"
            edge = DataEdge(_node.name, _child_node.name, link_type=link_type)
            graph.add_edge(edge)

        # if node does not parents, make invisible edge
        if len(_node.parents) == 0 and _node.level >= 0:
            # target node index of invisible edge
            nodes_in_level = graph.get_nodes_by_level(_node.level)
            nodes_in_before_level = graph.get_nodes_by_level(_node.level - 1)

            target_node_index = min(len(nodes_in_level), len(nodes_in_before_level)) - 1

            invisible_edge = DataEdge(
                nodes_in_before_level[target_node_index].name,
                _node.name,
                link_type="~~~",
            )
            graph.add_edge(invisible_edge)

        # add edge to output table
        if _node.join is not None:
            level_diff = max_level - _node.level
            if _node.keys is None:
                keys = ""
            else:
                keys = "<br>" + "<br>".join([f"- {key}" for key in _node.keys])
            link_type = "-" * (level_diff + 1) + f"->|{_node.join}{keys}|"
            edge = DataEdge(_node.name, "root_df", link_type=link_type)
            graph.add_edge(edge)

    graph.add_node(
        DataflowNode(name="root_df", join=None, keys=None, level=max_level + 1, shape="[[]]")
    )

    return graph


def render_datahub_html(datahub: DataHub) -> str:
    """Render an instance's dataflow for notebook display without executing it."""
    nodes = collect_dataflow_nodes(datahub, datahub.table_order)
    graph = get_dataflow_graph(nodes)
    html = (
        """
    <div class="mermaid">
    """
        + str(graph)
        + """
    </div>
    <script>
    if (window.mermaid) {{
        window.mermaid.contentLoaded();
    }}
    </script>
    """
    )
    return html
