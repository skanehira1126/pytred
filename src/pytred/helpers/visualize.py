from __future__ import annotations

import pathlib
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from dataclasses import field
from typing import Literal
from typing import Type

import polars as pl

from pytred.data_hub import DataHub
from pytred.data_node import DataEdge
from pytred.data_node import DataflowNode
from pytred.data_node import DummyDataNode


@dataclass
class DataflowGraph:
    nodes: list[DataflowNode] = field(default_factory=list)
    edges: list[DataEdge] = field(default_factory=list)
    graph_direction: Literal["LR", "TD"] = "TD"

    def add_node(self, node: DataflowNode):
        self.nodes.append(node)

    def add_edge(self, edge: DataEdge):
        self.edges.append(edge)

    def __str__(self):
        graph_str = f"graph {self.graph_direction}\n"
        for node in self.nodes:
            graph_str += f"    {node.fmt_mermaid()}\n"
        for edge in self.edges:
            graph_str += f"    {edge.fmt_mermaid()}\n"
        return graph_str


TEMPLATE = """## {datahub_name}
{datahub_description}

### {datahub_name} detail
| order | name | table type | join | keys | descriotion |
| :-: | :-: | :-: | :-: | :-: | :-: |
{detail}

### Dataflow image
```mermaid
{mermaid}
```
"""


ROW_TAMPLATE = "| {order} | {name} | {table_type} | {join} | {keys} | {description} |"


def report_datahub(datahub_class: Type[DataHub], *tables: DummyDataNode) -> str:
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
    *tables: DummyDataNode
        DummyDataNode used in input DataHub class

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
    dataflow_nodes = datahub_class.search_tables(*tables)
    graph = make_dataflow_graph(dataflow_nodes)
    template_variables["mermaid"] = str(graph)

    # make table detail
    detail = ""
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
            params["keys"] = ", ".join(node.keys)

        detail += ROW_TAMPLATE.format(**params) + "\n"

    template_variables["detail"] = detail

    return TEMPLATE.format(**template_variables)


def make_dataflow_graph_from_datahub(
    datahub_class: Type[DataHub],
    *tables: DummyDataNode,
    output: str | pathlib.Path = "./dataflow.png",
    direction: Literal["TD", "LR"] = "TD",
):
    """
    Visualize the hierarchical structure of data preprocessing nodes from DataHub.

    Parameters
    ----------
    datahub: DataHub class
        visualizing dataflow of this DataHub
    output: str | pathlib.Path
        file path of dataflow image
    direction: {'TD', 'LR'}, default 'TD'
        graph direction
    *tables: DummyDataNode
        DataNode used in input DataHub class

    """
    nodes = datahub_class.search_tables(*tables)

    graph = make_dataflow_graph(nodes, direction)

    with tempfile.TemporaryDirectory() as tmpdir:
        file_path = pathlib.Path(tmpdir, "temp.mmd")
        with file_path.open("w") as f:
            f.write(str(graph))

        subprocess.run(["mmdc", "-i", file_path.as_posix(), "-o", str(output)])


def make_dataflow_graph(
    nodes: list[DataflowNode], direction: Literal["TD", "LR"] = "TD"
) -> DataflowGraph:
    """
    Visualize the hierarchical structure of data preprocessing nodes.

    Given a list of ProcessingNode objects, which each have a name, level, and list of children,
    this function creates a directed graph that visualizes the dependencies between the nodes.
    Nodes at the same level are grouped together, and dependencies are shown with directed edges.

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

    level_map: dict[int, list[DataflowNode]] = {}
    for _node in sorted(nodes, key=lambda x: x.level):

        # for manegement of node position
        if _node.level not in level_map.keys():
            level_map[_node.level] = []
        level_map[_node.level].append(_node)

        # add edge to children
        for _child_node in _node.children:
            level_diff = abs(_node.level - _child_node.level)
            link_type = "-" * level_diff + "->"
            edge = DataEdge(_node.name, _child_node.name, link_type=link_type)
            graph.add_edge(edge)

        # if node does not parents, make invisible edge
        if len(_node.parents) == 0 and _node.level >= 0:
            # target node index of invisible edge
            node_order_in_level = len(level_map[_node.level])
            n_nodes_in_before_level = len(level_map[_node.level - 1])

            if node_order_in_level <= n_nodes_in_before_level:
                target_node_index = node_order_in_level - 1
            else:
                target_node_index = n_nodes_in_before_level - 1
            invisible_edge = DataEdge(
                level_map[_node.level - 1][target_node_index].name,
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

        graph.add_node(_node)

    graph.add_node(
        DataflowNode(name="root_df", join=None, keys=None, level=max_level + 1, shape="[[]]")
    )

    return graph


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
