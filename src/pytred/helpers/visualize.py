from __future__ import annotations

import pathlib
import subprocess
import tempfile
from dataclasses import dataclass, field
from typing import Literal

from pytred.data_hub import DataHub
from pytred.data_node import DataEdge, DataflowNode


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


def make_dataflow_graph_from_datahub(
    data_hub: DataHub,
    output: str | pathlib.Path = "./dataflow.png",
    direction: Literal["TD", "LR"] = "TD",
):
    """
    Visualize the hierarchical structure of data preprocessing nodes from DataHub.

    Parameters
    ----------
    data_hub: DataHub
        visualizing dataflow of this DataHub
    output: str | pathlib.Path
        file path of dataflow image
    direction: {'TD', 'LR'}, default 'TD'
        graph direction

    """
    nodes = data_hub.search_tables()

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

    level_map: dict[int, list[DataflowNode]] = {}
    for _node in sorted(nodes, key=lambda x: x.level):

        # for manegement of node position
        if _node.level not in level_map.keys():
            level_map[_node.level] = []
        level_map[_node.level].append(_node)

        # add edge
        for _child_node in _node.children:
            level_diff = abs(_node.level - _child_node.level)
            link_type = "-" * level_diff + "->"
            edge = DataEdge(_node.name, _child_node.name, link_type=link_type)
            graph.add_edge(edge)

        # if node does not parents, make invisible edge
        if len(_node.parents) == 0 and _node.level >= 0:
            invisible_edge = DataEdge(
                level_map[_node.level - 1][0].name,
                _node.name,
                link_type="~~~",
            )
            graph.add_edge(invisible_edge)

        graph.add_node(_node)

    return graph
