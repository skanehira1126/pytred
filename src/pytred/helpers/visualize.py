from __future__ import annotations

from graphviz import Digraph

from pytred.data_hub import DataHub
from pytred.data_node import ProcessingNode


def make_dataflow_graph_from_datahub(data_hub: DataHub) -> Digraph:
    """
    Visualize the hierarchical structure of data preprocessing nodes from DataHub.

    Parameters
    ----------
    data_hub: DataHub
        visualizing dataflow of this DataHub

    Returns
    -------
    Digraph
        A Graphviz Digraph object that can be rendered to visualize the tree structure.
    """
    nodes = data_hub.search_tables()

    return make_dataflow_graph(nodes)


def make_dataflow_graph(nodes: list[ProcessingNode]) -> Digraph:
    """
    Visualize the hierarchical structure of data preprocessing nodes.

    Given a list of ProcessingNode objects, which each have a name, level, and list of children,
    this function creates a directed graph that visualizes the dependencies between the nodes.
    Nodes at the same level are grouped together, and dependencies are shown with directed edges.

    Parameters
    ----------
    nodes: list of ProcessingNode
        A list of processing nodes, each with a name, level, and children.

    Returns
    -------
    Digraph
        A Graphviz Digraph object that can be rendered to visualize the tree structure.
    """

    graph = Digraph()
    # Configure the graph to flow from left to right
    graph.attr(rankdir="LR")

    # Set for tracking nodes that are already added to the graph
    added_nodes = set()

    # Add nodes to the graph level by level, group nodes of the same level
    for level in sorted({node.level for node in nodes}):
        with graph.subgraph() as s:
            s.attr(rank="same")
            for node in filter(lambda node: node.level == level, nodes):
                if node.name not in added_nodes:
                    s.node(node.name)
                    added_nodes.add(node.name)

    # Iterate through all nodes to add edges to their children
    for node in nodes:
        for child in node.children:
            graph.edge(node.name, child.name)

    return graph
