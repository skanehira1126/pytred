from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Sequence

import polars as pl


@dataclass
class DataNode:
    table: pl.DataFrame
    keys: Sequence[str] | None
    join: Literal["inner", "left", "outer", "semi", "anti", "cross"] | None
    name: str


@dataclass
class ProcessingNode:
    """
    A node in a processing graph representing a data preprocessing step.

    Attributes
    ----------
    name : str
        The name of the preprocessing step this node represents.
    level : int
        The hierarchical level of this node in the processing graph.
        Root level is indicated by 0, with higher numbers indicating deeper levels.
    children : list of ProcessingNode
        A list of child nodes that depend on this node's output.
    """

    name: str
    level: int
    children: list[ProcessingNode] = field(default_factory=list)

    def add_child(self, child: ProcessingNode):
        """
        Adds a child node to the list of this node's children, representing a
        processing step that directly depends on the output of this node.

        Parameters
        ----------
        child : ProcessingNode
            The child node to be added.
        """
        self.children.append(child)

    def __str__(self) -> str:
        """
        Returns a string representation of the processing node, including its
        name, level, and a visual hierarchy of its children.

        Returns
        -------
        str
            A string representation of the node.
        """
        ret = "\t" * self.level + repr(self.name) + "\n"
        for child in self.children:
            ret += child.__str__()
        return ret
