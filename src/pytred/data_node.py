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
class DataflowNode:
    """
    A node in a processing graph representing a data preprocessing step.

    Attributes
    ----------
    name : str
        The name of the preprocessing step this node represents.
    join: str or None
        Join type of this table with root_df.
        If join is None, this table does not joined with root_df.
    keys: Sequence of str or None
        join keys. If 'join' is None, keys is None too.
    level : int
        The hierarchical level of this node in the processing graph.
        Root level is indicated by 0, with higher numbers indicating deeper levels.
    parents: list of DataflowNode
        A list of parents nodes that depend on this node's inputs.
    children : list of DataflowNode
        A list of child nodes that depend on this node's output.
    """

    name: str
    join: str | None
    keys: Sequence[str] | None
    level: int
    parents: list[DataflowNode] = field(default_factory=list)
    children: list[DataflowNode] = field(default_factory=list)
    shape: str = "[]"

    def __eq__(self, other):
        return (
            (self.name == other.name)
            & (self.level == other.level)
            & (self.shape == other.shape)
            & (
                sorted([p.name for p in self.parents])
                == sorted([p.name for p in other.parents])
            )
            & (
                sorted([c.name for c in self.children])
                == sorted([c.name for c in other.children])
            )
        )

    def __ne__(self, other):
        return not self.__eq__(other)

    def add_child(self, child: DataflowNode):
        """
        Adds a child node to the list of this node's children, representing a
        processing step that directly depends on the output of this node.

        Parameters
        ----------
        child : ProcessingNode
            The child node to be added.
        """
        child.parents.append(self)
        self.children.append(child)

    def fmt_mermaid(self) -> str:
        """
        Returns a string representation of the processing node, including its
        name, level, and a visual hierarchy of its children.

        Returns
        -------
        str
            A string representation of the node.
        """
        shape_open = self.shape[: len(self.shape) // 2]
        shape_close = self.shape[len(self.shape) // 2 :]
        return f"{self.name}{shape_open}{self.name}{shape_close}"


@dataclass
class DataEdge:
    source: str
    target: str
    link_type: str

    def fmt_mermaid(self):
        return f"{self.source} {self.link_type} {self.target}"
