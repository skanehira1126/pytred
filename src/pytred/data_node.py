from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

import polars as pl

from pytred._types import POLARS_JOIN_METHOD
from pytred.helpers.dataflow import DataEdge as DataEdge
from pytred.helpers.dataflow import DataflowGraph as DataflowGraph
from pytred.helpers.dataflow import DataflowNode as DataflowNode


@dataclass
class EmptyDataNode:
    """
    DataNode class to no processing node
    """

    name: str
    keys: Sequence[str] | None
    join: POLARS_JOIN_METHOD | None
    is_optional: bool = False


@dataclass
class DataNode:
    table: pl.DataFrame
    keys: Sequence[str] | None
    join: POLARS_JOIN_METHOD | None
    name: str
