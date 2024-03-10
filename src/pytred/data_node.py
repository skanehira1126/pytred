from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Sequence

import polars as pl


@dataclass
class DataNode:
    table: pl.DataFrame
    keys: Sequence[str] | None
    join: Literal["inner", "left", "outer", "semi", "anti", "cross"] | None
    name: str
