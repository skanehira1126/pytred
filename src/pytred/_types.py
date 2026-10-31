from __future__ import annotations

from typing import Literal
from typing import TypedDict

from pydantic import BaseModel

from pytred.metadata import TableMetadata


META_KEYS = Literal["table_process_order", "join", "keys", "is_optional", "metadata"]

PolarsJoinMethod = Literal["inner", "left", "right", "full", "semi", "anti", "cross"]


# For Datahub annotation of Input table
class Input(BaseModel):
    metadata: TableMetadata | None


class Root(Input):
    pass


class InputDefinition(TypedDict):
    dtype: type
    marker: Input
