from __future__ import annotations

from typing import Literal
from typing import TypedDict

from pydantic import BaseModel

from pytred.metadata import TableMetadata


META_KEYS = Literal["table_process_order", "join", "keys", "is_optional", "metadata"]

PolarsJoinMethod = Literal["inner", "left", "right", "full", "semi", "anti", "cross"]


class Input(BaseModel):
    """Mark a required DataHub input in an Annotated field.

    The field name identifies the table passed to execute(). Supply
    metadata=None to declare the input without descriptive metadata.
    """

    metadata: TableMetadata | None


class Root(Input):
    """Mark the DataFrame that provides the starting rows and columns for joins.

    Supply this input by its declared name as a keyword argument to execute().
    """


class InputDefinition(TypedDict):
    """The annotated type and Input or Root marker collected for one input name."""

    dtype: type
    marker: Input
