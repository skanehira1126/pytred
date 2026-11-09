from __future__ import annotations

from typing import Literal
from typing import TypedDict
from typing import TypeGuard
from typing import get_args

from pydantic import BaseModel

from pytred.metadata import TableMetadata


META_KEYS = Literal["table_process_order", "join", "keys", "is_optional", "metadata"]

PolarsJoinMethod = Literal["inner", "left", "right", "full", "semi", "anti", "cross"]


def check_polars_join_keys(join: str | None) -> TypeGuard[PolarsJoinMethod | None]:
    """Return whether pytred accepts this Polars join strategy."""
    return join is None or join in get_args(PolarsJoinMethod)


def validate_polars_join(join: str | None) -> PolarsJoinMethod | None:
    if check_polars_join_keys(join):
        return join
    choices = ", ".join(get_args(PolarsJoinMethod))
    raise ValueError(f"Unsupported join {join!r}. Choose from {choices} or None.")


class Input(BaseModel):
    """Mark a required DataHub input in an Annotated field.

    The field name identifies the table passed to execute(). Supply
    metadata=None to declare the input without descriptive metadata.
    """

    metadata: TableMetadata | None
    """Table description. Pass `None` explicitly when no description is needed."""


class Root(Input):
    """Mark the DataFrame that provides the starting rows and columns for joins.

    Supply this input by its declared name as a keyword argument to execute().
    """


class InputDefinition(TypedDict):
    """The annotated type and Input or Root marker collected for one input name."""

    dtype: type
    marker: Input
