from __future__ import annotations

from collections.abc import Callable
from typing import Literal

from pytred._types import check_polars_join_keys as check_polars_join_keys
from pytred._types import validate_polars_join
from pytred.decorators.polars import polars_table


def table(
    engine: Literal["polars"],
    order: int,
    *keys: str,
    join: str | None = None,
    is_validate_unique: bool = False,
) -> Callable:
    """
    Parameters
    ----------
    engine : {"polars"}
        The backend engine used for data processing. Currently, only "polars" is supported.
    order : int
        An integer specifying the execution order of the data processing function.
    keys : str
        The names of the key columns used to join data tables.
    join : str, optional
        The type of join to use when combining data tables.
        If None, it implies that no join is applied.
    is_validate_unique : bool, default False
        Whether to validate the uniqueness of the specified keys. If True, checks for
        duplicate entries based on the keys.
    """

    if engine != "polars":
        raise ValueError("engine must be 'polars only'.")
    return polars_table(
        order, *keys, join=validate_polars_join(join), is_validate_unique=is_validate_unique
    )
