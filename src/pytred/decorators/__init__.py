from __future__ import annotations

from typing import Callable
from typing import Literal

from typing_extensions import TypeGuard

from pytred._types import POLARS_JOIN_METHOD
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

    if engine == "polars":
        if check_polars_join_keys(join):
            return polars_table(order, *keys, join=join, is_validate_unique=is_validate_unique)
        else:
            raise ValueError("join must be 'inner', 'left', 'outer', 'semi', 'anti' or 'cross'.")
    else:
        raise ValueError("engine must be 'polars only'.")


def check_polars_join_keys(
    join: str | None,
) -> TypeGuard[POLARS_JOIN_METHOD | None]:
    """
    Validates whether the specified join method is supported by the Polars engine.

    Parameters
    ----------
    join : str
        The join method to validate, which can be "inner", "left", "outer", "semi",
        "anti", "cross", or None.

    Returns
    -------
    bool
        Returns True if the join method is valid; otherwise, returns False.
    """
    if join in ["inner", "left", "outer", "semi", "anti", "cross"] or join is None:
        return True
    else:
        return False
