from __future__ import annotations

from functools import partial
from functools import wraps
from logging import getLogger
from typing import Callable

import polars as pl

from pytred._types import POLARS_JOIN_METHOD
from pytred.exceptions import DuplicatedError
from pytred.exceptions import InvalidReturnValueError


logger = getLogger(__name__)


def _validate_signature(order: int, keys: tuple[str, ...], join: POLARS_JOIN_METHOD | None = None):
    if not isinstance(order, int):
        raise ValueError("order must be int.")
    if isinstance(order, int) and order < 0:
        # -1 is used by DataHub
        raise ValueError("order must be more than 0.")

    if (join is None or join in ["cross"]) and (len(keys) >= 1 and keys[0] is not None):
        raise ValueError("When 'join' is None or 'cross', keys must be empty.")
    if (join is not None and join not in ["cross"]) and (len(keys) == 0 or keys[0] is None):
        raise ValueError(f"When 'join' is {join}, keys must not be empty.")


def _set_metadata_to_function(
    wrapper,
    order: int,
    join: POLARS_JOIN_METHOD | None,
    keys: tuple[str, ...],
    is_optional: bool,
):
    wrapper.__pytred_meta__ = {
        "table_process_order": order,
        "join": join,
        "keys": None if (len(keys) == 0 or keys[0] is None) else keys,
        "is_optional": is_optional,
    }

    return wrapper


def polars_table(
    order: int,
    *keys: str,
    join: POLARS_JOIN_METHOD | None = None,
    is_validate_unique: bool = True,
    is_optional: bool = False,
):
    """
    Decorator class for adding metadata to data processing functions, specifying their order of
    execution, join method, and uniqueness constraints on keys.

    Parameters
    ----------
    order : int
        The order in which the decorated function should be executed in the data pipeline.
    keys : tuple
        The keys to be used for joining data tables. If keys are provided, the decorator will check
        for their presence in the DataFrame returned by the function.
    join : {‘inner’, ‘left’, ‘outer’, ‘semi’, ‘anti’, ‘cross’, ‘outer_coalesce’}, optional
        The type of join to be used when combining data tables. Supported join types are 'inner',
        'left', 'outer', 'semi', 'anti', and 'cross'.
        If join is None, it implies that the resulting table from this function is intended for use
        only within preprocessing steps and will not be directly included in the final output of
        the DataHub pipeline.
    is_validate_unique : bool, default True
        Whether to validate the uniqueness of the specified keys in the DataFrame returned by the
        function. If True, a check for duplicate entries based on the keys is performed.
    is_optional: bool, default False
        If True, do not execute if input table does not exist

    Raises
    ------
    ValueError
        If the 'order' parameter is not an integer.
    """
    _validate_signature(order, keys, join)

    def decorator(func: Callable[..., pl.DataFrame]) -> Callable:
        """
        Wraps a data processing function, injecting additional logic to enforce metadata
        specifications such as join method and key uniqueness validation.

        Parameters
        ----------
        func : Callable[..., pl.DataFrame]
            The data processing function to be decorated. This function must return
            a polars.DataFrame.

        Returns
        -------
        Callable[..., pl.DataFrame]
            A wrapped version of the input function that, when called, performs additional checks
            and operations based on the metadata specified in the decorator.

        Raises
        ------
        InvalidReturnValueError
            If the decorated function does not return a polars.DataFrame
        DuplicatedError
            If key uniqueness validation fails.
        """
        logger.info(f"set table by {func.__name__}. keys: {keys}, join: {join}, order: {order}.")

        @wraps(func)
        def _wrapper(*args, **kwargs):
            df = func(*args, **kwargs)
            if not isinstance(df, pl.DataFrame):
                raise InvalidReturnValueError(
                    f"{func.__name__} must be return polars.DataFrame, not {type(df)}."
                )
            if keys:
                if not set(keys).issubset(set(df.columns)):
                    raise ValueError(
                        f"Expected keys not found in DataFrame columns: expected {keys}, "
                        f"found {df.columns}."
                    )

                if is_validate_unique:
                    if len(df.unique(subset=keys)) != len(df):
                        raise DuplicatedError(
                            f"There are duplicate values based on the specified keys {keys} "
                            f"returned by {func.__name__}."
                        )
            return df

        _wrapper = _set_metadata_to_function(
            _wrapper,
            order=order,
            join=join,
            keys=keys,
            is_optional=is_optional,
        )

        return _wrapper

    return decorator


polars_optional_table = partial(polars_table, is_optional=True)
