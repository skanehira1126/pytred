from __future__ import annotations

from collections.abc import Callable
from functools import partial
from functools import wraps
from logging import getLogger

import polars as pl

from pytred._types import PolarsJoinMethod
from pytred._types import validate_polars_join
from pytred.exceptions import DuplicatedError
from pytred.exceptions import InvalidReturnValueError
from pytred.metadata import TableMetadata


logger = getLogger(__name__)


def _validate_signature(order: int, keys: tuple[str, ...], join: PolarsJoinMethod | None = None):
    validate_polars_join(join)
    if not isinstance(order, int):
        raise ValueError("order must be int.")
    if isinstance(order, int) and order < 0:
        # -1 is used by DataHub
        raise ValueError("order must be more than 0.")

    if (join is None or join in ["cross"]) and (len(keys) >= 1 and keys[0] is not None):
        raise ValueError("When 'join' is None or 'cross', keys must be empty.")
    if (join is not None and join not in ["cross"]) and (len(keys) == 0 or keys[0] is None):
        raise ValueError(f"When 'join' is {join}, keys must not be empty.")


def _set_table_metadata_to_function(
    wrapper,
    order: int,
    join: PolarsJoinMethod | None,
    keys: tuple[str, ...],
    is_optional: bool,
    metadata: TableMetadata | None = None,
):
    wrapper.__pytred_meta__ = {
        "meta_type": "table",
        "table_process_order": order,
        "join": join,
        "keys": None if (len(keys) == 0 or keys[0] is None) else keys,
        "is_optional": is_optional,
        "metadata": metadata,
    }

    return wrapper


def _set_prepare_metadata_to_function(
    wrapper,
    target: str,
):
    wrapper.__pytred_meta__ = {
        "meta_type": "prepare",
        "target": target,
    }
    return wrapper


def _set_validate_metadata_to_function(wrapper):
    wrapper.__pytred_meta__ = {
        "meta_type": "validate",
    }

    return wrapper


def polars_table(
    order: int,
    *keys: str,
    join: PolarsJoinMethod | None = None,
    is_validate_unique: bool = True,
    is_optional: bool = False,
    metadata: TableMetadata | None = None,
):
    """Turn a method returning a Polars DataFrame into a pipeline step.

    Method parameter names identify input tables or earlier step results.

    Parameters
    ----------
    order : int
        Non-negative execution order. Producers must run before their consumers.
    keys : str
        Join column names returned by the method. Omit keys for `join=None`
        and `join="cross"`; other joins require at least one key.
    join : PolarsJoinMethod or None, optional
        Join strategy for adding the result to the root. `None` keeps it as an
        intermediate table. Use `"full"` for a full outer join.
    is_validate_unique : bool, default True
        Check that the returned join keys uniquely identify rows.
    is_optional : bool, default False
        Skip this step if a dependency is missing or was skipped. Declared
        `Input` tables are still required on every run.
    metadata : TableMetadata or None, optional
        Description of the produced table; does not validate its contents.

    Returns
    -------
    Callable
        Decorator for a method returning a Polars DataFrame.

    Raises
    ------
    ValueError
        The order is not a non-negative integer, the join strategy is unknown,
        or the keys are incompatible with the join strategy.
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

        _wrapper = _set_table_metadata_to_function(
            _wrapper,
            order=order,
            join=join,
            keys=keys,
            is_optional=is_optional,
            metadata=metadata,
        )

        return _wrapper

    return decorator


polars_optional_table = partial(polars_table, is_optional=True)


def prepare_input(table_name: str):
    """Use the decorated method's returned DataFrame as the input named table_name.

    Method parameters name raw input tables from the current execute() call.
    Each hook may read several inputs and prepares one target. Only one hook
    per hub may prepare a given target. All hooks see the raw frames and their
    replacements are registered together, preserving each node's join settings
    and metadata.

    During execution, a non-DataFrame return value raises InvalidReturnValueError
    before any prepared replacements are registered. Empty DataFrames are valid.

    Treat the supplied DataFrames as read-only and return a new frame for
    transformations. In-place edits affect other hooks and the caller.
    For a class method, place @classmethod above @prepare_input(...).
    """

    def decorator(func: Callable[..., pl.DataFrame]) -> Callable:
        func = _set_prepare_metadata_to_function(
            func,
            target=table_name,
        )
        return func

    return decorator


def validate_raw_inputs(func: Callable[..., None]):
    """Check raw input DataFrames before preparation and table execution.

    Name each input to check as a method parameter. Treat the supplied frames
    as read-only. Return None on success and raise an exception to stop the run.
    For a class method, place @classmethod above @validate_raw_inputs.
    """
    func = _set_validate_metadata_to_function(func)
    return func
