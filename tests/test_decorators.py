from functools import partial
from typing import Annotated
from typing import Literal

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from pytred import DataHub
from pytred._types import Root
from pytred.decorators import check_polars_join_keys
from pytred.decorators import polars_table
from pytred.decorators import table
from pytred.decorators.polars import polars_optional_table
from pytred.decorators.polars import prepare_input
from pytred.exceptions import DuplicatedError
from pytred.exceptions import InvalidReturnValueError
from pytred.helpers.decorator import get_metadata
from pytred.metadata import TableMetadata


@pytest.mark.parametrize("keys, join", [(("id",), "left"), ((), None), ((), "cross")])
def test_preserve_frame_and_metadata(keys, join):
    frame = pl.DataFrame({"id": [1], "value": [10]})

    @polars_table(2, *keys, join=join, is_optional=True)
    def table():
        return frame

    assert_frame_equal(table(), frame)
    assert get_metadata(table, "table_process_order") == 2
    assert get_metadata(table, "keys") == (keys or None)
    assert get_metadata(table, "join") == join
    assert get_metadata(table, "is_optional") is True
    assert get_metadata(table, "metadata") is None


@pytest.mark.parametrize(
    "decorate", [polars_table, polars_optional_table], ids=["regular", "optional"]
)
def test_read_table_metadata_without_execution(decorate):
    """Expose the supplied project metadata on a class method without executing it."""
    metadata = TableMetadata[Literal["feature"]](roles=("feature",))

    class Pipeline:
        @decorate(0, metadata=metadata)
        def features(self):
            pytest.fail("Reading metadata must not execute the processing method")

    assert get_metadata(Pipeline.features, "metadata") is metadata
    assert get_metadata(Pipeline().features, "metadata") is metadata


@pytest.mark.parametrize("order", [1.5, -1], ids=["non_integer", "negative"])
def test_reject_invalid_order(order):
    with pytest.raises(ValueError, match="order"):
        polars_table(order)


@pytest.mark.parametrize(
    "keys, join",
    [(("id",), None), (("id",), "cross"), ((), "left")],
    ids=["keys_without_join", "keys_for_cross_join", "join_without_keys"],
)
def test_reject_inconsistent_keys_and_join(keys, join):
    with pytest.raises(ValueError, match="keys"):
        polars_table(0, *keys, join=join)


def test_reject_non_dataframe_return():
    @polars_table(0)
    def table():
        return 1

    with pytest.raises(InvalidReturnValueError):
        table()


def test_reject_missing_key_column():
    @polars_table(0, "id", "day", join="left", is_validate_unique=False)
    def table():
        return pl.DataFrame({"id": [1], "value": [10]})

    with pytest.raises(ValueError, match="day"):
        table()


def test_uniqueness_applies_to_complete_key_and_excludes_payload_columns():
    @polars_table(0, "id", "day", join="left")
    def observations(frame):
        return frame

    distinct = pl.DataFrame({"id": [1, 1, 2], "day": [1, 2, 1], "value": [10, 20, 30]})
    assert_frame_equal(observations(distinct), distinct)

    repeated = pl.DataFrame({"id": [1], "day": [2], "value": [40]})
    with pytest.raises(DuplicatedError):
        observations(distinct.vstack(repeated))


def test_allow_duplicate_keys_when_validation_disabled():
    frame = pl.DataFrame({"id": [1, 1], "value": [10, 20]})

    @polars_table(0, "id", join="left", is_validate_unique=False)
    def table():
        return frame

    assert_frame_equal(table(), frame)


def test_generic_table_forwards_settings_and_allows_repeated_keys_by_default():
    @table("polars", 3, "id", join="inner")
    def prices(frame):
        return frame

    frame = pl.DataFrame({"id": [1, 1], "price": [10, 20]})
    assert_frame_equal(prices(frame), frame)
    assert get_metadata(prices, "keys") == ("id",)
    assert get_metadata(prices, "join") == "inner"
    assert get_metadata(prices, "table_process_order") == 3

    @table("polars", 0, "id", join="left", is_validate_unique=True)
    def unique_prices(frame):
        return frame

    with pytest.raises(DuplicatedError):
        unique_prices(frame)


@pytest.mark.parametrize(
    "engine, join, invalid_setting",
    [("unknown", "left", "engine"), ("polars", "sideways", "join")],
)
def test_generic_table_rejects_unsupported_settings(engine, join, invalid_setting):
    with pytest.raises(ValueError, match=invalid_setting):
        table(engine, 0, "id", join=join)


@pytest.mark.parametrize(
    "decorate", [polars_table, partial(table, "polars")], ids=["polars", "generic"]
)
def test_full_join_retains_matched_and_unmatched_rows_from_both_inputs(decorate):
    pytest.importorskip("polars", minversion="0.20.29", reason="Polars added full in 0.20.29")

    class Hub(DataHub):
        orders: Annotated[pl.DataFrame, Root(metadata=None)]

        @decorate(0, "id", join="full")
        def catalog(self, products):
            return products

    result = Hub().execute(
        orders=pl.DataFrame({"id": [1, 2], "order": ["a", "b"]}),
        products=pl.DataFrame({"id": [2, 3], "product": ["x", "y"]}),
    )
    assert_frame_equal(
        result.select("order", "product"),
        pl.DataFrame({"order": ["a", "b", None], "product": [None, "x", "y"]}),
        check_row_order=False,
    )


@pytest.mark.parametrize(
    "decorate", [polars_table, partial(table, "polars")], ids=["polars", "generic"]
)
def test_outer_join_is_rejected_at_declaration(decorate):
    with pytest.raises(ValueError, match="full"):
        decorate(0, "id", join="outer")


@pytest.mark.parametrize("join", [None, "inner", "left", "right", "full", "semi", "anti", "cross"])
def test_supported_join_names_can_be_used_in_generic_table_declarations(join):
    assert check_polars_join_keys(join)
    keys = () if join in (None, "cross") else ("id",)
    decorated = table("polars", 0, *keys, join=join)(lambda frame: frame)
    assert get_metadata(decorated, "join") == join


def test_metadata_lookup_distinguishes_undecorated_function_from_missing_setting():
    def clean(rows):
        return rows

    with pytest.raises(AttributeError):
        get_metadata(clean, "keys")

    prepare = prepare_input("rows")(clean)
    with pytest.raises(KeyError, match="keys"):
        get_metadata(prepare, "keys")
