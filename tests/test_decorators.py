import polars as pl
from polars.testing import assert_frame_equal
import pytest

from pytred.decorators import polars_table
from pytred.exceptions import DuplicatedError
from pytred.exceptions import InvalidReturnValueError
from pytred.helpers.decorator import get_metadata


@pytest.mark.parametrize("keys, join", [(("id",), "left"), ((), None)])
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


@pytest.mark.parametrize("order", [1.5, -1], ids=["non_integer", "negative"])
def test_reject_invalid_order(order):
    with pytest.raises(ValueError, match="order"):
        polars_table(order)


@pytest.mark.parametrize(
    "keys, join", [(("id",), None), ((), "left")], ids=["keys_without_join", "join_without_keys"]
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
    @polars_table(0, "id", join="left")
    def table():
        return pl.DataFrame({"value": [10]})

    with pytest.raises(ValueError, match="keys"):
        table()


def test_reject_duplicate_keys_by_default():
    @polars_table(0, "id", join="left")
    def table():
        return pl.DataFrame({"id": [1, 1], "value": [10, 20]})

    with pytest.raises(DuplicatedError):
        table()


def test_allow_duplicate_keys_when_validation_disabled():
    frame = pl.DataFrame({"id": [1, 1], "value": [10, 20]})

    @polars_table(0, "id", join="left", is_validate_unique=False)
    def table():
        return frame

    assert_frame_equal(table(), frame)
