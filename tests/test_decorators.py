import polars as pl
from polars.testing import assert_frame_equal
import pytest

from pytred.decorators import polars_table
from pytred.exceptions import DuplicatedError
from pytred.exceptions import InvalidReturnValueError


@pytest.fixture
def simple_df():
    df = pl.DataFrame(
        {
            "id": ["a", "b", "c"],
            "number": [1, 2, 3],
        }
    )

    return df


@pytest.fixture
def duplicate_df():
    df = pl.DataFrame(
        {
            "id": ["a", "a", "c"],  # a is duplicated.
            "number": [1, 2, 3],
        }
    )

    return df


class TestPolarsTable:

    def test__annotated_function_returns_same_df_and_keys(self, simple_df):
        @polars_table(0, "id", join="left")
        def prep_function():
            return simple_df

        actual_df = prep_function()
        expected_df = simple_df

        assert_frame_equal(actual_df, expected_df)
        assert prep_function.keys == ("id",)

    @pytest.mark.parametrize("order", [1.5, "aaa", {"a": 2}, [1, 2, 3]])
    def test__raise_ValueError_when_order_is_not_integer(self, simple_df, order):

        with pytest.raises(ValueError):

            @polars_table(order, "id", join="inner")
            def prep_function():
                return simple_df

    @pytest.mark.parametrize("keys, join", [[["id"], None], [[None], "inner"]])
    def test__raise_ValueError_when_keys_and_join_are_invalid_combination(
        self, keys, join, simple_df
    ):

        with pytest.raises(ValueError):

            @polars_table(0, *keys, join=join)
            def prep_function():
                return simple_df

    def test__raise_DuplicatedError_when_dataframe_has_duplicated_values(self, duplicate_df):

        @polars_table(0, "id", join="inner")
        def prep_function():
            return duplicate_df

        with pytest.raises(DuplicatedError):
            prep_function()

    def test__not_validate_unique_values(self, duplicate_df):

        @polars_table(0, "id", join="inner", is_validate_unique=False)
        def prep_function():
            return duplicate_df

        actual_df = prep_function()
        expected_df = duplicate_df

        assert_frame_equal(actual_df, expected_df)
        assert prep_function.keys == ("id",)

    def test__without_keys(self, simple_df):

        @polars_table(0)
        def prep_function():
            return simple_df

        actual_df = prep_function()
        expected_df = simple_df

        assert_frame_equal(actual_df, expected_df)
        assert prep_function.keys is None

    @pytest.mark.parametrize("return_value", [1.5, "aaa", {"a": 2}, [1, 2, 3]])
    def test__raise_ValueError_function_done_not_return_dataframe(self, return_value):

        @polars_table(0, "id", join="inner")
        def prep_function():
            return return_value

        with pytest.raises(InvalidReturnValueError):
            prep_function()

    def test__raise_ValueError_unknown_keys(self, simple_df):
        @polars_table(0, "unknown_id", join="inner")
        def prep_function():
            return simple_df

        with pytest.raises(ValueError):
            prep_function()

    @pytest.mark.parametrize(
        "actual, expected",
        [[[2, "id", "left", "True"], [2, ("id",), "left", True]]],
    )
    def test__validate_function_attributes(self, actual, expected, simple_df):
        @polars_table(actual[0], actual[1], join=actual[2], is_validate_unique=actual[3])
        def prep_function():
            return simple_df

        assert prep_function.table_process_order == expected[0]
        assert prep_function.join == expected[2]

    def test__raise_ValueError_order_is_less_than_0(self, simple_df):
        with pytest.raises(ValueError):

            @polars_table(-1, "id", join="inner")
            def prep_function():
                return simple_df
