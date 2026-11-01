from typing import Annotated

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from pytred import DataHub
from pytred import DataNode
from pytred._types import Input
from pytred._types import Root
from pytred.decorators.polars import polars_table
from pytred.decorators.polars import prepare_input
from pytred.decorators.polars import validate_raw_inputs
from pytred.exceptions import InvalidReturnValueError
from pytred.metadata import TableMetadata


def test_prepares_read_raw_inputs_and_join_their_replacements():
    class Hub(DataHub):
        orders: Annotated[pl.DataFrame, Root(metadata=None)]
        prices: Annotated[pl.DataFrame, Input(metadata=None)]

        @classmethod
        @prepare_input("orders")
        def select_orders(cls, orders, *, prices):
            return orders.join(prices, on="id", how="semi")

        @prepare_input("prices")
        def count_orders(self, prices, orders):
            return prices.head(1).with_columns(count=pl.lit(orders.height))

        @polars_table(0, "id", join="left")
        def totals(self, prices):
            return prices.select("id", total=pl.col("price") * pl.col("count"))

    orders = pl.DataFrame({"id": [1, 2, 3]})
    prices = pl.DataFrame({"id": [2, 3], "price": [10, 20]})
    metadata = TableMetadata(description="Catalog prices")
    node = DataNode(prices, keys=("id",), join="left", name="prices", metadata=metadata)
    hub = Hub()
    output = hub.execute(node, orders=orders)
    assert output.rows(named=True) == [
        {"id": 2, "price": 10, "count": 3, "total": 30},
        {"id": 3, "price": None, "count": None, "total": None},
    ]
    assert hub.get("prices").metadata is metadata
    assert_frame_equal(hub.get("orders").table, orders.tail(2))
    assert node.table is prices
    assert prices.columns == ["id", "price"]


def test_validation_failure_leaves_raw_input_and_next_run_can_recover():
    class Hub(DataHub):
        orders: Annotated[pl.DataFrame, Root(metadata=None)]

        @classmethod
        @validate_raw_inputs
        def check_amounts(cls, orders):
            if orders["amount"].min() < 0:
                raise ValueError("Negative amount")

        @prepare_input("orders")
        def discard_negative(self, orders):
            return orders.filter(pl.col("amount") > 0)

        @polars_table(0)
        def summary(self, orders):
            return orders.select(total=pl.col("amount").sum())

    orders = pl.DataFrame({"amount": [-1]})
    hub = Hub()
    with pytest.raises(ValueError, match="Negative amount"):
        hub.execute(orders=orders)
    assert_frame_equal(hub.get("orders").table, orders)
    with pytest.raises(KeyError, match="summary"):
        hub.get("summary")

    output = hub.execute(orders=pl.DataFrame({"amount": [0, 4]}))
    assert_frame_equal(output, pl.DataFrame({"amount": [4]}))
    assert_frame_equal(hub.get("summary").table, pl.DataFrame({"total": [4]}))


def test_failed_prepare_leaves_registered_inputs_raw():
    class Hub(DataHub):
        rows: Annotated[pl.DataFrame, Root(metadata=None)]
        lookup: Annotated[pl.DataFrame, Input(metadata=None)]

        @prepare_input("rows")
        def first(self, rows):
            return rows.head(0)

        @prepare_input("lookup")
        def second(self, lookup):
            raise ValueError("Incomplete lookup")

    rows = pl.DataFrame({"id": [1]})
    hub = Hub()
    with pytest.raises(ValueError, match="Incomplete lookup"):
        hub.execute(rows=rows, lookup=rows)
    assert_frame_equal(hub.root_df, rows)


@pytest.mark.parametrize("invalid_result", [None, pl.LazyFrame()], ids=["none", "lazy-frame"])
def test_invalid_prepare_result_leaves_all_registered_inputs_raw(invalid_result):
    class Hub(DataHub):
        rows: Annotated[pl.DataFrame, Root(metadata=None)]
        catalog: Annotated[pl.DataFrame, Input(metadata=None)]

        @prepare_input("rows")
        def first(self, rows):
            return rows.head(0)

        @prepare_input("catalog")
        def second(self, catalog):
            return invalid_result

        @polars_table(0)
        def summary(self, rows):
            pytest.fail("table method ran after preparation failed")

    rows = pl.DataFrame({"id": [1]})
    catalog = pl.DataFrame({"sku": [2]})
    hub = Hub()
    with pytest.raises(InvalidReturnValueError, match="Hub.second.*catalog"):
        hub.execute(rows=rows, catalog=catalog)
    assert_frame_equal(hub.root_df, rows)
    assert_frame_equal(hub.get("catalog").table, catalog)


def test_prepare_accepts_an_empty_dataframe():
    class Hub(DataHub):
        rows: Annotated[pl.DataFrame, Root(metadata=None)]

        @prepare_input("rows")
        def select_rows(self, rows):
            return rows.head(0)

    rows = pl.DataFrame({"id": [1]})
    assert_frame_equal(Hub().execute(rows=rows), pl.DataFrame(schema={"id": pl.Int64}))


def test_duplicate_prepare_target_identifies_both_hooks():
    with pytest.raises(ValueError) as error:

        class Hub(DataHub):
            rows: Annotated[pl.DataFrame, Root(metadata=None)]

            @prepare_input("rows")
            def trim(self, rows):
                return rows

            @prepare_input("rows")
            def clean(self, rows):
                return rows

    assert all(name in str(error.value) for name in ("Hub", "rows", "trim", "clean"))


@pytest.mark.parametrize(
    "hook", [validate_raw_inputs, prepare_input("rows")], ids=["validate", "prepare"]
)
def test_missing_hook_dependency_identifies_method_and_input(hook):
    class Hub(DataHub):
        rows: Annotated[pl.DataFrame, Root(metadata=None)]

        @hook
        def check_catalog(self, lookup):
            pytest.fail("hook ran with a missing input")

    with pytest.raises(KeyError) as error:
        Hub().execute(rows=pl.DataFrame())
    assert "Hub.check_catalog" in str(error.value)
    assert "lookup" in str(error.value)
