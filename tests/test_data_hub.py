from typing import Annotated
from typing import Literal

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from pytred import DataHub
from pytred import DataNode
from pytred._types import Input
from pytred._types import Root
from pytred.data_node import EmptyDataNode
from pytred.decorators import polars_table
from pytred.decorators.polars import polars_optional_table
from pytred.exceptions import TableNotFoundError
from pytred.metadata import TableMetadata

from .fixtures.data_hub import BranchingHub
from .fixtures.data_hub import RowsHub


def test_root_only_and_additional_joined_input():
    rows = pl.DataFrame({"id": [1, 2]})
    frame = pl.DataFrame({"id": [1], "value": [10]})
    node = DataNode(frame, keys=("id",), join="left", name="joined")
    hub = RowsHub()

    assert_frame_equal(
        hub.execute(node, rows=rows), pl.DataFrame({"id": [1, 2], "value": [10, None]})
    )
    assert hub.get("joined") is node
    replacement = pl.DataFrame({"id": [3]})
    assert_frame_equal(hub.execute(rows=replacement), replacement)
    assert_frame_equal(hub.get("rows").table, replacement)
    with pytest.raises(KeyError, match="joined"):
        hub.get("joined")


def test_reject_duplicate_input_names():
    frame = pl.DataFrame({"id": [1]})
    node = DataNode(frame, keys=("id",), join="left", name="source")
    with pytest.raises(ValueError, match="duplicated"):
        RowsHub().execute(node, rows=frame, source=frame)


def test_reject_dataframe_as_positional_input():
    with pytest.raises(TypeError, match="DataNode"):
        RowsHub().execute(pl.DataFrame(), rows=pl.DataFrame())


def test_reject_node_as_named_input():
    node = DataNode(pl.DataFrame(), keys=None, join=None, name="rows")
    with pytest.raises(TypeError, match="pl.DataFrame"):
        RowsHub().execute(rows=node)


def test_reject_positional_node_for_root():
    node = DataNode(pl.DataFrame(), keys=None, join=None, name="rows")
    with pytest.raises(ValueError, match="rows"):
        RowsHub().execute(node)


def test_positional_node_rejects_retired_outer_join():
    frame = pl.DataFrame({"id": [1]})
    node = DataNode(frame, keys=("id",), join="outer", name="prices")
    with pytest.raises(ValueError, match="full"):
        RowsHub().execute(node, rows=frame)


def test_execution_requires_a_root_declaration():
    class Hub(DataHub):
        source: Annotated[pl.DataFrame, Input(metadata=None)]

    with pytest.raises(RuntimeError, match="Root"):
        Hub().execute(source=pl.DataFrame())


@pytest.mark.parametrize("missing", ["rows", "source"])
def test_missing_declared_input_resets_previous_run(missing):
    hub = BranchingHub()
    inputs = {"rows": pl.DataFrame({"id": [1]}), "source": pl.DataFrame({"id": [1], "value": [3]})}
    hub.execute(**inputs)
    del inputs[missing]
    with pytest.raises(ValueError, match=missing):
        hub.execute(**inputs)
    with pytest.raises(KeyError, match="combined"):
        hub.get("combined")


def test_execute_dependencies_and_join_only_output():
    hub = BranchingHub()
    output = hub.execute(
        rows=pl.DataFrame({"id": [1, 2, 3, 4]}),
        source=pl.DataFrame({"id": [1, 2, 3], "value": [1, 2, 3]}),
    )
    assert_frame_equal(output, pl.DataFrame({"id": [1, 2, 3, 4], "score": [4, 7, 10, None]}))
    assert_frame_equal(
        hub.get("doubled").table, pl.DataFrame({"id": [1, 2, 3], "doubled": [2, 4, 6]})
    )


def test_filter_can_use_column_added_by_post_step():
    class Hub(DataHub):
        rows: Annotated[pl.DataFrame, Root(metadata=None)]

        def post_step(self, df):
            return df.with_columns(score=pl.col("amount") * 2)

    prices = DataNode(pl.DataFrame({"id": [2], "amount": [4]}), ("id",), "left", "prices")
    result = Hub()(prices, rows=pl.DataFrame({"id": [1, 2]}), filter=pl.col("score") > 2)
    assert_frame_equal(result, pl.DataFrame({"id": [2], "amount": [4], "score": [8]}))


def test_join_uses_keys_from_previous_join_and_disambiguates_column_names():
    class Hub(DataHub):
        orders: Annotated[pl.DataFrame, Root(metadata=None)]

        @polars_table(4, "region", "sku", join="inner")
        def catalog(self, offers):
            return offers

    region = DataNode(pl.DataFrame({"region": ["east", "west"]}), None, "cross", "regions")
    hub = Hub()
    result = hub.execute(
        region,
        orders=pl.DataFrame({"sku": [1, 2], "price": [5, 6]}),
        offers=pl.DataFrame({"region": ["east", "west"], "sku": [2, 1], "price": [20, 10]}),
    )
    assert_frame_equal(
        result.sort("sku"),
        pl.DataFrame(
            {"sku": [1, 2], "price": [5, 6], "region": ["west", "east"], "price_catalog": [10, 20]}
        ),
    )


def test_child_hub_uses_parent_intermediate_as_its_root():
    class Child(DataHub):
        rows: Annotated[pl.DataFrame, Root(metadata=None)]

        @polars_table(0, "id", join="left")
        def features(self, rows):
            return rows.select("id", score=pl.col("id") * 10)

    class Parent(DataHub):
        rows: Annotated[pl.DataFrame, Root(metadata=None)]

        def __init__(self):
            super().__init__()
            self.child = Child()

        @polars_table(0)
        def selected(self, rows):
            return rows.filter(pl.col("id") == 2)

        @polars_table(1, "id", join="left")
        def result(self, selected):
            return self.child.execute(rows=selected)

    hub = Parent()
    assert_frame_equal(
        hub.execute(rows=pl.DataFrame({"id": [1, 2]})),
        pl.DataFrame({"id": [1, 2], "score": [None, 20]}),
    )
    assert_frame_equal(hub.child.root_df, pl.DataFrame({"id": [2]}))


def test_optional_chain_distinguishes_missing_input_from_empty_frame_across_runs():
    metadata = TableMetadata(description="Result")

    class MetadataHub(DataHub):
        rows: Annotated[pl.DataFrame, Root(metadata=None)]

        @polars_optional_table(0)
        def prepared(self, source):
            return source

        @polars_table(1, "id", join="left", is_optional=True, metadata=metadata)
        def result(self, prepared):
            return prepared

    root = pl.DataFrame({"id": [1]})
    source = pl.DataFrame({"id": [1], "value": [10]})
    hub = MetadataHub()
    assert_frame_equal(hub.execute(rows=root, source=source), source)
    assert hub.get("prepared").metadata is None
    assert hub.get("result").metadata is metadata

    assert_frame_equal(hub.execute(rows=root), root)
    assert isinstance(hub.get("prepared"), EmptyDataNode)
    assert hub.get("prepared").metadata is None
    skipped = hub.get("result")
    assert isinstance(skipped, EmptyDataNode)
    assert (skipped.name, skipped.keys, skipped.join) == ("result", ("id",), "left")
    assert skipped.is_optional and skipped.metadata is metadata

    empty = source.head(0)
    assert_frame_equal(
        hub.execute(rows=root, source=empty),
        pl.DataFrame({"id": [1], "value": [None]}, schema=source.schema),
    )
    assert_frame_equal(hub.get("result").table, empty)


def test_required_method_with_missing_dependency_stops_execution():
    class Hub(DataHub):
        rows: Annotated[pl.DataFrame, Root(metadata=None)]

        @polars_table(0)
        def features(self, catalog):
            pytest.fail("method ran without its dependency")

    hub = Hub()
    rows = pl.DataFrame({"id": [1]})
    with pytest.raises(TableNotFoundError, match="catalog"):
        hub.execute(rows=rows)
    assert_frame_equal(hub.root_df, rows)
    with pytest.raises(KeyError, match="features"):
        hub.get("features")


@pytest.mark.parametrize("with_lookup", [True, False], ids=["skipped", "skipped-and-unregistered"])
def test_required_method_reports_all_unavailable_dependencies(with_lookup):
    class Hub(DataHub):
        rows: Annotated[pl.DataFrame, Root(metadata=None)]

        @polars_optional_table(0)
        def catalog(self, source):
            return source

        @polars_table(1)
        def prices(self, catalog, lookup):
            pytest.fail("required method ran with unavailable dependencies")

    rows = pl.DataFrame({"id": [1]})
    hub = Hub()
    inputs = {"lookup": rows} if with_lookup else {}
    with pytest.raises(TableNotFoundError) as error:
        hub.execute(rows=rows, **inputs)

    message = str(error.value)
    assert "Hub.prices" in message
    assert "catalog (skipped)" in message
    if with_lookup:
        assert "lookup" not in message
    else:
        assert "lookup (not registered)" in message
    assert_frame_equal(hub.root_df, rows)
    assert isinstance(hub.get("catalog"), EmptyDataNode)
    with pytest.raises(KeyError, match="prices"):
        hub.get("prices")


def test_read_output_metadata_without_instantiation():
    """Expose the supplied output metadata on the hub class without input data or execution."""
    metadata = TableMetadata[Literal["feature"]](roles=("feature",))

    class MetadataHub(DataHub, metadata=metadata):
        pass

    assert MetadataHub.metadata is metadata


def test_reject_multilevel_inheritance():
    with pytest.raises(TypeError, match="RowsHub"):

        class Child(RowsHub):
            pass


def test_collect_marked_inputs_per_hub():
    class OrderHub(DataHub):
        orders: Annotated[pl.DataFrame, Root(metadata=None), "extra"]
        label: Annotated[str, "description"]

    class PriceHub(DataHub):
        prices: Annotated[pl.DataFrame, Root(metadata=None)]

    assert OrderHub().get_input_table_names() == ["orders"]
    assert PriceHub().get_root_table_name() == "prices"
    assert set(BranchingHub().get_input_table_names()) == {"rows", "source"}
    assert OrderHub.input_tables["orders"]["dtype"] is pl.DataFrame
    assert isinstance(OrderHub.input_tables["orders"]["marker"], Root)


def test_undeclared_hub_has_no_inputs_or_tables_to_execute():
    class Hub(DataHub):
        pass

    hub = Hub()
    with pytest.raises(RuntimeError, match="Input"):
        hub.get_input_table_names()
    with pytest.raises(TableNotFoundError):
        hub.execute()


def test_reject_two_root_fields():
    with pytest.raises(ValueError, match="Root"):

        class Hub(DataHub):
            orders: Annotated[pl.DataFrame, Root(metadata=None)]
            prices: Annotated[pl.DataFrame, Root(metadata=None)]


def test_reject_conflicting_markers_on_one_input():
    with pytest.raises(ValueError):

        class Hub(DataHub):
            orders: Annotated[pl.DataFrame, Root(metadata=None), Input(metadata=None)]


def test_input_declarations_do_not_modify_runtime_nodes():
    declared = TableMetadata(description="Declared")
    supplied = TableMetadata(description="Supplied")

    class Hub(DataHub):
        rows: Annotated[pl.DataFrame, Root(metadata=None)]

        source: Annotated[pl.DataFrame, Input(metadata=declared)]
        frame: Annotated[pl.DataFrame, Input(metadata=declared)]

    node = DataNode(pl.DataFrame(), keys=None, join=None, name="source", metadata=supplied)
    hub = Hub()
    hub.execute(node, rows=pl.DataFrame(), frame=pl.DataFrame())
    assert hub.get("source") is node
    assert hub.get("source").metadata is supplied
    assert hub.get("frame").metadata is None
    assert Hub.input_tables["source"]["marker"].metadata is declared


def test_step_failure_keeps_only_completed_tables_from_current_run():
    class Hub(DataHub):
        rows: Annotated[pl.DataFrame, Root(metadata=None)]

        @polars_table(0)
        def prepared(self, rows):
            return rows

        @polars_table(1)
        def result(self, prepared):
            if prepared.is_empty():
                raise ValueError("empty batch")
            return prepared

    hub = Hub()
    hub.execute(rows=pl.DataFrame({"id": [1]}))
    empty = pl.DataFrame(schema={"id": pl.Int64})
    with pytest.raises(ValueError, match="empty batch"):
        hub.execute(rows=empty)
    assert_frame_equal(hub.get("prepared").table, empty)
    with pytest.raises(KeyError, match="result"):
        hub.get("result")
