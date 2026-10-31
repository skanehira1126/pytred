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
from pytred.metadata import TableMetadata

from .fixtures.data_hub import BranchingHub
from .fixtures.data_hub import OptionalHub
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
            return df.with_columns(score=pl.col("id") * 2)

    result = Hub()(rows=pl.DataFrame({"id": [1, 2]}), filter=pl.col("score") > 2)
    assert_frame_equal(result, pl.DataFrame({"id": [2], "score": [4]}))


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


@pytest.mark.parametrize("with_source", [True, False], ids=["present", "missing"])
def test_optional_dependency_chain(with_source):
    """Execute or skip the dependency chain while keeping omitted metadata as None."""
    root = pl.DataFrame({"id": [1]})
    source = pl.DataFrame({"id": [1], "value": [10]})
    hub = OptionalHub()
    output = hub.execute(rows=root, **({"source": source} if with_source else {}))

    assert_frame_equal(output, source if with_source else root)
    for name in ("prepared", "result"):
        assert isinstance(hub.get(name), EmptyDataNode) is (not with_source)
        assert hub.get(name).metadata is None


@pytest.mark.parametrize("with_source", [True, False], ids=["executed", "skipped"])
def test_carry_metadata_to_generated_nodes(with_source):
    """Preserve each method's metadata instance through execution or cascading skips."""
    project_metadata = TableMetadata[Literal["feature", "aggregate"]]
    prepared_metadata = project_metadata(roles=("aggregate",))
    result_metadata = project_metadata(roles=("feature",))

    class MetadataHub(DataHub):
        rows: Annotated[pl.DataFrame, Root(metadata=None)]

        @polars_table(0, is_optional=True, metadata=prepared_metadata)
        def prepared(self, source):
            return source

        @polars_table(1, "id", join="left", is_optional=True, metadata=result_metadata)
        def result(self, prepared):
            return prepared

    root = pl.DataFrame({"id": [1]})
    source = pl.DataFrame({"id": [1], "value": [10]})
    hub = MetadataHub()
    output = hub.execute(rows=root, **({"source": source} if with_source else {}))

    assert_frame_equal(output, source if with_source else root)
    expected_node_type = DataNode if with_source else EmptyDataNode
    for name, metadata in (("prepared", prepared_metadata), ("result", result_metadata)):
        node = hub.get(name)
        assert isinstance(node, expected_node_type)
        assert node.metadata is metadata


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

    assert set(OrderHub.input_tables) == {"orders"}
    assert set(PriceHub.input_tables) == {"prices"}
    assert OrderHub.input_tables["orders"]["dtype"] is pl.DataFrame
    assert isinstance(OrderHub.input_tables["orders"]["marker"], Root)


def test_reject_two_root_fields():
    with pytest.raises(ValueError, match="Root"):

        class Hub(DataHub):
            orders: Annotated[pl.DataFrame, Root(metadata=None)]
            prices: Annotated[pl.DataFrame, Root(metadata=None)]


def test_reject_conflicting_markers_on_one_input():
    with pytest.raises(ValueError):

        class Hub(DataHub):
            orders: Annotated[pl.DataFrame, Root(metadata=None), Input(metadata=None)]


def test_input_declarations_do_not_modify_runtime_nodes(caplog):
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
    assert not [record for record in caplog.records if record.name == "pytred.data_hub"]


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
