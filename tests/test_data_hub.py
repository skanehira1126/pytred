from typing import Literal

import polars as pl
from polars.testing import assert_frame_equal
import pytest

from pytred import DataHub
from pytred import DataNode
from pytred.data_node import EmptyDataNode
from pytred.decorators import polars_table
from pytred.exceptions import TableNotFoundError
from pytred.metadata import TableMetadata

from .fixtures.data_hub import BranchingHub
from .fixtures.data_hub import OptionalHub


@pytest.fixture
def hub():
    return BranchingHub(
        pl.DataFrame({"id": [1, 2, 3, 4]}),
        source=pl.DataFrame({"id": [1, 2, 3], "value": [1, 2, 3]}),
    )


def test_register_and_join_inputs():
    frame = pl.DataFrame({"id": [1], "value": [10]})
    node = DataNode(frame, keys=("id",), join="left", name="joined")
    hub = DataHub(pl.DataFrame({"id": [1, 2]}), node, source=frame)

    assert set(hub.tables) == {"joined", "source"}
    assert hub.get("joined") is node
    source = hub.get("source")
    assert (source.name, source.keys, source.join) == ("source", None, None)
    assert_frame_equal(source.table, frame)
    assert_frame_equal(hub(), pl.DataFrame({"id": [1, 2], "value": [10, None]}))


def test_reject_duplicate_input_names():
    frame = pl.DataFrame({"id": [1]})
    node = DataNode(frame, keys=("id",), join="left", name="source")

    with pytest.raises(ValueError, match="duplicated"):
        DataHub(frame, node, source=frame)


def test_reject_dataframe_as_positional_input():
    with pytest.raises(TypeError, match="DataNode"):
        DataHub(pl.DataFrame(), pl.DataFrame())


def test_reject_node_as_named_input():
    node = DataNode(pl.DataFrame(), keys=None, join=None, name="source")

    with pytest.raises(TypeError, match="pl.DataFrame"):
        DataHub(pl.DataFrame(), source=node)


def test_reject_hub_without_tables():
    with pytest.raises(TableNotFoundError):
        DataHub(pl.DataFrame())


def test_execute_dependencies_and_join_only_output(hub):
    assert_frame_equal(hub(), pl.DataFrame({"id": [1, 2, 3, 4], "score": [4, 7, 10, None]}))
    assert_frame_equal(
        hub.get("doubled").table, pl.DataFrame({"id": [1, 2, 3], "doubled": [2, 4, 6]})
    )
    assert_frame_equal(
        hub.get("offset").table, pl.DataFrame({"id": [1, 2, 3], "offset": [2, 3, 4]})
    )


@pytest.mark.parametrize(
    "filters, expected",
    [
        ([pl.col("score") > 4], {"id": [2, 3], "score": [7, 10]}),
        ([pl.col("score") > 4, pl.col("score") < 10], {"id": [2], "score": [7]}),
    ],
    ids=["single", "intersection"],
)
def test_filter_output(hub, filters, expected):
    assert_frame_equal(hub(*filters), pl.DataFrame(expected))


@pytest.mark.parametrize("with_source", [True, False], ids=["present", "missing"])
def test_optional_dependency_chain(with_source):
    """Execute or skip the dependency chain while keeping omitted metadata as None."""
    root = pl.DataFrame({"id": [1]})
    source = pl.DataFrame({"id": [1], "value": [10]})
    hub = OptionalHub(root, **({"source": source} if with_source else {}))

    assert_frame_equal(hub(), source if with_source else root)
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
        @polars_table(0, is_optional=True, metadata=prepared_metadata)
        def prepared(self, source):
            return source

        @polars_table(1, "id", join="left", is_optional=True, metadata=result_metadata)
        def result(self, prepared):
            return prepared

    root = pl.DataFrame({"id": [1]})
    source = pl.DataFrame({"id": [1], "value": [10]})
    hub = MetadataHub(root, **({"source": source} if with_source else {}))

    assert_frame_equal(hub(), source if with_source else root)
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


def test_omitted_output_metadata_does_not_inherit_parent_value():
    """Default a child's metadata to None without changing its parent's metadata."""
    metadata = TableMetadata(description="Parent output")

    class ParentHub(DataHub, metadata=metadata):
        pass

    class ChildHub(ParentHub):
        pass

    assert ChildHub.metadata is None
    assert ParentHub.metadata is metadata


def test_get_unknown_table(hub):
    hub()
    with pytest.raises(KeyError, match="missing"):
        hub.get("missing")
