import polars as pl
from polars.testing import assert_frame_equal
import pytest

from pytred import DataHub
from pytred import DataNode
from pytred.data_node import EmptyDataNode
from pytred.exceptions import TableNotFoundError

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
    root = pl.DataFrame({"id": [1]})
    source = pl.DataFrame({"id": [1], "value": [10]})
    hub = OptionalHub(root, **({"source": source} if with_source else {}))

    assert_frame_equal(hub(), source if with_source else root)
    for name in ("prepared", "result"):
        assert isinstance(hub.get(name), EmptyDataNode) is (not with_source)


def test_get_unknown_table(hub):
    hub()
    with pytest.raises(KeyError, match="missing"):
        hub.get("missing")
