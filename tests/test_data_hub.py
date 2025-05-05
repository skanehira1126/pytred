import polars as pl
import pytest

from pytred import DataHub
from pytred import DataNode
from pytred.data_node import DataflowNode
from pytred.exceptions import TableNotFoundError

from .fixtures.data_hub import DataHubWithOptionalTable


def test__initialize():
    """
    Test initialization of the DataHub with a base DataFrame and additional DataNodes
    or named DataFrames.
    """
    root_df = pl.DataFrame({"id": ["a", "b", "c", "d"]})

    # inputs tables
    # for positional arguments
    table1 = DataNode(
        pl.DataFrame({"id": ["a", "b", "c", "d", "e"], "table1": [1, 1, 1, 1, 1]}),
        join="left",
        keys=["id"],
        name="table1",
    )

    # for keyward arguments
    table2 = pl.DataFrame({"id": ["a", "b", "c", "d", "e"], "table2": [2, 2, 2, 2, 2]})

    datahub = DataHub(
        root_df,
        table1,
        table2=table2,
    )
    # verify ll tables (positional and keyword arguments) are correctly registered
    # within the DataHub instance.
    actual = datahub.tables
    expected = {
        "table1": table1,
        "table2": DataNode(table2, keys=None, join=None, name="table2"),
    }
    assert actual == expected


def test__raise_ValueError_there_are_duplicated_table_name():
    """Test that a ValueError is raised when there are duplicated table names."""
    root_df = pl.DataFrame({"id": ["a", "b", "c", "d"]})

    # for positional arguments
    table1 = DataNode(
        pl.DataFrame({"id": ["a", "b", "c", "d", "e"], "table1": [1, 1, 1, 1, 1]}),
        join="left",
        keys=["id"],
        name="table1",
    )

    # for keyward arguments
    table2 = pl.DataFrame({"id": ["a", "b", "c", "d", "e"], "table2": [2, 2, 2, 2, 2]})

    with pytest.raises(ValueError):
        DataHub(
            root_df,
            table1,
            table1=table2,  # set table2 as table1
        )


@pytest.mark.parametrize(
    "inputs",
    [
        pl.DataFrame({"id": ["a", "b", "c", "d", "e"], "table2": [2, 2, 2, 2, 2]}),
        1,
        "aaa",
        {"a": 1, "b": 2},
    ],
)
def test__raise_TypeError_when_invalid_positional_table(inputs):
    with pytest.raises(TypeError):
        DataHub(
            pl.DataFrame({"id": ["a", "b", "c"]}),
            inputs,
        )


@pytest.mark.parametrize(
    "inputs",
    [
        DataNode(
            pl.DataFrame({"id": ["a", "b", "c", "d", "e"], "table2": [2, 2, 2, 2, 2]}),
            keys=None,
            join=None,
            name="test",
        ),
        1,
        "aaa",
        {"a": 1, "b": 2},
    ],
)
def test__raise_TypeError_when_invalid_table(inputs):
    with pytest.raises(TypeError):
        DataHub(
            pl.DataFrame({"id": ["a", "b", "c"]}),
            table=inputs,
        )


def test__basic_process(basic_datahub):
    """
    Test the basic data processing pipeline of DataHub.
    """

    actual_result = basic_datahub()

    # check processing order
    assert basic_datahub.actual_called_order == basic_datahub.expected_called_order
    # check result dataframe
    assert actual_result.equals(basic_datahub.expected_result_table)


def test__optional_datahub_with_table():
    """
    Test the optional processing pipeline in DataHub
    """
    dh = DataHubWithOptionalTable(
        root_df=pl.DataFrame({"id": ["a", "b", "c"]}),
        table_in1=pl.DataFrame({"id": ["a", "b", "c"], "table_in1": [1, 1, 1]}),
        table_in2=pl.DataFrame({"id": ["a", "b", "c"], "table_in2": [1, 1, 1]}),
    )

    output = dh()

    assert "table_in1" in output.columns
    assert "table_in2" in output.columns


def test__optional_datahub_without_table():
    """
    Test the optional processing pipeline in DataHub
    """
    dh = DataHubWithOptionalTable(
        root_df=pl.DataFrame({"id": ["a", "b", "c"]}),
        table_in1=pl.DataFrame({"id": ["a", "b", "c"], "table_in1": [1, 1, 1]}),
    )

    output = dh()

    assert "table_in1" in output.columns
    assert "table_in2" not in output.columns


def test__raise_RuntimeError_no_tables():
    """Test that a RuntimeError is raised when no tables are provided to the DataHub."""
    from .fixtures.data_hub import InvalidDataHubNoTable

    with pytest.raises(TableNotFoundError):
        _ = InvalidDataHubNoTable()


@pytest.mark.parametrize(
    "filters",
    [
        [pl.col("id") == "a"],
        [pl.col("id") == "a", pl.col("id") != "b"],
    ],
)
def test__filterling_output_table(filters, basic_datahub):
    """
    Test the basic data processing pipeline of DataHub.
    """
    actual_result = basic_datahub(*filters)

    # check result dataframe
    expected_table = basic_datahub.expected_result_table
    for filter in filters:
        expected_table = expected_table.filter(filter)

    assert actual_result.equals(expected_table)


def test__get_tables(basic_datahub):
    """
    Test getting data by table name
    """
    basic_datahub()

    # check created tables
    assert basic_datahub.get("table1").table.equals(
        basic_datahub.return_tables_of_each_function["table1"]
    )
    assert basic_datahub.get("table2").table.equals(
        basic_datahub.return_tables_of_each_function["table2"]
    )


def test__raise_KeyError_get_unknown_tables(basic_datahub):
    """
    Test getting unknown data by table name
    """
    basic_datahub()

    with pytest.raises(KeyError):
        basic_datahub.get("aaa")


def test__search_table(inputs_visualize_test):
    datahub_class, inputs_tables = inputs_visualize_test
    actual = datahub_class.search_tables(*inputs_tables)

    expected = [
        DataflowNode("input_table1", keys=("id",), join="left", level=-1, shape="[()]"),
        DataflowNode("input_table2", keys=None, join=None, level=-1, shape="[()]"),
        DataflowNode("table1_1", keys=("id",), join="inner", level=0, shape="([])"),
        DataflowNode("table1_2", keys=None, join="inner", level=0, shape="[]"),
        DataflowNode("table1_3", keys=("id1", "id2"), join="inner", level=0, shape="([])"),
        DataflowNode("table1_4", keys=None, join=None, level=0, shape="[]"),
        DataflowNode("table2_1", keys=None, join=None, level=1, shape="[]"),
        DataflowNode("table2_2", keys=None, join=None, level=1, shape="[]"),
        DataflowNode("table2_3", keys=None, join=None, level=1, shape="[]"),
        DataflowNode("table2_4", keys=None, join=None, level=1, shape="[]"),
        DataflowNode("table3", keys=("id",), join="left", level=2, shape="([])"),
    ]

    # add children
    expected[1].add_child(expected[2])
    expected[1].add_child(expected[3])
    expected[1].add_child(expected[6])
    expected[1].add_child(expected[8])

    expected[2].add_child(expected[6])
    expected[2].add_child(expected[7])

    expected[3].add_child(expected[7])

    expected[4].add_child(expected[8])
    expected[4].add_child(expected[9])

    expected[5].add_child(expected[10])
    expected[8].add_child(expected[10])
    expected[9].add_child(expected[10])

    assert actual == expected
