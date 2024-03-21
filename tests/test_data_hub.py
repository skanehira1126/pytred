import polars as pl
import pytest

from pytred import DataHub, DataNode
from pytred.helpers.visualize import ProcessingNode


def test__initialize():
    """Test initialization of the DataHub with a base DataFrame and additional DataNodes or named DataFrames."""
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

    data_hub = DataHub(
        root_df,
        table1,
        table2=table2,
    )
    # verify ll tables (positional and keyword arguments) are correctly registered within the DataHub instance.
    actual = data_hub.tables
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
def test__raise_TypeError_when_invalid_l_table(inputs):
    with pytest.raises(TypeError):
        DataHub(
            pl.DataFrame({"id": ["a", "b", "c"]}),
            table=inputs,
        )


def test__basic_process(basic_data_hub):
    """
    Test the basic data processing pipeline of DataHub.
    """

    actual_result = basic_data_hub()

    # check processing order
    assert basic_data_hub.actual_called_order == basic_data_hub.expected_called_order
    # check result dataframe
    assert actual_result.equals(basic_data_hub.expected_result_table)


def test__raise_RuntimeError_no_tables():
    """Test that a RuntimeError is raised when no tables are provided to the DataHub."""
    from .fixtures.data_hub import InvalidDataHubNoTable

    with pytest.raises(RuntimeError):
        hub = InvalidDataHubNoTable()
        hub.execute()


@pytest.mark.parametrize(
    "filters",
    [
        [pl.col("id") == "a"],
        [pl.col("id") == "a", pl.col("id") != "b"],
    ],
)
def test__filterling_output_table(filters, basic_data_hub):
    """
    Test the basic data processing pipeline of DataHub.
    """
    actual_result = basic_data_hub(*filters)

    # check result dataframe
    expected_table = basic_data_hub.expected_result_table
    for filter in filters:
        expected_table = expected_table.filter(filter)

    assert actual_result.equals(expected_table)


def test__get_tables(basic_data_hub):
    """
    Test getting data by table name
    """
    basic_data_hub()

    # check created tables
    assert basic_data_hub.get("table1").table.equals(
        basic_data_hub.return_tables_of_each_function["table1"]
    )
    assert basic_data_hub.get("table2").table.equals(
        basic_data_hub.return_tables_of_each_function["table2"]
    )


def test__raise_KeyError_get_unknown_tables(basic_data_hub):
    """
    Test getting unknown data by table name
    """
    basic_data_hub()

    with pytest.raises(KeyError):
        basic_data_hub.get("aaa")


def test__search_table(complecated_data_hub):
    actual = complecated_data_hub.search_tables()

    expected = (
        [ProcessingNode("input_table", level=-1)]
        + [ProcessingNode(f"table1_{cnt}", level=0) for cnt in range(1, 5)]
        + [ProcessingNode(f"table2_{cnt}", level=1) for cnt in range(1, 5)]
        + [ProcessingNode("table3", level=2)]
    )

    # add childs
    expected[0].add_child(expected[1])
    expected[0].add_child(expected[2])
    expected[0].add_child(expected[5])
    expected[0].add_child(expected[7])

    expected[1].add_child(expected[5])
    expected[1].add_child(expected[6])

    expected[2].add_child(expected[6])

    expected[3].add_child(expected[7])
    expected[3].add_child(expected[8])

    expected[4].add_child(expected[9])
    expected[7].add_child(expected[9])
    expected[8].add_child(expected[9])

    assert actual == expected
