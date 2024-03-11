import polars as pl
import pytest

from pytred import DataHub, DataNode


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


def test__basic_process():
    """
    Test the basic data processing pipeline of DataHub.
    """
    from .helpers.data_hub import BasicDataHub

    hub = BasicDataHub()

    actual_result = hub()

    # check processing order
    assert hub.actual_called_order == hub.expected_called_order
    # check result dataframe
    assert actual_result.equals(hub.expected_result_table)


def test__raise_RuntimeError_no_tables():
    """Test that a RuntimeError is raised when no tables are provided to the DataHub."""
    from .helpers.data_hub import InvalidDataHubNoTable

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
def test__filterling_output_table(filters):
    """
    Test the basic data processing pipeline of DataHub.
    """
    from .helpers.data_hub import BasicDataHub

    hub = BasicDataHub()

    actual_result = hub(*filters)

    # check result dataframe
    expected_table = hub.expected_result_table
    for filter in filters:
        expected_table = expected_table.filter(filter)

    assert actual_result.equals(expected_table)


def test__get_tables():
    """
    Test getting data by table name
    """
    from .helpers.data_hub import BasicDataHub

    hub = BasicDataHub()

    hub()

    # check created tables
    assert hub.get("table1").table.equals(hub.return_tables_of_each_function["table1"])
    assert hub.get("table2").table.equals(hub.return_tables_of_each_function["table2"])


def test__raise_KeyError_get_unknown_tables():
    """
    Test getting unknown data by table name
    """
    from .helpers.data_hub import BasicDataHub

    hub = BasicDataHub()

    hub()

    with pytest.raises(KeyError):
        hub.get("aaa")
