import polars as pl

from pytred import DataHub, DataNode
from pytred.decorators import polars_table


class BasicDataHub(DataHub):

    def __init__(self):

        root_df = pl.DataFrame({"id": ["a", "b", "c"]})

        # inputs table
        inputs_table = DataNode(
            pl.DataFrame(
                {"id": ["a", "b", "c"], "inputs_table": ["input", "input", "input"]}
            ),
            join="left",
            keys=["id"],
            name="input_table",
        )

        super().__init__(root_df, inputs_table)

        # Variables for test
        self.return_tables_of_each_function = {
            "table1": pl.DataFrame({"id": ["a", "b", "c"], "col1": [1, 1, 1]}),
            "table2": pl.DataFrame({"id": ["a", "b", "c"], "col2": [2, 2, 2]}),
        }

        self.expected_called_order = [
            "table1",
            "table2",
            "table1_2",
            "table_not_in_output",
        ]
        self.expected_result_table = pl.DataFrame(
            {
                "id": ["a", "b", "c"],
                "inputs_table": ["input", "input", "input"],
                "col1": [1, 1, 1],
                "col2": [2, 2, 2],
                "col1_2": [2, 2, None],
            }
        )

        self.actual_called_order = []

    @polars_table(0, "id", join="left")
    def table1(self):
        self.actual_called_order.append("table1")
        return self.return_tables_of_each_function["table1"]

    @polars_table(1, "id", join="left")
    def table1_2(self, table1):
        self.actual_called_order.append("table1_2")
        return table1.select("id", col1_2=pl.col("col1") + 1).filter(
            pl.col("id") != "c"
        )

    @polars_table(0, "id", join="left")
    def table2(self):
        self.actual_called_order.append("table2")
        return self.return_tables_of_each_function["table2"]

    @polars_table(2, join=None)
    def table_not_in_output(self):
        self.actual_called_order.append("table_not_in_output")
        return pl.DataFrame()


class InvalidDataHubNoTable(DataHub):
    """
    結合しないテーブルが存在しない
    """

    def __init__(self):
        super().__init__(pl.DataFrame({"id": ["a", "b"]}))


class InvalidDataHubNotReturnDataFrame(DataHub):
    """
    polarsのDataFrameを返却しない関数がある
    """

    def __init__(self, invalid_function_return_value):
        self.invalid_function_reutrn_value = invalid_function_return_value
        super().__init__(pl.DataFrame({"id": ["a", "b"]}))

    @polars_table(0, "id", join="left")
    def test(self):
        return self.invalid_function_reutrn_value
