import polars as pl

from pytred import DataHub
from pytred import DataNode
from pytred.decorators import polars_table


class BasicDataHub(DataHub):

    def __init__(self):

        root_df = pl.DataFrame({"id": ["a", "b", "c"]})

        # inputs table
        inputs_table = DataNode(
            pl.DataFrame({"id": ["a", "b", "c"], "inputs_table": ["input", "input", "input"]}),
            join="left",
            keys=["id"],
            name="input_table2",
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
        return table1.select("id", col1_2=pl.col("col1") + 1).filter(pl.col("id") != "c")

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
    結合するテーブルが存在しない
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


class ComplecatedDataHub(DataHub):
    """
    This is example of visualizing data processing flow
    """

    @polars_table(0, "id", join="inner")
    def table1_1(self, input_table2):
        """
        Description of table1_1
        """
        ...

    @polars_table(0, None, join=None)
    def table1_2(self, input_table2):
        # this function has not docstrings
        ...

    @polars_table(0, "id1", "id2", join="left")
    def table1_3(self):
        """
        Description of table1_3
        """
        ...

    @polars_table(0, None, join=None)
    def table1_4(self):
        """
        multi lines description of table1_4
        multi lines description of table1_4
        """
        ...

    @polars_table(1, None, join=None)
    def table2_1(self, input_table2, table1_1):
        # this function has not docstrings
        ...

    @polars_table(1, None, join=None)
    def table2_2(self, table1_1, table1_2):
        # this function has not docstrings
        ...

    @polars_table(1, None, join=None)
    def table2_3(self, input_table2, table1_3):
        """
        Description of table2_3
        """
        ...

    @polars_table(1, None, join=None)
    def table2_4(self, table1_3):
        """
        Description of table2_4
        """
        ...

    @polars_table(2, "id", join="left")
    def table3(self, table1_4, table2_3, table2_4):
        """
        Description of table3
        """
        ...
