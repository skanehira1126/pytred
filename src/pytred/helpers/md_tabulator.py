from __future__ import annotations

from typing import Literal
from typing import Sequence

import pandas as pd

from pytred.exceptions import InvalidFunctionCalledError


class MarkdownTableTabulator:

    def __init__(
        self,
        mode: Literal["row", "column"],
    ):
        """
        Parameters
        ----------
        mode: {"row", "column"}
            data unit of table
        """
        self.mode = mode

        if mode == "row":
            self.row_data: list[dict] = []
            self.keys: set | None = None
        elif mode == "column":
            self.column_data: dict[str, Sequence] = {}
            self.length: int | None = None
        else:
            raise ValueError(f"mode must be 'row' or 'column', not {mode}.")

    def add_columns(
        self,
        **kwargs: Sequence,
    ):
        """
        Add columns
        """
        if self.mode != "column":
            raise InvalidFunctionCalledError("Column can be added when mode is 'column'.")

        for col_name, values in kwargs.items():
            if self.length is not None and self.length != len(values):
                raise ValueError("Length of input values is mismatch.")
            self.column_data[col_name] = values
            if self.length is None:
                self.length = len(values)

    def add_rows(
        self,
        *rows: dict,
    ):
        """
        Add rows
        """
        if self.mode != "row":
            raise InvalidFunctionCalledError("Row can be added when mode is 'row'.")

        for row in rows:
            if self.keys is not None and self.keys != set(row.keys()):
                raise ValueError("Columns are mismatch")

            self.row_data.append(row)
            if self.keys is None:
                self.keys = set(row.keys())

    def build(self, index: bool = False):
        """
        returns dataframe as markdown format

        Parameters
        ----------
        index: bool, default False
            include index column in output table
        """
        if self.mode == "row":
            table = pd.DataFrame(self.row_data)
        else:
            table = pd.DataFrame(self.column_data)

        return table.to_markdown(index=index)
