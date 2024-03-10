from __future__ import annotations

import inspect
from collections import OrderedDict
from functools import reduce
from logging import getLogger
from operator import and_
from typing import Callable

import numpy as np
import polars as pl

from pytred.data_node import DataNode

logger = getLogger(__name__)


class DataHub:
    tables: dict[str, DataNode]
    table_functions: OrderedDict[str, Callable] | None = None
    table_join_info: dict[str, str] | None = None

    def __init__(
        self,
        root_df: pl.DataFrame,
        *tables: DataNode,
        **named_tables: pl.DataFrame,
    ):
        """
        Initializes the DataHub with a base DataFrame and optionally additional DataNodes or named DataFrames.

        Parameters
        ----------
        root_df: pl.DataFrame
            The base DataFrame to be used as the primary table.
        tables: DataNode
            Positional arguments for DataNode objects to be registered.
        **named_tables: pl.DataFrame
            Keyword arguments for named DataFrames or DataNodes to be registered.
        """

        self.root_df = root_df

        # parse additional tables
        input_tables: list[DataNode] = []
        # tables of positional arguments
        if len(tables) >= 1:
            for data_node in tables:
                if isinstance(data_node, DataNode):
                    input_tables.append(data_node)
                else:
                    raise TypeError(f"tables must be DataNode, not {type(data_node)}.")
        # tables of keyword arguments
        if len(named_tables) >= 1:
            for name, df in named_tables.items():
                if isinstance(df, pl.DataFrame):
                    input_tables.append(DataNode(df, keys=None, join=None, name=name))
                else:
                    raise TypeError(
                        f"named_tables must be pl.DataFrame, not {type(df)}."
                    )
        self.tables = {}
        for data_node in input_tables:
            if data_node.name in self.tables.keys():
                raise ValueError(f"{data_node.name} is duplicated.")

            self.tables[data_node.name] = data_node

    def __init_subclass__(cls, **kwargs):
        """
        Collects annotated functions and their execution order when initializing a subclass.
        """
        super().__init_subclass__(**kwargs)
        # get anotatted functions
        tables, table_join_info, sort_index = cls._get_annotations()

        # sort tables
        if len(tables):
            cls.table_functions = OrderedDict(tables[sort_index])
            cls.table_join_info = table_join_info
        cls.tables = {}

    @classmethod
    def _get_annotations(cls) -> tuple[np.ndarray, dict, np.ndarray]:
        """
        Collects and organizes information from annotated functions within the class.
        It extracts the function names, their associated join methods, and their execution order.

        This method is used internally to prepare the data processing pipeline before execution,
        ensuring that tables are created and joined in the correct order as defined by the annotations.

        Returns
        -------
        tuple[np.ndarray, dict, np.ndarray]
            A tuple containing three elements:
            - An array of function and , each containing the name and reference to an annotated function.
            - A dictionary mapping each function name to its specified join method.
            - An array of indices representing the sorted execution order of the functions.
        """
        tables = []
        table_join_info = {}
        order = []
        for member in inspect.getmembers(cls):
            function_name = member[0]
            function = member[1]
            # annotated function has table_process_order
            if hasattr(function, "table_process_order"):
                tables.append([function_name, function])
                table_join_info[function_name] = function.join
                order.append(function.table_process_order)

        sort_index = np.argsort(order)
        return np.array(tables), table_join_info, sort_index

    def __call__(self, *filters: pl.Expr) -> pl.DataFrame:
        """
        Executes the data processing pipeline using the provided filter expressions as an alias to execute.

        Parameters
        ----------
        filters : pl.Expr
            Filter expressions to apply to the output DataFrame.

        Returns
        -------
        pl.DataFrame
            The resulting DataFrame after applying the data processing pipeline and filters.
        """
        return self.execute(*filters)

    def execute(self, *filters: pl.Expr) -> pl.DataFrame:
        """
        Executes the data processing pipeline, including table creation, joins, and applying filter expressions.

        Parameters
        ----------
        filters : pl.Expr
            Filter expressions to apply to the output DataFrame.

        Returns
        -------
        pl.DataFrame
            The resulting DataFrame after applying the data processing pipeline and filters.
        """
        # On calling execute(), annotated functions are executed to create each DataFrame as needed.
        if self.table_functions is not None:
            self.create_tables()

        # Validate to self.tables is not empty.
        if self.tables is None or len(self.tables) == 0:
            raise RuntimeError("There are not tables.")

        # join table
        df = self.steps()

        # processing of joined dataframe
        df = self.post_step(df)

        # filterling
        if filters:
            df = df.filter(reduce(and_, filters))

        return df

    def steps(self) -> pl.DataFrame:
        """
        Joins root_df and each tables according to the specified join conditions.

        Returns
        -------
        pl.DataFrame
            The DataFrame after joining the specified tables.
        """
        df = self.root_df.clone()
        for table_info in self.tables.values():
            if table_info.join is None:
                # This is used only preprocessing.
                continue
            df = df.join(
                table_info.table,
                on=table_info.keys,
                how=table_info.join,
                suffix=f"_{table_info.name}",
            )
        return df

    def create_tables(self):
        """
        Creates tables based on the annotated functions and their execution order.
        """
        for name, function in self.table_functions.items():
            # get function arguments to check input tables
            sig = inspect.signature(function)

            # Read arguments annotated with table information, executing past tables as arguments
            # Skip the first argument if it's declared as self or cls
            arg_tables = []
            for idx, arg_name in enumerate(sig.parameters.keys(), 1):
                if idx == 1 and (arg_name == "self" or arg_name == "cls"):
                    continue
                else:
                    arg_tables.append(self.get(arg_name).table)

            # set generated tables
            table, keys = function(self, *arg_tables)
            self.tables[name] = DataNode(
                table, keys, join=self.table_join_info[name], name=name
            )

    def post_step(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Performs any final processing on the joined DataFrame.

        Parameters
        ----------
        df : pl.DataFrame
            The DataFrame to be processed.

        Returns
        -------
        pl.DataFrame
            The processed DataFrame.
        """
        return df

    def get(self, table_name: str) -> DataNode:
        """
        Retrieves a DataNode by its name.

        Parameters
        ----------
        table_name: str
            The name of DataNode to retrieve

        Returns
        -------
        DataNode
            The requested DataNode object.

        Raises
        ------
        RuntimeError
            If the tables dictionary is empty or not found.
        KeyError
            If the specified table name is not found in the tables dictionary.
        """
        try:
            return self.tables[table_name]
        except KeyError:
            raise KeyError(
                f"table '{table_name}' is not found: Current table list {self.tables.keys()}."
            )
