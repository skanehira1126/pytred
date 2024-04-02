from __future__ import annotations

import inspect
from functools import reduce
from logging import getLogger
from operator import and_
from typing import Callable

import polars as pl

from pytred.data_node import DataNode, DummyDataNode

logger = getLogger(__name__)


class DataHub:
    tables: dict[str, DataNode]
    table_functions: dict[str, Callable] | None = None
    table_join_info: dict[str, str] | None = None
    table_order: dict[str, int] | None = None

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
            input_tables += self.validate_tables_is_data_node(*tables)
        # tables of keyword arguments
        if len(named_tables) >= 1:
            input_tables += self.parse_tables_to_data_node(**named_tables)

        self.tables = {}
        # If no superclass is specified, default to None and create an empty dictionary.
        if self.table_functions is None:
            self.table_functions = {}
        if self.table_join_info is None:
            self.table_join_info = {}
        if self.table_order is None:
            self.table_order = {}

        for data_node in input_tables:
            if data_node.name in self.tables.keys():
                raise ValueError(f"{data_node.name} is duplicated.")

            self.tables[data_node.name] = data_node

            self.table_order.update({name: -1 for name in self.tables.keys()})

    def __init_subclass__(cls, **kwargs):
        """
        Collects annotated functions and their execution order when initializing a subclass.
        """
        super().__init_subclass__(**kwargs)
        # get annotated functions
        table_functions, table_join_info, table_order = cls._get_decorators()

        # sort tables
        if len(table_functions):
            cls.table_functions = table_functions
            cls.table_join_info = table_join_info
            cls.table_order = table_order
        cls.tables = {}

    @staticmethod
    def validate_tables_is_data_node(*tables) -> list[DataNode]:
        """
        Validate `tables` are DataNode instance.
        """
        input_tables = []
        for data_node in tables:
            if isinstance(data_node, DataNode):
                input_tables.append(data_node)
            else:
                raise TypeError(f"tables must be DataNode, not {type(data_node)}.")
        return input_tables

    @staticmethod
    def parse_tables_to_data_node(**tables) -> list[DataNode]:
        """
        Parse `tables` to DataNode instance
        """
        input_tables = []
        for name, df in tables.items():
            if isinstance(df, pl.DataFrame):
                input_tables.append(DataNode(df, keys=None, join=None, name=name))
            else:
                raise TypeError(f"named_tables must be pl.DataFrame, not {type(df)}.")
        return input_tables

    @classmethod
    def _get_decorators(cls) -> tuple[dict, dict, dict]:
        """
        Collects and organizes information from annotated functions within the class.
        It extracts the function names, their associated join methods, and their execution order.

        This method is used internally to prepare the data processing pipeline before execution,
        ensuring that tables are created and joined in the correct order as defined by the annotations.

        Returns
        -------
        tuple[dict, dict, dict]
            A tuple containing three elements:
            - An dicionary of function, each containing the name and reference to an annotated function.
            - A dictionary mapping each function name to its specified join method.
            - A dictionary mapping each function name to its executing order
        """
        tables = {}
        table_join_info = {}
        table_order = {}
        for member in inspect.getmembers(cls):
            function_name = member[0]
            function = member[1]
            # annotated function has table_process_order
            if hasattr(function, "table_process_order"):
                tables[function_name] = function
                table_join_info[function_name] = function.join
                table_order[function_name] = function.table_process_order

        return tables, table_join_info, table_order

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
        if self.table_order is None:
            raise RuntimeError("Unexpected Error: table_order is None.")
        df = self.root_df.clone()

        for name, _ in sorted(self.table_order.items(), key=lambda x: x[1]):
            table_info = self.tables[name]
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

    def collect_table_and_arguments(self):

        for name, order in sorted(self.table_order.items(), key=lambda x: x[1]):
            if order == -1:
                continue
            function = self.table_functions[name]
            # get function arguments to check input tables
            sig = inspect.signature(function)

            # Read arguments annotated with table information, executing past tables as arguments
            # Skip the first argument if it's declared as self or cls
            arg_table_names = []
            for idx, arg_name in enumerate(sig.parameters.keys(), 1):
                if idx == 1 and (arg_name == "self" or arg_name == "cls"):
                    continue
                else:
                    arg_table_names.append(arg_name)

            yield order, name, function, arg_table_names

    def create_tables(self):
        """
        Creates tables based on the annotated functions and their execution order.
        """
        for _, name, function, arg_table_names in self.collect_table_and_arguments():
            # execute processing function
            table = function(self, *[self.get(table_name).table for table_name in arg_table_names])
            self.tables[name] = DataNode(
                table, function.keys, join=self.table_join_info[name], name=name
            )

    @classmethod
    def search_tables(cls, *input_tables: DummyDataNode) -> list:
        """
        Creates tables based on the annotated functions and their execution order.

        Parameters
        ----------
        input_table: DataNode

        Returns
        -------
        list of DataflowNode
        """
        from pytred.data_node import DataflowNode

        processing_nodes = [
            DataflowNode(
                data_node.name,
                join=data_node.join,
                keys=data_node.keys,
                level=-1,
                shape="[()]",
            )
            for data_node in input_tables
        ]

        for order, name, function, arg_table_names in cls.collect_table_and_arguments(cls):  # type: ignore
            # get function arguments to check input tables
            logger.info(f"target table name: {name}")

            join_type = function.join  # type: ignore
            if join_type is None:  # type: ignore
                shape = "[]"
            else:
                shape = "([])"

            node = DataflowNode(
                name,
                join=join_type,
                keys=cls.table_functions[name].keys,  # type: ignore
                level=order,
                shape=shape,
            )

            for table_name in arg_table_names:
                for parent_node in processing_nodes:
                    if parent_node.name == table_name:
                        parent_node.add_child(node)

            processing_nodes.append(node)

        return processing_nodes

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
