from __future__ import annotations

from collections.abc import Sequence
from functools import reduce
import inspect
from logging import getLogger
from operator import and_
from typing import Literal

import polars as pl

from pytred.data_node import DataEdge
from pytred.data_node import DataflowGraph
from pytred.data_node import DataflowNode
from pytred.data_node import DataNode
from pytred.data_node import EmptyDataNode
from pytred.exceptions import TableNotFoundError
from pytred.helpers.decorator import get_metadata


logger = getLogger(__name__)


class DataHub:
    table_join_info: dict[str, str] | None = None
    table_join_keys: dict[str, Sequence[str]] | None = None
    registerd_tables_order: dict[str, int] | None = None

    def __init__(
        self,
        root_df: pl.DataFrame,
        *tables: DataNode,
        **named_tables: pl.DataFrame,
    ):
        """
        Initializes the DataHub with a base DataFrame and optionally additional DataNodes or
        named DataFrames.

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
            data_nodes = self.validate_tables_is_node(*tables)
            input_tables += data_nodes
        # tables of keyword arguments
        if len(named_tables) >= 1:
            input_tables += self.parse_tables_to_node(**named_tables)

        self.tables: dict[str, DataNode] = {}
        self.table_order = {}

        # User defined tables
        if self.registerd_tables_order is not None:
            self.table_order.update(self.registerd_tables_order)

        # input tables
        for data_node in input_tables:
            if data_node.name in self.tables.keys():
                raise ValueError(f"{data_node.name} is duplicated.")

            self.tables[data_node.name] = data_node

            self.table_order[data_node.name] = -1

        # Check table
        if len(self.table_order) == 0:
            raise TableNotFoundError(f"There are no table in {self.__class__.__name__}.")

    def __init_subclass__(cls, **kwargs):
        """
        Collects annotated functions and their execution order when initializing a subclass.
        """
        super().__init_subclass__(**kwargs)

        cls.table_join_info = {}
        cls.table_join_keys = {}
        cls.registerd_tables_order = {}

        # get annotated functions
        table_join_info, table_join_keys, table_order = cls._get_decorators()

        # sort tables
        cls.table_join_info.update(table_join_info)
        cls.table_join_keys.update(table_join_keys)
        cls.registerd_tables_order.update(table_order)

    @staticmethod
    def validate_tables_is_node(*tables) -> list[DataNode]:
        """
        Validate `tables` are DataNode instance.
        """
        input_data_node = []
        for data_node in tables:
            if isinstance(data_node, DataNode):
                input_data_node.append(data_node)
            else:
                raise TypeError(f"tables must be DataNode, not {type(data_node)}.")
        return input_data_node

    @staticmethod
    def parse_tables_to_node(**tables) -> list[DataNode]:
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

    @staticmethod
    def sort_tables_by_execute_order(table_order: dict[str, int]) -> list[tuple[str, int]]:
        """
        convert dictionary to list
        """

        list_table_order = sorted(table_order.items(), key=lambda x: x[1])

        return list_table_order

    @classmethod
    def _get_decorators(cls) -> tuple[dict, dict, dict]:
        """
        Collects and organizes information from annotated functions within the class.
        It extracts the function names, their associated join methods, and their execution order.

        This method is used internally to prepare the data processing pipeline before execution,
        ensuring that tables are created and joined in the correct order as defined by the
        annotations.

        Returns
        -------
        tuple[dict, dict, dict]
            A tuple containing three elements:
            - A dictionary mapping each function name to its specified join method.
            - A dictionary mapping each function name to its specified join keys.
            - A dictionary mapping each function name to its executing order
        """
        table_join_info = {}
        table_join_keys = {}
        table_order = {}
        for member in inspect.getmembers(cls):
            function_name = member[0]
            function = member[1]
            # annotated function has table_process_order
            if pytred_meta := getattr(function, "__pytred_meta__", None):
                table_join_info[function_name] = pytred_meta["join"]
                table_order[function_name] = pytred_meta["table_process_order"]
                if keys := pytred_meta.get("keys", None):
                    table_join_keys[function_name] = keys

        return table_join_info, table_join_keys, table_order

    def __call__(self, *filters: pl.Expr) -> pl.DataFrame:
        """
        Executes the data processing pipeline using the provided filter expressions as an alias
        to execute.

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
        Executes the data processing pipeline, including table creation, joins, and applying
        filter expressions.

        Parameters
        ----------
        filters : pl.Expr
            Filter expressions to apply to the output DataFrame.

        Returns
        -------
        pl.DataFrame
            The resulting DataFrame after applying the data processing pipeline and filters.
        """
        # On calling execute, annotated functions are executed to create each DataFrame as needed.
        # if self.table_functions is not None:
        if self.table_order is not None and max(v for v in self.table_order.values()) >= 0:
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
            table_node = self.tables[name]
            if table_node.join is None or isinstance(table_node, EmptyDataNode):
                # This is used only preprocessing.
                continue
            df = df.join(
                table_node.table,
                on=table_node.keys,
                how=table_node.join,
                suffix=f"_{table_node.name}",
            )
        return df

    @classmethod
    def collect_table_and_arguments(
        cls, table_order: dict[str, int], include_input_table: bool = False
    ):
        list_table_order = cls.sort_tables_by_execute_order(table_order)

        for name, order in list_table_order:
            if order == -1 and not include_input_table:
                continue

            # Input tables do not have argments
            arg_table_names = []
            if order >= 0:
                # get function arguments to check input tables
                sig = inspect.signature(getattr(cls, name))

                # Read arguments annotated with table information
                # Skip the first argument if it's declared as self or cls
                for idx, arg_name in enumerate(sig.parameters.keys(), 1):
                    if idx == 1 and (arg_name == "self" or arg_name == "cls"):
                        continue
                    else:
                        arg_table_names.append(arg_name)

            yield order, name, arg_table_names

    def create_tables(self):
        """
        Creates tables based on the annotated functions and their execution order.
        """
        for _, name, arg_table_names in self.collect_table_and_arguments(self.table_order):
            # data processing function
            process_fn = getattr(self, name)

            # Collect argument tables that do not exist
            missing_tables = [
                t
                for t in arg_table_names
                if t not in self.tables or isinstance(self.tables[t], EmptyDataNode)
            ]
            if get_metadata(process_fn, "is_optional") and missing_tables:
                logger.debug(
                    f"Process '{name}' is skipped, because these tables are not found: "
                    f"{missing_tables}"
                )
                self.tables[name] = EmptyDataNode(
                    name=name,
                    join=self.table_join_info[name],
                    keys=self.table_join_keys.get(name),
                    is_optional=True,
                )
            else:
                table = process_fn(*[self.get(table_name).table for table_name in arg_table_names])
                self.tables[name] = DataNode(
                    table,
                    self.table_join_keys.get(name),
                    join=self.table_join_info[name],
                    name=name,
                )

    @classmethod
    def search_tables(cls, *input_tables: EmptyDataNode) -> list:
        """
        Creates tables based on the annotated functions and their execution order.

        Parameters
        ----------
        input_tables: EmptyDataNode

        Returns
        -------
        list of DataflowNode
        """
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

        if cls.registerd_tables_order is None:
            raise ValueError("Functions as table are not found.")

        for order, name, arg_table_names in cls.collect_table_and_arguments(
            cls.registerd_tables_order
        ):
            # get function arguments to check input tables
            logger.info(f"target table name: {name}")

            node = cls._get_dataflow_node(order, name)

            for table_name in arg_table_names:
                for parent_node in processing_nodes:
                    if parent_node.name == table_name:
                        parent_node.add_child(node)

            processing_nodes.append(node)

        return processing_nodes

    @classmethod
    def _get_dataflow_node(cls, level: int, name: str) -> DataflowNode:
        """
        Get DataflowNode to visualize data processing graph.
        """
        if level == -1:
            shape = "[()]"
            join_type = None
            keys = None
        else:
            target_function = getattr(cls, name)
            join_type = get_metadata(target_function, "join")
            keys = get_metadata(target_function, "keys")
            shape = "[]" if join_type is None else "([])"

        node = DataflowNode(
            name,
            join=join_type,
            keys=keys,
            level=level,
            shape=shape,
        )

        return node

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
        except KeyError as err:
            raise KeyError(
                f"table '{table_name}' is not found: Current table list {self.tables.keys()}."
            ) from err

    @staticmethod
    def get_dataflow_graph(
        nodes: list[DataflowNode], direction: Literal["TD", "LR"] = "TD"
    ) -> DataflowGraph:
        """
        Get the hierarchical structure of data preprocessing nodes.

        Given a list of ProcessingNode objects, which each have a name, level, and children,
        this function creates a directed graph that visualizes the dependencies between the nodes.
        Nodes at the same level are grouped together, and dependencies are shown with edges.

        Parameters
        ----------
        nodes: list of DataflowNode
            A list of dataflow nodes, each with a name, level, and children.
        direction: {'TD', 'LR'}, default 'TD'
            graph direction

        Returns
        -------
        DataflowGraph
            A DataflowGraph object that can be rendered to visualize the tree structure.
        """

        graph = DataflowGraph(graph_direction=direction)

        max_level = max([node.level for node in nodes])

        for _node in sorted(nodes, key=lambda x: x.level):
            graph.add_node(_node)

            # add edge to children
            for _child_node in _node.children:
                level_diff = abs(_node.level - _child_node.level)
                link_type = "-" * level_diff + "->"
                edge = DataEdge(_node.name, _child_node.name, link_type=link_type)
                graph.add_edge(edge)

            # if node does not parents, make invisible edge
            if len(_node.parents) == 0 and _node.level >= 0:
                # target node index of invisible edge
                nodes_in_level = graph.get_nodes_by_level(_node.level)
                nodes_in_before_level = graph.get_nodes_by_level(_node.level - 1)

                target_node_index = min(len(nodes_in_level), len(nodes_in_before_level)) - 1

                invisible_edge = DataEdge(
                    nodes_in_before_level[target_node_index].name,
                    _node.name,
                    link_type="~~~",
                )
                graph.add_edge(invisible_edge)

            # add edge to output table
            if _node.join is not None:
                level_diff = max_level - _node.level
                if _node.keys is None:
                    keys = ""
                else:
                    keys = "<br>" + "<br>".join([f"- {key}" for key in _node.keys])
                link_type = "-" * (level_diff + 1) + f"->|{_node.join}{keys}|"
                edge = DataEdge(_node.name, "root_df", link_type=link_type)
                graph.add_edge(edge)

        graph.add_node(
            DataflowNode(name="root_df", join=None, keys=None, level=max_level + 1, shape="[[]]")
        )

        return graph

    def _repr_html_(self):
        """
        Display dataprocessing graph on jupyter notebook
        """
        processing_nodes = []
        for order, name, arg_table_names in self.collect_table_and_arguments(
            self.table_order, include_input_table=True
        ):
            # get function arguments to check input tables
            logger.info(f"target table name: {name}")

            node = self._get_dataflow_node(order, name)

            for table_name in arg_table_names:
                for parent_node in processing_nodes:
                    if parent_node.name == table_name:
                        parent_node.add_child(node)

            processing_nodes.append(node)

        graph = self.get_dataflow_graph(processing_nodes)

        html = (
            """
        <div class="mermaid">
        """
            + str(graph)
            + """
        </div>
        <script>
        if (window.mermaid) {{
            window.mermaid.contentLoaded();
        }}
        </script>
        """
        )
        return html
