from __future__ import annotations

from collections.abc import Iterator
from collections.abc import Sequence
from dataclasses import replace
import inspect
from logging import getLogger
from typing import Annotated
from typing import ClassVar
from typing import get_args
from typing import get_origin
from typing import get_type_hints

import polars as pl

from pytred._types import Input
from pytred._types import InputDefinition
from pytred._types import PolarsJoinMethod
from pytred._types import Root
from pytred._types import validate_polars_join
from pytred.data_node import DataNode
from pytred.data_node import EmptyDataNode
from pytred.exceptions import InvalidReturnValueError
from pytred.exceptions import TableNotFoundError
from pytred.helpers import visualize
from pytred.helpers.decorator import get_metadata
from pytred.metadata import TableMetadata


logger = getLogger(__name__)

TableJoinMethod = dict[str, PolarsJoinMethod | None]
TableJoinKeys = dict[str, Sequence[str]]
TableExecutionOrders = dict[str, int]
PrepareFunctions = dict[str, str]
ValidateFunctions = list[str]


class DataHub:
    """Run an ordered pipeline of Polars tables and join its results to a root DataFrame.

    Define a pipeline as a direct subclass of DataHub. Mark one required input
    with ``Annotated[pl.DataFrame, Root(...)]`` to select the base for joins.
    Mark other required inputs with ``Input(...)``. Each decorated method receives
    tables by parameter name and runs in the order set by its ``@polars_table``
    decorator. The class keyword ``metadata`` describes the final output.

    Create the hub without data, then pass the inputs for each run to
    ``execute()``. A parent pipeline can pass an intermediate DataFrame as
    the named root of a child hub. Raw-input validators run before input
    preparation. Table methods receive the prepared DataFrames.

    Examples
    --------
    >>> from typing import Annotated
    >>> import polars as pl
    >>> from pytred import DataHub
    >>> from pytred._types import Root
    >>> from pytred.decorators import polars_table
    >>> class Scores(DataHub):
    ...     rows: Annotated[pl.DataFrame, Root(metadata=None)]
    ...
    ...     @polars_table(0, "id", join="left")
    ...     def scores(self, rows):
    ...         return rows.select("id", score=pl.col("id") * 10)
    >>> hub = Scores()
    >>> hub.execute(rows=pl.DataFrame({"id": [1, 2]})).rows()
    [(1, 10), (2, 20)]
    """

    table_join_info: TableJoinMethod | None = None
    table_join_keys: TableJoinKeys | None = None
    registered_tables_order: ClassVar[TableExecutionOrders] = {}
    prepare_functions: PrepareFunctions
    validate_functions: ValidateFunctions

    metadata: ClassVar[TableMetadata | None] = None
    input_tables: ClassVar[dict[str, InputDefinition]] = {}

    def __init__(self):
        self.tables: dict[str, DataNode | EmptyDataNode] = {}
        self.table_order = self.registered_tables_order.copy()

    def __init_subclass__(
        cls,
        *,
        metadata: TableMetadata | None = None,
        **kwargs,
    ):
        """Collect this pipeline's input declarations and decorated table methods.

        Raises TypeError for a subclass of a user-defined hub and ValueError
        for multiple input markers on one field, multiple root fields,
        input names conflicting with processing methods, or multiple prepare
        hooks targeting one input.
        """
        super().__init_subclass__(**kwargs)
        for base in cls.__bases__:
            if base is not DataHub and issubclass(base, DataHub):
                raise TypeError(
                    f"{cls.__name__} cannot inherit from {base.__name__}. "
                    "Inherit directly from DataHub."
                )

        cls.metadata = metadata

        hints = get_type_hints(cls, include_extras=True)

        definitions: dict[str, InputDefinition] = {}
        root_name: str | None = None
        for name, annotation in hints.items():
            if get_origin(annotation) is not Annotated:
                continue

            dtype, *extras = get_args(annotation)
            markers = [extra for extra in extras if isinstance(extra, Input)]
            if len(markers) >= 2:
                raise ValueError(
                    f"{cls.__name__}.{name} has multiple input markers. "
                    "Use a single Input or Root marker."
                )
            for extra in markers:
                if isinstance(extra, Root):
                    if root_name is not None:
                        raise ValueError(
                            f"{cls.__name__} has two Root fields ('{root_name}' and '{name}'). "
                            "Only one field can be marked as Root."
                        )
                    root_name = name
                definitions[name] = {"dtype": dtype, "marker": extra}

        cls.input_tables = definitions

        cls.table_join_info = {}
        cls.table_join_keys = {}
        cls.registered_tables_order = {}

        # get annotated functions
        (
            table_join_info,
            table_join_keys,
            table_order,
            prepare_functions,
            validate_functions,
        ) = cls._get_decorators()

        if conflicts := cls.input_tables.keys() & table_order.keys():
            raise ValueError(f"Input tables conflict with processing methods: {sorted(conflicts)}")

        cls.table_join_info.update(table_join_info)
        cls.table_join_keys.update(table_join_keys)
        cls.registered_tables_order.update(table_order)
        cls.prepare_functions = prepare_functions
        cls.validate_functions = validate_functions

    @property
    def root_df(self):
        """Return the root DataFrame registered in the current run.

        Raises KeyError if the declared root has not yet been supplied, or
        RuntimeError if the hub has no root declaration or the root was skipped.
        """
        name = self.get_root_table_name()
        node = self.get(name)
        if isinstance(node, DataNode):
            return node.table
        raise RuntimeError(f"Root table {name!r} was skipped.")

    def validate_tables_is_node(self, *tables) -> list[DataNode]:
        """Return positional DataNodes in order, preserving their join settings.

        Raises TypeError for a non-node argument or ValueError for an unsupported
        join or a node named after the root, which must be a keyword DataFrame.
        """
        input_data_node = []
        for data_node in tables:
            if isinstance(data_node, DataNode):
                validate_polars_join(data_node.join)
                input_data_node.append(data_node)
            else:
                raise TypeError(f"tables must be DataNode, not {type(data_node)}.")

        for node in input_data_node:
            if node.name == self.get_root_table_name():
                raise ValueError(
                    f"Pass the root table '{node.name}' as a named DataFrame argument."
                )
        return input_data_node

    @staticmethod
    def parse_tables_to_node(**tables) -> list[DataNode]:
        """
        Wrap named DataFrames as DataNodes.
        """
        input_tables: list[DataNode] = []
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
    def _get_decorators(
        cls,
    ) -> tuple[
        TableJoinMethod,
        TableJoinKeys,
        TableExecutionOrders,
        PrepareFunctions,
        ValidateFunctions,
    ]:
        """Collect join settings, execution orders, prepare targets and raw validators."""
        # tables
        table_join_info = {}
        table_join_keys = {}
        table_order = {}

        # prepare inputs
        prepare_input_fn: PrepareFunctions = {}

        # validate inputs
        validate_input_fn_list = []

        for member in inspect.getmembers(cls):
            function_name = member[0]
            function = member[1]
            # annotated function has table_process_order
            pytred_meta = getattr(function, "__pytred_meta__", None)
            if pytred_meta is None:
                continue

            if pytred_meta["meta_type"] == "table":
                table_join_info[function_name] = pytred_meta["join"]
                table_order[function_name] = pytred_meta["table_process_order"]
                if keys := pytred_meta.get("keys", None):
                    table_join_keys[function_name] = keys

            if pytred_meta["meta_type"] == "prepare":
                target = pytred_meta["target"]
                if target in prepare_input_fn:
                    previous = prepare_input_fn[target]
                    raise ValueError(
                        f"{cls.__name__} defines two prepare hooks for {target!r}: "
                        f"{previous} and {function_name}. Combine them into one method."
                    )
                prepare_input_fn[target] = function_name

            if pytred_meta["meta_type"] == "validate":
                validate_input_fn_list.append(function_name)

        return (
            table_join_info,
            table_join_keys,
            table_order,
            prepare_input_fn,
            validate_input_fn_list,
        )

    def get_root_table_name(self) -> str:
        """Return the declared root field name, raising RuntimeError if none is declared."""
        root_name = None
        for name, defs in self.input_tables.items():
            if isinstance(defs["marker"], Root):
                root_name = name

        if root_name is None:
            raise RuntimeError("Root table is not found.")
        else:
            return root_name

    def get_input_table_names(self) -> list[str]:
        """Return all declared input names, including the root.

        These names are available before execution. Raises RuntimeError when
        the pipeline declares no inputs.
        """
        inputs = []
        for name, defs in self.input_tables.items():
            if isinstance(defs["marker"], Input):
                inputs.append(name)

        if inputs:
            return inputs
        else:
            raise RuntimeError("Input table is not found.")

    def __call__(
        self,
        *tables: DataNode,
        filter: pl.Expr | None = None,
        **named_tables: pl.DataFrame,
    ) -> pl.DataFrame:
        """
        Execute the pipeline with the same inputs and filter as execute().
        """
        return self.execute(*tables, filter=filter, **named_tables)

    def execute(
        self,
        *tables: DataNode,
        filter: pl.Expr | None = None,
        **named_tables: pl.DataFrame,
    ) -> pl.DataFrame:
        """
        Clear previous results, register inputs, execute methods and join their outputs.

        Parameters
        ----------
        *tables : DataNode
            Computed non-root tables, each with its own name and join settings.
        filter : pl.Expr, optional
            Filter applied after all joins and post_step().
        **named_tables : pl.DataFrame
            Supply the root by its declared name. All declared inputs are required
            on every run. Additional named DataFrames can supply dependencies.

        Returns
        -------
        pl.DataFrame
            The root joined with the selected tables, after post_step() and
            the optional filter.

        Raises
        ------
        TypeError
            A positional input is not a DataNode or a named input is not a DataFrame.
        ValueError
            A declared input is missing, an input name is supplied more than
            once, or a positional DataNode has the root's name.
        TableNotFoundError
            A required processing method depends on unregistered or skipped tables.

        Notes
        -----
        Each call clears the previous run, registers inputs, and runs all raw
        validators. Prepare hooks read those inputs and their replacements are
        applied together before the table methods run. A validation or preparation
        error stops the run with the registered inputs still available via get().
        If a table method fails, get() also exposes completed intermediate tables.
        """
        # initialize
        self.tables = {}
        self.table_order = {}

        # parse additional tables
        input_tables: list[DataNode] = []
        # tables of positional arguments
        if len(tables) >= 1:
            data_nodes = self.validate_tables_is_node(*tables)
            input_tables += data_nodes

        # tables of keyword arguments
        if len(named_tables) >= 1:
            input_tables += self.parse_tables_to_node(**named_tables)

        provided_names = {node.name for node in input_tables}
        missing_names = set(self.input_tables) - provided_names
        if missing_names:
            names = ", ".join(sorted(missing_names))
            raise ValueError(f"Missing required input tables: {names}")

        # User defined tables
        self.table_order.update(self.registered_tables_order)

        # input tables
        for data_node in input_tables:
            if data_node.name in self.tables.keys():
                raise ValueError(f"{data_node.name} is duplicated.")

            self.tables[data_node.name] = data_node
            self.table_order[data_node.name] = -1

        # Check table
        if len(self.table_order) == 0:
            raise TableNotFoundError(f"There are no table in {self.__class__.__name__}.")

        self.validate_raw_tables()
        self.prepare_tables()

        # On calling execute, annotated functions are executed to create each DataFrame as needed.
        # if self.table_functions is not None:
        if self.table_order is not None and max(v for v in self.table_order.values()) >= 0:
            self.create_tables()

        # join table
        df = self.steps()

        # processing of joined dataframe
        df = self.post_step(df)

        # filter
        if filter is not None:
            df = df.filter(filter)

        return df

    def _hook_input_node(self, method_name: str, name: str) -> DataNode:
        try:
            node = self.tables[name]
        except KeyError as err:
            raise KeyError(
                f"{type(self).__name__}.{method_name} requires input table {name!r}. "
                "Supply it when calling execute()."
            ) from err
        if not isinstance(node, DataNode):
            raise TypeError(
                f"{type(self).__name__}.{method_name} requires a DataFrame for {name!r}. "
                "The table was skipped."
            )
        return node

    def validate_raw_tables(self):
        """Run validators on the registered inputs. A validator exception stops execution."""
        for method_name in self.validate_functions:
            method = getattr(self, method_name)
            input_names = inspect.signature(method).parameters
            inputs = {name: self._hook_input_node(method_name, name).table for name in input_names}
            method(**inputs)

    def prepare_tables(self):
        """Prepare replacements from raw inputs and register them once all hooks succeed.

        Raises InvalidReturnValueError if a hook returns anything other than
        a Polars DataFrame. Registered inputs retain their raw tables on failure.
        """
        prepared_nodes = {}
        for table_name, method_name in self.prepare_functions.items():
            data_node = self._hook_input_node(method_name, table_name)
            method = getattr(self, method_name)
            input_names = inspect.signature(method).parameters
            inputs = {name: self._hook_input_node(method_name, name).table for name in input_names}
            prepared = method(**inputs)
            if not isinstance(prepared, pl.DataFrame):
                raise InvalidReturnValueError(
                    f"{type(self).__name__}.{method_name} must return a polars.DataFrame "
                    f"for input {table_name!r}, got {type(prepared).__name__}."
                )
            prepared_nodes[table_name] = replace(data_node, table=prepared)

        self.tables.update(prepared_nodes)

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
    ) -> Iterator[tuple[int, str, list[str]]]:
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

        Raises TableNotFoundError with the method name and all unavailable
        dependencies when a required method cannot run.
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
            if missing_tables:
                if get_metadata(process_fn, "is_optional"):
                    logger.debug(
                        f"Process '{name}' is skipped, because these tables are not found: "
                        f"{missing_tables}"
                    )
                    self.tables[name] = EmptyDataNode(
                        name=name,
                        join=self.table_join_info[name],
                        keys=self.table_join_keys.get(name),
                        is_optional=True,
                        metadata=get_metadata(process_fn, "metadata"),
                    )
                else:
                    details = [
                        (
                            f"{table_name} (not registered)"
                            if table_name not in self.tables
                            else f"{table_name} (skipped)"
                        )
                        for table_name in missing_tables
                    ]
                    raise TableNotFoundError(
                        f"{type(self).__name__}.{name} cannot run because required tables "
                        f"are unavailable: {', '.join(details)}."
                    )
            else:
                table = process_fn(*[self.get(table_name).table for table_name in arg_table_names])
                self.tables[name] = DataNode(
                    table,
                    self.table_join_keys.get(name),
                    join=self.table_join_info[name],
                    name=name,
                    metadata=get_metadata(process_fn, "metadata"),
                )

        # Validate to self.tables is not empty.
        if self.tables is None or len(self.tables) == 0:
            raise RuntimeError("There are not tables.")

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

    def get(self, table_name: str) -> DataNode | EmptyDataNode:
        """Look up an input or intermediate table from the current run by name.

        Returns a DataNode for a registered DataFrame or an EmptyDataNode for
        a skipped optional step. Raises KeyError if the name has not been
        registered in this run, including before the first execution.
        """
        try:
            return self.tables[table_name]
        except KeyError as err:
            raise KeyError(
                f"table '{table_name}' is not found: Current table list {self.tables.keys()}."
            ) from err

    def _repr_html_(self):
        """Display a self-contained graph snapshot without running the pipeline."""
        return visualize.render_datahub_html(self)
