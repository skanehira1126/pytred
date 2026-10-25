from collections.abc import Sequence
from dataclasses import dataclass
from typing import Generic

import polars as pl

from pytred._types import PolarsJoinMethod
from pytred.metadata import RoleT
from pytred.metadata import TableMetadata


@dataclass
class EmptyDataNode(Generic[RoleT]):
    """Represent a skipped table without a DataFrame.

    DataHub creates this placeholder when an optional processing method has
    missing or skipped dependencies. It is excluded from joins and treated as
    a missing dependency by downstream methods. It is not an empty DataFrame
    and cannot be supplied as a positional input to DataHub.

    Parameters
    ----------
    name : str
        Name of the skipped processing method.
    keys : Sequence[str] or None
        Declared join keys, retained even though no join is performed.
    join : PolarsJoinMethod or None
        Declared join strategy.
    is_optional : bool, default False
        Whether the processing method is optional. DataHub sets this to True
        when creating a placeholder for a skipped optional method.
    metadata : TableMetadata[RoleT] or None, default None
        Definition of the table that would have been produced. DataHub carries
        over the metadata supplied to the processing method's decorator even
        when the method is skipped. None if no metadata was supplied.

    Notes
    -----
    RoleT describes the metadata role type for static type checking. This
    standard dataclass does not perform runtime validation.
    """

    name: str
    keys: Sequence[str] | None
    join: PolarsJoinMethod | None
    is_optional: bool = False
    metadata: TableMetadata[RoleT] | None = None


@dataclass
class DataNode(Generic[RoleT]):
    """Hold a computed Polars table, its join settings, and optional metadata.

    Supply existing nodes as positional arguments to DataHub after root_df:
    ``DataHub(root_df, node)``. The node's name makes its table available to
    processing methods with a matching parameter name. DataHub also creates
    nodes for keyword DataFrame inputs and processing results.

    Parameters
    ----------
    table : pl.DataFrame
        Computed table, retained by reference without copying.
    keys : Sequence[str] or None
        Columns used to join this table into root_df. Use None for cross joins
        or when join is None. These are join keys, not metadata primary keys.
    join : PolarsJoinMethod or None
        Join strategy used by DataHub. None keeps the table available as a
        dependency without joining it into root_df.
    name : str
        Table identifier, unique within a DataHub instance.
    metadata : TableMetadata[RoleT] or None, default None
        Metadata instance retained as supplied. Construct it before passing
        it here to apply its Pydantic validation. For processing results,
        DataHub carries over the metadata supplied to the method's decorator.
        None if no metadata was supplied.

    Notes
    -----
    This is a mutable standard dataclass: annotations do not validate input
    or assignments, and metadata is not checked against the DataFrame.
    DataNode[ProjectRole] preserves the role type for static type checking;
    it does not add runtime validation or preserve that type through DataHub.get.

    Examples
    --------
    >>> from typing import Literal
    >>> ProjectRole = Literal["feature", "master"]
    >>> ProjectDataNode = DataNode[ProjectRole]
    >>> metadata = TableMetadata[ProjectRole](roles=("feature",))
    >>> node = ProjectDataNode(
    ...     table=pl.DataFrame({"id": [1]}),
    ...     keys=("id",), join="left", name="features", metadata=metadata,
    ... )
    >>> node.metadata is metadata
    True
    """

    table: pl.DataFrame
    keys: Sequence[str] | None
    join: PolarsJoinMethod | None
    name: str
    metadata: TableMetadata[RoleT] | None = None
