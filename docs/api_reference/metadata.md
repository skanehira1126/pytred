# Table and column metadata

Import `TableMetadata` and `ColumnMetadata` from `pytred.metadata`. Metadata
describes a table's meaning, keys, and columns independently of its runtime
DataFrame. It does not enforce column types, nullability, or primary keys on
the data.

```python
from pytred.metadata import ColumnMetadata, TableMetadata


orders_metadata = TableMetadata(
    description="Customer orders",
    grain="One row per order",
    primary_key=("order_id",),
    columns=(
        ColumnMetadata(name="order_id", description="Order identifier"),
        ColumnMetadata(name="amount", unit="JPY", nullable=True),
    ),
)
```

Attach a `TableMetadata` instance to `Root(metadata=...)` or `Input(metadata=...)`
for a declared input, to `@polars_table(..., metadata=...)` for a processing
result, or to `class Orders(DataHub, metadata=...):` for the final output.

A subclass inherits the output metadata when it omits the class keyword.
Pass `metadata=None` to clear it or pass a new `TableMetadata` to replace the
whole output description for the subclass.

Use `update()` to derive an output definition. Each update requires a date,
appends new column definitions in order, and replaces the other supplied fields.
Omitted fields keep their values. A repeated column name raises a validation error.

```python
from datetime import date

paid_orders_metadata = orders_metadata.update(
    updated_on=date(2026, 11, 22),
    description="Orders with payment status",
    columns=(ColumnMetadata(name="is_paid"),),
)
```

Both models reject unknown fields and incompatible value types and prevent
field reassignment after construction. Use tuples for `roles`, `primary_key`,
`columns`, and `categories`; use `datetime.date` for `updated_on`. Duplicate
names in `columns` are rejected. Empty tuples mean that no values are specified.
`ColumnMetadata.name` is required; all other fields have defaults shown below.

Inspect class declarations with `pytred metadata module:DataHubSubclass` or
`get_datahub_metadata(DataHubSubclass)` from `pytred.helpers.metadata`.
Inspection does not construct or execute the hub, although the CLI imports the
target module. Missing metadata is returned as `metadata_status: "unset"` and
`metadata: null` in JSON. A supplied `DataNode` retains its own runtime metadata,
which can differ from the class declaration.

In pytred 1.1+, each hub and table entry also includes `source`, with an absolute
`file` path and a one-based starting `line` (including decorators). Declared
inputs point to annotations in the class that declares them. Inferred inputs and
definitions without retrievable source files use `source: null`.

Use `pytred metadata module:DataHubSubclass --compact` for a text outline with
source locations and no detailed metadata. Add `--table NAME` to select one
input or generated table in either output format. Selection retains the hub
and dependency names, without including dependency entries. An unknown table
produces a usage error with exit code 2.

<span id="tablemetadata"></span>

::: pytred.metadata.TableMetadata

<span id="columnmetadata"></span>

::: pytred.metadata.ColumnMetadata
