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

## TableMetadata

::: pytred.metadata.TableMetadata
    options:
      members:
        - description
        - roles
        - grain
        - primary_key
        - columns
        - updated_on

## ColumnMetadata

::: pytred.metadata.ColumnMetadata
    options:
      members:
        - name
        - description
        - unit
        - is_categorical
        - categories
        - nullable
        - null_meaning
        - updated_on
