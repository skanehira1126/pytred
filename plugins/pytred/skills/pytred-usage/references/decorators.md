# Decorators

## Table methods

Import `polars_table` from `pytred.decorators`. A decorated method returns a
Polars DataFrame and produces a table named after the method. Its parameters
name input or previously generated tables; execution follows numeric `order`,
not an automatic dependency sort.

| Argument | Contract |
| --- | --- |
| `order` | Required nonnegative integer; put producers before consumers. |
| `*keys` | Positional column names, including multiple names for a composite key. Required for keyed joins; omit for `join=None` or `"cross"`. |
| `join=None` | Keep the result as an intermediate dependency. To join it to the root, choose `inner`, `left`, `right`, `full`, `semi`, `anti`, or `cross`. Availability also depends on the installed Polars version. |
| `is_validate_unique=True` | Reject duplicate combinations of keys in the returned table. Set to `False` only when repeated keys and their effect on joined rows are intended. Key presence is still checked. |
| `is_optional=False` | Missing or skipped dependencies raise `TableNotFoundError`. With `True`, skip the method and register an `EmptyDataNode`. An empty DataFrame is an available dependency; exceptions raised inside the method are not suppressed. |
| `metadata=None` | Optional `TableMetadata` describing the generated table; see [metadata definitions](usage-guide.md#define-table-metadata). |

A non-DataFrame return raises `InvalidReturnValueError`, missing key columns
raise `ValueError`, and duplicate keys raise `DuplicatedError` when uniqueness
validation is enabled. Required `Root`/`Input` declarations must still be
supplied even if used only by an optional method. For an optional runtime
lookup, leave it undeclared and supply it when available.

`polars_optional_table` from `pytred.decorators.polars` is
`polars_table` with `is_optional=True` preset. Existing code may also use
`table(engine="polars", order=..., ...)` from `pytred.decorators`: it defaults
`is_validate_unique` to **False** and does not expose `is_optional` or `metadata`.
Use `polars_table` for the full set of options.

## Validate and prepare inputs

Import `validate_raw_inputs` and `prepare_input` from
`pytred.decorators.polars`. The execution sequence is:

1. Register inputs and check required declarations.
2. Run all `@validate_raw_inputs` methods on raw inputs.
3. Run `@prepare_input("target")` methods on raw inputs, then register their replacements together.
4. Run table methods, join tables, call `post_step(df)`, and apply the output filter.

Validator parameters name raw inputs. Return `None` on success; raise an
exception to stop execution. A prepare hook also names its raw dependencies
in its parameters and returns a DataFrame replacing the named target. It may
read several inputs, including inputs other than its target.

```python
from typing import Annotated

import polars as pl

from pytred import DataHub
from pytred._types import Root
from pytred.decorators.polars import prepare_input, validate_raw_inputs


class CleanOrders(DataHub):
    orders: Annotated[pl.DataFrame, Root(metadata=None)]

    @validate_raw_inputs
    def check_amounts(self, orders):
        if orders["amount"].null_count():
            raise ValueError("amount must not contain nulls")

    @prepare_input("orders")
    def cast_amounts(self, orders):
        return orders.with_columns(amount=pl.col("amount").cast(pl.Float64))


output = CleanOrders().execute(orders=pl.DataFrame({"amount": [1, 2]}))
# output contains amount=[1.0, 2.0] with dtype Float64.
```

All prepare hooks see the original frames, not other hooks' replacements.
Only one hook may prepare a given target. Targets and dependencies must be
present in the current run; they cannot refer to generated tables. Replacements
preserve the input DataNode's join settings and metadata. Empty DataFrames are
valid; other return types raise `InvalidReturnValueError` before any replacement
is registered. On validation or preparation failure, registered raw inputs
remain inspectable with `hub.get(name).table`.

Treat input frames as read-only and return new frames: in-place mutation can
affect other hooks and the caller. For class methods, put `@classmethod` above
the pytred decorator.
