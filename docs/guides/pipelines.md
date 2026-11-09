# Build a pipeline

Start with the [first pipeline](../getting-started.md) if you have not yet run
pytred. Use the patterns below to extend it.

## Reuse an intermediate table

Give each producer a lower execution order than the methods that consume it.
Method parameter names refer to input tables or earlier method results. A step
with no join remains available to later steps without adding columns to the output.

```python
from typing import Annotated

import polars as pl

from pytred import DataHub
from pytred._types import Input, Root
from pytred.decorators import polars_table


class OrderFeatures(DataHub):
    customers: Annotated[pl.DataFrame, Root(metadata=None)]
    orders: Annotated[pl.DataFrame, Input(metadata=None)]

    @polars_table(0)
    def paid(self, orders):
        return orders.filter(pl.col("paid"))

    @polars_table(1, "id", join="left")
    def totals(self, paid):
        return paid.group_by("id").agg(total=pl.col("amount").sum())


hub = OrderFeatures()
result = hub.execute(
    customers=pl.DataFrame({"id": [1, 2]}),
    orders=pl.DataFrame({
        "id": [1, 1, 2],
        "amount": [10, 20, 30],
        "paid": [True, True, False],
    }),
)
# Rows (id, total): (1, 30), (2, null).
```

`paid` is an intermediate table; only `totals` joins the root. Inspect either
with `hub.get("paid").table` or `hub.get("totals").table` after execution.

## Choose joins and output rows

Set `join="left"` to keep root rows, or choose another supported Polars join
strategy. Return all declared join keys from the method. Keys are checked for
uniqueness by default; decide whether repeated keys are intentional before
setting `is_validate_unique=False`.

Use `join="full"` for a full outer join. Polars must support the strategy you
select: `full` requires Polars 0.20.29 or later, and `right` requires 1.1.0 or later.
Omit keys for `join=None` and `join="cross"`.

To filter the final result, pass `filter=pl.col("total").is_not_null()` to
`execute(...)`. This runs after all joins and `post_step()`. Override
[`post_step`](../api_reference/data_hub.md) when you need a final transformation
of the joined DataFrame.

## Join a precomputed table

Pass a `DataNode` positionally when a table already has a name, keys, and join
settings. Supply the root as a keyword DataFrame.

```python
from typing import Annotated

import polars as pl

from pytred import DataHub, DataNode
from pytred._types import Input, Root


class JoinedScores(DataHub):
    rows: Annotated[pl.DataFrame, Root(metadata=None)]
    scores: Annotated[pl.DataFrame, Input(metadata=None)]


scores = DataNode(
    table=pl.DataFrame({"id": [1], "score": [7]}),
    keys=("id",), join="left", name="scores",
)
result = JoinedScores().execute(scores, rows=pl.DataFrame({"id": [1, 2]}))
# Rows (id, score): (1, 7), (2, null).
```

Passing `scores=some_dataframe` instead makes the table available to methods
without joining it automatically. A positional node retains its own metadata
and join settings.

## Validate and prepare inputs

Use `@validate_raw_inputs` for checks that should stop a run on invalid input,
and `@prepare_input("input_name")` to replace an input with a prepared DataFrame.
Import both from `pytred.decorators`. Validators run first; prepare hooks read
raw inputs and their replacements are applied together before processing steps.
See [decorator parameters](../api_reference/decorators.md).

## Make a step optional

Set `is_optional=True` on a table method to skip it when a dependency is missing
or was skipped. An `Input` declaration still requires that table on every run.
For a dependency that may be absent, leave it undeclared and supply it as an
additional named DataFrame only when available.

## Combine pipelines

Construct a child hub and call its `execute()` inside a parent processing method.
Pass the intermediate DataFrame under the child's declared root name and supply
its other required inputs. Each hub keeps its own run state.
