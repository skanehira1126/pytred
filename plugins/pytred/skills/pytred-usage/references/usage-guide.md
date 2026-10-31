# Pytred Usage Guide

## Core Model

Define each pipeline as a direct subclass of `DataHub`. Construct the hub
without input tables. Call `hub.execute(...)` or `hub(...)` with the tables
for that run.

- Declare exactly one `Root` and any other required `Input` fields with
  `Annotated`. Use `metadata=None` when there is no metadata. Supply every
  declared name on each run, even if only an optional method uses it.
- Pass the root as a named Polars DataFrame. Additional named DataFrames
  become dependencies with `join=None`. Positional DataNodes carry their own
  names, keys and join strategies.
- A method decorated with `@polars_table` produces one table named after the
  method. Its parameter names refer to inputs or earlier results. Set producers'
  order lower than their consumers' order; methods execute in ascending order.
- A non-None join adds the table to the root. A table with `join=None` stays
  available for later methods. The optional `filter=pl.Expr` runs after the joins
  and `post_step()`.
- Each call clears the previous run. Use `hub.get(name).table` to inspect a
  computed table, including an input. If a method raises, tables registered
  before the error in this run are still available.

## Build a Reusable Pipeline Class

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

    @polars_table(1, "customer_id", join="left")
    def totals(self, paid):
        return paid.group_by("customer_id").agg(total=pl.col("amount").sum())


hub = OrderFeatures()
output = hub.execute(
    customers=pl.DataFrame({"customer_id": [1, 2]}),
    orders=pl.DataFrame({
        "customer_id": [1, 1, 2],
        "amount": [10, 20, 30],
        "paid": [True, True, False],
    }),
    filter=pl.col("total").is_not_null(),
)
# output contains one row: customer_id=1, total=30.
```

A child pipeline can be constructed alongside its parent. In the parent method,
call the child's `execute()` and pass the desired intermediate DataFrame under
the child's root name. The child has its own inputs and run state.

## Compose Existing Tables with DataNode

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
output = JoinedScores().execute(scores, rows=pl.DataFrame({"id": [1, 2]}))
# Rows (id, score): (1, 7), (2, null).
```

The positional node supplies the declared name `scores`. Passing a DataFrame
as `scores=...` would make it available as a dependency with no join.

## Define Table Metadata

Use `Root(metadata=...)` or `Input(metadata=...)` for input metadata, the
decorator's `metadata` argument for a generated table, and the class keyword
`metadata` for the final output.

```python
from typing import Annotated

import polars as pl

from pytred import DataHub
from pytred._types import Root
from pytred.decorators import polars_table
from pytred.helpers.metadata import get_datahub_metadata
from pytred.metadata import ColumnMetadata, TableMetadata


class Scores(DataHub, metadata=TableMetadata(description="Scored orders")):
    orders: Annotated[pl.DataFrame, Root(metadata=TableMetadata(primary_key=("id",)))]

    @polars_table(0, "id", join="left", metadata=TableMetadata(
        columns=(ColumnMetadata(name="score", description="Twice the amount"),),
    ))
    def scores(self, orders):
        return orders.select("id", score=pl.col("amount") * 2)


definitions = get_datahub_metadata(Scores)
```

Metadata describes tables and columns independently of the values supplied at
runtime. A supplied DataNode retains its own metadata. CLI inspection and the
input nodes in the viewer use the class declarations. See the [CLI guide](cli-guide.md).

## Inspect and Debug

- For a missing required input error, compare the supplied names with all
  `Root` and `Input` declarations, including names supplied by DataNodes.
- For a missing method dependency, check the spelling and the producer's order.
  `is_optional=True` skips a step when a dependency is missing or was skipped.
  To make a lookup genuinely optional, accept it as an additional runtime input
  and leave it out of the required declarations.
- Return a Polars DataFrame from each table method. For keyed joins, return
  all join keys and check uniqueness (enabled by default). Omit keys for
  `cross` and `None`. Check the installed Polars for supported join strategies.

## Notebook Visualization

In a notebook with trusted HTML, JavaScript and sandboxed iframe support,
`display(hub)` shows a snapshot of the hub. Before execution you can inspect
processing methods, their dependencies and joins. After `execute(...)`, the
graph includes the registered inputs and their declared metadata. Taking a
snapshot does not execute any processing methods.

For a vertical layout use `display(visualize_datahub(hub, direction="TB"))`
with `visualize_datahub` from `pytred.helpers.visualize`. The package bundles
the JavaScript and CSS used for notebook interaction.

## Source Map

In a [pytred checkout](https://github.com/skanehira1126/pytred), see
`src/pytred/data_hub.py` for input registration, execution order and joins,
`src/pytred/decorators/polars.py` for table validation, and
`docs/tutorials/visualization.md` for viewer controls.
