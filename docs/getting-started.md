# Run your first pipeline

Build a customer-level table from customers and their orders. The result keeps
every customer and adds their total order amount.

## 1. Install pytred

Use Python 3.11–3.14. In your project's Python environment, install the latest stable release:

```bash
python -m pip install --upgrade "https://github.com/skanehira1126/pytred/archive/refs/tags/latest.zip"
```

The `latest` tag tracks the latest stable Python package release. pip builds the
package from its source archive; Git is not required. For reproducible environments,
replace `latest` in the URL with a version tag, such as `1.0.1`.

## 2. Define and run the pipeline

Copy this complete example into a Python file or notebook cell:

```python
from typing import Annotated

import polars as pl

from pytred import DataHub
from pytred._types import Input, Root
from pytred.decorators import polars_table


class CustomerTotals(DataHub):
    customers: Annotated[pl.DataFrame, Root(metadata=None)]
    orders: Annotated[pl.DataFrame, Input(metadata=None)]

    @polars_table(0, "id", join="left")
    def totals(self, orders):
        return orders.group_by("id").agg(total=pl.col("amount").sum())


hub = CustomerTotals()
result = hub.execute(
    customers=pl.DataFrame({"id": [1, 2]}),
    orders=pl.DataFrame({"id": [1, 1], "amount": [10, 20]}),
)
print(result.sort("id"))
```

Expected rows:

| id | total |
| --- | --- |
| 1 | 30 |
| 2 | null |

`customers` is the root, so its rows form the starting output. The `orders`
parameter in `totals` receives the input with that name. The decorator runs the
method at order `0` and left-joins its result on `id`. Customer 2 has no orders,
so its total is null.

Declare the pipeline as a direct subclass of `DataHub`. Supply the root by
keyword and every declared `Input` on each call to `execute()`. Use
`metadata=None` when you do not need a table description.

## 3. Inspect the intermediate table

```python
print(hub.get("totals").table)
```

This table contains just customer 1 and total 30, before the left join. Each
execution replaces the previous run's tables; `get()` shows the current run.

In a trusted notebook, explore the dependencies and join settings:

```python
from IPython.display import display

display(hub)
```

See [the viewer guide](tutorials/visualization.md) for supported display environments.

## Next steps

- [Add intermediate steps or join existing tables](guides/pipelines.md).
- [Describe tables and columns with metadata](api_reference/metadata.md).
- [Diagnose missing inputs and unexpected output](guides/troubleshooting.md).
