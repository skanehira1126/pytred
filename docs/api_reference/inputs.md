# Input declarations

Import `Root` and `Input` from `pytred._types` and use them in `Annotated` fields
on a direct subclass of `DataHub`. Declare one root and supply every declared
input on each call to `execute()`.
Input names must differ from decorated processing method names; a collision
raises `ValueError` when the class is defined.

```python
from typing import Annotated

import polars as pl

from pytred import DataHub
from pytred._types import Input, Root


class Orders(DataHub):
    customers: Annotated[pl.DataFrame, Root(metadata=None)]
    orders: Annotated[pl.DataFrame, Input(metadata=None)]


hub = Orders()
result = hub.execute(
    customers=pl.DataFrame({"id": [1, 2]}),
    orders=pl.DataFrame({"id": [1], "amount": [10]}),
)
```

The root supplies the starting rows and columns of the output. Named input
DataFrames are available to processing methods by name; they are not joined
automatically. In this example, the output is the `customers` table.

`metadata` is required on both markers. Pass `None` when no description is
needed, or a [TableMetadata](metadata.md) instance. Metadata describes the input;
it does not validate DataFrame contents. Use `validate_raw_inputs` for runtime
checks and `prepare_input` for input transformations; see [decorators](decorators.md).

A positional `DataNode` can supply a declared non-root input while retaining
its own metadata and join settings. Supply the root as a keyword DataFrame.
Undeclared named DataFrames can also be supplied as dependencies. Leave a name
undeclared if it should be absent on some runs; `Input` always makes it required,
even when only an optional processing method uses it.

## Root

::: pytred._types.Root

## Input

::: pytred._types.Input
