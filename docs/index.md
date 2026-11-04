# Welcome to Pytred Documentation

This documentation describes pytred 1.x. Install the wheel from GitHub Releases
with Python 3.11–3.14:

```bash
pip install https://github.com/skanehira1126/pytred/releases/download/1.0.1/pytred-1.0.1-py3-none-any.whl
```

Define a pipeline as a direct subclass of `DataHub`. Declare the base table
with `Annotated[pl.DataFrame, Root(metadata=None)]` and other required tables
with `Input`. Construct the hub without data, then call `hub.execute(...)` with
the root DataFrame and inputs for that run. A `@polars_table` method can use any
input or previously computed table by naming it as a parameter.

See [input declarations](api_reference/inputs.md) for `Root` and `Input`, and
[metadata](api_reference/metadata.md) for table and column descriptions.

## Examples

1. [basic preprocessing](./examples/01_basic_preprocessing.ipynb)
1. [Preprocessing with completed data](./examples/02_use_completed_data.ipynb)
1. [Visualize data preprocessing](./examples/03_visualize_workflow.ipynb)

