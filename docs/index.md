# Welcome to Pytred Documentation

Define a pipeline as a direct subclass of `DataHub`. Declare the base table
with `Annotated[pl.DataFrame, Root(metadata=None)]` and other required tables
with `Input`. Construct the hub without data, then call `hub.execute(...)` with
the root DataFrame and inputs for that run. A `@polars_table` method can use any
input or previously computed table by naming it as a parameter.

## Examples

1. [basic preprocessing](./examples/01_basic_preprocessing.ipynb)
1. [Preprocessing with completed data](./examples/02_use_completed_data.ipynb)
1. [Visualize data preprocessing](./examples/03_visualize_workflow.ipynb)

