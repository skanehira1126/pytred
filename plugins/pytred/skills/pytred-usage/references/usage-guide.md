# Pytred Usage Guide

## Core Model

- `DataHub` executes decorated methods in ascending `@polars_table(order, ...)` order, joins selected tables into `root_df`, and returns a `polars.DataFrame`. Give a producing method a lower order than its consumers; dependencies do not automatically reorder methods.
- A `@polars_table(...)` method defines one table in the pipeline. The method name becomes that table's name.
- Method parameter names are table dependencies. They can refer to:
  - keyword input tables passed into `DataHub(...)`
  - positional `DataNode` inputs
  - outputs of earlier decorated methods
- `root_df` is the output backbone. Any table with a non-`None` `join` is joined into `root_df`.
- Positional `DataNode` inputs are already computed tables. When supplying them, pass `root_df` as the first positional argument: `DataHub(root_df, node_a, node_b)`.
- Keyword `pl.DataFrame` inputs are named dependencies with `join=None`; they do not automatically join into the result.
- `join=None` means "build this table for downstream steps only." It is available to later methods but does not appear in the final output unless another joined table selects from it.
- `is_optional=True` skips a decorated method if a required table is missing or was skipped. It does not suppress errors from a method that runs.
- `hub()` and `hub.execute()` are equivalent. Passing `pl.Expr` filters applies them after all joins and `post_step()`.

## Build a Reusable Pipeline Class

Use a `DataHub` subclass when the preprocessing steps should be reusable and dependency-aware.

This snippet assumes an existing `titanic` DataFrame with `record_id`, `survived`, `sex`, `sibsp`, `parch`, and `age` columns, and unique `record_id` values.

```python
import polars as pl

from pytred import DataHub
from pytred.decorators import polars_table


class PassengerFeatures(DataHub):
    """Join reusable passenger features onto a root passenger table."""

    @polars_table(0)
    def replace_sex(self, titanic: pl.DataFrame) -> pl.DataFrame:
        """Encode `sex` as an integer feature."""
        return titanic.select(
            "record_id",
            sex_replaced=pl.when(pl.col("sex") == "male").then(1).otherwise(0),
        )

    @polars_table(1)
    def family_features(self, titanic: pl.DataFrame) -> pl.DataFrame:
        """Build an intermediate table used by later steps only."""
        return titanic.select(
            "record_id",
            cnt_family=pl.col("sibsp") + pl.col("parch"),
        )

    @polars_table(2, "record_id", join="left", is_optional=True)
    def filled_age(self, ages: pl.DataFrame) -> pl.DataFrame:
        """Fill missing ages when an auxiliary age table is available."""
        return ages.select(
            "record_id",
            filled_age=pl.col("age").fill_null(pl.col("age").mean()),
        )

    @polars_table(3, "record_id", join="left")
    def selected_features(
        self,
        replace_sex: pl.DataFrame,
        family_features: pl.DataFrame,
    ) -> pl.DataFrame:
        """Select intermediate features that should be exposed in the output."""
        return replace_sex.join(family_features, on="record_id", how="left").select(
            "record_id",
            "sex_replaced",
            "cnt_family",
        )

    def post_step(self, df: pl.DataFrame) -> pl.DataFrame:
        """Apply final shaping after all joins complete."""
        return df.sort("record_id")


hub = PassengerFeatures(
    root_df=titanic.select("record_id", "survived"),
    titanic=titanic,
    ages=titanic.select("record_id", "age"),
)

output = hub(pl.col("survived") == 1)
```

With the inputs above, `output` contains surviving passengers (`survived == 1`), sorted by `record_id`, with columns in this order: `record_id`, `survived`, `filled_age`, `sex_replaced`, `cnt_family`.

Important details:

- The keyword argument `titanic=...` creates an input table named `titanic`, so the `replace_sex()` and `family_features()` parameters must also be named `titanic`.
- The keyword argument `ages=...` is available to `filled_age()` because the parameter name matches the table name.
- `replace_sex()` and `family_features()` are intermediate tables because their decorators have no join information. Their features join the output once through `selected_features()`.
- `is_optional=True` skips `filled_age()` when `ages` is not provided.
- `post_step()` runs after all joins and before any filters passed to `hub(...)`.

## Compose Existing Tables with `DataNode`

Use plain `DataHub(...)` plus positional `DataNode` objects when the feature tables already exist and only need to be joined.

This snippet assumes existing `titanic`, `replaced_sex`, and `filled_age` DataFrames sharing the `record_id` key.

```python
import polars as pl

from pytred import DataHub
from pytred import DataNode


hub = DataHub(
    titanic.select("record_id"),
    DataNode(
        table=replaced_sex,
        keys=["record_id"],
        join="left",
        name="replaced_sex",
    ),
    DataNode(
        table=filled_age,
        keys=["record_id"],
        join="left",
        name="filled_age",
    ),
)

output = hub()
```

Guidance:

- Use positional `DataNode(...)` inputs for already-computed joinable tables.
- Use keyword `pl.DataFrame` inputs when the table is only a named dependency for decorated methods.
- Avoid passing a completed joinable table as `some_name=df` unless you intentionally want it registered as a non-joined input table.

## Inspect and Debug

- Call `hub.get("table_name")` after execution to access a specific created table as a `DataNode`.
- `@polars_table` enforces that the decorated function returns `pl.DataFrame`.
- If `join` is not `None`, the declared key columns must exist in the returned DataFrame.
- Duplicate key rows raise `DuplicatedError` unless `is_validate_unique=False`.
- Join names are passed to Polars. Check the project's installed Polars version for version-sensitive choices such as `full`/`outer` or `right`.
- `cross` and `None` should not declare keys.
- Instantiating `DataHub` with no tables raises `TableNotFoundError`.

## Visualize and Report

### Notebook visualization

`DataHub` implements `_repr_html_()`, so a notebook can render the dataflow directly.

```python
from IPython.display import display


display(hub)
```

Use this for dependency inspection while iterating on a `DataHub` subclass.

### CLI reporting

Use the packaged CLI when the user wants a markdown report from a `DataHub` class in a `.py` file.

```bash
pytred report sample_datahub.py SampleDataHub \
  --input-table '{"name": "input_table", "keys": ["key"], "join": "left"}' \
  > report_dataflow.md
```

Notes:

- `pytred report` imports the target class from `file_path`.
- Each `--input-table` is JSON with `name` and optional `keys` and `join`.
- The report includes the class docstring, method docstrings, a markdown table, and a Mermaid graph.

## Source Map

These paths belong to the [pytred source repository](https://github.com/skanehira1126/pytred), not the plugin installation or the user's project. They are optional references when that checkout is available. For version-specific behavior in another project, inspect its installed pytred package.

- `src/pytred/data_hub.py`: execution order, table creation, joining, `post_step()`, and filters.
- `src/pytred/decorators/polars.py`: decorator contract and validation rules.
- `tests/fixtures/data_hub.py`: small end-to-end examples for subclass usage and optional tables.
- `examples/01_basic_preprocessing.ipynb`: subclass-based preprocessing walkthrough.
- `examples/02_use_completed_data.ipynb`: `DataNode` composition workflow.
- `examples/03_visualize_workflow.ipynb`: notebook visualization examples.
- `docs/tutorials/cli.md`: `pytred report` usage.
