# pytred
Python Tools for Refinement and Enhancement of Data

Define a Polars pipeline as a direct subclass of `DataHub`. Each decorated method
produces a table. Method arguments name its dependencies, and the decorator
specifies its execution order and how to join its output to the root table.

## Quick start

This guide targets pytred 1.x (Python 3.11–3.14). Install the wheel from
GitHub Releases in your project's Python environment:

```bash
pip install https://github.com/skanehira1126/pytred/releases/download/1.0.1/pytred-1.0.1-py3-none-any.whl
```

For development, run `pip install -e .` from the repository root.

```python
from typing import Annotated

import polars as pl

from pytred import DataHub
from pytred._types import Input, Root
from pytred.decorators import polars_table
from pytred.metadata import TableMetadata


class CustomerTotals(DataHub):
    customers: Annotated[pl.DataFrame, Root(metadata=None)]
    orders: Annotated[pl.DataFrame, Input(metadata=TableMetadata(description="Order amounts"))]

    @polars_table(0, "id", join="left")
    def totals(self, orders):
        return orders.group_by("id").agg(total=pl.col("amount").sum())


hub = CustomerTotals()
result = hub.execute(
    customers=pl.DataFrame({"id": [1, 2]}),
    orders=pl.DataFrame({"id": [1, 1], "amount": [10, 20]}),
)
# result has columns id, total and rows (1, 30), (2, null).
```

Choose one `Root` as the starting table for joins. Supply it by keyword and
provide every declared input on each call. Additional named DataFrames are
available to methods by name. Pass a positional `DataNode` to `execute()` to
supply a computed, non-root table with its own name, join method and keys.

For a full outer join, use `join="full"` with a decorator or a `DataNode`.
Migrate existing `join="outer"` declarations to `join="full"`. The installed
Polars version must support the selected strategy (`full` from 0.20.29 and
`right` from 1.1.0).

Each call clears the previous run's tables. After execution,
`hub.get("totals").table` returns the intermediate result. After a processing
error, `get()` can inspect tables already registered in that run. To compose
pipelines, pass a parent step's DataFrame to a child hub's `execute()` as its
named root.

## Examples

1. [basic preprocessing](https://github.com/skanehira1126/pytred/blob/main/examples/01_basic_preprocessing.ipynb)
1. [Preprocessing with completed data](https://github.com/skanehira1126/pytred/blob/main/examples/02_use_completed_data.ipynb)
1. [visualize workflow](https://github.com/skanehira1126/pytred/blob/main/examples/03_visualize_workflow.ipynb)

## Notebook visualization

Use `display(hub)` in a notebook with trusted HTML, JavaScript and sandboxed iframe support to explore the
pipeline interactively. Zoom, pan, select a node to inspect dependencies and join
conditions, or switch the layout direction. Rendering does not execute the pipeline.
Cytoscape.js and Dagre are bundled with the package; no CDN or Node.js is required.
See the [visualization guide](https://github.com/skanehira1126/pytred/blob/main/docs/tutorials/visualization.md).

## Metadata inspection

Python 3.11 through 3.14 is supported. Define table and column metadata with
`pytred.metadata.TableMetadata` and `ColumnMetadata`. Pass table metadata to
`@polars_table(..., metadata=...)` or declare output metadata on a DataHub subclass
with `class MyDataHub(DataHub, metadata=...):`.

Inspect an importable DataHub class from the command line:

```bash
pytred --version
pytred metadata my_project.pipeline:MyDataHub
```

With pytred 1.1+, use `--compact` for a short text outline or `--table NAME` to
select one input or generated table (the options can be combined):

```bash
pytred metadata my_project.pipeline:MyDataHub --compact
pytred metadata my_project.pipeline:MyDataHub --table totals
```

Compact output includes execution order, dependencies, optional flags and
`FILE:LINE` source locations, without detailed metadata or method bodies.
Selecting a table retains the hub entry and dependency names; it does not add
the dependencies' entries. An unknown table is a usage error (exit code 2).

The same information is available as a JSON-compatible dictionary in Python:

```python
from pytred.helpers.metadata import get_datahub_metadata
from my_project.pipeline import MyDataHub

metadata = get_datahub_metadata(MyDataHub)
```

Inspection does not construct the DataHub or execute its processing methods.
The CLI imports the target module, so its top-level code runs normally.
The result contains `DataHub` and `tables`. Each entry has `metadata_status`:
`defined` for configured metadata or `unset` for missing metadata. The `metadata`
field is an object when defined and `null` otherwise in JSON.

Starting in 1.1, each entry also has `source`: an absolute `file` path and a
one-based `line` at the beginning of the definition, including decorators.
Declared inputs point to annotations directly in the class body. Inferred
inputs and definitions without retrievable source files have `source: null`
(`source unavailable` in compact output).

Each table entry also includes `is_optional`: `true` or `false` for processing
methods, and `null` for input tables where it does not apply. This describes
whether a method can be skipped when dependencies are missing, not whether it
was skipped during an execution.

Input definitions come from the class annotations shown in the quick start.
Set `metadata=None` when a table has no description. The CLI reads declared
metadata without inspecting DataFrame contents. A supplied DataNode keeps its
own metadata, so its runtime metadata may differ from the pipeline declaration.

## Codex plugin

This repository includes a Codex plugin that teaches AI agents how to use
`DataHub`, `DataNode`, `@polars_table`, and interactive notebook visualization without relying
on generic data-pipeline assumptions.

The plugin's guide targets pytred 1.x. Install the Python package as shown in
Quick start, then install the Codex plugin separately from the repository marketplace:

```bash
codex plugin marketplace add skanehira1126/pytred
codex plugin add pytred@pytred
```

To use a local checkout as the marketplace source, run from the repository root:

```bash
codex plugin marketplace add .
codex plugin add pytred@pytred
```

These commands register the marketplace and plugin in your Codex environment;
running them inside a repository does not limit the installation to that repository.
For use only in a particular repository, place the `pytred-usage` skill folder under
that repository's `.agents/skills/` instead.

Start a new Codex task after installation and select `pytred-usage`, or ask for help
with a pytred pipeline. The plugin includes its own usage guide and does not require
a pytred source checkout. It does not install Python dependencies; `pip install`
does not register the Codex plugin.

The plugin lives in `plugins/pytred`, with its own version in
`plugins/pytred/.codex-plugin/plugin.json`. The repository marketplace is defined in
`.agents/plugins/marketplace.json`. Repository skill links in `.agents/skills` and
`.codex/skills` point to the same plugin skill, so edit only
`plugins/pytred/skills/pytred-usage`. If you previously installed the standalone
skill into your user skill directory, remove that old copy when switching to the
plugin to avoid duplicate skills.
