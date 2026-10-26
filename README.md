# pytred
Python Tools for Refinement and Enhancement of Data

## Introduction

`pytred` is a Python toolkit designed to streamline the organization and execution of data processing tasks. 
Iht facilitates a structured approach to data operations, allowing each function to be treated as an independent data processing unit, similar to how one might think of tables in a relational database.

## Features
- **Structured Data Processing**: `pytred` enables users to organize their data processing logic in a clear and modular way, with each function acting as a table in relational database.
- **Flexible and Reusable**: By encapsulating data processing tasks within functions, pytred promotes reusability and flexibility, allowing for easy modification and extension of data processing workflows.
- **Clear and Maintainable** Code: The framework promotes a clean separation of concerns, which simplifies understanding, maintaining, and debugging the data processing steps.

## Examples

1. [basic preprocessing](./examples/01_basic_preprocessing.ipynb)
1. [Preprocessing with completed data](./examples/02_use_completed_data.ipynb)
1. [visualize workflow](./examples/03_visualize_workflow.ipynb)

## Notebook visualization

Use `display(hub)` in a notebook with Jupyter Widgets support to explore the
pipeline interactively. Zoom, pan, select a node to inspect dependencies and join
conditions, or switch the layout direction. Rendering does not execute the pipeline.
Cytoscape.js and Dagre are bundled with the package; no CDN or Node.js is required.
See the [visualization guide](docs/tutorials/visualization.md).

## Metadata inspection

Python 3.11 or later is required. Define table and column metadata with
`pytred.metadata.TableMetadata` and `ColumnMetadata`. Pass table metadata to
`@polars_table(..., metadata=...)` or declare output metadata on a DataHub subclass
with `class MyDataHub(DataHub, metadata=...):`.

Inspect an importable DataHub class from the command line:

```bash
pytred --version
pytred metadata my_project.pipeline:MyDataHub
```

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

Each table entry also includes `is_optional`: `true` or `false` for processing
methods, and `null` for input tables where it does not apply. This describes
whether a method can be skipped when dependencies are missing, not whether it
was skipped during an execution.

Declare input metadata independently of runtime data with `input_tables`:

```python
from pytred import DataHub
from pytred.metadata import TableMetadata

class MyDataHub(DataHub, input_tables={
    "source": TableMetadata(description="Source observations", primary_key=("id",)),
    "lookup": None,
}):
    pass

metadata = get_datahub_metadata(MyDataHub)
```

`MyDataHub.input_tables` contains the declarations, including unused inputs.
Omitting `input_tables` (or passing `None`) leaves declarations empty, just as
output `metadata` is not inherited automatically. Each class uses only the
declarations explicitly passed to it. To reuse a parent's declarations, pass
`input_tables=ParentHub.input_tables`, or extend them explicitly with
`{**ParentHub.input_tables, "extra": TableMetadata(...)}`. The mapping is copied.
Names must be
non-empty strings and must not collide with decorated processing methods.
Values must be `TableMetadata` or `None`; `None` declares a name without metadata.

Input declarations describe the pipeline independently of runtime DataNodes.
The CLI and visualization read input metadata from `input_tables`; they do not
fall back to metadata on supplied nodes. DataHub retains supplied DataNodes as-is,
including their metadata and join settings, without copying them or emitting warnings.
`hub.get(name).metadata` therefore need not match the pipeline declaration.
Named DataFrame inputs are wrapped in DataNodes without metadata. Undeclared
inputs remain accepted.
Declarations do not load data, validate DataFrame schemas, or make inputs required:
missing dependencies still skip optional steps and fail required steps as before.
Metadata output includes declared inputs and dependencies without a processing
method as their producer. Undeclared inputs have `metadata_status: "unset"` and
`metadata: null`; shared dependencies appear only once. A table produced by a
later processing method is not treated as an external input.
Input declarations are optional and do not affect execution.

`visualize_datahub(hub)` displays an existing DataHub instance. Input nodes include
metadata and descriptions from the hub's input declarations. Declarations do not
add graph nodes or change dependencies and joins; missing dependencies remain
missing even when their names are declared.

In 0.5.1, the internal type alias `POLARS_JOIN_METHOD` is renamed to `PolarsJoinMethod`.

In 0.5.0, `DataHub.registerd_tables_order` is renamed to
`DataHub.registered_tables_order`; update any direct references to the old name.

## Codex plugin

This repository includes a Codex plugin that teaches AI agents how to use
`DataHub`, `DataNode`, `@polars_table`, and interactive notebook visualization without relying
on generic data-pipeline assumptions.

Install the Python package in your project's environment as usual:

```bash
pip install pytred
```

Install the Codex plugin separately from the repository marketplace:

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
