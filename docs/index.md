---
hide:
  - toc
---

# Build reusable Polars pipelines

pytred connects your Polars transformations into a pipeline you can run,
inspect, and reuse. Write a method for each intermediate table, name its inputs,
and choose how its result joins the output.

[Run your first pipeline](getting-started.md){ .md-button .md-button--primary }
[Explore the interactive viewer](tutorials/visualization.md#try-the-viewer){ .md-button }

## Start with your task

<div class="grid cards" markdown>

- **Create a pipeline**

    Install pytred and turn two small input tables into customer totals.

    [Get started](getting-started.md)

- **Add processing steps and joins**

    Reuse intermediate tables, join precomputed results, and choose which rows to return.

    [Build a pipeline](guides/pipelines.md)

- **Inspect a pipeline**

    Explore dependencies, join keys, and Python source in a notebook or the live demo.

    [Use the viewer](tutorials/visualization.md)

- **Find a missing input or column**

    Check required inputs, skipped steps, join keys, and intermediate results.

    [Troubleshoot a run](guides/troubleshooting.md)

</div>

## How it fits together

A `DataHub` class defines a pipeline. Its `Root` input provides the starting
rows. Other inputs supply data to methods decorated with `@polars_table`.
Each method returns a Polars DataFrame; its decorator decides whether to join
that table to the root or keep it as an intermediate result.

Call `execute(...)` with fresh inputs for each run. Inspect intermediate tables
with `get(...)`, and use `display(hub)` in a trusted notebook to explore the graph.

## Look up the details

- [Input declarations](api_reference/inputs.md): required tables and root selection.
- [Table and column metadata](api_reference/metadata.md): describe data and inspect definitions.
- [API reference](api_reference/data_hub.md): signatures, parameters, and return values.
- [Example notebooks](examples.md): longer workflows to run and adapt.

These guides target pytred 1.x on Python 3.11–3.14.
