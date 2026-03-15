---
name: pytred-usage
description: Explain or generate usage of the `pytred` package in this repository. Use when Codex needs to answer how to use `DataHub`, `DataNode`, or `@polars_table`, create or modify `pytred` preprocessing pipelines, explain `post_step` and filtering behavior, visualize `pytred` workflows in notebooks, or show `pytred report` CLI usage.
---

# Pytred Usage

## Overview

Explain `pytred` from the repository's actual API and examples instead of from generic data-pipeline assumptions. Focus on `DataHub`, `DataNode`, `pytred.decorators.polars_table`, optional and intermediate tables, notebook visualization, and CLI reporting.

## Workflow

1. Identify the request shape before answering:
- Explain a reusable preprocessing class: read `references/usage-guide.md` sections `Core Model` and `Build a Reusable Pipeline Class`.
- Explain how to join already-computed tables: read `references/usage-guide.md` section `Compose Existing Tables with DataNode`.
- Explain inspection, visualization, or reporting: read `references/usage-guide.md` sections `Inspect and Debug` and `Visualize and Report`.

2. Base explanations and examples on repository sources:
- Prefer patterns from `tests/fixtures/data_hub.py` for minimal runnable code.
- Use `examples/01_basic_preprocessing.ipynb` for subclass-based preprocessing.
- Use `examples/02_use_completed_data.ipynb` for `DataNode` composition.
- Use `examples/03_visualize_workflow.ipynb` and `docs/tutorials/cli.md` for visualization and reporting.

3. Preserve `pytred`'s actual invariants in every answer:
- `root_df` is the base table that final joined tables attach to.
- Positional `DataNode` arguments are already-computed tables.
- Keyword `pl.DataFrame` arguments become named input tables and are not auto-joined to the output by themselves.
- Decorated method parameter names must match available input table names or prior method names.
- `join=None` means the table is intermediate-only and stays out of the final output.
- `is_optional=True` means the step is skipped when its required input tables are missing.

4. When generating code, prefer concise runnable examples with `polars as pl`, and include docstrings or short comments where they materially clarify the pipeline.

## Resources

- `references/usage-guide.md`: concrete mental model, code examples, visualization/reporting workflow, and common pitfalls.
