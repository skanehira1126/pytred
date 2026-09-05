---
name: pytred-usage
description: Build, explain, or debug pytred preprocessing pipelines with DataHub, DataNode, and @polars_table. Use for table dependencies, joins, optional steps, post_step filters, notebook visualization, and pytred report. Excludes generic Polars work without pytred and Codex plugin installation.
---

# Pytred Usage

Help the user obtain the intended output table or understand a specific pytred behavior. The user's explicit requirements take precedence over this skill's recommendations. Adapt the examples to their table names, keys, and expected output.

## Choose the relevant guidance

Read the relevant sections of the bundled [usage guide](references/usage-guide.md). Resolve its path relative to this skill, including when installed in another project.

| Request | Sections to read |
| --- | --- |
| Build or modify preprocessing | Core Model; Build a Reusable Pipeline Class |
| Join already computed tables | Core Model; Compose Existing Tables with DataNode |
| Explain missing columns, skipped steps, or errors | Core Model; Inspect and Debug |
| Explain post_step or output filters | Core Model |
| Inspect a notebook workflow or generate a report | Visualize and Report |

Use a `DataHub` subclass for reusable transformations and positional `DataNode` inputs for already computed tables. Keep the scope of the answer tied to the request; a usage explanation does not require creating a notebook or report.

## Use the right evidence

- For a usage question, start with the guide and the user's code. The plugin supplies instructions, not the Python package or the pytred source repository.
- Before executing a pipeline, use the project's Python environment and check that pytred is available. Resolve version-sensitive questions against that installed package; use a source checkout when the task is developing pytred itself.
- The guide's Source Map lists optional checkout references. Do not assume those files exist in the user's project, or require a checkout to use the bundled guide.
- Infer input schemas and join keys from supplied code or data. If a missing choice would change the resulting rows or columns, ask a focused question; otherwise make the assumption explicit and continue.

## Deliver and verify

For generated code, make the input tables and expected output clear. Include small synthetic inputs when a standalone runnable example is useful; otherwise identify the existing variables the snippet uses.

For a pipeline change, check the affected output columns and rows, table-name dependencies, execution order, and join keys using a small relevant case. For an explanation, verify the relevant API contract without requiring execution. Report what was checked and any remaining uncertainty; do not claim unrun code was tested.
