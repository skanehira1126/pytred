# Troubleshoot a run

## A required input is missing

Compare the names passed to `execute()` with every `Root` and `Input`
declaration. All declared inputs are required on every call, even if only an
optional step uses them. Supply the root as a keyword DataFrame; a positional
`DataNode` may supply a non-root input by its `name`.

## A method cannot find its dependency

Check that the method parameter matches the input or producer method name
exactly. Producers must have a lower execution order than their consumers.
If a dependency is optional, check whether it or an upstream step was skipped.

## A computed column is absent from the output

A step with `join=None` produces an intermediate table only. Add a join strategy
and keys to include it in the result. Named input DataFrames also have no join;
use a processing method or a positional `DataNode` to join them.

## A join fails or changes the row count

Return every join key from the step and check key types on both sides. Key
uniqueness is validated by default. If you disable that check, duplicate keys
can multiply rows. A left join preserves unmatched root rows with null values
on the right; an inner join removes unmatched rows.

For full outer joins use `join="full"`. Check that your installed Polars version
supports the selected strategy; see [join requirements](pipelines.md#choose-joins-and-output-rows).

## Inspect a failed run

After a processing error, call `hub.get("table_name").table` for inputs or
intermediate tables already registered in that run. Each `execute()` clears
previous results, so these are not a cache of the last successful run.

Use `display(hub)` in a trusted notebook to inspect dependencies and join
settings. The graph shows pipeline structure, not proof that validation or a
step succeeded. See [viewer controls and display requirements](../tutorials/visualization.md).
