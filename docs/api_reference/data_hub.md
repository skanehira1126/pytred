# DataHub

Define a pipeline, execute it with fresh inputs, and inspect intermediate tables.
For a complete example, see [Get started](../getting-started.md).

::: pytred.data_hub.DataHub
    options:
      members:
        - execute
        - get
        - root_df
        - get_root_table_name
        - get_input_table_names
        - post_step

## Notebook display

Use `display(hub)` to inspect a snapshot without executing the pipeline.
See [Explore the graph](../tutorials/visualization.md) for controls and display requirements.
