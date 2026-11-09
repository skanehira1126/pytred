# DataNode

Wrap a precomputed table with its name, join keys, and metadata.
See [Join a precomputed table](../guides/pipelines.md#join-a-precomputed-table) for a runnable example.

::: pytred.data_node.DataNode
    options:
      members: false
      merge_init_into_class: true

## Skipped optional steps

`DataHub.get()` returns an `EmptyDataNode` when an optional step was skipped.

::: pytred.data_node.EmptyDataNode
    options:
      members: false
      merge_init_into_class: true
