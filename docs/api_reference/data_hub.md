::: pytred.data_hub.DataHub

## Visualization

`DataHub` owns table registration, dependency inspection, and pipeline execution.
Mermaid graph construction and notebook HTML rendering are implemented in
`pytred.helpers.visualize`; the graph models live in `pytred.helpers.dataflow`.

Notebook display remains available through `display(hub)`: `DataHub._repr_html_()`
delegates to `render_datahub_html(hub)` without executing the pipeline.
For class-based graphs and reports, use `make_dataflow_graph_from_datahub()` and
`report_datahub()` from `pytred.helpers.visualize`.

`DataHub.search_tables()` and `DataHub.get_dataflow_graph()` remain as compatibility
entry points. Existing imports of `DataflowNode`, `DataEdge`, and `DataflowGraph`
from `pytred.data_node` also remain supported.
