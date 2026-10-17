::: pytred.data_hub.DataHub

## Visualization

`DataHub` owns table registration, dependency inspection, and pipeline execution.
Mermaid graph construction and notebook HTML rendering are implemented in
`pytred.helpers.visualize`; the graph models live in `pytred.helpers.dataflow`.

Notebook display remains available through `display(hub)`: `DataHub._repr_html_()`
delegates to `render_datahub_html(hub)` without executing the pipeline.
For class-based graphs and reports, use `make_dataflow_graph_from_datahub()` and
`report_datahub()` from `pytred.helpers.visualize`.

Use `search_tables()` and `get_dataflow_graph()` from `pytred.helpers.visualize`
when working with visualization nodes directly. Import `DataflowNode`, `DataEdge`,
and `DataflowGraph` from `pytred.helpers.dataflow`.
