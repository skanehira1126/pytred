::: pytred.data_hub.DataHub

## Visualization

`DataHub` owns table registration, dependency inspection, and pipeline execution.
Notebook visualization is implemented in `pytred.helpers.visualize` using
Cytoscape.js and Dagre. `display(hub)` delegates through `_repr_html_()` without
executing the pipeline. Each output is an independent HTML snapshot with
browser-only interaction; no Python widget state or comm is created.

For a displayable snapshot, use `visualize_datahub(hub, direction="LR")` from
`pytred.helpers.visualize`. See the [visualization tutorial](../tutorials/visualization.md)
for controls and display requirements.
