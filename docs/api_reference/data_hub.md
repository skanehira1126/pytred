::: pytred.data_hub.DataHub

## Visualization

`DataHub` owns table registration, dependency inspection, and pipeline execution.
Notebook visualization is implemented in `pytred.helpers.visualize` using
Cytoscape.js, Dagre, and anywidget. `display(hub)` delegates through
`_repr_mimebundle_()` without executing the pipeline. `_repr_html_()` delegates
to a static summary for HTML-only frontends.

For an explicit widget, use `visualize_datahub(hub, direction="LR")` from
`pytred.helpers.visualize`. See the [visualization tutorial](../tutorials/visualization.md)
for controls and installation requirements.
