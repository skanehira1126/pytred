# Interactive pipeline visualization

In a notebook with Jupyter Widgets support, display a DataHub instance:

```python
from IPython.display import display


display(hub)
```

Repeated `display(hub)` calls reuse the same widget model and comm. Each call
refreshes the graph definition, including views of that hub already displayed
in other cells. When finished, call `hub.close_visualization()` to close the
model and all its views. Displaying the hub afterward creates a new widget.

The graph shows the pipeline definition without executing any transformation.
Input tables are blue, processing steps are gray, and the output is green.
Dashed edges show joins, including join methods and keys. Missing input tables
are marked in orange; optional steps remain visible even if they would be skipped.
The output summarizes the joins followed by `post_step` and any final filters;
it does not expand the code inside those operations.

Scroll to zoom, drag the background to pan, or drag individual nodes. Select a
node on the graph or in the **Inspect** menu to highlight its upstream and
downstream dependencies and show its docstring and join metadata. **Fit** frames
the graph and **Reset** restores automatic placement. **Direction** switches
between top-to-bottom and left-to-right placement.

For an explicit widget, including a horizontal initial layout:

```python
from pytred.helpers.visualize import visualize_datahub


view = visualize_datahub(hub, direction="LR")
display(view)
```

Each explicit widget captures an independent snapshot of the definition.
Create a new widget after changing the pipeline. Call `view.close()` when it is
no longer needed; `hub.close_visualization()` does not close explicit widgets.

## Installation and display environments

`pip install pytred` installs the Python dependencies, including `anywidget`.
Cytoscape.js, Dagre, and the widget styles are included in the Python package.
No Node.js installation, CDN access, or initialization cell is needed to display
a graph. The notebook frontend must have a working Jupyter Widgets manager;
if the frontend and kernel use separate environments, both need their respective
widget support installed. The bundled graph code does not make network requests.

`DataHub._repr_mimebundle_()` supplies the interactive widget. HTML-only clients
can use `_repr_html_()` for a static summary; this summary is not a graph.
A saved notebook is not a standalone HTML application, and static Markdown/HTML
renderers do not automatically display the interactive widget.

The previous Mermaid renderer, image-export helper, and `pytred report` CLI
have been removed. Report and image export are not provided by this version.
