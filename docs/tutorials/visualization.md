# Interactive pipeline visualization

In a notebook with Jupyter Widgets support, display a DataHub instance:

```python
from IPython.display import display


display(hub)
```

Each display is an independent snapshot of the current DataHub definition.
Changing or replacing the hub does not modify previous outputs. Display it again
to inspect its new state. No widget model, comm, or explicit close is needed.

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

For a displayable HTML snapshot with a horizontal initial layout:

```python
from pytred.helpers.visualize import visualize_datahub


view = visualize_datahub(hub, direction="LR")
display(view)
```

The returned view stores HTML only. Its graph remains interactive in the browser
without a live connection to Python.

## Installation and display environments

Cytoscape.js, Dagre, styles, and third-party license notices are embedded in the
HTML output from the assets bundled with pytred. No Node.js installation, CDN,
widget manager, or initialization cell is needed.

`DataHub._repr_html_()` returns a sandboxed iframe containing the graph snapshot.
The notebook must allow trusted HTML with JavaScript and iframe `srcdoc` support.
An untrusted notebook or a renderer that strips scripts/iframes cannot show the
interactive graph. Trust and rerun the notebook when appropriate. Exported HTML
can retain the graph if the exporter preserves the iframe; Markdown and GitHub's
notebook preview do not provide this interactive display.

The previous Mermaid renderer, image-export helper, and `pytred report` CLI
have been removed. Report and image export are not provided by this version.
