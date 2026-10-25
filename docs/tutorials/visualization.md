# Interactive pipeline visualization

In a notebook with trusted HTML and JavaScript support, display a DataHub instance:

```python
from IPython.display import display


display(hub)
```

Each display is an independent snapshot of the current DataHub definition.
Changing or replacing the hub does not modify previous outputs. Display it again
to inspect its new state. No widget model, comm, or explicit close is needed.

The graph shows the pipeline definition without executing any transformation.
Input tables are blue, processing steps are gray, and the output is green.
The blue base path follows `root_df` through numbered join operations to Output.
Each join displays its method and keys; a dashed green edge supplies the right
table. Gray edges show preparation dependencies. Missing input tables
are marked in orange; optional steps remain visible even if they would be skipped.
The output summarizes the joins followed by `post_step` and any final filters;
it does not expand the code inside those operations.

Scroll to zoom, drag the background to pan, or drag individual nodes. Select a
node on the graph or in the **Inspect** menu to highlight its upstream and
downstream dependencies and show its docstring and join metadata in the right panel. **Fit** frames
the graph and **Reset** restores automatic placement. **Direction** switches
between top-to-bottom and left-to-right placement of the base path. Left-to-right
is the default. Scroll horizontally in narrow notebook outputs to reach the inspector.

Hover over a processing step to preview its Python implementation. Select the
node (or choose it in **Inspect**) to read, scroll, and select syntax-highlighted Python source in
the details panel. Input, missing-input, root, and output nodes have no function
preview. Moving off the node or pressing Escape dismisses the preview.

Source is captured when the snapshot is created, without executing the function.
Decorated functions are unwrapped to show the original implementation. When
Python cannot retrieve source, such as for some dynamically created functions or
notebook cells, the panel shows the function name and available docstring instead.
The HTML snapshot includes this source code, including when saved or exported.

For a displayable HTML snapshot with a vertical initial layout:

```python
from pytred.helpers.visualize import visualize_datahub


view = visualize_datahub(hub, direction="TB")
display(view)
```

The returned view stores HTML only. Its graph remains interactive in the browser
without a live connection to Python.

## Installation and display environments

Cytoscape.js, Dagre, the Python grammar of highlight.js, styles, and third-party license notices are embedded in the
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

## Try the viewer

This sample uses the same HTML snapshot as the notebook viewer and works on
GitHub Pages without a Python server. Hover over `combined`, click a join to
inspect its keys, or change direction. The sample is generated from
`docs/demos/viewer_sample.py` when the documentation is built. It displays the
declared pipeline, including optional joins even when they could be skipped;
it does not execute transformations or show runtime results.

<!-- pytred-interactive-demo -->
