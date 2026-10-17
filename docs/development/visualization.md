# Maintaining the notebook widget

`src/pytred/helpers/visualize.py` converts DataHub metadata to JSON-compatible
nodes and edges and provides the anywidget view. It does not run transformations.
IDs are generated independently of table names, including the root and output.
Missing dependencies are explicit nodes; input joins use the input DataNode's
actual join metadata. Execution-order values are inspection metadata, not ranks.

`frontend/widget.js` owns interaction and drawing. Dagre places the nodes from
actual edges; Cytoscape.js renders the graph. Each anywidget view owns a separate
Cytoscape instance. The AFM cleanup callback removes model listeners, disconnects
the resize observer, and destroys the renderer. User-provided strings are passed
as graph data or DOM text, never interpolated into HTML.

## Rebuild the bundled assets

From the repository root, with Node.js and npm installed:

```bash
npm ci --prefix frontend
npm run build --prefix frontend
```

Commit `frontend/package-lock.json` and the generated files under
`src/pytred/static/` together with source changes. The build checks that the JS
bundle has no external imports. Following NumPy's bundled-license inventory format,
`src/pytred/static/LICENSES_bundled.txt` lists each bundled library's name, version,
containing file, license, and path to its full license text. The build copies the
original license files of Cytoscape.js, Dagre, and Graphlib unchanged into
`src/pytred/static/licenses/`. Both the inventory and license files ship in wheels
and source distributions; no license display is added to the widget UI.
Review the notices and transitive dependencies when updating
versions. Python wheel/sdist builds use these checked-in assets and do not invoke
npm. End users do not need Node.js.

Dependabot checks `frontend/` npm dependencies monthly. Minor and patch updates
are grouped; major updates remain separate. For an update PR, regenerate and
commit the bundled assets and license notices before merging. The existing CI
build comparison detects stale assets. Dependabot does not rebuild or release
the Python package.

## Verify

```bash
python -m pytest tests
npm exec --prefix frontend -- playwright install chromium
npm test --prefix frontend
uv build
```

For an existing local Chrome installation, run
`PYTRED_BROWSER_CHANNEL=chrome npm test --prefix frontend`.
Browser tests load the actual bundle, block external requests, and exercise
selection, layout, zoom, separate views, and disposal. Python tests cover graph
semantics and the widget MIME bundle. CI rebuilds the assets and rejects stale
bundles before running the browser tests.
