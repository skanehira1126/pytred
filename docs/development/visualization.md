# Maintaining the notebook graph

`src/pytred/helpers/visualize.py` converts DataHub metadata to JSON-compatible
nodes and edges and embeds a snapshot in HTML. It does not run transformations.
IDs are generated independently of table names, including the root and output.
Missing dependencies are explicit nodes; input joins use the input DataNode's
actual join metadata. Execution-order values are inspection metadata, not ranks.

`frontend/dataflow.js` owns interaction and drawing. Dagre places nodes from
actual edges; Cytoscape.js renders the graph. Each output has an isolated,
sandboxed iframe with inline JS, CSS, and JSON. No Python model, event subscription,
or comm survives display. Removing the output discards the iframe document.
User-provided strings are escaped for JSON script text and the HTML attribute;
the frontend inserts them as graph data or DOM text, never HTML.

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
and source distributions; no license display is added to the graph UI.
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
npm run build --prefix frontend
uv build
```

Python tests cover graph semantics, HTML snapshots, and safe data embedding. CI rebuilds the
assets and rejects stale bundles. Browser interactions are not automatically
tested; check rendering and interaction in a Jupyter notebook when changing the
graph or its JavaScript dependencies.
