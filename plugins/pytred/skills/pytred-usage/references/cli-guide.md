# Pytred CLI

Requires pytred 0.5.0 or later in the project's Python environment. The plugin
does not install the Python package. Replace the target with an importable
`module:DataHubSubclass` from the user's project.

```bash
pytred --version
pytred metadata my_project.pipeline:MyDataHub
```

The command prints JSON without constructing or executing the DataHub. It imports
the target module, so module-level code still runs.

- `DataHub`: class name and output metadata.
- `tables`: generated tables and inferred inputs, with `order`, `name`, `depends`,
  and `is_optional`. Input tables use order `-1` and `is_optional: null`.
- `is_optional` is a definition flag, not an execution or skip result.
- `metadata_status`: `defined` (metadata object), `unset` (not configured), or
  `unsupported` (input metadata inspection is not yet supported). The latter two
  have `metadata: null`. Declared metadata is not an inferred DataFrame schema.

For a dictionary in Python, pass the class directly:

```python
from pytred.helpers.metadata import get_datahub_metadata

metadata = get_datahub_metadata(MyDataHub)
```
