# Pytred CLI

This guide describes pytred 1.x in the project's Python environment. The plugin
does not install the Python package. Replace the target with an importable
`module:DataHubSubclass` from the user's project.

```bash
pytred --version
pytred metadata my_project.pipeline:MyDataHub
```

The command prints JSON without constructing or executing the DataHub. It imports
the target module, so module-level code still runs.

- `DataHub`: class name and output metadata.
- `tables`: generated tables, inputs declared with Root/Input, and inferred inputs,
  with `order`, `name`, `depends`,
  and `is_optional`. Input tables use order `-1` and `is_optional: null`.
- `is_optional` is a definition flag, not an execution or skip result.
- `metadata_status`: `defined` (metadata object) or `unset` (`metadata: null`).
  Declared metadata is not an inferred DataFrame schema.

For declaration syntax and its relationship to runtime DataNodes, read
[Define Table Metadata](usage-guide.md#define-table-metadata).
Dependencies without a processing method as their producer appear once as
input entries with `unset` metadata when undeclared.

For a dictionary in Python, pass the class directly:

```python
from pytred.helpers.metadata import get_datahub_metadata

metadata = get_datahub_metadata(MyDataHub)
```
