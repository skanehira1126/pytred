# Pytred CLI

This guide describes pytred 1.x in the project's Python environment. The plugin
does not install the Python package. Replace the target with an importable
`module:DataHubSubclass` from the user's project.

```bash
pytred --version
pytred metadata my_project.pipeline:MyDataHub
```

By default the command prints JSON without constructing or executing the DataHub.
It imports the target module, so module-level code still runs.

- `DataHub`: class name and output metadata.
- `tables`: generated tables, inputs declared with Root/Input, and inferred inputs,
  with `order`, `name`, `depends`,
  and `is_optional`. Input tables use order `-1` and `is_optional: null`.
- `is_optional` is a definition flag, not an execution or skip result.
- `metadata_status`: `defined` (metadata object) or `unset` (`metadata: null`).
  Declared metadata is not an inferred DataFrame schema.

## Focused inspection (pytred 1.1+)

Start with a compact outline for a large hub, then select the input or generated
table needed for the change:

```bash
pytred metadata my_project.pipeline:MyDataHub --compact
pytred metadata my_project.pipeline:MyDataHub --table combined
pytred metadata my_project.pipeline:MyDataHub --table combined --compact
```

`--compact` emits text with the hub name and one line per table: `input NAME` or
`ORDER NAME(DEPENDENCIES)`, plus `[optional]` when applicable. Every line ends
with `@ FILE:LINE`, or `@ source unavailable`. Detailed metadata and method
bodies are omitted. Read the relevant source at the reported location when
needed.

`--table NAME` keeps the hub entry and exactly one matching table. Dependencies
remain listed by name; their entries are not included automatically. Unknown
names produce an error on stderr with exit code 2 and no stdout.

JSON and the Python dictionary also include `source` on the hub and each table:
`{"file": "/absolute/path/pipeline.py", "line": 12}`. Lines are one-based and
start at the first decorator, if present. Declared inputs point to their
annotation directly in the class body. Inferred inputs and definitions without
retrievable source files have `source: null`.

For pytred 1.0, use the default JSON command above; the inspection options and
source locations require 1.1 or later.

For declaration syntax and its relationship to runtime DataNodes, read
[Define Table Metadata](usage-guide.md#define-table-metadata).
Dependencies without a processing method as their producer appear once as
input entries with `unset` metadata when undeclared.

For a dictionary in Python, pass the class directly:

```python
from pytred.helpers.metadata import get_datahub_metadata

metadata = get_datahub_metadata(MyDataHub)
```
