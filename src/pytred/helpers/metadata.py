import ast
import inspect
from pathlib import Path
from textwrap import dedent

from pytred.data_hub import DataHub
from pytred.helpers.decorator import get_metadata


def _source_location(obj) -> dict | None:
    obj = inspect.unwrap(obj)
    try:
        filename = inspect.getsourcefile(obj)
        if filename is None or not Path(filename).is_file():
            return None
        _, line = inspect.getsourcelines(obj)
    except (OSError, TypeError):
        return None
    return {"file": str(Path(filename).resolve()), "line": line}


def _input_source_locations(hub_class: type[DataHub], source: dict | None) -> dict:
    if source is None:
        return {}
    try:
        lines, start = inspect.getsourcelines(hub_class)
        definition = ast.parse(dedent("".join(lines))).body[0]
    except (OSError, TypeError, SyntaxError):
        return {}
    if not isinstance(definition, ast.ClassDef):
        return {}
    return {
        node.target.id: {"file": source["file"], "line": start + node.lineno - 1}
        for node in definition.body
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name)
    }


def get_datahub_metadata(hub_class: type[DataHub]) -> dict:
    """Return metadata and source locations without constructing or running the hub.

    Source locations contain an absolute file path and a one-based starting line
    (including decorators), or None when no source file can be inspected. Input
    locations point to annotations directly in the class body; inferred inputs
    have no declaration and therefore no source location.
    """
    if not isinstance(hub_class, type) or not issubclass(hub_class, DataHub):
        raise TypeError("The target must be a DataHub subclass")

    datahub_name = f"{hub_class.__module__}:{hub_class.__qualname__}"
    hub_source = _source_location(hub_class)
    input_sources = _input_source_locations(hub_class, hub_source)

    # Of output table from DataHub
    metadata_datahub: dict[str, object]
    if hub_class.metadata is None:
        metadata_datahub = {
            "name": datahub_name,
            "metadata_status": "unset",
            "metadata": None,
        }
    else:
        metadata_datahub = {
            "name": datahub_name,
            "metadata_status": "defined",
            "metadata": hub_class.metadata.model_dump(mode="json"),
        }
    metadata_datahub["source"] = hub_source

    tables: list[dict] = []
    table_order = hub_class.registered_tables_order
    seen = set(hub_class.input_tables) | set(table_order)
    for name, input_defs in hub_class.input_tables.items():
        metadata = input_defs["marker"].metadata
        tables.append(
            {
                "order": -1,
                "name": name,
                "depends": [],
                "is_optional": None,
                "metadata_status": "defined" if metadata is not None else "unset",
                "metadata": metadata.model_dump(mode="json") if metadata is not None else None,
                "source": input_sources.get(name),
            }
        )
    for order, table_name, depends in hub_class.collect_table_and_arguments(table_order):
        function = getattr(hub_class, table_name)
        metadata = get_metadata(function, "metadata")
        tables.append(
            {
                "order": order,
                "name": table_name,
                "depends": depends,
                "is_optional": get_metadata(function, "is_optional"),
                "metadata_status": "defined" if metadata is not None else "unset",
                "metadata": metadata.model_dump(mode="json") if metadata is not None else None,
                "source": _source_location(function),
            }
        )
        for source in depends:
            if source not in seen:
                tables.append(
                    {
                        "order": -1,
                        "name": source,
                        "depends": [],
                        "is_optional": None,
                        "metadata_status": "unset",
                        "metadata": None,
                        "source": None,
                    }
                )
                seen.add(source)

    output_metadata = {
        "DataHub": metadata_datahub,
        "tables": sorted(tables, key=lambda x: (x["order"], x["name"])),
    }

    return output_metadata
