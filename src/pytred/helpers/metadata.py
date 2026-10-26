from pytred.data_hub import DataHub
from pytred.helpers.decorator import get_metadata


def get_datahub_metadata(hub_class: type[DataHub]) -> dict:
    """Return JSON-compatible metadata without constructing or running the DataHub."""
    if not isinstance(hub_class, type) or not issubclass(hub_class, DataHub):
        raise TypeError("The target must be a DataHub subclass")

    datahub_name = f"{hub_class.__module__}:{hub_class.__qualname__}"
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

    tables: list[dict] = []
    table_order = hub_class.registered_tables_order
    seen = set(hub_class.input_tables) | set(table_order)
    for name, metadata in hub_class.input_tables.items():
        tables.append(
            {
                "order": -1,
                "name": name,
                "depends": [],
                "is_optional": None,
                "metadata_status": "defined" if metadata is not None else "unset",
                "metadata": metadata.model_dump(mode="json") if metadata is not None else None,
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
                    }
                )
                seen.add(source)

    output_metadata = {
        "DataHub": metadata_datahub,
        "tables": sorted(tables, key=lambda x: (x["order"], x["name"])),
    }

    return output_metadata
