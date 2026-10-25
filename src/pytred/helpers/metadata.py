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
    # Infer input tables from dependencies until explicit input declarations are supported.
    seen = []

    table_order = hub_class.registered_tables_order
    for order, table_name, depends in hub_class.collect_table_and_arguments(table_order):
        seen.append(table_name)

        metadata = get_metadata(getattr(hub_class, table_name), "metadata")
        tables.append(
            {
                "order": order,
                "name": table_name,
                "depends": depends,
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
                        "metadata_status": "unsupported",
                        "metadata": None,
                    }
                )
                seen.append(source)

    output_metadata = {
        "DataHub": metadata_datahub,
        "tables": sorted(tables, key=lambda x: (x["order"], x["name"])),
    }

    return output_metadata
