from datetime import date
import json
from pathlib import Path
from typing import Annotated

import polars as pl
import pytest

from pytred._types import Input
from pytred._types import Root
from pytred.data_hub import DataHub
from pytred.decorators import polars_table
from pytred.helpers.metadata import get_datahub_metadata
from pytred.metadata import TableMetadata


class MetadataHub(DataHub):
    def __init__(self):
        pytest.fail("Metadata inspection must not construct the pipeline")

    @polars_table(0, metadata=TableMetadata(updated_on=date(2026, 10, 25)))
    def first(self, source):
        pytest.fail("Metadata inspection must not execute table methods")

    @polars_table(1, is_optional=True)
    def second(self, source, first):
        pytest.fail("Metadata inspection must not execute table methods")


def test_declared_inputs_and_later_producers_are_distinct():
    class Hub(DataHub):
        source: Annotated[pl.DataFrame, Root(metadata=TableMetadata(description="Source"))]
        unused: Annotated[pl.DataFrame, Input(metadata=None)]

        @polars_table(2)
        def before(self, later, undeclared):
            pytest.fail("Inspection must not execute methods")

        @polars_table(3)
        def later(self):
            pytest.fail("Inspection must not execute methods")

    result = get_datahub_metadata(Hub)
    tables = result["tables"]
    assert sorted(table["name"] for table in tables) == [
        "before",
        "later",
        "source",
        "undeclared",
        "unused",
    ]
    by_name = {table["name"]: table for table in tables}
    assert by_name["source"]["metadata"]["description"] == "Source"
    assert by_name["source"]["metadata_status"] == "defined"
    assert by_name["unused"]["metadata_status"] == "unset"
    assert by_name["unused"]["metadata"] is None
    assert by_name["undeclared"]["metadata_status"] == "unset"
    assert by_name["undeclared"]["metadata"] is None
    assert by_name["before"]["depends"] == ["later", "undeclared"]
    assert by_name["later"]["order"] == 3
    lines = Path(__file__).read_text().splitlines()
    hub_source = result["DataHub"]["source"]
    assert hub_source["file"] == str(Path(__file__).resolve())
    assert lines[hub_source["line"] - 1].strip() == "class Hub(DataHub):"
    for name in ("source", "unused"):
        location = by_name[name]["source"]
        assert location["file"] == hub_source["file"]
        assert lines[location["line"] - 1].strip().startswith(f"{name}: Annotated[")
    location = by_name["before"]["source"]
    assert location["file"] == hub_source["file"]
    assert lines[location["line"] - 1].strip() == "@polars_table(2)"
    assert by_name["undeclared"]["source"] is None


def test_collect_metadata_without_execution(capsys):
    result = get_datahub_metadata(MetadataHub)

    assert capsys.readouterr().out == ""
    assert result["DataHub"]["metadata_status"] == "unset"
    assert result["DataHub"]["metadata"] is None
    assert [(table["order"], table["name"]) for table in result["tables"]] == [
        (-1, "source"),
        (0, "first"),
        (1, "second"),
    ]
    source, first, second = result["tables"]
    assert source["metadata_status"] == "unset"
    assert source["metadata"] is None
    assert source["depends"] == []
    assert source["is_optional"] is None
    assert first["depends"] == ["source"]
    assert first["metadata_status"] == "defined"
    assert first["metadata"]["updated_on"] == "2026-10-25"
    assert first["is_optional"] is False
    assert second["metadata_status"] == "unset"
    assert second["metadata"] is None
    assert second["depends"] == ["source", "first"]
    assert second["is_optional"] is True
    json.dumps(result)


def test_collect_configured_output_metadata():
    class OutputHub(DataHub, metadata=TableMetadata(updated_on=date(2026, 10, 25))):
        pass

    result = get_datahub_metadata(OutputHub)

    assert result["DataHub"]["name"] == f"{OutputHub.__module__}:{OutputHub.__qualname__}"
    assert result["DataHub"]["metadata_status"] == "defined"
    assert result["DataHub"]["metadata"]["updated_on"] == "2026-10-25"
    assert result["tables"] == []


def test_dynamic_definitions_without_source_still_return_metadata():
    namespace: dict = {}
    exec(compile("def result(self, rows):\n    return rows\n", "<dynamic>", "exec"), namespace)
    hub = type(
        "DynamicHub",
        (DataHub,),
        {"__module__": __name__, "result": polars_table(0)(namespace["result"])},
    )

    result = get_datahub_metadata(hub)

    assert result["DataHub"]["source"] is None
    assert [(table["name"], table["source"]) for table in result["tables"]] == [
        ("rows", None),
        ("result", None),
    ]
    assert result["tables"][1]["depends"] == ["rows"]


@pytest.mark.parametrize("target", [object(), int], ids=["instance", "unrelated_class"])
def test_reject_non_datahub_target(target):
    with pytest.raises(TypeError, match="DataHub subclass"):
        get_datahub_metadata(target)
