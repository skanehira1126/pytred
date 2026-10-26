from datetime import date
import json

import pytest

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
    class Hub(
        MetadataHub, input_tables={"source": TableMetadata(description="Source"), "unused": None}
    ):
        @polars_table(2)
        def before(self, later, undeclared):
            pytest.fail("Inspection must not execute methods")

        @polars_table(3)
        def later(self):
            pytest.fail("Inspection must not execute methods")

    tables = get_datahub_metadata(Hub)["tables"]
    assert len([table for table in tables if table["name"] == "later"]) == 1
    by_name = {table["name"]: table for table in tables}
    assert by_name["source"]["metadata"]["description"] == "Source"
    assert by_name["source"]["metadata_status"] == "defined"
    assert by_name["unused"]["metadata_status"] == "unset"
    assert len(tables) == len(by_name)
    assert by_name["undeclared"]["metadata_status"] == "unset"
    assert by_name["undeclared"]["metadata"] is None
    assert by_name["before"]["depends"] == ["later", "undeclared"]
    assert by_name["later"]["order"] == 3


def test_collect_metadata_without_execution(capsys):
    result = get_datahub_metadata(MetadataHub)

    assert capsys.readouterr().out == ""
    assert result["DataHub"]["metadata_status"] == "unset"
    assert result["DataHub"]["metadata"] is None
    assert [table["name"] for table in result["tables"]] == ["source", "first", "second"]
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


@pytest.mark.parametrize("target", [object(), int], ids=["instance", "unrelated_class"])
def test_reject_non_datahub_target(target):
    with pytest.raises(TypeError, match="DataHub subclass"):
        get_datahub_metadata(target)
