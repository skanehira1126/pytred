from datetime import date
import json

import pytest

from pytred.cli.cli import print_datahub_metadata
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


def test_collect_metadata_without_execution(capsys):
    result = get_datahub_metadata(MetadataHub)

    assert capsys.readouterr().out == ""
    assert result["DataHub"]["metadata_status"] == "unset"
    assert result["DataHub"]["metadata"] is None
    assert [table["name"] for table in result["tables"]] == ["source", "first", "second"]
    source, first, second = result["tables"]
    assert source["metadata_status"] == "unsupported"
    assert source["metadata"] is None
    assert source["depends"] == []
    assert source["is_optional"] is None
    assert first["metadata_status"] == "defined"
    assert first["metadata"]["updated_on"] == "2026-10-25"
    assert first["is_optional"] is False
    assert second["metadata_status"] == "unset"
    assert second["metadata"] is None
    assert second["depends"] == ["source", "first"]
    assert second["is_optional"] is True
    json.dumps(result)


def test_cli_prints_metadata(capsys):
    print_datahub_metadata(f"{__name__}:MetadataHub")

    assert json.loads(capsys.readouterr().out) == get_datahub_metadata(MetadataHub)
