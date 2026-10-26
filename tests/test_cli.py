from importlib.metadata import version
import json
import sys

import pytest

from pytred import DataHub
from pytred.cli.cli import cli
from pytred.decorators import polars_table
from pytred.metadata import TableMetadata


class CliHub(
    DataHub,
    metadata=TableMetadata(description="CLI example"),
    input_tables={"source": TableMetadata(description="CLI input")},
):
    def __init__(self):
        pytest.fail("The CLI must not construct the pipeline")

    @polars_table(0, is_optional=True)
    def result(self, source):
        pytest.fail("The CLI must not execute processing methods")


def test_metadata_command(monkeypatch, capsys):
    target = f"{__name__}:CliHub"
    monkeypatch.setattr(sys, "argv", ["pytred", "metadata", target])

    cli()

    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert captured.err == ""
    assert result["DataHub"]["name"] == target
    assert result["DataHub"]["metadata"]["description"] == "CLI example"
    source, table = result["tables"]
    assert source["name"] == "source"
    assert source["is_optional"] is None
    assert source["metadata"]["description"] == "CLI input"
    assert table["name"] == "result"
    assert table["is_optional"] is True


def test_no_command_prints_help(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["pytred"])

    cli()

    captured = capsys.readouterr()
    assert "usage: pytred" in captured.out
    assert "metadata" in captured.out
    assert captured.err == ""


def test_version_command(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["pytred", "--version"])

    with pytest.raises(SystemExit) as exc_info:
        cli()

    assert exc_info.value.code == 0
    assert capsys.readouterr().out.strip() == f"pytred {version('pytred')}"
