from importlib.metadata import version
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from typing import Annotated

import polars as pl
import pytest

from pytred import DataHub
from pytred._types import Root
from pytred.cli.cli import cli
from pytred.decorators import polars_table
from pytred.metadata import TableMetadata


class CliHub(DataHub, metadata=TableMetadata(description="注文の集計")):
    source: Annotated[pl.DataFrame, Root(metadata=TableMetadata(description="CLI input"))]

    def __init__(self):
        pytest.fail("The CLI must not construct the pipeline")

    @polars_table(0, is_optional=True)
    def result(self, source):
        pytest.fail("The CLI must not execute processing methods")


def test_installed_metadata_command_resolves_target_and_prints_only_json(tmp_path, monkeypatch):
    target = f"{__name__}:CliHub"
    entry_point = shutil.which("pytred", path=str(Path(sys.executable).parent))
    assert entry_point is not None, "Missing pytred console script in this Python environment"
    monkeypatch.setenv("PYTHONPATH", str(Path(__file__).resolve().parents[1]), prepend=os.pathsep)
    process = subprocess.run(
        [entry_point, "metadata", target],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=True,
    )
    result = json.loads(process.stdout)
    assert process.stderr == ""
    assert result["DataHub"]["name"] == target
    assert result["DataHub"]["metadata"]["description"] == "注文の集計"
    source, table = result["tables"]
    assert source["name"] == "source"
    assert source["is_optional"] is None
    assert source["metadata"]["description"] == "CLI input"
    assert table["name"] == "result"
    assert table["is_optional"] is True
    for entry in (result["DataHub"], source, table):
        assert entry["source"]["file"] == str(Path(__file__).resolve())
        assert entry["source"]["line"] > 0


@pytest.mark.parametrize("table", ["source", "result"])
def test_metadata_table_selects_one_entry_and_retains_hub(table, monkeypatch, capsys):
    target = f"{__name__}:CliHub"
    monkeypatch.setattr(sys, "argv", ["pytred", "metadata", target, "--table", table])

    cli()

    captured = capsys.readouterr()
    result = json.loads(captured.out)
    assert captured.err == ""
    assert result["DataHub"]["metadata"]["description"] == "注文の集計"
    assert [entry["name"] for entry in result["tables"]] == [table]
    assert result["tables"][0]["source"]["file"] == str(Path(__file__).resolve())
    if table == "result":
        assert result["tables"][0]["depends"] == ["source"]


@pytest.mark.parametrize("selection", [[], ["--table", "result"]], ids=["all", "selected"])
def test_compact_outline_omits_metadata_and_keeps_locations(selection, monkeypatch, capsys):
    target = f"{__name__}:CliHub"
    monkeypatch.setattr(sys, "argv", ["pytred", "metadata", target, "--compact", *selection])

    cli()

    captured = capsys.readouterr()
    lines = captured.out.splitlines()
    labels = [line.split(" @ ")[0] for line in lines]
    assert labels == (
        [target, "0 result(source) [optional]"]
        if selection
        else [target, "input source", "0 result(source) [optional]"]
    )
    for line in lines:
        assert f" @ {Path(__file__).resolve()}:" in line
        assert int(line.rsplit(":", 1)[1]) > 0
    assert "注文の集計" not in captured.out
    assert "CLI input" not in captured.out
    assert captured.err == ""


def test_compact_outline_marks_inferred_input_location_unavailable(monkeypatch, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        ["pytred", "metadata", "tests.fixtures.data_hub:OptionalHub", "--compact"],
    )

    cli()

    assert "input source @ source unavailable" in capsys.readouterr().out


def test_unknown_table_exits_with_usage_error(monkeypatch, capsys):
    monkeypatch.setattr(
        sys, "argv", ["pytred", "metadata", f"{__name__}:CliHub", "--table", "missing"]
    )

    with pytest.raises(SystemExit) as error:
        cli()

    assert error.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "unknown table 'missing'" in captured.err
    assert "Traceback" not in captured.err


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


def test_metadata_command_without_target_exits_with_usage_error(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["pytred", "metadata"])
    with pytest.raises(SystemExit) as error:
        cli()
    assert error.value.code == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "target" in captured.err
