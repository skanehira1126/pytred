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
