from pathlib import Path
import subprocess
import sysconfig

import pytest

import pytred


CLI = str(Path(sysconfig.get_path("scripts")) / "pytred")


@pytest.fixture
def report_command():
    return [CLI, "report", str(Path(__file__).parent / "fixtures/data_hub.py"), "BranchingHub"]


def test_report_cli(report_command):
    result = subprocess.run(
        report_command
        + [
            "--input-table",
            '{"name": "source"}',
            "--input-table",
            '{"name": "lookup", "keys": ["id"], "join": "left"}',
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    assert result.stdout.startswith("## BranchingHub\n")
    assert "| -1 | lookup | input | | left | id |" in " ".join(result.stdout.split())
    assert "```mermaid\ngraph TD" in result.stdout
    assert "source --> doubled" in result.stdout


def test_report_cli_rejects_invalid_json(report_command):
    result = subprocess.run(
        report_command + ["--input-table", '{"name": "source",}'],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "JSONDecodeError" in result.stderr
    assert result.stdout == ""


def test_version_cli():
    result = subprocess.run([CLI, "--version"], capture_output=True, text=True, check=True)

    assert result.stdout == f"pytred cli {pytred.__version__}\n"
