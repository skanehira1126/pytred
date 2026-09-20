from pathlib import Path

import pytest

from pytred.data_node import EmptyDataNode
from pytred.helpers.visualize import make_dataflow_graph_from_datahub
from pytred.helpers.visualize import report_datahub

from ..fixtures.data_hub import BranchingHub


@pytest.fixture
def source():
    return EmptyDataNode(name="source", keys=None, join=None)


def test_graph_describes_dependencies_and_joins(source):
    graph, nodes = make_dataflow_graph_from_datahub(BranchingHub, source)

    assert [(n.name, n.level, n.keys, n.join, n.shape) for n in nodes] == [
        ("source", -1, None, None, "[()]"),
        ("doubled", 0, None, None, "[]"),
        ("offset", 0, None, None, "[]"),
        ("combined", 1, ("id",), "left", "([])"),
    ]
    dependencies = {
        ("source", "doubled"),
        ("source", "offset"),
        ("doubled", "combined"),
        ("offset", "combined"),
    }
    assert {(n.name, child.name) for n in nodes for child in n.children} == dependencies
    assert {(parent.name, n.name) for n in nodes for parent in n.parents} == dependencies
    assert str(graph).splitlines() == [
        "graph TD",
        "    source[(source)]",
        "    doubled[doubled]",
        "    offset[offset]",
        "    combined([combined])",
        "    root_df[[root_df]]",
        "    source --> doubled",
        "    source --> offset",
        "    doubled --> combined",
        "    offset --> combined",
        "    combined -->|left<br>- id| root_df",
    ]


def test_report_contains_documentation_table_and_graph(source):
    report = report_datahub(BranchingHub, source)

    assert report.startswith("## BranchingHub\nCombine two features derived from one input.")
    assert "### BranchingHub detail" in report
    normalized = " ".join(report.split())
    assert "| -1 | source | input | | | |" in normalized
    assert "| 0 | offset | function | | | |" in normalized
    assert (
        "| 1 | combined | function | Join both features.<br><br>"
        "Add them to produce the score. | left | id |"
    ) in normalized
    assert "### Dataflow image\n```mermaid\ngraph TD" in report
    assert "doubled --> combined" in report


def test_image_export_passes_graph_and_destination_to_mmdc(source, tmp_path, monkeypatch):
    destination = tmp_path / "graph.svg"
    captured = []

    def run(command):
        executable, input_flag, input_path, output_flag, output_path = command
        captured.append(
            (executable, input_flag, Path(input_path).read_text(), output_flag, output_path)
        )

    monkeypatch.setattr("pytred.helpers.visualize.subprocess.run", run)
    graph, _ = make_dataflow_graph_from_datahub(
        BranchingHub, source, output=destination, direction="LR"
    )

    assert str(graph).startswith("graph LR\n")
    assert captured == [("mmdc", "-i", str(graph), "-o", str(destination))]
