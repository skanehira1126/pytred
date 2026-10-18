from html import unescape
import json
import re

from IPython.core.formatters import DisplayFormatter
import polars as pl

from pytred import DataHub
from pytred import DataNode
from pytred.decorators import polars_table
from pytred.helpers.visualize import collect_dataflow_elements
from pytred.helpers.visualize import visualize_datahub

from ..fixtures.data_hub import BranchingHub
from ..fixtures.data_hub import OptionalHub


def test_graph_describes_dependencies_and_joins_without_execution():
    # Empty frames deliberately cannot execute BranchingHub's transformations.
    hub = BranchingHub(pl.DataFrame(), source=pl.DataFrame())
    elements = collect_dataflow_elements(hub)
    nodes = {e["data"]["id"]: e["data"] for e in elements if "source" not in e["data"]}
    edges = [e["data"] for e in elements if "source" in e["data"]]

    assert [(n["label"], n["kind"]) for n in nodes.values()] == [
        ("source", "input"),
        ("doubled", "process"),
        ("offset", "process"),
        ("combined", "process"),
        ("01  LEFT JOIN", "join-step"),
        ("root_df", "root"),
        ("Output", "output"),
    ]
    assert {
        (nodes[e["source"]]["label"], nodes[e["target"]]["label"], e["kind"]) for e in edges
    } == {
        ("source", "doubled", "dependency"),
        ("source", "offset", "dependency"),
        ("doubled", "combined", "dependency"),
        ("offset", "combined", "dependency"),
        ("combined", "01  LEFT JOIN", "join"),
        ("root_df", "01  LEFT JOIN", "base"),
        ("01  LEFT JOIN", "Output", "base"),
    }
    combined = next(n for n in nodes.values() if n["label"] == "combined")
    assert (combined["join"], combined["keys"], combined["order"]) == ("left", ["id"], 1)
    assert "Join both features." in combined["description"]
    assert combined["implementation"].startswith('@polars_table(1, "id", join="left")')
    assert "def combined(self, doubled, offset):" in combined["implementation"]
    assert 'return doubled.join(offset, on="id")' in combined["implementation"]
    assert "_wrapper" not in combined["implementation"]
    assert nodes["table-0"]["implementation"] == ""
    assert list(hub.tables) == ["source"]


def test_independent_steps_and_sparse_execution_order():
    class IndependentHub(DataHub):
        @polars_table(0)
        def first(self):
            raise AssertionError("Visualization must not execute steps")

        @polars_table(10, "id", join="left")
        def last(self):
            raise AssertionError("Visualization must not execute steps")

    elements = collect_dataflow_elements(IndependentHub(pl.DataFrame()))
    edges = [e["data"] for e in elements if "source" in e["data"]]
    assert {e["kind"] for e in edges} == {"join", "base"}
    assert [(e["source"], e["target"]) for e in edges if e["kind"] == "base"] == [
        ("root", "join-1"),
        ("join-1", "output"),
    ]


def test_input_joins_and_arbitrary_names_are_preserved():
    names = ["root_df", "output", 'end </script><b> & "quoted"']
    hub = DataHub(
        pl.DataFrame(),
        *[DataNode(pl.DataFrame(), keys=('a|b<"c',), join="left", name=name) for name in names],
    )
    elements = collect_dataflow_elements(hub)
    json.dumps(elements)
    assert len({e["data"]["id"] for e in elements}) == len(elements)
    assert [e["data"]["label"] for e in elements if e["data"]["kind"] == "input"] == names
    joins = [e["data"] for e in elements if e["data"]["kind"] == "join"]
    assert len(joins) == 3
    steps = [e["data"] for e in elements if e["data"]["kind"] == "join-step"]
    assert [(s["table"], s["sequence"]) for s in steps] == list(zip(names, [1, 2, 3]))
    assert all(s["join"] == "left" and s["keys"] == ['a|b<"c'] for s in steps)
    assert [
        (e["data"]["source"], e["data"]["target"]) for e in elements if e["data"]["kind"] == "base"
    ] == [("root", "join-1"), ("join-1", "join-2"), ("join-2", "join-3"), ("join-3", "output")]
    assert "</script>" not in hub._repr_html_()
    assert "&lt;/script&gt;" in hub._repr_html_()


def test_missing_optional_dependency_is_visible():
    elements = collect_dataflow_elements(OptionalHub(pl.DataFrame()))
    missing = [e["data"] for e in elements if e["data"]["kind"] == "missing"]
    assert len(missing) == 1
    assert missing[0]["label"] == "source"
    prepared = next(e["data"] for e in elements if e["data"].get("label") == "prepared")
    assert prepared["optional"] is True
    join = next(e["data"] for e in elements if e["data"].get("kind") == "join-step")
    assert join["optional"] is True
    assert join["table"] == "result"
    assert any(
        e["data"].get("source") == missing[0]["id"] and e["data"].get("target") == prepared["id"]
        for e in elements
    )


def test_input_join_precedes_generated_join():
    hub = BranchingHub(
        pl.DataFrame(),
        DataNode(pl.DataFrame(), keys=None, join="cross", name="attributes"),
        source=pl.DataFrame(),
    )
    elements = collect_dataflow_elements(hub)
    joins = [e["data"] for e in elements if e["data"]["kind"] == "join-step"]
    assert [(j["table"], j["join"], j["keys"]) for j in joins] == [
        ("attributes", "cross", []),
        ("combined", "left", ["id"]),
    ]


def snapshot_document(view):
    # 表示用iframeから内側のHTMLを取り出し、ブラウザへ渡す内容を検査する。
    return unescape(re.search(r'srcdoc="([^"]*)"', view._repr_html_()).group(1))


def snapshot_data(view):
    document = snapshot_document(view)
    return json.loads(re.search(r'id="pytred-data">(.*?)</script>', document).group(1))


def test_notebook_formatter_uses_self_contained_html_without_widget_protocol():
    hub = DataHub(pl.DataFrame(), source=pl.DataFrame())
    bundle, _ = DisplayFormatter().format(hub)
    assert "text/html" in bundle
    assert "application/vnd.jupyter.widget-view+json" not in bundle
    assert 'sandbox="allow-scripts"' in bundle["text/html"]
    document = snapshot_document(visualize_datahub(hub))
    assert "pytred-dataflow" in document
    assert "Permission is hereby granted" in document
    assert "<script src=" not in document
    assert snapshot_data(visualize_datahub(hub))["elements"] == collect_dataflow_elements(hub)


def test_html_snapshots_are_independent_and_refresh_only_when_created():
    hub = DataHub(pl.DataFrame(), source=pl.DataFrame())
    first = visualize_datahub(hub, direction="TB")
    hub.tables["source"].join = "left"
    hub.tables["source"].keys = ["id"]
    second = visualize_datahub(hub)
    assert snapshot_data(first)["direction"] == "TB"
    assert snapshot_data(second)["direction"] == "LR"
    assert not any(e["data"]["kind"] == "join-step" for e in snapshot_data(first)["elements"])
    assert snapshot_data(first)["elements"] != snapshot_data(second)["elements"]
    assert snapshot_data(second)["elements"] == collect_dataflow_elements(hub)
    # 同じhubを繰り返し表示しても、HTMLを返すだけでPython側に表示状態を追加しない。
    state = dict(vars(hub))
    for _ in range(3):
        assert hub._repr_html_() == second._repr_html_()
    assert vars(hub) == state


def test_pipeline_text_cannot_escape_json_script_or_iframe():
    name = '</script><script>window.injected=true</script><img src=x onerror="alert(1)">'
    hub = DataHub(pl.DataFrame(), DataNode(pl.DataFrame(), keys=None, join=None, name=name))
    view = visualize_datahub(hub)
    document = snapshot_document(view)
    assert name not in document
    assert name in [e["data"].get("label") for e in snapshot_data(view)["elements"]]
    # JSON用・実行用の2つ以外のscriptタグが生成されていない。
    assert document.count("</script>") == 2


def test_dynamic_function_without_source_keeps_description():
    namespace = {}
    exec(
        'def generated(self):\n    "A generated step."\n    raise AssertionError("not run")',
        namespace,
    )
    hub_type = type(
        "DynamicHub", (DataHub,), {"generated": polars_table(0)(namespace["generated"])}
    )
    elements = collect_dataflow_elements(hub_type(pl.DataFrame()))
    step = next(e["data"] for e in elements if e["data"].get("kind") == "process")
    assert step["implementation"] == ""
    assert step["description"] == "A generated step."


def test_implementation_is_embedded_as_text_without_executing():
    class SourceHub(DataHub):
        @polars_table(0)
        def payload(self):
            text = "</script><script>window.injected=true</script>"
            raise AssertionError(text)

    view = visualize_datahub(SourceHub(pl.DataFrame()))
    document = snapshot_document(view)
    step = next(
        e["data"] for e in snapshot_data(view)["elements"] if e["data"].get("kind") == "process"
    )
    assert "</script><script>window.injected=true</script>" in step["implementation"]
    assert document.count("</script>") == 2
