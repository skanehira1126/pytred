import json

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
        ("combined", "Output", "join"),
        ("root_df", "Output", "root"),
    }
    combined = next(n for n in nodes.values() if n["label"] == "combined")
    assert (combined["join"], combined["keys"], combined["order"]) == ("left", ["id"], 1)
    assert "Join both features." in combined["description"]
    assert list(hub.tables) == ["source"]


def test_independent_steps_and_sparse_execution_order():
    class IndependentHub(DataHub):
        @polars_table(0)
        def first(self):
            raise AssertionError("Visualization must not execute steps")

        @polars_table(10, "id", join="left")
        def last(self):
            raise AssertionError("Visualization must not execute steps")

    widget = visualize_datahub(IndependentHub(pl.DataFrame()))
    try:
        edges = [e["data"] for e in widget.elements if "source" in e["data"]]
        assert {e["kind"] for e in edges} == {"join", "root"}
        assert all(e["target"] == "output" for e in edges)
    finally:
        widget.close()


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
    assert all(e["label"] == 'left: a|b<"c' for e in joins)
    assert "</script>" not in hub._repr_html_()
    assert "&lt;/script&gt;" in hub._repr_html_()


def test_missing_optional_dependency_is_visible():
    elements = collect_dataflow_elements(OptionalHub(pl.DataFrame()))
    missing = [e["data"] for e in elements if e["data"]["kind"] == "missing"]
    assert len(missing) == 1
    assert missing[0]["label"] == "source"
    prepared = next(e["data"] for e in elements if e["data"].get("label") == "prepared")
    assert prepared["optional"] is True
    assert any(
        e["data"].get("source") == missing[0]["id"] and e["data"].get("target") == prepared["id"]
        for e in elements
    )


def test_notebook_formatter_uses_widget_mime_and_bundled_assets():
    import ipywidgets

    hub = DataHub(pl.DataFrame(), source=pl.DataFrame())
    bundle, _ = DisplayFormatter().format(hub)
    view = bundle["application/vnd.jupyter.widget-view+json"]
    # Inspect the actual comm state referenced by the displayed MIME bundle.
    state = ipywidgets.Widget.get_manager_state()["state"][view["model_id"]]["state"]
    assert state["elements"] == collect_dataflow_elements(hub)
    assert "export" in state["_esm"]
    assert "pytred-dataflow" in state["_css"]
    assert bundle["text/plain"] == "DataflowWidget(3 nodes)"
    hub.close_visualization()


def test_repeated_display_reuses_model_and_refreshes_graph():
    import ipywidgets

    hub = DataHub(pl.DataFrame(), source=pl.DataFrame())
    formatter = DisplayFormatter()
    try:
        first, _ = formatter.format(hub)
        model_id = first["application/vnd.jupyter.widget-view+json"]["model_id"]
        model_ids = set(ipywidgets.Widget.get_manager_state()["state"])
        # 同じhubの結合条件を変え、再表示で既存モデルの内容が更新されるか確認する。
        hub.tables["source"].join = "left"
        hub.tables["source"].keys = ["id"]
        for _ in range(3):
            bundle, _ = formatter.format(hub)
            assert bundle["application/vnd.jupyter.widget-view+json"]["model_id"] == model_id
        state = ipywidgets.Widget.get_manager_state()["state"]
        assert set(state) == model_ids
        assert state[model_id]["state"]["elements"] == collect_dataflow_elements(hub)
        assert any(
            e["data"].get("label") == "left: id" for e in state[model_id]["state"]["elements"]
        )
    finally:
        hub.close_visualization()


def test_closing_hub_visualization_releases_model_and_allows_redisplay():
    import ipywidgets

    hub = DataHub(pl.DataFrame(), source=pl.DataFrame())
    other = DataHub(pl.DataFrame(), source=pl.DataFrame())
    formatter = DisplayFormatter()
    try:
        first, _ = formatter.format(hub)
        second, _ = formatter.format(other)
        model_id = first["application/vnd.jupyter.widget-view+json"]["model_id"]
        other_id = second["application/vnd.jupyter.widget-view+json"]["model_id"]
        assert model_id != other_id
        hub.close_visualization()
        hub.close_visualization()
        state = ipywidgets.Widget.get_manager_state()["state"]
        assert model_id not in state
        assert other_id in state
        redisplayed, _ = formatter.format(hub)
        assert redisplayed["application/vnd.jupyter.widget-view+json"]["model_id"] != model_id
    finally:
        hub.close_visualization()
        other.close_visualization()


def test_widget_instances_have_independent_state():
    first = visualize_datahub(DataHub(pl.DataFrame(), source=pl.DataFrame()), direction="LR")
    second = visualize_datahub(DataHub(pl.DataFrame(), other=pl.DataFrame()))
    try:
        assert first.model_id != second.model_id
        assert first.direction == "LR"
        assert second.direction == "TB"
        assert first.elements != second.elements
    finally:
        first.close()
        second.close()
