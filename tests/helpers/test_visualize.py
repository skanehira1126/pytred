from html import unescape
import json
import re
from typing import Annotated

from IPython.core.formatters import DisplayFormatter
import polars as pl
import pytest

from pytred import DataHub
from pytred import DataNode
from pytred._types import Input
from pytred._types import Root
from pytred.decorators import polars_table
from pytred.decorators.polars import prepare_input
from pytred.decorators.polars import validate_raw_inputs
from pytred.helpers.visualize import collect_dataflow_elements
from pytred.helpers.visualize import visualize_datahub
from pytred.metadata import TableMetadata

from ..fixtures.data_hub import BranchingHub
from ..fixtures.data_hub import OptionalHub
from ..fixtures.data_hub import RowsHub


@pytest.mark.parametrize("executed", [False, True], ids=["definition", "after_run"])
def test_hooks_show_raw_validators_and_route_prepared_tables(executed):
    calls = []

    class Hub(DataHub):
        orders: Annotated[pl.DataFrame, Root(metadata=None)]
        prices: Annotated[pl.DataFrame, Input(metadata=None)]

        @classmethod
        @validate_raw_inputs
        def check_ids(cls, orders, *, prices):
            calls.append("check_ids")

        @validate_raw_inputs
        def check_prices(self, prices):
            calls.append("check_prices")

        @prepare_input("orders")
        def select_orders(self, orders, prices):
            calls.append("select_orders")
            return orders.join(prices, on="id", how="semi")

        @classmethod
        @prepare_input("prices")
        def clean_prices(cls, prices):
            calls.append("clean_prices")
            return prices.drop_nulls()

        @polars_table(0)
        def score(self, orders, prices):
            calls.append("score")
            return orders.join(prices, on="id")

    hub = Hub()
    if executed:
        frame = pl.DataFrame({"id": [1]})
        hub.execute(DataNode(frame, ("id",), "left", "prices"), orders=frame)
    calls.clear()
    elements = collect_dataflow_elements(hub)
    assert not calls
    nodes = {e["data"]["id"]: e["data"] for e in elements if "source" not in e["data"]}
    by_label = {(n["kind"], n["label"]): n for n in nodes.values()}
    raw = by_label["root", "orders"]
    prices = by_label["input", "prices"]
    check_ids = {"name": "check_ids", "inputs": ["orders", "prices"]}
    assert raw["validators"] == [check_ids]
    assert prices["validators"] == [check_ids, {"name": "check_prices", "inputs": ["prices"]}]
    assert all(not n.get("validators") for n in nodes.values() if n not in (raw, prices))
    assert "def clean_prices(" in by_label["prepare", "clean_prices"]["implementation"]

    labels = {key: (n["kind"], n["label"]) for key, n in nodes.items()}

    def connections(kind):
        return {
            (labels[e["source"]], labels[e["target"]])
            for e in (item["data"] for item in elements)
            if e["kind"] == kind
        }

    assert connections("dependency") == {
        (("root", "orders"), ("prepare", "select_orders")),
        (("input", "prices"), ("prepare", "select_orders")),
        (("input", "prices"), ("prepare", "clean_prices")),
        (("prepare", "select_orders"), ("prepared", "orders")),
        (("prepare", "clean_prices"), ("prepared", "prices")),
        (("prepared", "orders"), ("process", "score")),
        (("prepared", "prices"), ("process", "score")),
    }
    if executed:
        step = by_label["join-step", "01  LEFT JOIN"]
        assert step["keys"] == ["id"]
        assert connections("join") == {(("prepared", "prices"), ("join-step", step["label"]))}
        assert (("prepared", "orders"), ("join-step", step["label"])) in connections("base")
    else:
        assert connections("base") == {(("prepared", "orders"), ("output", "Output"))}


def test_input_node_displays_declared_metadata():
    declared = TableMetadata(description="Declared source", primary_key=("id",))
    upstream = TableMetadata(description="Upstream source")

    class Hub(DataHub):
        rows: Annotated[pl.DataFrame, Root(metadata=None)]
        source: Annotated[pl.DataFrame, Input(metadata=declared)]

    frame = pl.DataFrame({"id": [1]})
    node = DataNode(frame, keys=("id",), join="left", name="source", metadata=upstream)
    hub = Hub()
    hub.execute(node, rows=frame)
    elements = snapshot_data(snapshot_document(visualize_datahub(hub)._repr_html_()))["elements"]
    source = next(e["data"] for e in elements if e["data"].get("label") == "source")
    assert source["metadata"] == declared.model_dump(mode="json")
    assert source["description"] == "Declared source"
    assert source["join"] == "left"


def test_graph_describes_dependencies_and_joins():
    hub = BranchingHub()
    hub.execute(rows=pl.DataFrame({"id": [1]}), source=pl.DataFrame({"id": [1], "value": [2]}))
    elements = collect_dataflow_elements(hub)
    nodes = {e["data"]["id"]: e["data"] for e in elements if "source" not in e["data"]}
    edges = [e["data"] for e in elements if "source" in e["data"]]
    assert {
        (nodes[e["source"]]["label"], nodes[e["target"]]["label"])
        for e in edges
        if e["kind"] == "dependency"
    } == {
        ("source", "doubled"),
        ("source", "offset"),
        ("doubled", "combined"),
        ("offset", "combined"),
    }
    assert [
        (nodes[e["source"]]["kind"], nodes[e["target"]]["kind"])
        for e in edges
        if e["kind"] == "base"
    ] == [("root", "join-step"), ("join-step", "output")]
    assert [
        (nodes[e["source"]]["label"], nodes[e["target"]]["table"])
        for e in edges
        if e["kind"] == "join"
    ] == [("combined", "combined")]
    combined = next(n for n in nodes.values() if n["label"] == "combined")
    assert (combined["join"], combined["keys"], combined["order"]) == ("left", ["id"], 1)
    assert "Join both features." in combined["description"]
    assert combined["implementation"].startswith('@polars_table(1, "id", join="left")')
    assert "def combined(self, doubled, offset):" in combined["implementation"]
    assert 'return doubled.join(offset, on="id")' in combined["implementation"]
    assert "_wrapper" not in combined["implementation"]


def test_independent_steps_and_sparse_execution_order():
    class IndependentHub(DataHub):
        @polars_table(0)
        def first(self):
            raise AssertionError("Visualization must not execute steps")

        @polars_table(10, "id", join="left")
        def last(self):
            raise AssertionError("Visualization must not execute steps")

    elements = collect_dataflow_elements(IndependentHub())
    nodes = {e["data"]["id"]: e["data"] for e in elements if "source" not in e["data"]}
    edges = [e["data"] for e in elements if "source" in e["data"]]
    assert {e["kind"] for e in edges} == {"join", "base"}
    assert {n["label"]: n["order"] for n in nodes.values() if n["kind"] == "process"} == {
        "first": 0,
        "last": 10,
    }
    assert {
        (nodes[e["source"]]["kind"], nodes[e["target"]]["kind"])
        for e in edges
        if e["kind"] == "base"
    } == {("root", "join-step"), ("join-step", "output")}


def test_input_joins_and_arbitrary_names_are_preserved():
    names = ["root_df", "output", 'end </script><b> & "quoted"']
    hub = RowsHub()
    key = 'a|b<"c'
    frame = pl.DataFrame({key: [1]})
    hub.execute(
        *[DataNode(frame, keys=(key,), join="left", name=name) for name in names], rows=frame
    )
    elements = collect_dataflow_elements(hub)
    json.dumps(elements)
    assert len({e["data"]["id"] for e in elements}) == len(elements)
    assert [
        e["data"]["label"]
        for e in elements
        if e["data"]["kind"] == "input" and e["data"]["label"] in names
    ] == names
    by_id = {e["data"]["id"]: e["data"] for e in elements}
    joins = [e["data"] for e in elements if e["data"]["kind"] == "join"]
    assert {(by_id[e["source"]]["label"], by_id[e["target"]]["table"]) for e in joins} == {
        (name, name) for name in names
    }
    steps = [e["data"] for e in elements if e["data"]["kind"] == "join-step"]
    assert [(s["sequence"], s["table"]) for s in steps] == list(enumerate(names, start=1))
    assert all(s["join"] == "left" and s["keys"] == ['a|b<"c'] for s in steps)
    root = next(node for node in by_id.values() if node["kind"] == "root")
    output = next(node for node in by_id.values() if node["kind"] == "output")
    path = [node["id"] for node in (root, *steps, output)]
    assert {(e["source"], e["target"]) for e in by_id.values() if e["kind"] == "base"} == set(
        zip(path[:-1], path[1:], strict=True)
    )


def test_missing_optional_dependency_is_visible():
    elements = collect_dataflow_elements(OptionalHub())
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
    hub = BranchingHub()
    hub.execute(
        DataNode(pl.DataFrame({"tag": ["A"]}), keys=None, join="cross", name="attributes"),
        rows=pl.DataFrame({"id": [1]}),
        source=pl.DataFrame({"id": [1], "value": [2]}),
    )
    elements = collect_dataflow_elements(hub)
    joins = [e["data"] for e in elements if e["data"]["kind"] == "join-step"]
    assert [(j["table"], j["join"], j["keys"]) for j in joins] == [
        ("attributes", "cross", []),
        ("combined", "left", ["id"]),
    ]


def snapshot_document(html):
    # 表示用iframeから内側のHTMLを取り出し、ブラウザへ渡す内容を検査する。
    return unescape(re.search(r'srcdoc="([^"]*)"', html).group(1))


def snapshot_data(document):
    return json.loads(re.search(r'id="pytred-data">(.*?)</script>', document).group(1))


def test_notebook_formatter_produces_self_contained_html():
    hub = RowsHub()
    bundle, _ = DisplayFormatter().format(hub)
    assert "text/html" in bundle
    assert 'sandbox="allow-scripts"' in bundle["text/html"]
    document = snapshot_document(bundle["text/html"])
    assert "pytred-dataflow" in document
    assert "Permission is hereby granted" in document
    assert "<script src=" not in document
    assert snapshot_data(document)["elements"] == collect_dataflow_elements(hub)


def test_html_snapshots_are_independent_and_refresh_only_when_created():
    hub = RowsHub()
    first = visualize_datahub(hub, direction="TB")
    frame = pl.DataFrame({"id": [1]})
    hub.execute(DataNode(frame, keys=("id",), join="left", name="source"), rows=frame)
    before = snapshot_data(snapshot_document(first._repr_html_()))
    after = snapshot_data(snapshot_document(visualize_datahub(hub)._repr_html_()))
    assert before["direction"] == "TB"
    assert after["direction"] == "LR"
    assert not any(e["data"]["kind"] == "join-step" for e in before["elements"])
    assert [e["data"]["table"] for e in after["elements"] if e["data"]["kind"] == "join-step"] == [
        "source"
    ]


def test_invalid_layout_direction_is_rejected():
    with pytest.raises(ValueError, match="direction"):
        visualize_datahub(RowsHub(), direction="diagonal")


def test_pipeline_text_cannot_escape_json_script_or_iframe():
    name = '</script><script>window.injected=true</script><img src=x onerror="alert(1)">'
    hub = RowsHub()
    hub.execute(rows=pl.DataFrame(), **{name: pl.DataFrame()})
    view = visualize_datahub(hub)
    html = view._repr_html_()
    assert name not in html
    assert "</script>" not in html
    document = snapshot_document(html)
    assert name not in document
    assert name in [e["data"].get("label") for e in snapshot_data(document)["elements"]]
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
    elements = collect_dataflow_elements(hub_type())
    step = next(e["data"] for e in elements if e["data"].get("kind") == "process")
    assert step["implementation"] == ""
    assert step["description"] == "A generated step."


def test_implementation_is_embedded_as_text_without_executing():
    class SourceHub(DataHub):
        @polars_table(0)
        def payload(self):
            text = "</script><script>window.injected=true</script>"
            raise AssertionError(text)

    view = visualize_datahub(SourceHub())
    document = snapshot_document(view._repr_html_())
    step = next(
        e["data"]
        for e in snapshot_data(document)["elements"]
        if e["data"].get("kind") == "process"
    )
    assert "</script><script>window.injected=true</script>" in step["implementation"]
    assert document.count("</script>") == 2
