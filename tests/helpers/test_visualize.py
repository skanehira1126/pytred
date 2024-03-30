import tempfile

from pytred.helpers.visualize import make_dataflow_graph_from_datahub, report_datahub


def test__execution_test_make_dataflow_graph_from_datahub(complecated_datahub):

    with tempfile.TemporaryFile(mode="w") as f:
        make_dataflow_graph_from_datahub(complecated_datahub, output=str(f))


def test__make_report(complecated_datahub, expected_report_of_complecated_datahub):
    actual = report_datahub(complecated_datahub)
    assert actual == expected_report_of_complecated_datahub
