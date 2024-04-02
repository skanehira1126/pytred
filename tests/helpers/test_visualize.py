import tempfile

from pytred.helpers.visualize import make_dataflow_graph_from_datahub, report_datahub


def test__execution_test_make_dataflow_graph_from_datahub(inputs_visualize_test):

    class_datahub, table = inputs_visualize_test
    with tempfile.TemporaryFile(mode="w") as f:
        make_dataflow_graph_from_datahub(class_datahub, *table, output=str(f))


def test__make_report(inputs_visualize_test, expected_report_of_complecated_datahub):
    class_datahub, table = inputs_visualize_test
    actual = report_datahub(class_datahub, *table)
    assert actual == expected_report_of_complecated_datahub
