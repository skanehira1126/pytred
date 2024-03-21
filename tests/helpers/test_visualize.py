import polars as pl

from pytred.helpers.visualize import make_dataflow_graph_from_datahub


def test__execution_test_make_dataflow_graph_from_datahub(complecated_data_hub):

    make_dataflow_graph_from_datahub(complecated_data_hub)
