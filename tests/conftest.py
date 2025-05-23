from logging import INFO
from logging import getLogger
import pathlib

import polars as pl
import pytest

from pytred.data_node import DataNode
from pytred.data_node import EmptyDataNode

from .fixtures.data_hub import BasicDataHub
from .fixtures.data_hub import ComplecatedDataHub


@pytest.fixture(scope="class", autouse=True)
def set_logging_level():
    logger = getLogger("pytred")
    logger.setLevel(INFO)
    logger.error("Called set_loging_level")


@pytest.fixture(scope="function")
def basic_datahub():
    return BasicDataHub()


@pytest.fixture(scope="function")
def complecated_datahub():
    return ComplecatedDataHub(
        pl.DataFrame(),
        DataNode(pl.DataFrame(), keys=("id",), join="left", name="input_table1"),
        input_table2=pl.DataFrame(),
    )


@pytest.fixture()
def expected_report_of_complecated_datahub():
    path = pathlib.Path(__file__).parent / "fixtures/expected_report_of_complecated_datahub.md"
    with path.open("r") as f:
        return f.read()


@pytest.fixture(scope="function")
def inputs_visualize_test():
    return (
        ComplecatedDataHub,
        [
            EmptyDataNode(
                name="input_table1",
                keys=[
                    "id",
                ],
                join="left",
            ),
            EmptyDataNode(name="input_table2", keys=None, join=None),
        ],
    )
