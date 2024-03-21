from logging import INFO, getLogger

import polars as pl
import pytest

from .fixtures.data_hub import BasicDataHub, ComplecatedDataHub


@pytest.fixture(scope="class", autouse=True)
def set_logging_level():
    logger = getLogger("pytred")
    logger.setLevel(INFO)
    logger.error("Called set_loging_level")


@pytest.fixture(scope="function")
def basic_data_hub():
    return BasicDataHub()


@pytest.fixture(scope="function")
def complecated_data_hub():
    return ComplecatedDataHub(
        root_df=pl.DataFrame(),
        input_table=pl.DataFrame(),
    )
