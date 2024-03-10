from logging import INFO, getLogger

import pytest


@pytest.fixture(scope="class", autouse=True)
def set_logging_level():
    logger = getLogger("pytred")
    logger.setLevel(INFO)
    logger.error("Called set_lloging_level")
