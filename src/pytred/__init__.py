__all__ = [
    "DataHub",
    "DataNode",
]

from logging import DEBUG
from logging import WARN
from logging import Formatter
from logging import StreamHandler
from logging import getLogger

from pytred.data_hub import DataHub
from pytred.data_node import DataNode


__version__ = "0.3.1"

logger = getLogger(__name__)
fmt = Formatter("[%(levelname)s] %(name)s %(asctime)s - %(filename)s: %(lineno)d: %(message)s")
sh = StreamHandler()
sh.setLevel(DEBUG)
sh.setFormatter(fmt)
logger.addHandler(sh)
logger.setLevel(WARN)
