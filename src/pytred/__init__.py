from logging import DEBUG, WARN, Formatter, StreamHandler, getLogger

from pytred.data_hub import DataHub
from pytred.data_node import DataNode

__version__ = "0.1.0"

logger = getLogger(__name__)
fmt = Formatter(
    "[%(levelname)s] %(name)s %(asctime)s - %(filename)s: %(lineno)d: %(message)s"
)
sh = StreamHandler()
sh.setLevel(DEBUG)
sh.setFormatter(fmt)
logger.addHandler(sh)
logger.setLevel(WARN)
