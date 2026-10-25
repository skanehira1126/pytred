from __future__ import annotations

from typing import Literal


META_KEYS = Literal["table_process_order", "join", "keys", "is_optional", "metadata"]

PolarsJoinMethod = Literal["inner", "left", "right", "full", "semi", "anti", "cross"]
