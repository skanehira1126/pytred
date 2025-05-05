from __future__ import annotations

from typing import Literal


META_KEYS = Literal["table_process_order", "join", "keys", "is_optional"]

POLARS_JOIN_METHOD = Literal["inner", "left", "right", "full", "semi", "anti", "cross"]
