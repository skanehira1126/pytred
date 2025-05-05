from typing import Callable
from typing import Literal


META_KEYS = Literal["table_process_order", "join", "keys"]


def get_metadata(target: Callable, key: META_KEYS):
    """
    Retrieve the value associated with the given key from the __pytred_meta__ metadata
    attached to the target function or class.

    Parameters
    ----------
    target : Callable
        The function or class object decorated with @polars_table, which must have a
        __pytred_meta__ attribute.
    key : str
        The metadata key whose value should be returned.

    Returns
    -------
    Any
        The metadata value corresponding to the specified key.

    Raises
    ------
    AttributeError
        If the target object does not have a __pytred_meta__ attribute.
    KeyError
        If the specified key is not found within the __pytred_meta__ dictionary.
    """
    meta = getattr(target, "__pytred_meta__", None)
    if meta is None:
        raise AttributeError(f"Object {target!r} does not contain __pytred_meta__.")
    try:
        return meta[key]
    except KeyError as err:
        raise KeyError(f"Key '{key}' not found in __pytred_meta__.") from err
