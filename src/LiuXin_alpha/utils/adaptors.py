from __future__ import annotations

from typing import Any, Optional


def _boolish_to_bool(val: Any) -> Optional[bool]:
    """
    Convert typical DB "bool-ish" values to Python bool.

    Accepts: None, bool, 0/1 ints, and '0'/'1' strings.
    Anything else is returned unchanged as None (to avoid surprising coercions).
    """
    if val is None:
        return None
    if isinstance(val, bool):
        return val
    if isinstance(val, int):
        if val == 0:
            return False
        if val == 1:
            return True
        return None
    if isinstance(val, str):
        if val == "0":
            return False
        if val == "1":
            return True
        return None
    return None


def _bool_to_int_or_none(val: Optional[bool]) -> Optional[int]:
    if val is None:
        return None
    return 1 if val else 0
