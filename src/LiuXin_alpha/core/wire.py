"""
Convert supported Core results to the JSON-compatible value shapes shared by local and RPC calls.

Bytes, dates/times, and decimals use tagged dictionaries; paths and UUIDs become
text. This module encodes values, not JSON text, and provides no inverse decoder.
Its tags are ordinary mapping keys rather than a reserved namespace enforced on
caller-supplied dictionaries.
"""

from __future__ import annotations

import base64
import dataclasses
import datetime
import decimal
import enum
import math
import pathlib
import uuid
from collections.abc import Mapping, Sequence
from typing import Any


class CoreWireError(TypeError):
    """
    Reject an unsupported result value or a detected ambiguity during Core wire conversion.

    Conversion messages normally include the logical result path. This exception
    does not imply rollback of the endpoint operation that produced the value.

    Example:
        >>> isinstance(CoreWireError("unsupported result"), TypeError)
        True
    """


def to_wire(
    value: Any,
    *,
    _path: str = "$",
    max_bytes: int = 8 * 1024 * 1024,
    max_depth: int = 48,
    max_values: int = 100_000,
) -> Any:
    """Detach and encode a bounded result, rejecting cycles and ambiguous keys.

    Limits apply equally to direct and HTTP dispatch. Shared, acyclic subtrees are
    allowed. Dataclass fields are visited without ``asdict`` so recursive values
    cannot escape the depth/cycle guard. Byte limits use strict UTF-8 JSON.
    """
    import json

    active: set[int] = set()
    remaining = max_values

    def visit(item: Any, path: str, depth: int) -> Any:
        nonlocal remaining
        remaining -= 1
        if depth > max_depth or remaining < 0:
            raise CoreWireError(f"Core result at {path} exceeds the structural budget.")
        if item is None or isinstance(item, (bool, int)):
            return item
        if isinstance(item, str):
            if len(item) > max_bytes:
                raise CoreWireError(f"Core result at {path} exceeds the byte budget.")
            return item
        if isinstance(item, float):
            if not math.isfinite(item):
                raise CoreWireError(
                    f"Core result at {path} contains a non-finite float."
                )
            return item
        if isinstance(item, enum.Enum):
            return visit(item.value, path, depth + 1)
        if isinstance(item, bytes):
            if len(item) > max_bytes * 3 // 4:
                raise CoreWireError(f"Core result at {path} exceeds the byte budget.")
            return {"$type": "bytes", "base64": base64.b64encode(item).decode("ascii")}
        if isinstance(item, (datetime.datetime, datetime.date, datetime.time)):
            return {"$type": type(item).__name__, "iso": item.isoformat()}
        if isinstance(item, decimal.Decimal):
            if not item.is_finite():
                raise CoreWireError(
                    f"Core result at {path} contains a non-finite decimal."
                )
            return {"$type": "decimal", "value": str(item)}
        if isinstance(item, (pathlib.Path, uuid.UUID)):
            return visit(str(item), path, depth + 1)
        identity = id(item)
        if identity in active:
            raise CoreWireError(f"Core result at {path} contains a cycle.")
        active.add(identity)
        try:
            if dataclasses.is_dataclass(item) and not isinstance(item, type):
                pairs = (
                    (field.name, getattr(item, field.name))
                    for field in dataclasses.fields(item)
                )
            elif isinstance(item, Mapping):
                pairs = item.items()
            elif isinstance(getattr(item, "row_dict", None), Mapping):
                return visit(item.row_dict, path, depth + 1)
            elif isinstance(item, (set, frozenset)):
                if len(item) > remaining:
                    raise CoreWireError(
                        f"Core result at {path} exceeds the structural budget."
                    )
                return [
                    visit(child, f"{path}[]", depth + 1)
                    for child in sorted(item, key=repr)
                ]
            elif isinstance(item, Sequence) and not isinstance(
                item, (str, bytes, bytearray)
            ):
                return [
                    visit(child, f"{path}[{index}]", depth + 1)
                    for index, child in enumerate(item)
                ]
            elif callable(getattr(item, "keys", None)) and callable(
                getattr(item, "__getitem__", None)
            ):
                pairs = ((key, item[key]) for key in item.keys())
            else:
                raise CoreWireError(
                    f"Core result at {path} is not transport-safe: {type(item).__name__}"
                )
            converted: dict[str, Any] = {}
            for key, child in pairs:
                wire_key = str(key)
                if wire_key in converted:
                    raise CoreWireError(
                        f"Core result at {path} has colliding mapping key {wire_key!r}."
                    )
                if len(wire_key) > max_bytes:
                    raise CoreWireError(
                        f"Core result at {path} exceeds the byte budget."
                    )
                converted[wire_key] = visit(child, f"{path}.{wire_key}", depth + 1)
            return converted
        finally:
            active.remove(identity)

    result = visit(value, _path, 0)
    size = 0
    try:
        for chunk in json.JSONEncoder(ensure_ascii=True, allow_nan=False).iterencode(
            result
        ):
            size += len(chunk)
            if size > max_bytes:
                raise CoreWireError(f"Core result at {_path} exceeds the byte budget.")
    except (ValueError, OverflowError) as exc:
        raise CoreWireError(f"Core result at {_path} cannot be encoded: {exc}") from exc
    return result


__all__ = ["CoreWireError", "to_wire"]
