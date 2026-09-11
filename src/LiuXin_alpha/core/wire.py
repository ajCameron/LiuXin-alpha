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

from collections.abc import Iterable, Mapping, Sequence
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


def to_wire(value: Any, *, _path: str = "$") -> Any:
    """
    Recursively encode supported values and reject unsupported leaves and nonfinite floats.

    Stable Core handlers call this before returning, including for local calls.
    That keeps in-process and RPC results identical instead of allowing local
    callers to accidentally depend on returned database rows or cache records.
    Primitive subclasses pass through; other enums encode their values. Bytes
    use base64, temporal objects use ISO text, and decimals retain their string
    spelling without a separate finiteness check. Dataclasses use ``asdict``;
    mapping-valued ``row_dict`` attributes are then preferred over mapping access.

    Mapping keys are stringified and checked for collisions. The later duck-typed
    ``keys``/``__getitem__`` fallback builds a dictionary first, so its colliding
    keys can already have collapsed. Sets are sorted by ``repr``, not a guarantee
    of cross-process order for custom objects. Sequences become lists; arbitrary
    iterators and bytearrays are not accepted as sequences. There is no cycle or
    depth guard, and some attribute/dataclass errors propagate without wrapping.

    Example:
        >>> to_wire({"payload": b"hi", "values": (1, 2)})
        {'payload': {'$type': 'bytes', 'base64': 'aGk='}, 'values': [1, 2]}
        >>> to_wire({"score": float("nan")})
        Traceback (most recent call last):
        ...
        LiuXin_alpha.core.wire.CoreWireError: Core result at $.score contains a non-finite float.


    :param value: Result tree whose supported leaves and containers are converted recursively.
    :param _path: Diagnostic location of the current value; recursive calls extend this label.
    :return: JSON-compatible scalar, list, or dictionary representation of supported input.
    :raises CoreWireError: For nonfinite floats, detected mapping collisions, unsupported values, or failed row-like materialization.
    """

    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CoreWireError(
                "Core result at {} contains a non-finite float.".format(_path)
            )
        return value
    if isinstance(value, enum.Enum):
        return to_wire(value.value, _path=_path)
    if isinstance(value, bytes):
        return {
            "$type": "bytes",
            "base64": base64.b64encode(value).decode("ascii"),
        }
    if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
        return {
            "$type": type(value).__name__,
            "iso": value.isoformat(),
        }
    if isinstance(value, decimal.Decimal):
        return {
            "$type": "decimal",
            "value": str(value),
        }
    if isinstance(value, (pathlib.Path, uuid.UUID)):
        return str(value)
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return to_wire(dataclasses.asdict(value), _path=_path)

    row_dict = getattr(value, "row_dict", None)
    if isinstance(row_dict, Mapping):
        return to_wire(dict(row_dict), _path=_path)

    if isinstance(value, Mapping):
        converted: dict[str, Any] = {}
        for key, item in value.items():
            wire_key = str(key)
            if wire_key in converted:
                raise CoreWireError(
                    "Core result at {} has colliding mapping key {!r}.".format(
                        _path,
                        wire_key,
                    )
                )
            converted[wire_key] = to_wire(
                item,
                _path="{}.{}".format(_path, wire_key),
            )
        return converted
    if isinstance(value, (set, frozenset)):
        return [
            to_wire(item, _path="{}[]".format(_path))
            for item in sorted(value, key=repr)
        ]
    if isinstance(value, Sequence) and not isinstance(
        value,
        (str, bytes, bytearray),
    ):
        return [
            to_wire(item, _path="{}[{}]".format(_path, index))
            for index, item in enumerate(value)
        ]

    keys = getattr(value, "keys", None)
    get_item = getattr(value, "__getitem__", None)
    if callable(keys) and callable(get_item):
        try:
            raw_keys = keys()
            if not isinstance(raw_keys, Iterable):
                raise TypeError("keys() did not return an iterable")
            return to_wire(
                {str(key): get_item(key) for key in raw_keys},
                _path=_path,
            )
        except Exception as exc:
            raise CoreWireError(
                "Core result at {} exposes keys but cannot be materialized: {}".format(
                    _path,
                    exc,
                )
            ) from exc

    raise CoreWireError(
        "Core result at {} is not transport-safe: {}".format(
            _path,
            type(value).__name__,
        )
    )


__all__ = [
    "CoreWireError",
    "to_wire",
]
