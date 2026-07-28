"""Canonical JSON-safe values for the transport-neutral Core API."""

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
    """Raised when a stable Core endpoint returns a non-transport value."""


def to_wire(value: Any, *, _path: str = "$") -> Any:
    """Convert a value to the canonical JSON-safe Core wire representation.

    Stable Core handlers call this before returning, including for local calls.
    That keeps in-process and RPC results identical instead of allowing local
    callers to accidentally depend on database rows, cache records, or other
    process-owned objects.
    """

    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise CoreWireError(
                "Core result at {} contains a non-finite float.".format(
                    _path
                )
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
                {
                    str(key): get_item(key)
                    for key in raw_keys
                },
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
