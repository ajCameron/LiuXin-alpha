"""Encode the typed JSON values stored by the durable storage repository.

The codec is deliberately separate from database row access: it knows the stable
wire names and supported Python shapes, but it does not open transactions, query
tables, invalidate caches, or decide which records belong in an envelope.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable, Mapping
from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from LiuXin_alpha.storage.api import models as storage_models
from LiuXin_alpha.storage.api import (
    placement_hints_api,
    storage_manager_api,
    store_api,
    store_driver_api,
    workflow_api,
)
from LiuXin_alpha.storage.storage_manager.mixins._types import (
    _AdoptIngestRequest,
    _IdentifiedStreamIngestRequest,
    _IngestOperation,
    _StoreObjectIngestRequest,
    _StreamIngestRequest,
)

_INGEST_JOURNAL_TYPE_NAMES: dict[type[Any], str] = {
    _AdoptIngestRequest: "LiuXin_alpha.storage.storage_manager.manager._AdoptIngestRequest",
    _IdentifiedStreamIngestRequest: "LiuXin_alpha.storage.storage_manager.manager._IdentifiedStreamIngestRequest",
    _IngestOperation: "LiuXin_alpha.storage.storage_manager.manager._IngestOperation",
    _StoreObjectIngestRequest: "LiuXin_alpha.storage.storage_manager.manager._StoreObjectIngestRequest",
    _StreamIngestRequest: "LiuXin_alpha.storage.storage_manager.manager._StreamIngestRequest",
}

_PUBLIC_VALUE_MODULES = (
    storage_models,
    placement_hints_api,
    storage_manager_api,
    store_api,
    store_driver_api,
    workflow_api,
)


def _storage_value_types(additional: Iterable[type[Any]]) -> dict[str, type[Any]]:
    """Build the explicit decoder registry from API values and additional private types.

    Example:
        >>> values = _storage_value_types(())
        >>> values[_type_name(storage_models.Digest)] is storage_models.Digest
        True

    :param additional: Extra constructor types permitted during decoding.
    :return: Stable wire-name to constructor mapping.
    """

    values: set[type[Any]] = set(additional)
    for module in _PUBLIC_VALUE_MODULES:
        for name in module.__all__:
            value = getattr(module, name)
            if isinstance(value, type) and (
                dataclasses.is_dataclass(value) or issubclass(value, Enum)
            ):
                values.add(value)
    return {_type_name(value): value for value in values}


def _type_name(value: type[Any]) -> str:
    """Return a stable journal tag or the type's module-qualified name.

    Example:
        >>> _type_name(int)
        'builtins.int'

    :param value: Type to identify in a storage envelope.
    :return: Stable wire name.
    """

    return _INGEST_JOURNAL_TYPE_NAMES.get(
        value, f"{value.__module__}.{value.__qualname__}"
    )


def _encode(value: Any) -> Any:
    """Convert one supported value into tagged JSON-compatible structures.

    Example:
        >>> _encode((1, "book"))
        {'$tuple': [1, 'book']}

    :param value: Supported storage value or recursively composed container.
    :return: Tagged containers or JSON primitive values.
    """

    if isinstance(value, Enum):
        return {"$enum": _type_name(type(value)), "value": value.value}
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            "$dataclass": _type_name(type(value)),
            "fields": {
                field.name: _encode(getattr(value, field.name))
                for field in dataclasses.fields(value)
            },
        }
    if isinstance(value, UUID):
        return {"$uuid": str(value)}
    if isinstance(value, datetime):
        return {"$datetime": value.isoformat()}
    if isinstance(value, tuple):
        return {"$tuple": [_encode(item) for item in value]}
    if isinstance(value, frozenset):
        return {"$frozenset": [_encode(item) for item in sorted(value, key=str)]}
    if isinstance(value, list):
        return [_encode(item) for item in value]
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            return {
                "$mapping": [
                    [_encode(key), _encode(item)] for key, item in value.items()
                ]
            }
        return {key: _encode(item) for key, item in value.items()}
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported durable storage value: {type(value).__name__}")


def _decode(value: Any, types: Mapping[str, type[Any]]) -> Any:
    """Reconstruct tagged values using only constructors in the supplied registry.

    Example:
        >>> _decode({'$tuple': [1, "book"]}, {})
        (1, 'book')

    :param value: Parsed JSON value, possibly containing codec markers.
    :param types: Explicit constructors permitted for enum and dataclass markers.
    :return: Reconstructed value or unchanged primitive.
    """

    if isinstance(value, list):
        return [_decode(item, types) for item in value]
    if not isinstance(value, dict):
        return value
    if "$uuid" in value:
        return UUID(str(value["$uuid"]))
    if "$datetime" in value:
        return datetime.fromisoformat(str(value["$datetime"]))
    if "$tuple" in value:
        return tuple(_decode(item, types) for item in value["$tuple"])
    if "$frozenset" in value:
        return frozenset(_decode(item, types) for item in value["$frozenset"])
    if "$mapping" in value:
        return {
            _decode(key, types): _decode(item, types) for key, item in value["$mapping"]
        }
    if "$enum" in value:
        type_name = str(value["$enum"])
        if type_name not in types:
            raise ValueError(f"unknown storage enum type {type_name!r}.")
        return types[type_name](value["value"])
    if "$dataclass" in value:
        type_name = str(value["$dataclass"])
        if type_name not in types:
            raise ValueError(f"unknown storage value type {type_name!r}.")
        fields = value.get("fields")
        if not isinstance(fields, dict):
            raise ValueError("dataclass storage envelope has no fields mapping.")
        return types[type_name](
            **{key: _decode(item, types) for key, item in fields.items()}
        )
    return {key: _decode(item, types) for key, item in value.items()}


__all__ = ["_decode", "_encode", "_storage_value_types", "_type_name"]
