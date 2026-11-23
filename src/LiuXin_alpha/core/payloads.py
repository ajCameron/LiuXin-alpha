"""Validate Core wire primitives without coercing identifiers or control flags.

CLI conveniences belong at the surface boundary. These validators run before
handler effects; Catalog and storage retain validation of domain payloads.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, Any, NoReturn, cast

from LiuXin_alpha.core.errors import CoreDispatchError

if TYPE_CHECKING:
    from LiuXin_alpha.core.description import CorePayloadFieldDescription


def invalid_payload(name: str, message: str) -> NoReturn:
    """Raise the same structured validation error for direct and RPC calls."""
    raise CoreDispatchError(
        f"`{name}` {message}", code="invalid_payload", details={"parameter": name}
    )


def integer_value(value: object, name: str) -> int:
    """Accept an integer, excluding booleans, floats and numeric strings."""
    if type(value) is not int:
        invalid_payload(name, "must be an integer.")
    return value


def number_value(value: object, name: str) -> float:
    """Accept a finite JSON number, excluding booleans and numeric strings."""
    if type(value) not in (int, float):
        invalid_payload(name, "must be a finite number.")
    try:
        result = float(cast(int | float, value))
    except OverflowError, ValueError:
        invalid_payload(name, "must be a finite number.")
    if not math.isfinite(result):
        invalid_payload(name, "must be a finite number.")
    return result


def required_text(
    payload: Mapping[str, Any], name: str, *, choices: Iterable[str] | None = None
) -> str:
    """Require nonblank text, optionally limited to explicit case-sensitive choices."""
    raw = payload.get(name)
    if not isinstance(raw, str) or not raw.strip():
        invalid_payload(name, "must be a nonempty string.")
    value = raw.strip()
    if choices is not None:
        allowed = tuple(choices)
        if value not in allowed:
            invalid_payload(name, f"must be one of: {', '.join(allowed)}.")
    return value


def required_int(payload: Mapping[str, Any], name: str) -> int:
    """Require an integer field; domain owners decide its valid identity range."""
    return integer_value(payload.get(name), name)


def optional_int(
    payload: Mapping[str, Any],
    name: str,
    *,
    default: int | None = None,
    minimum: int | None = None,
) -> int | None:
    """Read a nullable integer and enforce an optional inclusive lower bound."""
    raw = payload.get(name, default)
    if raw is None:
        return None
    value = integer_value(raw, name)
    if minimum is not None and value < minimum:
        invalid_payload(name, f"must be >= {minimum}.")
    return value


def boolean(payload: Mapping[str, Any], name: str, default: bool = False) -> bool:
    """Read a boolean control flag; only absence chooses the default."""
    value = payload.get(name, default)
    if type(value) is not bool:
        invalid_payload(name, "must be a boolean.")
    return value


def _matches(value: object, kind: str) -> bool:
    if kind == "null":
        return value is None
    if kind == "boolean":
        return type(value) is bool
    if kind == "integer":
        return type(value) is int
    if kind == "number":
        if type(value) not in (int, float):
            return False
        try:
            return math.isfinite(cast(int | float, value))
        except OverflowError:
            return False
    if kind == "string":
        return isinstance(value, str)
    if kind == "object":
        return isinstance(value, Mapping)
    if kind == "array":
        return isinstance(value, (list, tuple))
    return False


_FIELD_TYPES = frozenset(
    {"null", "boolean", "integer", "number", "string", "object", "array"}
)


def validate_field_declarations(fields: Iterable[CorePayloadFieldDescription]) -> None:
    """Reject ambiguous or unsupported declarations before replacing a route."""
    seen: set[str] = set()
    for field in fields:
        if not field.name or field.name in seen:
            raise ValueError("Payload field names must be nonempty and unique")
        seen.add(field.name)
        if (
            field.field_type is not None
            and not set(field.field_type.split("|")) <= _FIELD_TYPES
        ):
            raise ValueError(
                f"Unsupported Core payload field type: {field.field_type!r}"
            )


def validate_payload(
    payload: object, fields: Iterable[CorePayloadFieldDescription]
) -> None:
    """Validate declared top-level fields before effects without inventing domain schemas.

    Unlisted fields remain available to domain-specific adapters. Optional fields
    may be absent; null is accepted only when their declaration includes null.
    Nested object/array contents remain the receiving domain owner's contract.
    """
    if not isinstance(payload, Mapping) or any(
        not isinstance(key, str) for key in payload
    ):
        invalid_payload("payload", "must be an object with string keys.")
    for field in fields:
        if field.name not in payload:
            if field.required:
                invalid_payload(field.name, "is required.")
            continue
        if field.field_type is not None and not any(
            _matches(payload[field.name], kind) for kind in field.field_type.split("|")
        ):
            invalid_payload(field.name, f"must have type {field.field_type}.")
