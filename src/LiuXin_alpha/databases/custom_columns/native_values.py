"""One lossless typed value contract for writes, queries and migrations."""

from __future__ import annotations

import hashlib
import json
import math
import unicodedata
from datetime import UTC, datetime
from typing import Any

from LiuXin_alpha.databases.constants import CUSTOM_DATA_TYPES
from LiuXin_alpha.databases.custom_columns.native_types import (
    CustomFieldError,
    FieldDefinition,
    Scalar,
    SeriesValue,
    ValueEntry,
)


def adapt(value: Any, definition: FieldDefinition) -> Scalar:
    kind = definition.datatype
    if value is None:
        raise CustomFieldError("Collection entries cannot be null")
    if kind in {"text", "comments", "enumeration", "series"}:
        if not isinstance(value, str):
            raise CustomFieldError(f"{kind} requires text")
        if kind == "enumeration" and value not in definition.display.get(
            "enum_values", ()
        ):
            raise CustomFieldError(f"{value!r} is not an allowed enumeration choice")
        return value
    if kind == "bool" and isinstance(value, bool):
        return value
    if (
        kind in {"int", "rating"}
        and isinstance(value, int)
        and not isinstance(value, bool)
    ):
        if not -(2**63) <= value < 2**63:
            raise CustomFieldError("Integers must fit the portable signed 64-bit range")
        if kind == "rating" and not 0 <= value <= 10:
            raise CustomFieldError("Ratings must be between 0 and 10")
        return value
    if (
        kind == "float"
        and isinstance(value, (int, float))
        and not isinstance(value, bool)
    ):
        if isinstance(value, int) and int(float(value)) != value:
            raise CustomFieldError("Integer-to-float conversion would lose precision")
        if math.isfinite(value):
            return float(value)
    if kind == "datetime" and isinstance(value, datetime):
        return value.astimezone(UTC) if value.tzinfo else value
    raise CustomFieldError(f"Invalid {kind} value: {value!r}")


def encode(value: Scalar) -> str | int | float:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, bool):
        return int(value)
    return value


def decode(value: Any, definition: FieldDefinition) -> Scalar:
    if definition.datatype == "datetime" and isinstance(value, str):
        value = datetime.fromisoformat(value)
    if definition.datatype == "bool" and value in (0, 1):
        value = bool(value)
    return adapt(value, definition)


def identity(value: Scalar, definition: FieldDefinition) -> str:
    canonical = encode(value)
    if definition.datatype in {"text", "enumeration", "series"}:
        canonical = unicodedata.normalize("NFC", str(canonical)).casefold()
    if definition.datatype == "float" and canonical == 0:
        canonical = 0.0
    return hashlib.sha256(
        json.dumps([definition.datatype, canonical], ensure_ascii=False).encode()
    ).hexdigest()


def entries(
    value: Any, definition: FieldDefinition
) -> list[tuple[Scalar, float | None, int | None]]:
    if definition.datatype == "composite":
        raise CustomFieldError("Composite fields are computed and cannot be written")
    if value is None:
        return []
    supplied = [value] if definition.mode == "scalar" else value
    if not isinstance(supplied, (list, tuple)):
        raise CustomFieldError("A collection requires a list or tuple")
    result = []
    seen: set[str] = set()
    for item in supplied:
        entry_id = item.id if isinstance(item, ValueEntry) else None
        position = None
        if isinstance(item, (SeriesValue, ValueEntry)):
            position, item = item.position, item.value
        if definition.datatype == "series":
            position = 1.0 if position is None else position
            if (
                isinstance(position, bool)
                or not isinstance(position, (int, float))
                or not math.isfinite(position)
            ):
                raise CustomFieldError("Series position must be a finite number")
            position = float(position)
        elif position is not None:
            raise CustomFieldError("Only Series entries have positions")
        adapted = adapt(item, definition)
        key = identity(adapted, definition)
        if definition.mode == "set" and key in seen:
            continue
        seen.add(key)
        result.append((adapted, position, entry_id))
    return result


def validate_definition(definition: FieldDefinition) -> None:
    if not isinstance(definition.attachment, str) or not definition.attachment:
        raise CustomFieldError("Attachment must name an owner table")
    if not isinstance(definition.name, str) or not definition.name.strip():
        raise CustomFieldError("Field name must be nonempty text")
    if not isinstance(definition.label, str) or not isinstance(
        definition.editable, bool
    ):
        raise CustomFieldError("Invalid field label or editable flag")
    if definition.datatype not in CUSTOM_DATA_TYPES:
        raise CustomFieldError(f"Unknown datatype {definition.datatype!r}")
    if definition.mode not in {"scalar", "set", "list"}:
        raise CustomFieldError("Collection mode must be scalar, set or list")
    label = definition.label
    if (
        not label
        or not label[0].isalpha()
        or not label.replace("_", "").isalnum()
        or label.lower() != label
    ):
        raise CustomFieldError(
            "Labels require lowercase letters, digits or underscores and an initial letter"
        )
    if definition.datatype == "enumeration":
        choices = definition.display.get("enum_values")
        if not isinstance(choices, list) or not all(
            isinstance(v, str) for v in choices
        ):
            raise CustomFieldError(
                "Enumeration definitions require a text enum_values list"
            )
        if len({identity(v, definition) for v in choices}) != len(choices):
            raise CustomFieldError("Enumeration choices have duplicate identities")
    if definition.datatype == "composite" and not isinstance(
        definition.display.get("composite_template"), str
    ):
        raise CustomFieldError("Composite definitions require a template")
    json.dumps(definition.display, allow_nan=False)
