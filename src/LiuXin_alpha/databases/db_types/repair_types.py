"""Encode database scalar values losslessly in versioned Catalog repair receipts."""

import base64
from typing import Literal, NotRequired, TypedDict
from datetime import date, datetime, time
from decimal import Decimal


def repair_json_scalar(value: object) -> dict[str, str]:
    """Preserve native temporal, decimal and binary values with explicit type tags.

    JSON-native values retain their ordinary shape. This default hook is used for
    both state fingerprints and stored receipts; unsupported values are refused,
    never silently replaced by repr or truncated. Receipts are inspection records,
    not a JSON command format for replaying database writes.
    """
    if isinstance(value, (datetime, date, time)):
        return {"$type": type(value).__name__, "iso": value.isoformat()}
    if isinstance(value, Decimal):
        return {"$type": "decimal", "value": str(value)}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {
            "$type": "bytes",
            "base64": base64.b64encode(bytes(value)).decode("ascii"),
        }
    raise TypeError(f"Unsupported repair receipt value: {type(value).__name__}")


class PhysicalReference(TypedDict):
    """One physical inbound constraint or polymorphic reference declaration.

    Column order matters for composite foreign keys. An external schema is
    reported for refusal by Catalog; the database layer does not choose repairs.
    """

    table: str
    columns: list[str]
    target_columns: list[str]
    external_schema: str | None
    kind: Literal["foreign_key", "polymorphic"]
    type_column: NotRequired[str]
