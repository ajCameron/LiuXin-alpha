from __future__ import annotations

import datetime
import decimal
import enum
import pathlib
import uuid

from dataclasses import dataclass

import pytest

from LiuXin_alpha.core import CoreWireError, to_wire


class _Mode(enum.Enum):
    READY = "ready"


@dataclass(frozen=True)
class _WireRecord:
    name: str
    created: datetime.date


def test_core_wire_converts_supported_transport_values() -> None:
    identifier = uuid.UUID("12345678-1234-5678-1234-567812345678")

    assert to_wire(
        {
            "mode": _Mode.READY,
            "payload": b"\x00\xff",
            "record": _WireRecord(
                name="雪",
                created=datetime.date(2026, 7, 25),
            ),
            "amount": decimal.Decimal("1.2300"),
            "path": pathlib.Path("library/雪.epub"),
            "identifier": identifier,
            "values": {"beta", "alpha"},
        }
    ) == {
        "mode": "ready",
        "payload": {
            "$type": "bytes",
            "base64": "AP8=",
        },
        "record": {
            "name": "雪",
            "created": {
                "$type": "date",
                "iso": "2026-07-25",
            },
        },
        "amount": {
            "$type": "decimal",
            "value": "1.2300",
        },
        "path": "library/雪.epub",
        "identifier": str(identifier),
        "values": ["alpha", "beta"],
    }


@pytest.mark.parametrize("value", [float("nan"), float("inf"), float("-inf")])
def test_core_wire_rejects_non_finite_floats(value: float) -> None:
    with pytest.raises(CoreWireError, match="non-finite"):
        to_wire(value)


def test_core_wire_rejects_mapping_key_collisions() -> None:
    with pytest.raises(CoreWireError, match="colliding mapping key"):
        to_wire({1: "integer", "1": "string"})


def test_core_wire_rejects_process_owned_objects() -> None:
    with pytest.raises(CoreWireError, match="not transport-safe"):
        to_wire(object())
