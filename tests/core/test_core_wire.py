"""
Check supported Core wire shapes and rejection of nonfinite floats, colliding keys, and opaque objects.

These pure conversion tests require no database or RPC server. They cover the
explicit shapes below, not every row-like fallback or cyclic input.
"""

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
    """
    Supply a non-string-subclass enum to exercise conversion through its value.

    Example:
        >>> to_wire(_Mode.READY)
        'ready'
    """

    READY = "ready"


@dataclass(frozen=True)
class _WireRecord:
    """
    Supply nested Unicode and date fields for dataclass-to-wire conversion.

    Example:
        >>> _WireRecord("雪", datetime.date(2026, 7, 25)).name
        '雪'
    """

    name: str
    created: datetime.date


def test_core_wire_converts_supported_transport_values() -> None:
    """
    Assert exact enum, byte-tag, dataclass/date, decimal, path, UUID, and sorted-set output shapes.

    Example:
        >>> test_core_wire_converts_supported_transport_values()


    :return: ``None`` if the nested converted result equals the expected wire representation.
    """
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
    """
    Require a wire error for each nonfinite floating-point value supplied by pytest.

    Example:
        >>> test_core_wire_rejects_non_finite_floats(float("inf"))


    :param value: Parametrized NaN, positive infinity, or negative infinity.
    :return: ``None`` if conversion raises CoreWireError identifying a nonfinite float.
    """
    with pytest.raises(CoreWireError, match="non-finite"):
        to_wire(value)


def test_core_wire_rejects_mapping_key_collisions() -> None:
    """
    Reject a true mapping whose integer and string keys become the same wire key.

    Example:
        >>> test_core_wire_rejects_mapping_key_collisions()


    :return: ``None`` if conversion reports the collision rather than discarding an entry.
    """
    with pytest.raises(CoreWireError, match="colliding mapping key"):
        to_wire({1: "integer", "1": "string"})


def test_core_wire_rejects_process_owned_objects() -> None:
    """
    Reject an opaque object with no supported value or row-like conversion path.

    Example:
        >>> test_core_wire_rejects_process_owned_objects()


    :return: ``None`` if CoreWireError reports that the object is not transport-safe.
    """
    with pytest.raises(CoreWireError, match="not transport-safe"):
        to_wire(object())
