"""
Provide test json modernized utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test json modernized through a consuming regression::

        python -m pytest -q tests/file_formats/json/test_json_modernized.py
"""
from __future__ import annotations

import datetime
import importlib
import json

import pytest


def test_json_module_import_smoke() -> None:
    """
    Perform the test json module import smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test json module import smoke through a consuming regression::

            python -m pytest -q tests/file_formats/json/test_json_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    importlib.import_module("LiuXin_alpha.file_formats.json")


def test_to_json_bytearray_is_json_serializable_and_roundtrips() -> None:
    """
    Perform the test to json bytearray is json serializable and roundtrips operation under explicit file-format and conversion rules.

    Example:
        Exercise test to json bytearray is json serializable and roundtrips through a consuming regression::

            python -m pytest -q tests/file_formats/json/test_json_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.json import from_json, to_json

    raw = bytearray(b"\x00\x01Smoke\xff")
    encoded = to_json(raw)

    assert encoded["__class__"] == "bytearray"
    assert isinstance(encoded["__value__"], str)
    json.dumps(encoded)

    decoded = from_json(encoded)
    assert isinstance(decoded, bytearray)
    assert decoded == raw


def test_to_json_accepts_bytes_like_inputs() -> None:
    """
    Perform the test to json accepts bytes like inputs operation under explicit file-format and conversion rules.

    Example:
        Exercise test to json accepts bytes like inputs through a consuming regression::

            python -m pytest -q tests/file_formats/json/test_json_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.json import from_json, to_json

    for raw in (b"abc", memoryview(b"abc")):
        encoded = to_json(raw)
        assert encoded["__class__"] == "bytearray"
        assert from_json(encoded) == bytearray(b"abc")


def test_to_json_datetime_roundtrip_utc() -> None:
    """
    Perform the test to json datetime roundtrip utc operation under explicit file-format and conversion rules.

    Example:
        Exercise test to json datetime roundtrip utc through a consuming regression::

            python -m pytest -q tests/file_formats/json/test_json_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.json import from_json, to_json

    dt = datetime.datetime(2020, 1, 2, 3, 4, 5, tzinfo=datetime.timezone.utc)
    encoded = to_json(dt)

    assert encoded["__class__"] == "datetime.datetime"
    assert encoded["__value__"].endswith("+00:00")

    decoded = from_json(encoded)
    assert isinstance(decoded, datetime.datetime)
    assert decoded.utcoffset() == datetime.timedelta(0)
    assert decoded.year == 2020
    assert decoded.month == 1
    assert decoded.day == 2
    assert decoded.hour == 3
    assert decoded.minute == 4
    assert decoded.second == 5


def test_from_json_passthrough_for_unrelated_objects() -> None:
    """
    Perform the test from json passthrough for unrelated objects operation under explicit file-format and conversion rules.

    Example:
        Exercise test from json passthrough for unrelated objects through a consuming regression::

            python -m pytest -q tests/file_formats/json/test_json_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.json import from_json

    marker = {"x": 1}
    assert from_json(marker) is marker
    assert from_json([1, 2, 3]) == [1, 2, 3]


def test_to_json_unsupported_type_raises_typeerror() -> None:
    """
    Perform the test to json unsupported type raises typeerror operation under explicit file-format and conversion rules.

    Example:
        Exercise test to json unsupported type raises typeerror through a consuming regression::

            python -m pytest -q tests/file_formats/json/test_json_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.json import to_json

    with pytest.raises(TypeError):
        to_json({"not": "supported"})
