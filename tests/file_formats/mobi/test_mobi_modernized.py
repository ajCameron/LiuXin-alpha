"""
Provide test mobi modernized utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test mobi modernized through a consuming regression::

        python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py
"""
from __future__ import annotations

import importlib
import pkgutil
import unicodedata

import pytest


def test_mobi_modules_import_smoke() -> None:
    """
    Perform the test mobi modules import smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test mobi modules import smoke through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    pkg = importlib.import_module("LiuXin_alpha.file_formats.mobi")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.mobi_input")
    importlib.import_module("LiuXin_alpha.file_formats.conversion.plugins.mobi_output")

    failed: list[tuple[str, Exception]] = []
    for mod in pkgutil.walk_packages(pkg.__path__, pkg.__name__ + "."):
        try:
            importlib.import_module(mod.name)
        except Exception as exc:  # pragma: no cover - hit only on failures
            failed.append((mod.name, exc))

    assert not failed, "MOBI import smoke failures: " + ", ".join(f"{name}: {exc!r}" for name, exc in failed)


@pytest.mark.parametrize("forward", [True, False])
@pytest.mark.parametrize("value", [0, 1, 127, 128, 255, 16384, 2**31 - 1])
def test_mobi_vwi_roundtrip(value: int, forward: bool) -> None:
    """
    Perform the test mobi vwi roundtrip operation under explicit file-format and conversion rules.

    Example:
        Exercise test mobi vwi roundtrip through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py


    :param value: Value normalized, stored, formatted or returned.
    :param forward: Value supplied for forward under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.mobi.utils import decint, encint

    raw = encint(value, forward=forward)
    parsed, consumed = decint(raw, forward=forward)
    assert parsed == value
    assert consumed == len(raw)


def test_mobi_decode_string_and_hex_unicode_torture() -> None:
    """
    Perform the test mobi decode string and hex unicode torture operation under explicit file-format and conversion rules.

    Example:
        Exercise test mobi decode string and hex unicode torture through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.mobi.utils import decode_hex_number, decode_string

    ordt_map = "Aé🙂Ω"
    text, consumed = decode_string(bytes([3, 0, 1, 2]), ordt_map=ordt_map)
    assert text == "Aé🙂"
    assert consumed == 4

    number, consumed2 = decode_hex_number(bytes([2]) + b"FF")
    assert number == 255
    assert consumed2 == 3


def test_mobi_decode_string_invalid_utf8_raises() -> None:
    """
    Perform the test mobi decode string invalid utf8 raises operation under explicit file-format and conversion rules.

    Example:
        Exercise test mobi decode string invalid utf8 raises through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.mobi.utils import decode_string

    with pytest.raises(UnicodeDecodeError):
        decode_string(bytes([1, 0xFF]), codec="utf-8")


def test_mobi_trailing_data_roundtrip() -> None:
    """
    Perform the test mobi trailing data roundtrip operation under explicit file-format and conversion rules.

    Example:
        Exercise test mobi trailing data roundtrip through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.mobi.utils import encode_trailing_data, get_trailing_data

    payload = "Åß漢🙂".encode("utf-8")
    record = b"BODY" + encode_trailing_data(payload)
    trailing, body = get_trailing_data(record, extra_data_flags=0b10)

    assert body == b"BODY"
    assert trailing[1] == payload


def test_mobi_tbs_roundtrip() -> None:
    """
    Perform the test mobi tbs roundtrip operation under explicit file-format and conversion rules.

    Example:
        Exercise test mobi tbs roundtrip through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.mobi.utils import decode_tbs, encode_tbs

    value = 12345
    extra = {0b0010: 99, 0b0100: 7, 0b0001: 1001}
    raw = encode_tbs(value, extra)
    decoded_value, decoded_extra, consumed = decode_tbs(raw)

    assert decoded_value == value
    assert decoded_extra == extra
    assert consumed == len(raw)


def test_mobi_utf8_text_unicode_torture() -> None:
    """
    Perform the test mobi utf8 text unicode torture operation under explicit file-format and conversion rules.

    Example:
        Exercise test mobi utf8 text unicode torture through a consuming regression::

            python -m pytest -q tests/file_formats/mobi/test_mobi_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.mobi.utils import utf8_text

    text = "cafe\u0301 — Καλημέρα — नमस्ते — 漢字 — 🙂"
    encoded = utf8_text(text)
    assert isinstance(encoded, bytes)
    assert unicodedata.normalize("NFC", encoded.decode("utf-8")) == unicodedata.normalize("NFC", text)
