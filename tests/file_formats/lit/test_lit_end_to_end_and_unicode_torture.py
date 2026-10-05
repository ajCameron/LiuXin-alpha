"""
Provide test lit end to end and unicode torture utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test lit end to end and unicode torture through a consuming regression::

        python -m pytest -q tests/file_formats/lit/test_lit_end_to_end_and_unicode_torture.py
"""
from __future__ import annotations

from pathlib import Path

import pytest

from LiuXin_alpha.file_formats.lit import LitError
from LiuXin_alpha.file_formats.lit.reader import (
    LitFile,
    consume_sized_utf8_string,
    read_utf8_char,
)
from tests.support.file_format_lit import LitLog, lit_options, lit_sized_utf8
from tests.support.file_format_unicode import assert_no_replacement_chars


def _lit_paths(md_test_files_by_ext: dict[str, list[Path]]) -> list[Path]:
    """
    Perform the lit paths operation under explicit file-format and conversion rules.

    Example:
        Exercise  lit paths through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_end_to_end_and_unicode_torture.py


    :param md_test_files_by_ext: Value supplied for md test files by ext under the
        utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    paths = list(md_test_files_by_ext.get("lit", []))
    if not paths:
        pytest.skip("No .lit fixtures found in optional LiuXin_alpha_data corpus")
    return paths


def test_lit_input_end_to_end_on_real_fixtures(md_test_files_by_ext: dict[str, list[Path]]) -> None:
    """
    Perform the test lit input end to end on real fixtures operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit input end to end on real fixtures through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_end_to_end_and_unicode_torture.py


    :param md_test_files_by_ext: Value supplied for md test files by ext under the
        utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.lit_input import LITInput

    for lit_path in _lit_paths(md_test_files_by_ext):
        plugin = LITInput(None)
        log = LitLog()
        with lit_path.open("rb") as stream:
            oeb = plugin.convert(stream, lit_options(), "lit", log, {})

        assert len(list(oeb.manifest.values())) >= 1
        assert len(list(oeb.spine)) >= 1
        assert bool(oeb.metadata.title)
        product_text = []
        for field in ("title", "creator", "language"):
            for value in getattr(oeb.metadata, field, []):
                product_text.append(str(getattr(value, "value", value)))
        for item in oeb.spine:
            data = getattr(item, "data", None)
            if hasattr(data, "itertext"):
                product_text.append("".join(data.itertext()))
        assert_no_replacement_chars("\n".join(product_text), context=lit_path.name)


def test_lit_input_recovers_when_opf_spine_is_empty_after_prune(
    md_test_files_by_ext: dict[str, list[Path]]
) -> None:
    """
    Perform the test lit input recovers when opf spine is empty after prune operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit input recovers when opf spine is empty after prune through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_end_to_end_and_unicode_torture.py


    :param md_test_files_by_ext: Value supplied for md test files by ext under the
        utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.lit_input import LITInput

    target = next((p for p in _lit_paths(md_test_files_by_ext) if p.name == "lit_md_test_file_1.lit"), None)
    if target is None:
        pytest.skip("Fixture lit_md_test_file_1.lit not available")

    log = LitLog()
    with target.open("rb") as stream:
        oeb = LITInput(None).convert(stream, lit_options(), "lit", log, {})

    assert len(list(oeb.spine)) >= 1
    assert any("rebuilding from manifest documents" in msg.lower() for msg in log.messages)
    assert any(getattr(item, "href", "").endswith("contents.xhtml") for item in oeb.spine)


def test_lit_reader_best_effort_drm_mode_blocks_encrypted_sections(
    md_test_files_by_ext: dict[str, list[Path]]
) -> None:
    """
    Perform the test lit reader best effort drm mode blocks encrypted sections operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit reader best effort drm mode blocks encrypted sections through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_end_to_end_and_unicode_torture.py


    :param md_test_files_by_ext: Value supplied for md test files by ext under the
        utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    target = next((p for p in _lit_paths(md_test_files_by_ext) if p.name == "lit_md_test_file_2.lit"), None)
    if target is None:
        pytest.skip("Fixture lit_md_test_file_2.lit not available")

    lit = LitFile(str(target), LitLog())

    assert lit.drmlevel >= 0
    if lit.drm_fallback and "/DRMStorage/ValidationStream" in lit.entries:
        with pytest.raises(LitError, match="title key"):
            lit.get_file("/DRMStorage/ValidationStream")


def test_lit_input_reports_clear_error_for_broken_manifest_utf8(
    tmp_path: Path, md_test_files_by_ext: dict[str, list[Path]]
) -> None:
    """
    Perform the test lit input reports clear error for broken manifest utf8 operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit input reports clear error for broken manifest utf8 through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_end_to_end_and_unicode_torture.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :param md_test_files_by_ext: Value supplied for md test files by ext under the
        utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.conversion.plugins.lit_input import LITInput

    source = next((p for p in _lit_paths(md_test_files_by_ext) if p.name == "lit_md_test_file_2.lit"), None)
    if source is None:
        pytest.skip("Fixture lit_md_test_file_2.lit not available")

    lit = LitFile(str(source), LitLog())
    entry = lit.entries.get("/manifest")
    if entry is None or entry.section != 0 or entry.size < 2:
        pytest.skip("Could not locate a mutable section-0 /manifest entry in fixture")

    payload = bytearray(source.read_bytes())
    abs_pos = lit.content_offset + entry.offset + 1
    if abs_pos >= len(payload):
        pytest.skip("Computed manifest offset is out of bounds")
    payload[abs_pos] = 0xFF

    broken_path = tmp_path / "broken_manifest_utf8.lit"
    broken_path.write_bytes(payload)

    with broken_path.open("rb") as stream:
        with pytest.raises(LitError, match="Manifest contains invalid UTF-8"):
            LITInput(None).convert(stream, lit_options(), "lit", LitLog(), {})


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (b"A", "A"),
        (b"\xc2\x80", "\u0080"),
        ("é".encode("utf-8"), "é"),
        ("Ж".encode("utf-8"), "Ж"),
        ("न".encode("utf-8"), "न"),
        ("🙂".encode("utf-8"), "🙂"),
        (b"\xf4\x8f\xbf\xbf", "\U0010FFFF"),
    ],
)
def test_lit_utf8_torture_valid_sequences(payload: bytes, expected: str) -> None:
    """
    Perform the test lit utf8 torture valid sequences operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit utf8 torture valid sequences through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_end_to_end_and_unicode_torture.py


    :param payload: Value supplied for payload under the utility contract.
    :param expected: Value supplied for expected under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    ch, pos = read_utf8_char(payload, 0)
    assert ch == expected
    assert pos == len(payload)


@pytest.mark.parametrize(
    "payload",
    [
        b"\x80",  # stray continuation byte
        b"\xc0\xaf",  # overlong two-byte encoding
        b"\xe0\x80\x80",  # overlong three-byte encoding
        b"\xf0\x80\x80\x80",  # overlong four-byte encoding
        b"\xed\xa0\x80",  # UTF-16 surrogate U+D800
        b"\xf4\x90\x80\x80",  # > U+10FFFF
        b"\xe2\x82",  # truncated multi-byte sequence
        b"\xe2(\xa1",  # invalid continuation
        b"\xf8\x88\x80\x80\x80",  # invalid starter byte
    ],
)
def test_lit_utf8_torture_invalid_sequences(payload: bytes) -> None:
    """
    Perform the test lit utf8 torture invalid sequences operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit utf8 torture invalid sequences through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_end_to_end_and_unicode_torture.py


    :param payload: Value supplied for payload under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(LitError):
        read_utf8_char(payload, 0)


def test_lit_consume_sized_utf8_string_unicode_torture() -> None:
    """
    Perform the test lit consume sized utf8 string unicode torture operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit consume sized utf8 string unicode torture through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_end_to_end_and_unicode_torture.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    text = "Åß漢🙂e\u0301Ωж🧪"
    payload = lit_sized_utf8(text, zpad=True) + b"TAIL"
    parsed, remainder = consume_sized_utf8_string(payload, zpad=True)

    assert parsed == text
    assert remainder == b"TAIL"


@pytest.mark.parametrize(
    "payload",
    [
        bytes([5]) + b"a",  # declared character length longer than payload
        bytes([2]) + b"\xc3(",  # malformed 2-byte sequence
    ],
)
def test_lit_consume_sized_utf8_string_broken_encoding_raises(payload: bytes) -> None:
    """
    Perform the test lit consume sized utf8 string broken encoding raises operation under explicit file-format and conversion rules.

    Example:
        Exercise test lit consume sized utf8 string broken encoding raises through a consuming regression::

            python -m pytest -q tests/file_formats/lit/test_lit_end_to_end_and_unicode_torture.py


    :param payload: Value supplied for payload under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(LitError):
        consume_sized_utf8_string(payload)
