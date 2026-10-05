"""
Verify plain-text metadata inference and encoding behavior.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test txt metadata source through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py
"""
from __future__ import annotations

import io
from collections.abc import Mapping
from pathlib import Path


def _values(raw):
    """
    Perform the values test-helper operation with deterministic inputs.

    Example:
        Exercise values through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :param raw: Value supplied for raw in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    if raw is None:
        return []
    if isinstance(raw, Mapping):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    try:
        return list(raw)
    except TypeError:
        return [raw]


def _first(raw):
    """
    Perform the first test-helper operation with deterministic inputs.

    Example:
        Exercise first through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :param raw: Value supplied for raw in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    vals = _values(raw)
    return vals[0] if vals else None


def _snapshot(md) -> dict:
    """
    Perform the snapshot test-helper operation with deterministic inputs.

    Example:
        Exercise snapshot through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :param md: Value supplied for md in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    return {
        "title": _first(getattr(md, "title", None)),
        "authors": sorted(_values(getattr(md, "authors", None))),
    }


def test_txt_metadata_module_import_smoke() -> None:
    """
    Verify txt metadata module import smoke.

    Example:
        Exercise test txt metadata module import smoke through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.txt as txt_md

    assert txt_md is not None


def test_txt_reader_plugin_is_available_and_preserves_stream_position() -> None:
    """
    Verify txt reader plugin remains available and preserves stream position.

    Example:
        Exercise test txt reader plugin is available and preserves stream position through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    payload = b"Plugin Title\n\n\nPlugin Author\nBody text.\n"
    stream = io.BytesIO(payload)
    stream.seek(7)

    plugins = get_metadata_reader_plugins()
    txt_cls = next((p for p in plugins if p.__name__ == "TXTMetadataReader"), None)
    assert txt_cls is not None

    reader = txt_cls(None)
    md = reader.get_metadata(stream=stream, ftype="txt")

    assert stream.tell() == 7
    assert md.title == "Plugin Title"
    assert _values(md.authors) == ["Plugin Author"]


def test_txt_get_metadata_parses_project_gutenberg_header_with_wrapped_author() -> None:
    """
    Verify txt get metadata parses project gutenberg header with wrapped author.

    Example:
        Exercise test txt get metadata parses project gutenberg header with wrapped author through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.txt import get_metadata

    payload = (
        b"\xef\xbb\xbfThe Project Gutenberg Etext of 20000 Leagues Under the Seas by Jules\r\n"
        b"Verne\r\n\r\nCopyright notice.\r\n"
    )
    md = get_metadata(io.BytesIO(payload))

    assert md.title == "20000 Leagues Under the Seas"
    assert _values(md.authors) == ["Jules Verne"]


def test_txt_get_metadata_parses_unicode_title_and_byline() -> None:
    """
    Verify txt get metadata parses unicode title and byline.

    Example:
        Exercise test txt get metadata parses unicode title and byline through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.txt import get_metadata

    payload = (
        "Καλημέρα こんにちは 😀\n\n\nby Renée & 李白\n\nBody text.\n".encode("utf-8")
    )
    md = get_metadata(io.BytesIO(payload))

    assert md.title == "Καλημέρα こんにちは 😀"
    assert _values(md.authors) == ["Renée", "李白"]


def test_txt_get_metadata_cp1252_fallback_decode() -> None:
    """
    Verify txt get metadata cp1252 fallback decode.

    Example:
        Exercise test txt get metadata cp1252 fallback decode through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.txt import get_metadata

    payload = b"Caf\xe9 na\xefve title\n\n\nby Jos\xe9 & Ana\xefs\n"
    md = get_metadata(io.BytesIO(payload))

    assert md.title == "Café naïve title"
    assert _values(md.authors) == ["José", "Anaïs"]


def test_txt_get_metadata_pathlike_input_and_filename_fallback(tmp_path: Path) -> None:
    """
    Verify txt get metadata pathlike input and filename fallback.

    Example:
        Exercise test txt get metadata pathlike input and filename fallback through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.txt import get_metadata

    path = tmp_path / "fallback_name.txt"
    path.write_bytes(b"")
    md = get_metadata(path)

    assert md.title == "fallback_name"
    assert _values(md.authors) == ["Unknown"]


def test_txt_invalid_payload_returns_safe_default() -> None:
    """
    Verify txt invalid payload returns safe default.

    Example:
        Exercise test txt invalid payload returns safe default through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.txt import get_metadata

    stream = io.BytesIO(b"\x00\x01\x02\x03")
    md = get_metadata(stream)
    assert _first(md.title) == "Unknown"
    assert _values(md.authors) == ["Unknown"]


def test_txt_binary_signatures_do_not_become_titles() -> None:
    """
    Verify txt binary signatures do not become titles.

    Example:
        Exercise test txt binary signatures do not become titles through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.txt import get_metadata

    for payload in (
        b"\x89PNG\r\n\x1a\n" + b"\x00" * 32,
        b"%PDF-1.7\nnot really text",
        b"PK\x03\x04" + b"\x00" * 20,
    ):
        md = get_metadata(io.BytesIO(payload))
        assert _first(md.title) == "Unknown"
        assert _values(md.authors) == ["Unknown"]


def test_txt_utf16_bom_is_not_rejected_as_binary() -> None:
    """
    Verify txt utf16 bom remains not rejected as binary.

    Example:
        Exercise test txt utf16 bom is not rejected as binary through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.txt import get_metadata

    payload = "UTF16 Title\n\n\nby UTF16 Author\n".encode("utf-16")
    md = get_metadata(io.BytesIO(payload))

    assert md.title == "UTF16 Title"
    assert _values(md.authors) == ["UTF16 Author"]


def test_txt_unicode_torture_and_controls_are_sanitized() -> None:
    """
    Verify txt unicode torture and controls remain sanitized.

    Example:
        Exercise test txt unicode torture and controls are sanitized through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.txt import get_metadata

    payload = "Title\x00 With Controls — Καλημέρα — こんにちは — 😀\n\n\nby Renée & 李白\x07\n".encode("utf-8")
    md = get_metadata(io.BytesIO(payload))

    assert "\x00" not in md.title
    assert "\x07" not in md.title
    assert "Καλημέρα" in md.title
    assert _values(md.authors) == ["Renée", "李白"]


def test_txt_md_fixture_smoke_and_deterministic(md_test_fixture) -> None:
    """
    Verify txt md fixture smoke and deterministic.

    Example:
        Exercise test txt md fixture smoke and deterministic through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_txt_metadata_source.py


    :param md_test_fixture: Value supplied for md test fixture in the focused test
        operation.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.txt import get_metadata

    fixture = md_test_fixture(file_ext="txt", file_num=1, verify_hash=True)
    md_1 = get_metadata(fixture)
    md_2 = get_metadata(fixture)

    assert _snapshot(md_1) == _snapshot(md_2)
    assert md_1.title == "20000 Leagues Under the Seas"
    assert _values(md_1.authors) == ["Jules Verne"]
