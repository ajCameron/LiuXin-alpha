"""
Verify Rocket eBook binary metadata parsing.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test rb metadata source through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_rb_metadata_source.py
"""
from __future__ import annotations

import io
import struct
from collections.abc import Mapping
from pathlib import Path

import pytest


def _values(raw):
    """
    Perform the values test-helper operation with deterministic inputs.

    Example:
        Exercise values through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rb_metadata_source.py


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

            python -m pytest -q tests/metadata/file_sources/test_rb_metadata_source.py


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

            python -m pytest -q tests/metadata/file_sources/test_rb_metadata_source.py


    :param md: Value supplied for md in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    identifiers = {}
    try:
        identifiers = {str(k): sorted(str(v) for v in vals) for k, vals in (md.get_identifiers() or {}).items()}
    except Exception:
        identifiers = {}
    return {
        "title": _first(getattr(md, "title", None)),
        "authors": sorted(_values(getattr(md, "authors", None))),
        "identifiers": identifiers,
    }


def _build_rb_bytes(entries: list[tuple[str, int, bytes]]) -> bytes:
    """
    Perform the build rb bytes test-helper operation with deterministic inputs.

    Example:
        Exercise build rb bytes through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rb_metadata_source.py


    :param entries: Value supplied for entries in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    from LiuXin_alpha.file_formats.rb import HEADER

    toc_offset = 0x128
    out = io.BytesIO()
    out.write(HEADER)
    out.write(struct.pack("<I", 0))
    out.write(struct.pack("<IH", 0, 0))
    out.write(struct.pack("<I", toc_offset))
    out.write(struct.pack("<I", 0))
    for _ in range(0x20, toc_offset, 4):
        out.write(struct.pack("<I", 0))

    out.write(struct.pack("<I", len(entries)))
    offset = toc_offset + 4 + (44 * len(entries))
    for name, _flags, payload in entries:
        out.write(name.encode("utf-8", "replace")[:32].ljust(32, b"\x00"))
        out.write(struct.pack("<I", len(payload)))
        out.write(struct.pack("<I", offset))
        out.write(struct.pack("<I", _flags))
        offset += len(payload)

    for _name, _flags, payload in entries:
        out.write(payload)

    total_size = out.tell()
    out.seek(0x1C)
    out.write(struct.pack("<I", total_size))
    return out.getvalue()


def test_rb_metadata_module_import_smoke() -> None:
    """
    Verify rb metadata module import smoke.

    Example:
        Exercise test rb metadata module import smoke through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rb_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.rb as rb_md

    assert rb_md is not None


def test_rb_reader_plugin_is_available_and_preserves_stream_position() -> None:
    """
    Verify rb reader plugin remains available and preserves stream position.

    Example:
        Exercise test rb reader plugin is available and preserves stream position through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rb_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    info_payload = b"TYPE=2\nTITLE=Plugin Title\nAUTHOR=Alice & Bob\n"
    rb_data = _build_rb_bytes([("info.info", 2, info_payload)])
    stream = io.BytesIO(rb_data)
    stream.seek(7)

    plugins = get_metadata_reader_plugins()
    rb_cls = next((p for p in plugins if p.__name__ == "RBMetadataReader"), None)
    assert rb_cls is not None

    reader = rb_cls(None)
    md = reader.get_metadata(stream=stream, ftype="rb")
    assert stream.tell() == 7
    assert md.title == "Plugin Title"
    assert _values(md.authors) == ["Alice", "Bob"]


def test_rb_get_metadata_reads_utf8_unicode_torture() -> None:
    """
    Verify rb get metadata reads utf8 unicode torture.

    Example:
        Exercise test rb get metadata reads utf8 unicode torture through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rb_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.rb import get_metadata

    info_payload = (
        "TYPE=2\n"
        "TITLE=Καλημέρα こんにちは 😀\n"
        "AUTHOR=Renée & 李白\n"
    ).encode("utf-8")
    rb_data = _build_rb_bytes([("info.info", 2, info_payload)])

    md = get_metadata(io.BytesIO(rb_data))
    assert md.title == "Καλημέρα こんにちは 😀"
    assert _values(md.authors) == ["Renée", "李白"]


def test_rb_get_metadata_cp1252_fallback_decode() -> None:
    """
    Verify rb get metadata cp1252 fallback decode.

    Example:
        Exercise test rb get metadata cp1252 fallback decode through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rb_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.rb import get_metadata

    info_payload = b"TYPE=2\nTITLE=Caf\xe9 na\xefve\nAUTHOR=Jos\xe9 & Ana\xefs\n"
    rb_data = _build_rb_bytes([("info.info", 2, info_payload)])

    md = get_metadata(io.BytesIO(rb_data))
    assert md.title == "Café naïve"
    assert _values(md.authors) == ["José", "Anaïs"]


def test_rb_get_metadata_pathlike_input(tmp_path: Path) -> None:
    """
    Verify rb get metadata pathlike input.

    Example:
        Exercise test rb get metadata pathlike input through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rb_metadata_source.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.rb import get_metadata

    info_payload = b"TYPE=2\nTITLE=Path Title\nAUTHOR=Path Author\n"
    rb_data = _build_rb_bytes([("info.info", 2, info_payload)])
    path = tmp_path / "sample.rb"
    path.write_bytes(rb_data)

    md = get_metadata(path)
    assert md.title == "Path Title"
    assert _values(md.authors) == ["Path Author"]


def test_rb_invalid_header_raises_by_default_and_can_opt_into_fallback() -> None:
    """
    Verify rb invalid header raises by default and can opt into fallback.

    Example:
        Exercise test rb invalid header raises by default and can opt into fallback through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rb_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.rb import RbFormatError, get_metadata

    stream = io.BytesIO(b"not-a-valid-rb")
    stream.name = "fallback_name.rb"
    with pytest.raises(RbFormatError):
        get_metadata(stream)

    md = get_metadata(stream, fallback_on_parse_error=True)

    assert md.title == "fallback_name"
    assert _values(md.authors) == ["Unknown"]


def test_rb_truncated_payload_raises_by_default_and_can_opt_into_fallback() -> None:
    """
    Verify rb truncated payload raises by default and can opt into fallback.

    Example:
        Exercise test rb truncated payload raises by default and can opt into fallback through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rb_metadata_source.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.rb import RbFormatError, get_metadata

    truncated = b"\xb0\x0c\xb0\x0c\x02\x00NUVO\x00\x00\x00\x00" + b"\x00" * 12
    stream = io.BytesIO(truncated)
    stream.name = "truncated.rb"
    with pytest.raises(RbFormatError):
        get_metadata(stream)

    md = get_metadata(stream, fallback_on_parse_error=True)

    assert md.title == "truncated"
    assert _values(md.authors) == ["Unknown"]


def test_rb_md_fixture_smoke_and_deterministic(md_test_fixture) -> None:
    """
    Verify rb md fixture smoke and deterministic.

    Example:
        Exercise test rb md fixture smoke and deterministic through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_rb_metadata_source.py


    :param md_test_fixture: Value supplied for md test fixture in the focused test
        operation.
    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.file_sources.rb import get_metadata

    fixture = md_test_fixture(file_ext="rb", file_num=1, verify_hash=True)
    md_1 = get_metadata(fixture)
    md_2 = get_metadata(fixture)

    assert _snapshot(md_1) == _snapshot(md_2)
    assert bool(_first(md_1.title))
