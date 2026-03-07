from __future__ import annotations

import io
import struct
from collections.abc import Mapping
from pathlib import Path


def _values(raw):
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
    vals = _values(raw)
    return vals[0] if vals else None


def _snapshot(md) -> dict:
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
    import LiuXin_alpha.metadata.file_sources.rb as rb_md

    assert rb_md is not None


def test_rb_reader_plugin_is_available_and_preserves_stream_position() -> None:
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
    from LiuXin_alpha.metadata.file_sources.rb import get_metadata

    info_payload = b"TYPE=2\nTITLE=Caf\xe9 na\xefve\nAUTHOR=Jos\xe9 & Ana\xefs\n"
    rb_data = _build_rb_bytes([("info.info", 2, info_payload)])

    md = get_metadata(io.BytesIO(rb_data))
    assert md.title == "Café naïve"
    assert _values(md.authors) == ["José", "Anaïs"]


def test_rb_get_metadata_pathlike_input(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.rb import get_metadata

    info_payload = b"TYPE=2\nTITLE=Path Title\nAUTHOR=Path Author\n"
    rb_data = _build_rb_bytes([("info.info", 2, info_payload)])
    path = tmp_path / "sample.rb"
    path.write_bytes(rb_data)

    md = get_metadata(path)
    assert md.title == "Path Title"
    assert _values(md.authors) == ["Path Author"]


def test_rb_invalid_header_returns_safe_default_title() -> None:
    from LiuXin_alpha.metadata.file_sources.rb import get_metadata

    stream = io.BytesIO(b"not-a-valid-rb")
    stream.name = "fallback_name.rb"
    md = get_metadata(stream)

    assert md.title == "fallback_name"
    assert _values(md.authors) == ["Unknown"]


def test_rb_truncated_payload_fails_gracefully_without_raise() -> None:
    from LiuXin_alpha.metadata.file_sources.rb import get_metadata

    truncated = b"\xb0\x0c\xb0\x0c\x02\x00NUVO\x00\x00\x00\x00" + b"\x00" * 12
    stream = io.BytesIO(truncated)
    stream.name = "truncated.rb"
    md = get_metadata(stream)

    assert md.title == "truncated"
    assert _values(md.authors) == ["Unknown"]


def test_rb_md_fixture_smoke_and_deterministic(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.rb import get_metadata

    fixture = md_test_fixture(file_ext="rb", file_num=1, verify_hash=True)
    md_1 = get_metadata(fixture)
    md_2 = get_metadata(fixture)

    assert _snapshot(md_1) == _snapshot(md_2)
    assert bool(_first(md_1.title))
