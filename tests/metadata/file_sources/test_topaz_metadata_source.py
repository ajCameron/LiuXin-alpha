from __future__ import annotations

import io
from collections.abc import Mapping
from pathlib import Path

import pytest

from LiuXin_alpha.metadata.utils import calibreMetaInformation


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
    return {
        "title": _first(getattr(md, "title", None)),
        "authors": sorted(_values(getattr(md, "authors", None))),
    }


def _encode_vwi(value: int) -> bytes:
    if value < 0:
        raise ValueError("negative values are not supported")
    parts = [value & 0x7F]
    value >>= 7
    while value:
        parts.append((value & 0x7F) | 0x80)
        value >>= 7
    return bytes(reversed(parts))


def _build_topaz_bytes(
    *,
    title: str = "Topaz Title",
    authors: str = "Topaz Author",
    extra_fields: dict[str, bytes] | None = None,
    trailing: bytes = b"",
) -> bytes:
    fields = [
        ("Title", title.encode("utf-8", "replace")),
        ("Authors", authors.encode("utf-8", "replace")),
    ]
    for key, value in (extra_fields or {}).items():
        fields.append((str(key), bytes(value)))

    metadata_block = bytearray()
    metadata_block.extend(_encode_vwi(len(b"metadata")))
    metadata_block.extend(b"metadata")
    metadata_block.append(0)
    metadata_block.append(len(fields) & 0xFF)

    for key, value in fields:
        key_bytes = key.encode("ascii", "replace")
        metadata_block.extend(_encode_vwi(len(key_bytes)))
        metadata_block.extend(key_bytes)
        metadata_block.extend(_encode_vwi(len(value)))
        metadata_block.extend(value)

    # Keep compatibility with update logic in TopazMetadataUpdater:
    # len_uncomp tracks payload length relative to the historical 10-byte prefix.
    len_uncomp = max(0, len(metadata_block) - (len("metadata") + 2))

    header = bytearray(b"TPZ0")
    header.extend(_encode_vwi(1))  # number of header records
    header.extend(b"c")
    header.extend(_encode_vwi(len(b"metadata")))
    header.extend(b"metadata")
    header.extend(_encode_vwi(1))  # one metadata block
    header.extend(_encode_vwi(0))  # metadata block offset from base
    header.extend(_encode_vwi(len_uncomp))
    header.extend(_encode_vwi(len_uncomp))
    header.extend(b"d")  # eoth marker

    return bytes(header) + bytes(metadata_block) + trailing


def test_topaz_metadata_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.topaz as topaz_md

    assert topaz_md is not None


def test_topaz_reader_plugin_is_available_and_preserves_stream_position() -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    stream = io.BytesIO(_build_topaz_bytes(title="Plugin Title", authors="Alice; Bob"))
    stream.seek(4)

    plugins = get_metadata_reader_plugins()
    topaz_cls = next((p for p in plugins if p.__name__ == "TOPAZMetadataReader"), None)
    assert topaz_cls is not None

    reader = topaz_cls(None)
    md = reader.get_metadata(stream=stream, ftype="tpz")

    assert stream.tell() == 4
    assert md.title == "Plugin Title"
    assert _values(md.authors) == ["Alice", "Bob"]


def test_topaz_get_metadata_parses_unicode_and_multiauthor() -> None:
    from LiuXin_alpha.metadata.file_sources.topaz import get_metadata

    payload = _build_topaz_bytes(title="Καλημέρα こんにちは 😀", authors="Renée; 李白; Alice & Bob")
    md = get_metadata(io.BytesIO(payload))

    assert md.title == "Καλημέρα こんにちは 😀"
    assert _values(md.authors) == ["Renée", "李白", "Alice", "Bob"]


def test_topaz_get_metadata_pathlike_input(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.topaz import get_metadata

    path = tmp_path / "sample.tpz"
    path.write_bytes(_build_topaz_bytes(title="Path Title", authors="Path Author"))

    md = get_metadata(path)
    assert md.title == "Path Title"
    assert _values(md.authors) == ["Path Author"]


def test_topaz_set_metadata_roundtrip_unicode() -> None:
    from LiuXin_alpha.metadata.file_sources.topaz import get_metadata, set_metadata

    stream = io.BytesIO(_build_topaz_bytes(title="Before", authors="Old One; Old Two", trailing=b"TAIL"))
    mi = calibreMetaInformation("Updated Καλημέρα 😀", ["Alice", "Боб"])

    set_metadata(stream, mi)
    stream.seek(0)
    out = stream.read()
    assert out.startswith(b"TPZ")
    assert b"TAIL" in out

    parsed = get_metadata(io.BytesIO(out))
    assert parsed.title == "Updated Καλημέρα 😀"
    assert _values(parsed.authors) == ["Alice", "Боб"]


def test_topaz_set_metadata_sanitizes_hostile_text_and_preserves_payload() -> None:
    from LiuXin_alpha.metadata.file_sources.topaz import get_metadata, set_metadata

    stream = io.BytesIO(
        _build_topaz_bytes(
            title="Before",
            authors="Old One; Old Two",
            extra_fields={"bookLength": b"12345"},
            trailing=b"TAIL",
        )
    )
    title = "Topaz\x00Title\ud800 😀"
    authors = ["Alice\x01 One", "Bob\udfff Two", "李白"]
    mi = calibreMetaInformation(title, authors)

    set_metadata(stream, mi)
    out = stream.getvalue()

    assert out.startswith(b"TPZ")
    assert b"bookLength" in out
    assert b"12345" in out
    assert b"TAIL" in out

    parsed = get_metadata(io.BytesIO(out))
    assert parsed.title == "TopazTitle 😀"
    assert _values(parsed.authors) == ["Alice One", "Bob Two", "李白"]

    assert mi.title == title
    assert mi.authors == authors


def test_topaz_set_metadata_invalid_payload_raises_clean_error() -> None:
    from LiuXin_alpha.metadata.file_sources.topaz import set_metadata

    stream = io.BytesIO(b"not-a-topaz-file")
    mi = calibreMetaInformation("Updated", ["Author"])

    with pytest.raises(ValueError, match="Not a Topaz file"):
        set_metadata(stream, mi)


def test_topaz_invalid_payload_raises_by_default_and_can_opt_into_fallback() -> None:
    from LiuXin_alpha.metadata.file_sources.topaz import TopazFormatError, get_metadata

    with pytest.raises(TopazFormatError):
        get_metadata(io.BytesIO(b"not-a-topaz-file"))

    md = get_metadata(io.BytesIO(b"not-a-topaz-file"), fallback_on_parse_error=True)
    assert _first(md.title) == "Unknown"
    assert _values(md.authors) == ["Unknown"]


def test_topaz_metadata_deterministic_for_same_payload() -> None:
    from LiuXin_alpha.metadata.file_sources.topaz import get_metadata

    payload = _build_topaz_bytes(
        title="Deterministic",
        authors="One; Two",
        extra_fields={"bookLength": b"12345"},
    )
    md_1 = get_metadata(io.BytesIO(payload))
    md_2 = get_metadata(io.BytesIO(payload))
    assert _snapshot(md_1) == _snapshot(md_2)
