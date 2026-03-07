from __future__ import annotations

import io
from collections.abc import Mapping
from pathlib import Path

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
    identifiers = {}
    try:
        identifiers = {str(k): sorted(str(v) for v in vals) for k, vals in (md.get_identifiers() or {}).items()}
    except Exception:
        identifiers = {}
    return {
        "title": _first(getattr(md, "title", None)),
        "authors": sorted(_values(getattr(md, "authors", None))),
        "comments": sorted(_values(getattr(md, "comments", None))),
        "publisher": _first(getattr(md, "publisher", None)),
        "tags": sorted(_values(getattr(md, "tags", None))),
        "identifiers": identifiers,
    }


def _rtf_payload(info_block: bytes) -> bytes:
    return b"{\\rtf1\\ansi\\ansicpg1252" + info_block + b"\\n\\sect\\nBody\\n}"


def test_rtf_metadata_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.rtf as rtf_md

    assert rtf_md is not None


def test_rtf_reader_plugin_is_available_and_preserves_stream_position() -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    payload = _rtf_payload(b"{\\info{\\title Plugin Title}{\\author Plugin Author}}")
    stream = io.BytesIO(payload)
    stream.seek(6)

    plugins = get_metadata_reader_plugins()
    rtf_cls = next((p for p in plugins if p.__name__ == "RTFMetadataReader"), None)
    assert rtf_cls is not None

    reader = rtf_cls(None)
    md = reader.get_metadata(stream=stream, ftype="rtf")

    assert stream.tell() == 6
    assert md.title == "Plugin Title"
    assert _values(md.authors) == ["Plugin Author"]


def test_rtf_get_metadata_parses_unicode_and_cp1252_escapes() -> None:
    from LiuXin_alpha.metadata.file_sources.rtf import get_metadata

    info = (
        b"{\\info"
        b"{\\title Caf\\'e9 \\u945? \\u26481?}"
        b"{\\author Jos\\'e9 and Ana\\'efs}"
        b"{\\subject Subject Text}"
        b"{\\comment Comment Wins}"
        b"{\\manager Pub House}"
        b"{\\category tag-one, tag-two}"
        b"{\\keywords tag-three}"
        b"}"
    )
    md = get_metadata(io.BytesIO(_rtf_payload(info)))

    assert "Café" in md.title
    assert "α" in md.title
    assert "東" in md.title
    assert _values(md.authors) == ["José", "Anaïs"]
    assert _first(md.comments) == "Comment Wins"
    assert _first(md.publisher) == "Pub House"
    tags = {x.lower() for x in _values(md.tags)}
    assert tags >= {"tag-one", "tag-two", "tag-three"}


def test_rtf_get_metadata_pathlike_input(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.rtf import get_metadata

    path = tmp_path / "sample.rtf"
    payload = _rtf_payload(b"{\\info{\\title Path Title}{\\author Path Author}}")
    path.write_bytes(payload)

    md = get_metadata(path)
    assert md.title == "Path Title"
    assert _values(md.authors) == ["Path Author"]


def test_rtf_invalid_payload_returns_safe_default() -> None:
    from LiuXin_alpha.metadata.file_sources.rtf import get_metadata

    md = get_metadata(io.BytesIO(b"not-an-rtf"))
    assert _first(md.title) == "Unknown"
    assert _values(md.authors) == ["Unknown"]


def test_rtf_set_metadata_roundtrip_unicode() -> None:
    from LiuXin_alpha.metadata.file_sources.rtf import get_metadata, set_metadata

    stream = io.BytesIO(b"{\\rtf1\\ansi\\ansicpg1252\\deff0 {\\fonttbl{\\f0 Arial;}}\\n\\par body\\n}")
    mi = calibreMetaInformation("Updated Καλημέρα 😀", ["Alice", "Боб"])
    mi.comments = "Comment こんにちは"
    mi.publisher = "Pub Δ"
    mi.tags = ["one", "two"]

    set_metadata(stream, mi)
    stream.seek(0)
    out = stream.read()
    assert b"{\\info" in out

    parsed = get_metadata(io.BytesIO(out))
    assert "Updated" in parsed.title
    assert _values(parsed.authors) == ["Alice", "Боб"]
    assert "Comment" in _first(parsed.comments)
    assert _first(parsed.publisher) == "Pub Δ"
    assert {"one", "two"} <= set(_values(parsed.tags))


def test_rtf_set_metadata_salvages_malformed_info_block() -> None:
    from LiuXin_alpha.metadata.file_sources.rtf import get_metadata, set_metadata

    # Existing info block contains invalid bytes and malformed structure.
    stream = io.BytesIO(
        b"{\\rtf1\\ansi\\ansicpg1252{\\info{\\title Broken\xff\xfe}{\\author A\\'e9}}\\n\\par body\\n}"
    )
    mi = calibreMetaInformation("Rescue Καλημέρα 😀", ["Alice", "Боб"])
    mi.comments = "Recovered comments"
    mi.tags = ["rtf-rescue", "δοκιμή"]

    set_metadata(stream, mi)
    stream.seek(0)
    out = stream.read()
    assert b"{\\info" in out

    parsed = get_metadata(io.BytesIO(out))
    assert "Rescue" in str(parsed.title)
    assert any("Alice" in str(author) for author in _values(parsed.authors))
    assert "Recovered comments" in str(_first(parsed.comments))
    assert "rtf-rescue" in {str(tag).casefold() for tag in _values(parsed.tags)}


def test_rtf_md_fixture_smoke_and_deterministic(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.rtf import get_metadata

    fixture = md_test_fixture(file_ext="rtf", file_num=1, verify_hash=True)
    md_1 = get_metadata(fixture)
    md_2 = get_metadata(fixture)

    assert _snapshot(md_1) == _snapshot(md_2)
    assert bool(_first(md_1.title))
