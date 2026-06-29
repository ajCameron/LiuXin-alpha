from __future__ import annotations

import io
import zipfile
from collections.abc import Mapping
from pathlib import Path

import pytest


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
        "publisher": _first(getattr(md, "publisher", None)),
        "rights": _first(getattr(md, "rights", None)),
        "isbn": _first(getattr(md, "isbn", None)),
        "cover": bool(_first(getattr(md, "cover_data", None))),
        "identifiers": identifiers,
    }


def _build_pml_comment(**fields: str) -> bytes:
    inner = " ".join(f'{key}="{value}"' for key, value in fields.items())
    return f"\\v{inner}\\v".encode("utf-8")


def _zip_bytes(entries: dict[str, bytes]) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, payload in entries.items():
            zf.writestr(name, payload)
    return out.getvalue()


def test_pml_metadata_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.pml as pml_md

    assert pml_md is not None


def test_pml_reader_plugin_is_available_and_preserves_stream_position() -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    payload = _build_pml_comment(TITLE="Plugin Title", AUTHOR="Plugin Author")
    stream = io.BytesIO(payload)
    stream.name = "plugin.pml"
    stream.seek(5)

    plugins = get_metadata_reader_plugins()
    pml_cls = next((p for p in plugins if p.__name__ == "PMLMetadataReader"), None)
    assert pml_cls is not None

    reader = pml_cls(None)
    md = reader.get_metadata(stream=stream, ftype="pml")

    assert stream.tell() == 5
    assert md.title == "Plugin Title"
    assert _values(md.authors) == ["Plugin Author"]


def test_pml_get_metadata_parses_cp1252_and_sanitizes_xml_controls() -> None:
    from LiuXin_alpha.metadata.file_sources.pml import get_metadata

    payload = (
        b'\\vTITLE="Caf\xe9 <Book>\x01" AUTHOR="Alice and Bob" '
        b'PUBLISHER="Pub & Co" COPYRIGHT="(c) 2026" ISBN="9780306406157"\\v'
    )
    md = get_metadata(io.BytesIO(payload), extract_cover=False)

    assert md.title == "Café &lt;Book&gt;"
    assert _values(md.authors) == ["Alice", "Bob"]
    assert _first(md.publisher) == "Pub &amp; Co"
    assert _first(md.rights) == "(c) 2026"
    assert _first(md.isbn) == "9780306406157"


def test_pml_get_metadata_parses_utf8_unicode_torture() -> None:
    from LiuXin_alpha.metadata.file_sources.pml import get_metadata

    payload = _build_pml_comment(
        TITLE="Καλημέρα こんにちは 😀",
        AUTHOR="Renée & 李白",
        PUBLISHER="出版者",
    )
    md = get_metadata(io.BytesIO(payload), extract_cover=False)

    assert md.title == "Καλημέρα こんにちは 😀"
    assert _values(md.authors) == ["Renée", "李白"]
    assert _first(md.publisher) == "出版者"


def test_plain_pml_binaryish_payload_returns_safe_default() -> None:
    from LiuXin_alpha.metadata.file_sources.pml import get_metadata

    for payload in (
        b"\x89PNG\r\n\x1a\n" + b"\x00" * 32,
        b"%PDF-1.7\nnot really pml",
        b"\x00\x01\x02\x03\x04",
    ):
        md = get_metadata(io.BytesIO(payload), extract_cover=False)
        assert _first(md.title) == "Unknown"
        assert _values(md.authors) == ["Unknown"]


def test_plain_pml_malformed_comments_are_ignored_safely() -> None:
    from LiuXin_alpha.metadata.file_sources.pml import get_metadata

    payload = b'\\vTITLE="Good Title" AUTHOR="Alice and Bob"\\v \\vTITLE="unterminated AUTHOR=Ignored\\v'
    md = get_metadata(io.BytesIO(payload), extract_cover=False)

    assert md.title == "Good Title"
    assert _values(md.authors) == ["Alice", "Bob"]


def test_plain_pml_control_characters_do_not_leak_from_fields() -> None:
    from LiuXin_alpha.metadata.file_sources.pml import get_metadata

    payload = _build_pml_comment(TITLE="A\x00B\x01C", PUBLISHER="Pub\x02 & Co", AUTHOR="Renée & 李白")
    md = get_metadata(io.BytesIO(payload), extract_cover=False)

    assert md.title == "ABC"
    assert _first(md.publisher) == "Pub &amp; Co"
    assert _values(md.authors) == ["Renée", "李白"]


def test_pmlz_metadata_extracts_from_embedded_pml_and_index_cover() -> None:
    from LiuXin_alpha.metadata.file_sources.pml import get_metadata

    payload = _zip_bytes(
        {
            "index.pml": _build_pml_comment(TITLE="Zip Title", AUTHOR="Zip Author"),
            "index_img/cover.png": b"\x89PNG\r\nzip-cover",
        }
    )
    stream = io.BytesIO(payload)
    stream.name = "library_copy_001.pmlz"
    md = get_metadata(stream, extract_cover=True)

    assert md.title == "Zip Title"
    assert _values(md.authors) == ["Zip Author"]
    ext, data = _first(md.cover_data)
    assert ext == "png"
    assert data == b"\x89PNG\r\nzip-cover"


def test_pmlz_metadata_works_for_nameless_stream_via_zip_detection() -> None:
    from LiuXin_alpha.metadata.file_sources.pml import get_metadata

    payload = _zip_bytes({"book.pml": _build_pml_comment(TITLE="No Name Stream")})
    stream = io.BytesIO(payload)
    md = get_metadata(stream, extract_cover=False)

    assert md.title == "No Name Stream"


def test_pmlz_invalid_archive_raises_by_default_and_can_opt_into_fallback() -> None:
    from LiuXin_alpha.metadata.file_sources.pml import PmlFormatError, get_metadata

    stream = io.BytesIO(b"not-a-zip")
    stream.name = "broken.pmlz"
    with pytest.raises(PmlFormatError):
        get_metadata(stream)

    md = get_metadata(stream, fallback_on_parse_error=True)
    assert _first(md.title) == "Unknown"
    assert _values(md.authors) == ["Unknown"]


def test_pml_pathlike_cover_lookup_uses_name_img_folder(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.pml import get_metadata

    pml_path = tmp_path / "sample.pml"
    pml_path.write_bytes(_build_pml_comment(TITLE="Path Title", AUTHOR="Path Author"))
    (tmp_path / "sample_img").mkdir()
    (tmp_path / "sample_img" / "cover.png").write_bytes(b"\x89PNG\r\npath-cover")

    md = get_metadata(pml_path, extract_cover=True)
    assert md.title == "Path Title"
    ext, data = _first(md.cover_data)
    assert ext == "png"
    assert data == b"\x89PNG\r\npath-cover"


def test_pml_md_fixtures_read_smoke_and_deterministic(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.pml import get_metadata_inplace

    pml_path = md_test_fixture(file_ext="pml", file_num=1, verify_hash=True)
    pmlz_path = md_test_fixture(file_ext="pmlz", file_num=1, verify_hash=True)

    pml_1 = get_metadata_inplace(pml_path)
    pml_2 = get_metadata_inplace(pml_path)
    pmlz_1 = get_metadata_inplace(pmlz_path)
    pmlz_2 = get_metadata_inplace(pmlz_path)

    assert _snapshot(pml_1) == _snapshot(pml_2)
    assert _snapshot(pmlz_1) == _snapshot(pmlz_2)
    assert _first(pml_1.title) is not None
    assert _first(pmlz_1.title) is not None
