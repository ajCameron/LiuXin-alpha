from __future__ import annotations

import io
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


def _build_imp_payload(
    *,
    category: str = "Fiction",
    title: str = "A Sample Title",
    author: str = "Alice and Bob",
    magic: bytes = b"\x00\x01BOOKDOUG",
    encoding: str = "utf-8",
) -> bytes:
    return b"".join(
        [
            magic,
            b"\x00" * 38,
            "ignored".encode(encoding) + b"\x00",
            category.encode(encoding) + b"\x00",
            b"skip-title\x00" + title.encode(encoding) + b"\x00",
            b"skip-author-1\x00skip-author-2\x00" + author.encode(encoding) + b"\x00",
        ]
    )


def test_imp_metadata_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.imp as imp_md

    assert imp_md is not None


def test_imp_reader_plugin_is_available_and_uses_stream_without_cursor_drift() -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    payload = _build_imp_payload(title="Plugin Title", author="Plugin Author")
    stream = io.BytesIO(payload)

    plugins = get_metadata_reader_plugins()
    imp_cls = next((p for p in plugins if p.__name__ == "IMPMetadataReader"), None)
    assert imp_cls is not None

    reader = imp_cls(None)
    metadata = reader.get_metadata(stream=stream, ftype="imp")
    assert stream.tell() == 0
    assert metadata.title == "Plugin Title"
    assert _values(metadata.authors) == ["Plugin Author"]


def test_imp_get_metadata_reads_title_authors_and_category() -> None:
    from LiuXin_alpha.metadata.file_sources.imp import get_metadata

    payload = _build_imp_payload(
        category="Science Fiction",
        title="Across The Stars",
        author="Renée Faßbinder and 李白",
    )
    md = get_metadata(io.BytesIO(payload))

    assert md.title == "Across The Stars"
    assert _values(md.authors) == ["Renée Faßbinder", "李白"]
    assert getattr(md, "category", None) == "Science Fiction"


def test_imp_get_metadata_pathlike_input(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.imp import get_metadata

    path = tmp_path / "sample.imp"
    path.write_bytes(_build_imp_payload(title="Path Title", author="Path Author"))

    md = get_metadata(path)
    assert md.title == "Path Title"
    assert _first(md.authors) == "Path Author"


def test_imp_invalid_magic_returns_safe_default() -> None:
    from LiuXin_alpha.metadata.file_sources.imp import get_metadata

    md = get_metadata(io.BytesIO(b"not-an-imp-file"))
    assert md.title == "Unknown"
    assert _first(md.authors) == "Unknown"


def test_imp_truncated_payload_fails_gracefully() -> None:
    from LiuXin_alpha.metadata.file_sources.imp import get_metadata

    payload = (
        b"\x00\x01BOOKDOUG"
        + b"\x00" * 38
        + b"ignored\x00"
        + b"category-without-null-termination-and-no-more-bytes"
    )
    md = get_metadata(io.BytesIO(payload))

    assert md is not None
    assert _first(md.title) in {"Unknown", "category-without-null-termination-and-no-more-bytes"}


def test_imp_cp1252_payload_decodes_accented_text() -> None:
    from LiuXin_alpha.metadata.file_sources.imp import get_metadata

    payload = _build_imp_payload(
        category="Roman",
        title="Café naïve",
        author="José and Anaïs",
        encoding="cp1252",
    )
    md = get_metadata(io.BytesIO(payload))

    assert md.title == "Café naïve"
    assert _values(md.authors) == ["José", "Anaïs"]
