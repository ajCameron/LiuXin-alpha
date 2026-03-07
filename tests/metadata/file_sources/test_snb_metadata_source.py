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


def _snapshot(md) -> dict:
    return {
        "title": _first(getattr(md, "title", None)),
        "authors": sorted(_values(getattr(md, "authors", None))),
        "language": _first(getattr(md, "language", None)),
        "publisher": _first(getattr(md, "publisher", None)),
        "tags": sorted(_values(getattr(md, "tags", None))),
    }


def _build_snb_bytes(tmp_path: Path, payloads: dict[str, bytes]) -> bytes:
    from LiuXin_alpha.file_formats.snb.snbfile import SNBFile

    src = tmp_path / "snb_src"
    src.mkdir()
    for rel, blob in payloads.items():
        p = src / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(blob)

    snb = SNBFile()
    snb.FromDir(str(src))
    out = io.BytesIO()
    snb.Output(out)
    return out.getvalue()


def test_snb_metadata_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.snb as snb_md

    assert snb_md is not None


def test_snb_reader_plugin_is_available_and_preserves_stream_position(tmp_path: Path) -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    book = (
        "<book-snbf><head><name>Plugin Title</name><author>Plugin Author</author>"
        "<language>EN_US</language><publisher>Unit</publisher></head></book-snbf>"
    ).encode("utf-8")
    snb_bytes = _build_snb_bytes(tmp_path, {"snbf/book.snbf": book})
    stream = io.BytesIO(snb_bytes)
    stream.seek(9)

    plugins = get_metadata_reader_plugins()
    snb_cls = next((p for p in plugins if p.__name__ == "SNBMetadataReader"), None)
    assert snb_cls is not None

    reader = snb_cls(None)
    md = reader.get_metadata(stream=stream, ftype="snb")

    assert stream.tell() == 9
    assert md.title == "Plugin Title"
    assert _values(md.authors) == ["Plugin Author"]
    assert _first(md.language) == "en-us"
    assert _first(md.publisher) == "Unit"


def test_snb_get_metadata_parses_unicode_tags_and_cover(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.snb import get_metadata

    book = (
        "<book-snbf><head>"
        "<name>Καλημέρα こんにちは 😀</name>"
        "<author>Renée &amp; 李白</author>"
        "<language>ZH_CN</language>"
        "<publisher>Pub Ω</publisher>"
        "<keywords>tag-one, tag-two; tag-three</keywords>"
        "<cover>cover.jpeg</cover>"
        "</head></book-snbf>"
    ).encode("utf-8")
    snb_bytes = _build_snb_bytes(
        tmp_path,
        {
            "snbf/book.snbf": book,
            "snbc/images/cover.jpeg": b"\xff\xd8\xff\xe0fake-jpeg",
        },
    )

    stream = io.BytesIO(snb_bytes)
    stream.seek(5)
    md = get_metadata(stream)

    assert stream.tell() == 5
    assert md.title == "Καλημέρα こんにちは 😀"
    assert _values(md.authors) == ["Renée", "李白"]
    assert _first(md.language) == "zh-cn"
    assert _first(md.publisher) == "Pub Ω"
    assert {"tag-one", "tag-two", "tag-three"} <= set(_values(md.tags))
    cover_fmt, cover_blob = md.cover_data
    assert cover_fmt == "jpg"
    assert cover_blob.startswith(b"\xff\xd8\xff")


def test_snb_get_metadata_pathlike_input(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.snb import get_metadata

    book = (
        "<book-snbf><head><name>Path Title</name><author>Path Author</author></head></book-snbf>"
    ).encode("utf-8")
    snb_bytes = _build_snb_bytes(tmp_path, {"snbf/book.snbf": book})

    path = tmp_path / "path_case.snb"
    path.write_bytes(snb_bytes)

    md = get_metadata(path)
    assert md.title == "Path Title"
    assert _values(md.authors) == ["Path Author"]


def test_snb_invalid_payload_returns_safe_default() -> None:
    from LiuXin_alpha.metadata.file_sources.snb import get_metadata

    md = get_metadata(io.BytesIO(b"not-a-valid-snb"))
    assert _first(md.title) == "Unknown"
    assert _values(md.authors) == ["Unknown"]


def test_snb_missing_book_metadata_returns_safe_default(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.snb import get_metadata

    snb_bytes = _build_snb_bytes(tmp_path, {"snbf/toc.snbf": b"<toc-snbf><head/></toc-snbf>"})
    md = get_metadata(io.BytesIO(snb_bytes))

    assert _first(md.title) == "Unknown"
    assert _values(md.authors) == ["Unknown"]


def test_snb_malformed_book_metadata_xml_fails_gracefully(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.snb import get_metadata

    snb_bytes = _build_snb_bytes(tmp_path, {"snbf/book.snbf": b"<book-snbf><head><name>Broken"})
    md = get_metadata(io.BytesIO(snb_bytes))

    assert _first(md.title) in {"Unknown", "Broken"}
    assert _values(md.authors) == ["Unknown"]


def test_snb_md_fixture_smoke_and_deterministic(md_test_fixture) -> None:
    from LiuXin_alpha.metadata.file_sources.snb import get_metadata

    fixture = md_test_fixture(file_ext="snb", file_num=1, verify_hash=True)
    md_1 = get_metadata(fixture)
    md_2 = get_metadata(fixture)

    assert _snapshot(md_1) == _snapshot(md_2)
    assert md_1.title == "20,000 Leagues Under the Sea"
    assert _values(md_1.authors) == ["Jules Verne"]
