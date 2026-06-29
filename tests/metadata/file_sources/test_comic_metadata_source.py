from __future__ import annotations

import io
import json
import zipfile
from pathlib import Path

import pytest


def _build_cbz_stream(comment: bytes = b"", members: dict[str, bytes] | None = None) -> io.BytesIO:
    if members is None:
        members = {"page-002.png": b"png bytes", "page-001.jpg": b"jpg bytes"}
    stream = io.BytesIO()
    with zipfile.ZipFile(stream, "w") as zf:
        for name, payload in members.items():
            zf.writestr(name, payload)
        zf.comment = comment
    stream.seek(0)
    stream.name = "comic.cbz"
    return stream


def test_comic_metadata_module_import_smoke() -> None:
    import LiuXin_alpha.metadata.file_sources.comic as comic_md

    assert comic_md is not None


def test_cbz_metadata_reads_comment_cover_and_preserves_cursor() -> None:
    from LiuXin_alpha.metadata.file_sources.comic import get_metadata

    payload = {
        "ComicBookInfo/1.0": {
            "series": "Mystery Files",
            "issue": 12,
            "volume": 2,
            "title": "The Red Door",
            "credits": [{"role": "Creator", "person": "Lane, Alex"}],
        }
    }
    stream = _build_cbz_stream(json.dumps(payload).encode("utf-8"))
    stream.seek(7)

    mi = get_metadata(stream, ftype="cbz", series_index="issue")

    assert mi.series == "Mystery Files"
    assert mi.series_index == 12.0
    assert mi.title == "The Red Door"
    assert mi.authors == ["Alex Lane"]
    assert mi.cover_data == ("jpg", b"jpg bytes")
    assert stream.tell() == 7


def test_cbz_metadata_allows_valid_comic_without_comment() -> None:
    from LiuXin_alpha.metadata.file_sources.comic import get_metadata

    mi = get_metadata(_build_cbz_stream(b"not-json"), ftype="cbz")

    assert mi.title == "Unknown"
    assert mi.authors == ["Unknown"]
    assert mi.cover_data == ("jpg", b"jpg bytes")


def test_comic_metadata_rejects_empty_or_non_comic_cbz() -> None:
    from LiuXin_alpha.metadata.file_sources.comic import ComicFormatError, get_metadata

    empty_zip = _build_cbz_stream(members={})
    with pytest.raises(ComicFormatError, match="does not contain"):
        get_metadata(empty_zip, ftype="cbz")

    text_zip = _build_cbz_stream(members={"readme.txt": b"text"})
    with pytest.raises(ComicFormatError, match="does not contain"):
        get_metadata(text_zip, ftype="cbz")


def test_comic_metadata_rejects_wrong_format_and_fallback_is_opt_in() -> None:
    from LiuXin_alpha.metadata.file_sources.comic import ComicFormatError, get_metadata

    stream = io.BytesIO(b"not a comic")
    stream.name = "Bad Comic.cbz"
    stream.seek(4)

    with pytest.raises(ComicFormatError, match="Not a valid comic archive"):
        get_metadata(stream, ftype="cbz")
    assert stream.tell() == 4

    mi = get_metadata(stream, ftype="cbz", fallback_on_parse_error=True)
    assert mi.title == "Bad Comic"
    assert mi.authors == ["Unknown"]
    assert stream.tell() == 4


def test_comic_metadata_accepts_pathlike_input(tmp_path: Path) -> None:
    from LiuXin_alpha.metadata.file_sources.comic import get_metadata_inplace

    path = tmp_path / "pathlike.cbz"
    path.write_bytes(_build_cbz_stream().getvalue())

    mi = get_metadata_inplace(path, ftype="cbz")

    assert mi.cover_data == ("jpg", b"jpg bytes")


def test_comic_metadata_reader_plugin_uses_series_index_customization() -> None:
    from LiuXin_alpha.customize.builtins.metadata_readers import get_metadata_reader_plugins

    payload = {
        "ComicBookInfo/1.0": {
            "series": "Customized",
            "issue": 9,
            "volume": 1,
            "title": "Custom Issue",
        }
    }
    stream = _build_cbz_stream(json.dumps(payload).encode("utf-8"))

    plugins = get_metadata_reader_plugins()
    comic_cls = next((p for p in plugins if p.__name__ == "ComicMetadataReader"), None)
    assert comic_cls is not None

    reader = comic_cls(None)
    reader.site_customization = "issue"
    mi = reader.get_metadata(stream=stream, ftype="cbz")

    assert mi.series == "Customized"
    assert mi.series_index == 9.0


def test_cbr_metadata_uses_rar_extractor_and_comment_metadata(monkeypatch) -> None:
    from LiuXin_alpha.metadata.file_sources import comic as comic_md

    stream = io.BytesIO(b"Rar!payload")
    stream.name = "comic.cbr"
    stream.seek(4)

    monkeypatch.setattr(comic_md, "archive_type", lambda _stream: "rar")

    import LiuXin_alpha.utils.decompression.unrar as unrar_mod

    monkeypatch.setattr(unrar_mod, "extract_first_alphabetically", lambda _stream: ("page-001.webp", b"webp bytes"))

    def _fake_comment(_stream, _stream_type, series_index="volume"):
        from LiuXin_alpha.metadata.utils import calibreMetaInformation

        md = calibreMetaInformation("CBR Title", ["Artist"])
        md.series = "CBR Series"
        md.series_index = 4.0 if series_index == "issue" else 1.0
        return md

    monkeypatch.setattr(comic_md, "get_comic_metadata", _fake_comment)

    mi = comic_md.get_metadata(stream, ftype="cbr", series_index="issue")

    assert mi.title == "CBR Title"
    assert mi.authors == ["Artist"]
    assert mi.series == "CBR Series"
    assert mi.series_index == 4.0
    assert mi.cover_data == ("webp", b"webp bytes")
    assert stream.tell() == 4
