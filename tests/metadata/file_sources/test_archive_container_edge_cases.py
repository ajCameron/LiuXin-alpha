from __future__ import annotations

import io
import json
import os
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.metadata.utils import calibreMetaInformation
from LiuXin_alpha.utils.libraries.liuxin_etree import etree


def _values(raw):
    if raw is None:
        return []
    if isinstance(raw, dict):
        return list(raw.keys())
    if isinstance(raw, str):
        return [raw]
    try:
        return list(raw)
    except TypeError:
        return [raw]


def _zip_bytes(entries: dict[str, bytes], *, comment: bytes = b"") -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as zf:
        for name, payload in entries.items():
            zf.writestr(name, payload)
        zf.comment = comment
    return out.getvalue()


class _TextHeaderStream:
    def read(self, _n):
        return "Rar!"

    def tell(self):
        raise OSError("tell unavailable")


class _SeekBrokenBytes(io.BytesIO):
    name = "seek-broken.zip"

    def seek(self, pos, whence=os.SEEK_SET):
        if getattr(self, "_break_restore", False) and pos != 0:
            raise OSError("restore unavailable")
        return super().seek(pos, whence)


class _TimestampRaises:
    @property
    def timestamp(self):
        return "old"

    @timestamp.setter
    def timestamp(self, _value):
        raise RuntimeError("timestamp backend unavailable")


def test_archive_private_decoders_and_comic_edges(monkeypatch, tmp_path: Path) -> None:
    import LiuXin_alpha.metadata.file_sources.archive as archive_md

    assert list(archive_md._iter_clean_names(["folder", "Thumbs.db", r"dir\page.JPG"])) == ["dir/page.JPG"]
    assert archive_md.archive_type(_TextHeaderStream()) == "rar"
    assert archive_md._safe_int("7") == 7
    assert archive_md._safe_int("bad", default=3) == 3
    assert archive_md._safe_float("7.5") == 7.5
    assert archive_md._safe_float("bad") is None

    wrapped = b"\xef\xbb\xbfnoise " + json.dumps({"ComicBookInfo/1.0": {"title": "Wrapped"}}).encode() + b" tail\x00"
    assert archive_md._decode_json_payload(wrapped)["ComicBookInfo/1.0"]["title"] == "Wrapped"
    cp1252 = '{"ComicBookInfo/1.0": {"title": "Caf\xe9"}}'.encode("cp1252")
    assert archive_md._decode_json_payload(cp1252)["ComicBookInfo/1.0"]["title"] == "Café"
    assert archive_md._decode_json_payload(b"\xff\xff") is None
    assert archive_md._decode_json_payload("[]") is None
    assert archive_md._decode_json_payload("\0") is None

    mi = calibreMetaInformation(None, ["Unknown"])
    archive_md.get_comic_book_info("not-a-mapping", mi)
    assert mi.title == "Unknown"

    monkeypatch.setattr(archive_md, "parse_only_date", lambda _raw: (_ for _ in ()).throw(RuntimeError("date fail")))
    archive_md.get_comic_book_info(
        {
            "series": "Unicode Série",
            "issue": "12.5",
            "rating": "-1",
            "title": "Issue 😀",
            "publicationYear": "2024",
            "publicationMonth": "99",
            "credits": [
                {"role": "Cartoonist", "person": "Faßbinder, Renée"},
                {"role": "Writer", "person": ""},
                "bad-credit",
            ],
        },
        mi,
        series_index="volume",
    )
    assert mi.series == "Unicode Série"
    assert mi.series_index == 12.5
    assert mi.title == "Issue 😀"
    assert mi.authors == ["Renée Faßbinder"]
    assert mi.pubdate.month == 6

    bad_zip = tmp_path / "bad.zip"
    bad_zip.write_bytes(b"not a zip")
    events = []
    monkeypatch.setattr(archive_md.default_log, "log_exception", lambda *args, **_kwargs: events.append(args))
    assert archive_md.ArchiveExtract(None).run(str(bad_zip)) == str(bad_zip)
    assert events and "Unable to inspect archive" in str(events[0][0])


def test_archive_comic_metadata_cbr_and_failure_paths(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.archive as archive_md
    import LiuXin_alpha.utils.decompression.unrar as unrar_mod

    class _FakeRARFile:
        def __init__(self, _stream, get_comment=False):
            assert get_comment is True
            self.comment = json.dumps(
                {"ComicBookInfo/1.0": {"series": "CBR Série", "volume": 3, "title": "CBR Title"}}
            )

    monkeypatch.setattr(unrar_mod, "RARFile", _FakeRARFile)
    stream = io.BytesIO(b"Rar!payload")
    stream.seek(4)
    mi = archive_md.get_comic_metadata(stream, "cbr")
    assert mi.series == "CBR Série"
    assert mi.series_index == 3.0
    assert mi.title == "CBR Title"
    assert stream.tell() == 4

    events = []
    monkeypatch.setattr(archive_md.default_log, "log_exception", lambda *args, **_kwargs: events.append(args))

    class _BrokenRARFile:
        def __init__(self, *_args, **_kwargs):
            raise RuntimeError("rar comment failed")

    monkeypatch.setattr(unrar_mod, "RARFile", _BrokenRARFile)
    assert archive_md.get_comic_metadata(io.BytesIO(b"Rar!payload"), "cbr").title == "Unknown"
    assert any("comic archive metadata comment" in str(event[0]) for event in events)


def test_zip_helpers_opf_cover_resolution_and_stream_edges(monkeypatch, tmp_path: Path) -> None:
    import LiuXin_alpha.file_formats.opf.opf2 as opf2_mod
    import LiuXin_alpha.metadata.file_sources.zip as zip_md

    assert zip_md._normalize_member_name(r".\Books\Name.OPF") == "Books/Name.OPF"
    assert zip_md._member_type("Books/Name.OPF") == "opf"
    assert zip_md._find_first_supported_member(["readme.txt", "book.PDF"]) == ("book.PDF", "pdf")
    assert zip_md._find_first_supported_member(["readme.txt"]) is None
    assert zip_md._source_label(SimpleNamespace(name="/tmp/Bundle.zip")) == "Bundle.zip"
    assert zip_md._source_label(SimpleNamespace()) == "<stream>"

    zip_md._set_timestamp_none(_TimestampRaises())

    class _FakeOPF:
        def __init__(self, stream, opf_dir):
            self.stream = stream
            self.opf_dir = opf_dir

        def to_book_metadata(self):
            mi = calibreMetaInformation("OPF Zip — 世界", ["Zip Author"])
            mi.cover = "cover.jpg"
            return mi

    monkeypatch.setattr(opf2_mod, "OPF", _FakeOPF)
    payload = _zip_bytes(
        {
            "OEBPS/content.opf": b"<package/>",
            "OEBPS/cover.jpg": b"cover-data",
            "cover.jpg": b"root-cover",
            "OEBPS/": b"",
        }
    )
    from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile

    with ZipFile(io.BytesIO(payload), "r") as zf:
        mi = zip_md.zip_opf_metadata("OEBPS/content.opf", zf)
    assert mi.title == "OPF Zip — 世界"
    assert mi.cover is None
    assert mi.cover_data == ("jpg", b"cover-data")

    monkeypatch.setattr(zip_md, "_read_metadata_from_zip_stream", lambda _stream: calibreMetaInformation("Restored", ["A"]))
    broken = _SeekBrokenBytes(b"not-really-used")
    broken.seek(3)
    broken._break_restore = True
    assert zip_md.get_metadata(broken).title == "Restored"

    with pytest.raises(TypeError):
        zip_md.get_metadata(object())
    path = tmp_path / "path.zip"
    path.write_bytes(b"unused")
    assert zip_md.get_metadata_inplace(path).title == "Restored"


def test_extz_helpers_cover_resolution_writer_and_error_edges(monkeypatch, tmp_path: Path) -> None:
    import LiuXin_alpha.metadata.file_sources.extz as extz
    from LiuXin_alpha.utils.calibre_compat.ebooks.metadata.book.base import Metadata as OPFCalibreMetadata

    assert extz._is_path_like(b"bytes-path")
    assert extz._source_name(SimpleNamespace(name="stream.extz")) == "stream.extz"
    assert extz._fallback_metadata().title == "Unknown"
    assert extz._looks_like_cover_path("cover.WEBP")
    assert not extz._looks_like_cover_path("cover.txt")
    assert extz._manifest_href_by_id(SimpleNamespace(itermanifest=lambda: [{"id": "cover", "href": "img.png"}]), "cover") == "img.png"
    assert extz._manifest_href_by_id(SimpleNamespace(itermanifest=lambda: [{"id": "cover"}]), "cover") is None
    assert extz._manifest_href_by_id(SimpleNamespace(itermanifest=lambda: []), None) is None

    opf_md = OPFCalibreMetadata("OPF Calibre", ["Author"])
    assert extz._as_opf_calibre_metadata(opf_md) is opf_md
    assert extz._as_opf_calibre_metadata(SimpleNamespace(to_calibre=lambda: opf_md)) is opf_md
    converted = calibreMetaInformation("Converted", ["Author"])
    assert extz._as_opf_calibre_metadata(SimpleNamespace(to_calibre=lambda: converted)).title == "Converted"
    assert extz._as_opf_calibre_metadata(SimpleNamespace(title="Duck", authors=["Typed"], tags=[])).title == "Duck"

    class _MetaNode:
        def __init__(self, content):
            self.content = content

        def get(self, key):
            return self.content if key == "content" else None

    class _Root:
        def __init__(self, values=(), raises=False):
            self.values = values
            self.raises = raises

        def xpath(self, _expr):
            if self.raises:
                raise RuntimeError("xpath fail")
            return [SimpleNamespace(text=value) for value in self.values]

    class _Metadata:
        def __init__(self, values=()):
            self.values = values

        def xpath(self, _expr):
            return [_MetaNode(value) for value in self.values]

    fake = SimpleNamespace(raster_cover="raster.jpg")
    assert extz._resolve_cover_href(fake) == "raster.jpg"
    fake = SimpleNamespace(raster_cover=None, guide_raster_cover={"path": "guide.png"})
    assert extz._resolve_cover_href(fake) == "guide.png"
    fake = SimpleNamespace(raster_cover=None, guide_raster_cover=None, metadata=_Metadata(["direct.jpg"]))
    assert extz._resolve_cover_href(fake) == "direct.jpg"
    fake = SimpleNamespace(
        raster_cover=None,
        guide_raster_cover=None,
        metadata=_Metadata(["cover-id"]),
        itermanifest=lambda: [{"id": "cover-id", "href": "images/cover.png"}],
    )
    assert extz._resolve_cover_href(fake) == "images/cover.png"
    fake = SimpleNamespace(
        raster_cover=None,
        guide_raster_cover=None,
        metadata=_Metadata(["missing-id"]),
        itermanifest=lambda: [],
        guide_cover_path=lambda _root: ["not-cover.txt", "fallback.jpeg"],
        root=object(),
    )
    assert extz._resolve_cover_href(fake) == "fallback.jpeg"
    fake = SimpleNamespace(
        raster_cover=None,
        guide_raster_cover=None,
        metadata=_Metadata([]),
        guide_cover_path=lambda _root: (_ for _ in ()).throw(RuntimeError("guide fail")),
        root=_Root(["rel/cover.gif"]),
    )
    assert extz._resolve_cover_href(fake) == "rel/cover.gif"
    fake.root = _Root(raises=True)
    assert extz._resolve_cover_href(fake) is None
    assert extz._cover_member_from_opf(SimpleNamespace(raster_cover="/absolute.png"), "OPS/content.opf") == "absolute.png"

    archive = tmp_path / "bad-container.extz"
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("META-INF/container.xml", b"<container><rootfiles><rootfile full-path='nested/book.opf' /></rootfiles>")
        zf.writestr("nested/book.opf", b"<package/>")
    from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile

    with ZipFile(archive, "r") as zf:
        assert extz.get_first_opf_name(zf) == "nested/book.opf"

    with pytest.raises(TypeError):
        extz.get_metadata(object())
    with pytest.raises(TypeError):
        extz.set_metadata(object(), calibreMetaInformation("x", ["y"]))


def test_docx_fake_container_cover_read_and_write_edges(monkeypatch, tmp_path: Path) -> None:
    import LiuXin_alpha.metadata.file_sources.docx as docx_md

    class _Namespace:
        namespaces = {"ep": "urn:extended"}

        @staticmethod
        def get(image, key):
            return image.get(key)

        @staticmethod
        def XPath(_expr):
            return lambda _document: [
                {"r:embed": "missing"},
                {"r:embed": "bad-read"},
                {"r:embed": "tiny"},
                {"r:id": "wide"},
                {"r:embed": "valid"},
            ]

    class _CoverDocx:
        document = object()
        namespace = _Namespace()
        document_relationships = [
            {
                "bad-read": "bad.bin",
                "tiny": "tiny.png",
                "wide": "wide.png",
                "valid": "valid.png",
            }
        ]

        def read(self, name):
            if name == "bad.bin":
                raise RuntimeError("read failed")
            return name.encode()

    def fake_identify(raw):
        if raw == b"tiny.png":
            return "png", 0, 100
        if raw == b"wide.png":
            return "png", 1000, 10
        return "png", 500, 400

    monkeypatch.setattr(docx_md, "identify", fake_identify)
    assert docx_md.get_cover(_CoverDocx()) == ("png", b"valid.png")
    assert docx_md._is_path_like(tmp_path / "book.docx")
    with pytest.raises(TypeError):
        docx_md.get_metadata(object())
    with pytest.raises(TypeError):
        docx_md.set_metadata(object(), calibreMetaInformation("x", ["y"]))

    closed = []
    replacement = {}

    class _WriteDocx:
        def __init__(self, _stream, extract=False):
            assert extract is False
            self.namespace = SimpleNamespace(namespaces={"ep": "urn:extended"})

        def get_document_properties_names(self):
            return "docProps/core.xml", "docProps/app.xml"

        def read(self, name):
            if name.endswith("core.xml"):
                return b"<cp:coreProperties xmlns:cp='urn:core'/>"
            return b"<Properties xmlns='urn:extended'><Company>Old</Company></Properties>"

        def close(self):
            closed.append(True)

    monkeypatch.setattr(docx_md, "DOCX", _WriteDocx)
    monkeypatch.setattr(docx_md, "update_doc_props", lambda root, mi, ns: root.set("title", mi.title))
    monkeypatch.setattr(docx_md, "xml2str", lambda root: etree.tostring(root, encoding="utf-8"))
    monkeypatch.setattr(
        docx_md,
        "safe_replace",
        lambda _stream, name, payload, extra_replacements=None: replacement.update(
            {
                "name": name,
                "payload": payload.read(),
                "extra": {k: v.read() for k, v in (extra_replacements or {}).items()},
            }
        ),
    )
    stream = io.BytesIO(b"docx")
    mi = calibreMetaInformation("DOCX Fake — 世界", ["Author"])
    mi.publisher = "Publisher 😀"
    docx_md.set_metadata(stream, mi)
    assert stream.tell() == 0
    assert replacement["name"] == "docProps/core.xml"
    assert b"DOCX Fake" in replacement["payload"]
    assert b"Publisher" in replacement["extra"]["docProps/app.xml"]
    assert closed

    class _MissingCore(_WriteDocx):
        def get_document_properties_names(self):
            return None, None

    monkeypatch.setattr(docx_md, "DOCX", _MissingCore)
    with pytest.raises(ValueError, match="missing core"):
        docx_md.set_metadata(io.BytesIO(b"docx"), mi)
