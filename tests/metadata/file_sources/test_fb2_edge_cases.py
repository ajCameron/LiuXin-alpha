from __future__ import annotations

import base64
import io
import zipfile
from pathlib import Path

from LiuXin_alpha.metadata.metadata import MetaData
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


def _first(raw):
    vals = _values(raw)
    return vals[0] if vals else None


class _NamedBytes(io.BytesIO):
    def __init__(self, payload: bytes, name: str = "stream.fb2") -> None:
        super().__init__(payload)
        self.name = name


class _TellBrokenStream(_NamedBytes):
    def tell(self) -> int:
        raise RuntimeError("no tell")


class _SeekBrokenStream(_NamedBytes):
    def seek(self, *_args, **_kwargs):
        raise RuntimeError("no seek")


class _NullLike:
    title = ""
    comments = ""

    def is_null(self, _field: str) -> bool:
        raise RuntimeError("boom")


def _fb2_payload(*, include_cover: bool = True) -> bytes:
    cover_payload = b"\x89PNG\r\n\x1a\nnot-a-real-image-but-good-enough"
    cover_b64 = base64.b64encode(cover_payload).decode("ascii")
    coverpage = (
        '<coverpage><image xlink:href="#cover.png"/></coverpage>'
        if include_cover
        else ""
    )
    binary = (
        f'<binary id="cover.png" content-type="image/png">{cover_b64}</binary>'
        if include_cover
        else ""
    )
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<FictionBook xmlns="http://www.gribuser.ru/xml/fictionbook/2.0"
             xmlns:xlink="http://www.w3.org/1999/xlink">
  <description>
    <title-info>
      <genre>fantasy</genre>
      <genre>fantasy</genre>
      <genre>δοκιμή</genre>
      <author><nickname>Никнейм 😀</nickname></author>
      <book-title>Καλημέρα 世界 — مرحبا</book-title>
      <annotation>
        <p>First paragraph café\u0301.</p>
        <empty-line/>
        <p>Second paragraph עברית عربى.</p>
        <empty-line/>
      </annotation>
      <sequence name="Σειρά シリーズ" number="12,5"/>
      <lang>el</lang>
      {coverpage}
    </title-info>
    <publish-info>
      <publisher>دار النشر</publisher>
      <year>2024</year>
      <isbn>978-0-306-40615-7, ignored</isbn>
    </publish-info>
  </description>
  {binary}
  <body><section><p>body</p></section></body>
</FictionBook>
""".encode("utf-8")


def test_fb2_inline_unicode_torture_reads_all_core_fields() -> None:
    from LiuXin_alpha.metadata.file_sources.fb2 import get_metadata

    stream = _NamedBytes(_fb2_payload(), "unicode_torture.fb2")
    stream.seek(17)

    md = get_metadata(stream)

    assert md.title == "Καλημέρα 世界 — مرحبا"
    assert _values(md.authors) == ["Никнейм 😀"]
    assert _values(md.tags) == ["fantasy", "δοκιμή"]
    assert _first(md.series) == "Σειρά シリーズ"
    assert _first(md.publisher) == "دار النشر"
    assert getattr(md.pubdate, "year", None) == 2024
    assert _first(md.isbn) == "9780306406157"
    assert md.language == "el"
    assert "Second paragraph" in _first(md.comments)
    assert "\n\n" in _first(md.comments)
    assert md.cover_data is not None
    assert stream.tell() == 17


def test_fb2_document_info_author_and_shell_title_fallback() -> None:
    from LiuXin_alpha.metadata.file_sources.fb2 import get_metadata

    payload = b"""<?xml version="1.0" encoding="UTF-8"?>
<FictionBook xmlns="http://www.gribuser.ru/xml/fictionbook/2.0">
  <description>
    <publish-info><book-title>Document Title</book-title></publish-info>
    <document-info>
      <author><first-name>Doc</first-name><middle-name>Middle</middle-name><last-name>Author</last-name></author>
    </document-info>
  </description>
</FictionBook>
"""
    md = get_metadata(_NamedBytes(payload, "fallback-title.fb2"))

    assert md.title == "Document Title"
    assert _values(md.authors) == ["Doc Middle Author"]


def test_fb2_bad_parser_step_logs_and_keeps_other_metadata(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.fb2 as fb2

    events: list[str] = []

    def _raise(_root, _mi):
        raise RuntimeError("parser boom")

    def _record(message, _err, _level, *args):
        del args
        events.append(str(message))

    monkeypatch.setattr(fb2, "_parse_comments", _raise)
    monkeypatch.setattr(fb2.default_log, "log_exception", _record)

    md = fb2.get_metadata(_NamedBytes(_fb2_payload(include_cover=False)))

    assert md.title == "Καλημέρα 世界 — مرحبا"
    assert "fantasy" in _values(md.tags)
    assert "FB2 metadata parser step failed." in events


def test_fb2_helper_edges_for_payloads_covers_and_text(tmp_path: Path, monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.fb2 as fb2

    assert fb2._source_name(tmp_path / "book.fb2").endswith("book.fb2")
    assert fb2._source_title(_NamedBytes(b"", "")) == "Unknown"
    assert fb2._ensure_bytes(bytearray(b"abc")) == b"abc"
    assert fb2._ensure_bytes("é") == "é".encode("utf-8")
    assert fb2._namespace_from_tag("plain") is None
    assert fb2._metadata_values(None) == []
    assert fb2._metadata_values({"a": 1}) == ["a"]
    assert fb2._metadata_values(("fmt", b"payload")) == [("fmt", b"payload")]
    assert fb2._metadata_values(7) == [7]
    assert fb2._first_metadata_text([" ", " value "]) == "value"
    assert fb2._is_null_field(_NullLike(), "title") is True
    assert fb2._safe_float(None) is None
    assert fb2._safe_float("") is None
    assert fb2._safe_float("3 5") == 3.5
    assert fb2._safe_float("bad") is None
    assert fb2._safe_int(None) is None
    assert fb2._safe_int("bad") is None

    root = etree.fromstring(b"<annotation><p>One</p><empty-line/><p>Two</p><empty-line/></annotation>")
    assert fb2._annotation_to_text(None) == ""
    assert fb2._annotation_to_text(etree.fromstring(b"<annotation>plain text</annotation>")) == "plain text"
    assert fb2._annotation_to_text(root) == "One\n\nTwo"
    assert fb2._htmlish_to_text("<p>One</p><broken>Two") == "OneTwo"
    assert fb2._htmlish_to_text("plain") == "plain"

    cover_path = tmp_path / "cover.webp"
    cover_path.write_bytes(b"cover-bytes")
    mi = calibreMetaInformation("Title", ["Author"])
    mi.cover = str(cover_path)
    assert fb2._extract_cover_payload(mi) == ("webp", b"cover-bytes")
    mi.cover = str(tmp_path / "missing.jpg")
    assert fb2._extract_cover_payload(mi) is None

    mi = calibreMetaInformation("Title", ["Author"])
    mi.cover_data = ("png", b"payload")
    assert fb2._extract_cover_payload(mi) == ("png", b"payload")

    monkeypatch.setattr(fb2, "save_cover_data_to", None)
    assert fb2._coerce_cover_to_jpeg_bytes(b"raw") == b"raw"
    monkeypatch.setattr(fb2, "save_cover_data_to", lambda *_args, **_kwargs: "converted")
    assert fb2._coerce_cover_to_jpeg_bytes(b"raw") == b"converted"
    monkeypatch.setattr(fb2, "save_cover_data_to", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("x")))
    assert fb2._coerce_cover_to_jpeg_bytes(b"raw") == b"raw"

    monkeypatch.setattr(fb2, "identify", lambda _payload: ("webp", None))
    assert fb2._cover_format_from_payload("jpg", b"payload") == "webp"
    monkeypatch.setattr(fb2, "identify", lambda _payload: (_ for _ in ()).throw(RuntimeError("x")))
    assert fb2._cover_format_from_payload("jpg", b"payload") == "jpeg"


def test_fb2_zip_payload_selection_and_bad_zip_fallback(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.fb2 as fb2

    assert fb2._extract_fb2_payload(b"") == (b"", None)

    raw_bad_zip = b"PKnot-really-a-zip"
    assert fb2._extract_fb2_payload(raw_bad_zip) == (raw_bad_zip, None)

    archive = io.BytesIO()
    with zipfile.ZipFile(archive, "w") as zf:
        zf.writestr("z-last.txt", b"last")
        zf.writestr("a-first.dat", b"first")
    payload, member = fb2._extract_fb2_payload(archive.getvalue())
    assert payload == b"first"
    assert member == "a-first.dat"

    events: list[str] = []

    class _ExplodingZip:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def __enter__(self):
            raise RuntimeError("zip boom")

        def __exit__(self, *_args) -> None:
            return None

    monkeypatch.setattr(fb2, "ZipFile", _ExplodingZip)
    monkeypatch.setattr(fb2.default_log, "log_exception", lambda message, *_args: events.append(str(message)))
    assert fb2._extract_fb2_payload(raw_bad_zip) == (raw_bad_zip, None)
    assert events == ["Failed to inspect potential FB2 zip payload; using raw bytes."]


def test_fb2_apply_null_write_clears_existing_fields() -> None:
    from LiuXin_alpha.metadata.file_sources.fb2 import get_metadata, set_metadata

    stream = _NamedBytes(_fb2_payload(include_cover=False), "clear.fb2")
    empty = MetaData()

    set_metadata(stream, empty, apply_null=True)
    payload = stream.getvalue().decode("utf-8")
    md = get_metadata(stream)

    assert md.title == "clear"
    assert _values(md.authors) == ["Unknown"]
    assert "<book-title" not in payload
    assert "<author" not in payload
    assert "<genre" not in payload
    assert "<sequence" not in payload


def test_fb2_set_metadata_cover_and_single_name_author_paths(monkeypatch) -> None:
    from LiuXin_alpha.metadata.file_sources.fb2 import get_metadata, set_metadata

    import LiuXin_alpha.metadata.file_sources.fb2 as fb2

    monkeypatch.setattr(fb2, "identify", lambda _payload: ("png", None))
    monkeypatch.setattr(fb2, "_rnd_pic_file_name", lambda prefix="cover", size=32, ext="png": f"{prefix}.{ext}")

    stream = _NamedBytes(_fb2_payload(include_cover=False), "cover.fb2")
    updated = calibreMetaInformation("Cover Title", ["Solo"])
    updated.cover_data = ("jpg", b"image-payload")
    updated.series = "Series Without Number"

    set_metadata(stream, updated)
    payload = stream.getvalue().decode("utf-8")
    md = get_metadata(stream)

    assert md.title == "Cover Title"
    assert _values(md.authors) == ["Solo"]
    assert "cover.png" in payload
    assert "image/png" in payload
    assert 'number="1"' in payload
    assert _first(md.series) == "Series Without Number"


def test_fb2_stream_type_and_seek_failures_are_soft() -> None:
    from LiuXin_alpha.metadata.file_sources.fb2 import get_metadata, set_metadata

    try:
        get_metadata(object())
    except TypeError as err:
        assert "readable binary stream" in str(err)
    else:
        raise AssertionError("expected TypeError")

    try:
        set_metadata(io.BytesIO(b""), object())
    except Exception:
        pass

    md = get_metadata(_TellBrokenStream(_fb2_payload(), "tell-broken.fb2"))
    assert md.title == "Καλημέρα 世界 — مرحبا"

    md = get_metadata(_SeekBrokenStream(_fb2_payload(), "seek-broken.fb2"))
    assert md.title == "Καλημέρα 世界 — مرحبا"
