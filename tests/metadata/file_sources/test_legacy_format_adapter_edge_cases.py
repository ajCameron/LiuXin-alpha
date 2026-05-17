from __future__ import annotations

import io
import struct
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


def _first(raw):
    vals = _values(raw)
    return vals[0] if vals else None


def _encode_vwi(value: int) -> bytes:
    parts = [value & 0x7F]
    value >>= 7
    while value:
        parts.append((value & 0x7F) | 0x80)
        value >>= 7
    return bytes(reversed(parts))


def _minimal_topaz_with_empty_metadata_header() -> bytes:
    return (
        b"TPZ0"
        + _encode_vwi(1)
        + b"c"
        + _encode_vwi(len(b"metadata"))
        + b"metadata"
        + _encode_vwi(0)
        + b"d"
    )


class _SeekBroken(io.BytesIO):
    def seek(self, *args, **kwargs):
        raise OSError("seek unavailable")


def test_rar_helper_type_and_restore_edges(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.rar as rar_md

    assert rar_md._member_type(r"nested\BOOK.PMLZ") == "pmlz"
    assert rar_md._find_first_supported_member(["page.jpg", "book.RTF"]) == ("book.RTF", "rtf")
    assert rar_md._find_first_supported_member(["page.jpg"]) is None
    assert rar_md._source_label(io.BytesIO(b"rar")) == "<stream>"
    named = io.BytesIO(b"rar")
    named.name = "/tmp/archive.rar"
    assert rar_md._source_label(named) == "archive.rar"

    class _TimestampRaises:
        @property
        def timestamp(self):
            return None

        @timestamp.setter
        def timestamp(self, _value):
            raise RuntimeError("cannot clear")

    rar_md._set_timestamp_none(_TimestampRaises())

    with pytest.raises(TypeError, match="RAR metadata reader"):
        rar_md.get_metadata(object())

    stream = io.BytesIO(b"rar")
    stream.seek(1)
    monkeypatch.setattr(rar_md, "names", lambda _stream: ["book.epub"])
    monkeypatch.setattr(rar_md, "extract_member", lambda *_a, **_k: None)
    with pytest.raises(ValueError, match="Unable to extract"):
        rar_md.get_metadata(stream)
    assert stream.tell() == 1


def test_imp_helper_decode_cstring_and_type_edges(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.imp as imp_md

    assert imp_md._default_metadata().title == "Unknown"
    assert imp_md._decode_bytes(b"") == ""
    assert imp_md._decode_bytes(b"Caf\xe9") == "Café"
    assert imp_md._read_cstring(io.BytesIO(b"skip\x00wanted\x00"), skip=1) == "wanted"
    with pytest.raises(ValueError, match="safety limit"):
        imp_md._read_cstring(io.BytesIO(b"abcdef"), max_bytes=3)

    class _NullRaises:
        def is_null(self, _field):
            raise RuntimeError("no null check")

    target = _NullRaises()
    imp_md._ensure_default_authors(target)
    assert target.authors == ["Unknown"]

    with pytest.raises(TypeError, match="target_file"):
        imp_md.get_metadata(object())

    with pytest.raises(imp_md.ImpFormatError):
        imp_md.read_metadata_from_stream(io.BytesIO(b"not-an-imp"))
    md = imp_md.read_metadata_from_stream(io.BytesIO(b"not-an-imp"), fallback_on_parse_error=True)
    assert md.title == "Unknown"


def test_lrx_private_xml_and_invalid_stream_edges(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.lrx as lrx_md

    assert lrx_md._default_metadata("/tmp/Named.lrx").title == "Named"
    assert lrx_md._read_at(io.BytesIO(b"abcdef"), 2, 3) == b"cde"
    with pytest.raises(ValueError, match="binary stream"):
        lrx_md._read_at(io.StringIO("abcdef"), 0, 1)
    with pytest.raises(ValueError, match="truncated"):
        lrx_md._read_at(io.BytesIO(b"ab"), 0, 3)
    assert lrx_md._word_be(b"\x00\x00\x00\x02") == 2
    assert lrx_md._word_le(b"\x02\x00\x00\x00") == 2
    assert lrx_md._short_le(b"\x02\x00") == 2
    assert lrx_md._clean_text("  x  ") == "x"
    assert lrx_md._clean_text("   ") is None

    mi = calibreMetaInformation("Before", ["Unknown"])
    lrx_md._parse_lrx_xml(b"<Root/>", mi)
    assert mi.title == "Before"

    payload = (
        b"<Root><BookInfo><Title reading='Sort'>Title</Title><Author reading='AS'>Alice and Bob</Author>"
        b"<Publisher>Pub</Publisher><Category>One</Category><Category>Two</Category></BookInfo>"
        b"<DocInfo><Language>en</Language></DocInfo></Root>"
    )
    lrx_md._parse_lrx_xml(payload, mi)
    assert mi.title == "Title"
    assert _values(mi.authors) == ["Alice", "Bob"]
    assert _values(mi.tags) == ["One", "Two"]
    assert _first(mi.language) == "en"

    class _LoggerNoLogException:
        messages: list[str] = []

        @classmethod
        def warning(cls, message):
            cls.messages.append(message)

    monkeypatch.setattr(lrx_md, "default_log", _LoggerNoLogException)
    lrx_md._log_exception("base", RuntimeError("boom"), "source.lrx")
    assert any("base" in message for message in _LoggerNoLogException.messages)

    with pytest.raises(lrx_md.LrxFormatError):
        lrx_md.read_metadata_from_stream(io.BytesIO(b"short"), source_name="short.lrx")
    assert (
        lrx_md.read_metadata_from_stream(
            io.BytesIO(b"short"),
            source_name="short.lrx",
            fallback_on_parse_error=True,
        ).title
        == "short"
    )
    assert lrx_md.read_metadata_from_stream(io.BytesIO(b"\x00\x00\x00\x00LRX-----"), source_name="librie.lrx").title == "librie"
    with pytest.raises(TypeError, match="target_file"):
        lrx_md.get_metadata(object())


def test_rb_helper_no_info_and_error_edges() -> None:
    import LiuXin_alpha.metadata.file_sources.rb as rb_md

    assert rb_md._default_metadata("/tmp/Named.rb").title == "Named"
    assert rb_md._decode_info_line(b"Caf\xe9") == "Café"
    with pytest.raises(TypeError, match="target_file"):
        rb_md.get_metadata(object())

    data = bytearray(rb_md.MAGIC + b"\x00" * 10 + struct.pack("<I", 28))
    while len(data) < 28:
        data.append(0)
    data.extend(struct.pack("<I", 0))
    md = rb_md.read_metadata_from_stream(io.BytesIO(bytes(data)), source_name="noinfo.rb")
    assert md.title == "noinfo"
    assert _values(md.authors) == ["Unknown"]

    with pytest.raises(rb_md.RbFormatError):
        rb_md.read_metadata_from_stream(io.BytesIO(rb_md.MAGIC + b"\x00"), source_name="broken.rb")
    broken = rb_md.read_metadata_from_stream(
        io.BytesIO(rb_md.MAGIC + b"\x00"),
        source_name="broken.rb",
        fallback_on_parse_error=True,
    )
    assert broken.title == "broken"


def test_rtf_helper_detection_decode_and_replace_edges() -> None:
    import LiuXin_alpha.metadata.file_sources.rtf as rtf_md

    assert rtf_md._source_name(Path("/tmp/book.rtf")) == "/tmp/book.rtf"
    assert rtf_md._source_name(SimpleNamespace(name="named.rtf")) == "named.rtf"
    assert rtf_md._to_bytes("Café") == b"Caf\xe9"
    assert rtf_md._normalize_text("  a\n b\tc  ") == "a b c"
    rtf_md._safe_seek(_SeekBroken(b""), 0)
    assert rtf_md.get_document_info(io.BytesIO(b"{\\rtf1\\ansi\\sect body}")) == (None, 0)
    assert rtf_md.detect_codepage(io.BytesIO(b"{\\rtf1\\ansi\\ansicpg0 body}")) == "cp1252"
    assert rtf_md.detect_codepage(io.BytesIO(b"{\\rtf1\\ansi\\ansicpg999999 body}")) is None
    assert "\\u233?" in rtf_md.encode("é")
    assert rtf_md.decode(r"bad \'ff \u-1?", "not-a-codec") == "bad ?"
    with pytest.raises(TypeError, match="RTF metadata reader"):
        rtf_md.get_metadata(object())

    stream = io.BytesIO(
        b"{\\rtf1\\ansi\\ansicpg1252{\\info{\\title Old}{\\author Old Author}{\\subject Old}}\\par body}"
    )
    options = calibreMetaInformation("New Καλημέρα", ["Alice", "Bob"])
    options.comments = "Fresh comment"
    options.tags = ["one", "two"]
    options.publisher = "Publisher"
    rtf_md.set_metadata(stream, options)
    parsed = rtf_md.get_metadata(io.BytesIO(stream.getvalue()))
    assert parsed.title == "New Καλημέρα"
    assert _values(parsed.authors) == ["Alice", "Bob"]
    assert _first(parsed.comments) == "Fresh comment"
    assert _first(parsed.publisher) == "Publisher"


def test_snb_helper_cover_parse_and_error_edges(monkeypatch) -> None:
    import LiuXin_alpha.metadata.file_sources.snb as snb_md

    assert snb_md._source_name(Path("/tmp/book.snb")) == "/tmp/book.snb"
    snb_md._safe_seek(_SeekBroken(b""), 0)

    root = etree.fromstring(b"<root xmlns='urn:x'><title>Namespaced</title><empty/></root>")
    assert [node.text for node in snb_md._iter_local(root, "title")] == ["Namespaced"]
    assert snb_md._first_text(root, "missing", "title") == "Namespaced"
    assert tuple(snb_md._cover_candidates("")) == ()
    assert tuple(snb_md._cover_candidates("cover")) == ("cover", "snbc/cover", "snbc/images/cover")

    class _FakeSNB:
        def __init__(self, files: dict[str, bytes]) -> None:
            self.files = files

        def GetFileStream(self, name):
            return self.files.get(name)

    assert snb_md._read_cover_data(_FakeSNB({"snbc/images/cover": b"cover"}), "cover") == ("jpg", b"cover")
    assert snb_md._read_cover_data(_FakeSNB({}), "missing.png") is None

    md = calibreMetaInformation("Unknown", ["Unknown"])
    snb_md._parse_book_snbf(b"<broken", mi=md, snb_file=_FakeSNB({}), extract_cover=True)
    assert md.title == "Unknown"

    class _SNBFileRaises:
        def Parse(self, *_args, **_kwargs):
            raise RuntimeError("parse fail")

    monkeypatch.setattr(snb_md, "SNBFile", _SNBFileRaises)
    with pytest.raises(TypeError):
        snb_md.get_metadata(object())
    with pytest.raises(snb_md.SnbFormatError):
        snb_md.get_metadata(b"not-snb")
    assert snb_md.get_metadata(b"not-snb", fallback_on_parse_error=True).title == "Unknown"


def test_topaz_stream_slicer_metadata_updater_and_type_edges(tmp_path: Path) -> None:
    import LiuXin_alpha.metadata.file_sources.topaz as topaz_md

    stream = io.BytesIO(b"abcdef")
    slicer = topaz_md.StreamSlicer(stream, 1, 5)
    assert len(slicer) == 4
    assert slicer[0] == b"b"
    assert slicer[1:3] == b"cd"
    assert slicer[0:4:2] == b"bd"
    assert slicer[3:1:-1] == b"dc"
    with pytest.raises(TypeError, match="stream indices"):
        _ = slicer["bad"]
    slicer.update([b"XY", b"Z"])
    assert stream.getvalue() == b"aXYZ"
    slicer.truncate(2)
    assert stream.getvalue() == b"aX"

    assert topaz_md._byte_as_int(b"") == 0
    assert topaz_md._byte_as_int(b"\x07") == 7
    assert topaz_md._decode_tag(b"\xff") == "�"
    assert topaz_md._decode_text(None) == ""
    assert topaz_md._source_name(Path("/tmp/book.tpz")) == "/tmp/book.tpz"
    topaz_md._safe_seek(_SeekBroken(b""), 0)
    with pytest.raises(ValueError, match="negative"):
        topaz_md.MetadataUpdater.encode_vwi(-1)

    with pytest.raises(ValueError, match="no metadata record"):
        topaz_md.MetadataUpdater(io.BytesIO(b"TPZ0\x00"))
    with pytest.raises(ValueError, match="metadata header has no blocks"):
        topaz_md.MetadataUpdater(io.BytesIO(_minimal_topaz_with_empty_metadata_header()))

    with pytest.raises(TypeError, match="Topaz metadata reader"):
        topaz_md.get_metadata(object())
    with pytest.raises(TypeError, match="Topaz metadata writer"):
        topaz_md.set_metadata(object(), calibreMetaInformation("Title", ["Author"]))

    path = tmp_path / "invalid.tpz"
    path.write_bytes(b"not-topaz")
    with pytest.raises(topaz_md.TopazFormatError):
        topaz_md.get_metadata(path)
    assert topaz_md.get_metadata(path, fallback_on_parse_error=True).title == "Unknown"
