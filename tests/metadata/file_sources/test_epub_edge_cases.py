from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.metadata.file_sources import epub
from LiuXin_alpha.metadata.utils import calibreMetaInformation


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


class _Bytesable:
    def __bytes__(self):
        return b"bytesable"


class _BadCoverDict(dict):
    def keys(self):
        raise RuntimeError("cover keys unavailable")


class _ToCalibre:
    def __init__(self, converted):
        self._converted = converted

    def to_calibre(self):
        return self._converted


class _FakeEncryption:
    def __init__(self, encrypted=()):
        self.encrypted = set(encrypted)

    def is_encrypted(self, uri):
        return uri in self.encrypted


class _FakeCoverReader:
    def __init__(self, *, encrypted=(), payloads=None, extract_raises=False, write_spine=False):
        self.encryption_meta = _FakeEncryption(encrypted)
        self.payloads = payloads or {}
        self.archive = SimpleNamespace(extractall=self._extractall)
        self.extract_raises = extract_raises
        self.write_spine = write_spine

    def read_bytes(self, name):
        if name not in self.payloads:
            raise KeyError(name)
        return self.payloads[name]

    def _extractall(self, path):
        if self.extract_raises:
            raise RuntimeError("extract failed")
        if self.write_spine:
            spine = Path(path) / "chap.xhtml"
            spine.write_text("<html><body>cover</body></html>", encoding="utf-8")


class _TellBrokenBytes(io.BytesIO):
    def tell(self):
        raise OSError("tell unavailable")


class _RestoreBrokenBytes(io.BytesIO):
    def seek(self, pos, whence=os.SEEK_SET):
        if getattr(self, "_break_restore", False) and pos != 0:
            raise OSError("restore unavailable")
        return super().seek(pos, whence)


class _FakeOCFReader(epub.OCFReader):
    def __init__(self, files):
        self.files = files
        super().__init__()

    def open(self, name, *_args, **_kwargs):
        if name not in self.files:
            raise KeyError(name)
        return io.BytesIO(epub._ensure_bytes(self.files[name]))


class _FakeOPF:
    raw_languages = ["de"]

    def __init__(self):
        self.smart_updates = []
        self.identifiers = {"old": "keep"}
        self.application_id = None
        self.timestamp = None

    def smart_update(self, mi, apply_null=False):
        self.smart_updates.append((mi, apply_null))

    def get_identifiers(self):
        return dict(self.identifiers)

    def set_identifiers(self, identifiers):
        self.identifiers = identifiers


def _container_xml(opf_path="OEBPS/content.opf", media_type="application/oebps-package+xml") -> bytes:
    media = f' media-type="{media_type}"' if media_type else ""
    return (
        b'<?xml version="1.0" encoding="utf-8"?>'
        b'<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">'
        b"<rootfiles>"
        + f'<rootfile full-path="{opf_path}"{media}/>'.encode()
        + b"</rootfiles>"
        b"</container>"
    )


def _opf_payload() -> bytes:
    return """<?xml version="1.0" encoding="utf-8"?>
<package xmlns="http://www.idpf.org/2007/opf"
         xmlns:dc="http://purl.org/dc/elements/1.1/"
         version="2.0">
  <metadata>
    <dc:title>EPUB Inline — café — 世界 😀</dc:title>
    <dc:creator>Renée Faßbinder</dc:creator>
    <dc:language>ja</dc:language>
  </metadata>
  <manifest>
    <item id="cover" href="Images/cover.png" media-type="image/png"/>
    <item id="chap1" href="Text/chapter.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine>
    <itemref idref="chap1"/>
  </spine>
</package>
""".encode("utf-8")


def _epub_bytes(*, opf_path="OEBPS/content.opf", include_cover=True) -> bytes:
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as zf:
        zf.writestr("mimetype", "application/epub+zip")
        zf.writestr("META-INF/container.xml", _container_xml(opf_path=opf_path))
        zf.writestr(opf_path, _opf_payload())
        if include_cover:
            zf.writestr("OEBPS/Images/cover.png", b"\x89PNG\r\n\x1a\ninline-cover")
    return out.getvalue()


def test_epub_private_helpers_cover_payloads_and_type_edges(tmp_path: Path, monkeypatch) -> None:
    assert epub._is_path_like("book.epub")
    assert epub._is_path_like(Path("book.epub"))
    assert epub._source_name(tmp_path / "book.epub").endswith("book.epub")
    assert epub._source_name(io.BytesIO()) == "<stream>"
    assert epub._ensure_bytes(b"raw") == b"raw"
    assert epub._ensure_bytes(bytearray(b"raw")) == b"raw"
    assert epub._ensure_bytes("café") == "café".encode()
    assert epub._ensure_bytes(_Bytesable()) == b"bytesable"
    assert epub._cover_format_from_path("cover.jpg") == "jpeg"
    assert epub._cover_format_from_path("cover.webp") == "webp"
    assert epub._cover_format_from_path(None) == "jpeg"
    assert epub._resolve_member("OEBPS/content.opf", None) is None
    assert epub._resolve_member("OEBPS/content.opf", "/Images/cover.png") == "Images/cover.png"
    assert epub._resolve_member("OEBPS/Text/content.opf", "../Images/cover.png") == "OEBPS/Images/cover.png"

    assert epub._serialize_cover_data(b"not-image-data", "cover.jpg") == b"not-image-data"

    cover_path = tmp_path / "cover.bin"
    cover_path.write_bytes(b"path-cover")
    assert epub._extract_cover_payload(SimpleNamespace(cover_data=("png", b"tuple-cover"))) == b"tuple-cover"
    assert epub._extract_cover_payload(SimpleNamespace(cover_data={(None, b"dict-cover"): True})) == b"dict-cover"
    assert epub._extract_cover_payload(SimpleNamespace(cover_data=_BadCoverDict())) is None
    assert epub._extract_cover_payload(SimpleNamespace(cover=str(cover_path))) == b"path-cover"
    assert epub._extract_cover_payload(SimpleNamespace(cover=str(tmp_path / "missing.bin"))) is None
    assert epub._extract_cover_payload(SimpleNamespace()) is None

    mi = calibreMetaInformation("Calibre Title", ["Calibre Author"])
    assert epub._as_opf_calibre_metadata(mi).title == "Calibre Title"
    from LiuXin_alpha.utils.calibre_compat.ebooks.metadata.book.base import Metadata as OPFCalibreMetadata

    opf_calibre = OPFCalibreMetadata("OPF Calibre", ["OPF Author"])
    assert epub._as_opf_calibre_metadata(opf_calibre) is opf_calibre
    assert epub._as_opf_calibre_metadata(_ToCalibre(opf_calibre)) is opf_calibre
    converted = calibreMetaInformation("Converted Title", ["Converted Author"])
    assert epub._as_opf_calibre_metadata(_ToCalibre(converted)).title == "Converted Title"
    with pytest.raises(TypeError):
        epub._as_opf_calibre_metadata(object())

    monkeypatch.setattr(epub.CalibreLikeLiuXinBookMetaData, "from_calibre", lambda md: ("liuxin", md.title))
    assert epub._to_liuxin_metadata(mi) == ("liuxin", "Calibre Title")


def test_epub_container_encryption_and_ocf_reader_edges(monkeypatch) -> None:
    assert epub.Container() == {}
    with pytest.raises(epub.OCFException):
        epub.Container(io.BytesIO(b""))
    with pytest.raises(epub.EPubException):
        epub.Container(io.BytesIO(b"<container version='2.0'><rootfiles /></container>"))
    with pytest.raises(epub.EPubException):
        epub.Container(io.BytesIO(b"<container><rootfiles /></container>"))

    container = epub.Container(
        io.BytesIO(
            b"<container><rootfiles>"
            b"<rootfile full-path='' media-type='application/oebps-package+xml'/>"
            b"<rootfile full-path='OPS/\xce\x94.opf'/>"
            b"</rootfiles></container>"
        )
    )
    assert container[epub.OPF.MIMETYPE] == "OPS/Δ.opf"

    assert epub.Encryption(None).entries == {}
    assert epub.Encryption(b"<not-xml").entries == {}
    enc = epub.Encryption(
        b"""<encryption xmlns:enc="http://www.w3.org/2001/04/xmlenc#">
        <enc:EncryptedData>
          <enc:EncryptionMethod Algorithm="http://www.w3.org/2001/04/xmlenc#aes256-cbc"/>
          <enc:CipherData><enc:CipherReference URI="OPS/secret.xhtml"/></enc:CipherData>
        </enc:EncryptedData>
        <enc:EncryptedData>
          <enc:EncryptionMethod Algorithm="http://ns.adobe.com/pdf/enc#RC"/>
          <enc:CipherData><enc:CipherReference URI="OPS/font.otf"/></enc:CipherData>
        </enc:EncryptedData>
        </encryption>"""
    )
    assert enc.is_encrypted("OPS/secret.xhtml")
    assert not enc.is_encrypted("OPS/font.otf")
    assert not enc.is_encrypted(None)

    with pytest.raises(NotImplementedError):
        epub.OCF()

    warnings: list[str] = []
    monkeypatch.setattr(epub.default_log, "warning", lambda msg: warnings.append(str(msg)))
    reader = _FakeOCFReader(
        {
            "mimetype": "wrong/type",
            epub.OCF.CONTAINER_PATH: _container_xml(opf_path="OPS/package.opf"),
            epub.OCF.ENCRYPTION_PATH: b"<encryption/>",
            "OPS/package.opf": b"<package/>",
        }
    )
    assert reader.opf_path == "OPS/package.opf"
    assert reader.read_bytes("OPS/package.opf") == b"<package/>"
    assert reader.encryption_meta is reader.encryption_meta
    assert warnings and "Invalid EPUB mimetype" in warnings[0]

    with pytest.raises(epub.EPubException):
        _FakeOCFReader({"mimetype": epub.OCF.MIMETYPE})
    with pytest.raises(epub.EPubException):
        _FakeOCFReader({"mimetype": epub.OCF.MIMETYPE, epub.OCF.CONTAINER_PATH: b"<container><rootfiles /></container>"})
    with pytest.raises(epub.EPubException):
        _FakeOCFReader(
            {
                epub.OCF.CONTAINER_PATH: b"<container><rootfiles><rootfile full-path='notes.txt'/></rootfiles></container>"
            }
        )

    missing_mimetype_warnings: list[str] = []
    monkeypatch.setattr(epub.default_log, "warning", lambda msg: missing_mimetype_warnings.append(str(msg)))
    _FakeOCFReader(
        {
            epub.OCF.CONTAINER_PATH: _container_xml(opf_path="OPS/no-mimetype.opf"),
            "OPS/no-mimetype.opf": b"<package/>",
        }
    )
    assert any("no readable mimetype" in msg for msg in missing_mimetype_warnings)


def test_epub_get_metadata_inline_zip_cover_liuxin_and_error_paths(monkeypatch) -> None:
    mi = calibreMetaInformation("Inline EPUB — δοκιμή 😀", ["Author Ω"])
    calls = []

    monkeypatch.setattr(
        epub,
        "get_metadata_from_opf",
        lambda payload: (mi, "2.0", "Images/cover.png", "Text/chapter.xhtml"),
    )
    stream = io.BytesIO(_epub_bytes())
    stream.name = "inline.epub"
    stream.seek(7)

    metadata = epub.get_metadata(stream, extract_cover=True, calibre_metadata=True)
    assert metadata.title == "Inline EPUB — δοκιμή 😀"
    assert metadata.cover_data[0] == "png"
    assert metadata.cover_data[1].startswith(b"\x89PNG")
    assert stream.tell() == 7

    monkeypatch.setattr(epub, "_to_liuxin_metadata", lambda md: calls.append(md) or ("liuxin", md.title))
    assert epub.get_metadata(io.BytesIO(_epub_bytes()), extract_cover=False, calibre_metadata=False) == (
        "liuxin",
        "Inline EPUB — δοκιμή 😀",
    )
    assert calls == [mi]
    assert epub.get_quick_metadata(io.BytesIO(_epub_bytes())).title == mi.title

    log_events = []
    monkeypatch.setattr(epub.default_log, "log_exception", lambda *args, **_kwargs: log_events.append(args))
    monkeypatch.setattr(epub, "get_cover", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("cover boom")))
    assert epub.get_metadata(io.BytesIO(_epub_bytes()), extract_cover=True).title == mi.title
    assert any("Failed while extracting EPUB cover" in str(event[0]) for event in log_events)

    with pytest.raises(TypeError):
        epub.get_metadata(object())

    fake_reader = SimpleNamespace(opf_path="OEBPS/content.opf", read_bytes=lambda _name: _opf_payload())
    monkeypatch.setattr(epub, "get_zip_reader", lambda _stream: fake_reader)
    monkeypatch.setattr(epub, "get_cover", lambda *_args, **_kwargs: None)
    tell_broken = _TellBrokenBytes(_epub_bytes())
    assert epub.get_metadata(tell_broken, extract_cover=False).title == mi.title

    restore_broken = _RestoreBrokenBytes(_epub_bytes())
    restore_broken.seek(5)
    restore_broken._break_restore = True
    assert epub.get_metadata(restore_broken, extract_cover=False).title == mi.title

    monkeypatch.setattr(epub, "get_cover", lambda *_args, **_kwargs: b"not-image-data")
    monkeypatch.setattr(epub, "identify", lambda _payload: (_ for _ in ()).throw(RuntimeError("identify failed")))
    assert epub.get_metadata(io.BytesIO(_epub_bytes()), extract_cover=True).cover_data[0] == "png"


def test_epub_cover_helpers_render_and_encryption_edges(monkeypatch) -> None:
    reader = _FakeCoverReader(payloads={"cover.png": b"cover-bytes"})
    assert epub._extract_cover_from_member(reader, None) is None
    assert epub._extract_cover_from_member(reader, "cover.png") == b"cover-bytes"
    assert epub._extract_cover_from_member(_FakeCoverReader(encrypted={"cover.png"}), "cover.png") is None
    assert epub._extract_cover_from_member(_FakeCoverReader(), "missing.png") is None

    assert epub._render_cover_from_spine(reader, None) is None
    assert epub._render_cover_from_spine(_FakeCoverReader(encrypted={"chap.xhtml"}), "chap.xhtml") is None
    assert epub._render_cover_from_spine(reader, "missing.xhtml") is None

    import LiuXin_alpha.file_formats as file_formats

    monkeypatch.setattr(file_formats, "render_html_svg_workaround", lambda _path, _log: b"rendered-cover")
    render_reader = _FakeCoverReader(write_spine=True)
    assert epub._render_cover_from_spine(render_reader, "chap.xhtml") == b"rendered-cover"
    assert epub.get_cover("missing.png", "chap.xhtml", render_reader) == b"rendered-cover"

    events = []
    monkeypatch.setattr(epub.default_log, "log_exception", lambda *args, **_kwargs: events.append(args))
    assert epub._render_cover_from_spine(_FakeCoverReader(extract_raises=True), "chap.xhtml") is None
    assert any("Failed to render EPUB spine item" in str(event[0]) for event in events)
    assert epub.get_cover("cover.png", "chap.xhtml", reader) == b"cover-bytes"


def test_epub_update_metadata_and_set_metadata_fake_writer(monkeypatch, tmp_path: Path) -> None:
    opf_obj = _FakeOPF()
    mi = calibreMetaInformation("Updated EPUB", ["Writer"])
    mi.languages = ["fr"]
    mi.uuid = "uuid-value"
    mi.timestamp = "timestamp-value"
    mi.set_identifiers({"new": "id"})

    epub.update_metadata(opf_obj, mi, update_timestamp=True)
    updated_mi, apply_null = opf_obj.smart_updates[0]
    assert not apply_null
    assert updated_mi.languages
    assert opf_obj.application_id == "uuid-value"
    assert opf_obj.identifiers == {"old": "keep", "new": "id"}
    assert opf_obj.timestamp == "timestamp-value"

    epub.update_metadata(opf_obj, mi, apply_null=True, force_identifiers=True)
    assert opf_obj.identifiers == {"new": "id"}

    class _FakeWriterReader:
        opf_path = "OEBPS/content.opf"
        container = {epub.OPF.MIMETYPE: "OEBPS/content.opf"}

        def __init__(self):
            self.encryption_meta = _FakeEncryption()
            self.archive = object()

        def read_bytes(self, name):
            assert name == self.opf_path
            return _opf_payload()

    replacements_seen = {}
    monkeypatch.setattr(epub, "get_zip_reader", lambda _stream, root=None: _FakeWriterReader())
    monkeypatch.setattr(
        epub,
        "set_metadata_opf",
        lambda *_args, **_kwargs: (b"<package>updated</package>", "2.0", "Images/cover.jpg"),
    )
    monkeypatch.setattr(epub, "_serialize_cover_data", lambda data, _path: b"serialized-" + data)
    monkeypatch.setattr(
        epub,
        "safe_replace",
        lambda _stream, name, payload, extra_replacements=None, add_missing=False: replacements_seen.update(
            {
                "name": name,
                "payload": payload.read(),
                "extra": {k: v.read() for k, v in (extra_replacements or {}).items()},
                "add_missing": add_missing,
            }
        ),
    )

    mi.cover_data = ("jpeg", b"cover-data")
    stream = io.BytesIO(_epub_bytes())
    epub.set_metadata(stream, mi)
    assert stream.tell() == 0
    assert replacements_seen["name"] == "OEBPS/content.opf"
    assert replacements_seen["payload"] == b"<package>updated</package>"
    assert replacements_seen["extra"] == {"OEBPS/Images/cover.jpg": b"serialized-cover-data"}
    assert replacements_seen["add_missing"] is True

    path = tmp_path / "writer-path.epub"
    path.write_bytes(_epub_bytes())
    epub.set_metadata(path, mi)

    monkeypatch.setattr(epub, "get_zip_reader", lambda *_args, **_kwargs: (_ for _ in ()).throw(epub.EPubException("boom")))
    with pytest.raises(epub.EPubException, match="boom"):
        epub.set_metadata(io.BytesIO(_epub_bytes()), mi)

    with pytest.raises(TypeError):
        epub.set_metadata(object(), mi)
