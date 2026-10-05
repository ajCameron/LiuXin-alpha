"""
Exercise EPUB container, OPF, encryption and malformed-package edge cases.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test epub edge cases through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py
"""
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
    """
    Perform the values test-helper operation with deterministic inputs.

    Example:
        Exercise values through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


    :param raw: Value supplied for raw in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
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
    """
    Provide the Bytesable test fixture or double with explicit deterministic behavior.

    Example:
        Exercise Bytesable through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py
    """
    def __bytes__(self):
        """
        Perform the bytes test-helper operation with deterministic inputs.

        Example:
            Exercise Bytesable.bytes through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return b"bytesable"


class _BadCoverDict(dict):
    """
    Provide the BadCoverDict test fixture or double with explicit deterministic behavior.

    Example:
        Exercise BadCoverDict through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py
    """
    def keys(self):
        """
        Perform the keys test-helper operation with deterministic inputs.

        Example:
            Exercise BadCoverDict.keys through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        raise RuntimeError("cover keys unavailable")


class _ToCalibre:
    """
    Provide the ToCalibre test fixture or double with explicit deterministic behavior.

    Example:
        Exercise ToCalibre through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py
    """
    def __init__(self, converted):
        """
        Initialize the ToCalibre test double.

        Example:
            Exercise ToCalibre.init through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


        :param converted: Value supplied for converted in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self._converted = converted

    def to_calibre(self):
        """
        Perform the to calibre test-helper operation with deterministic inputs.

        Example:
            Exercise ToCalibre.to calibre through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self._converted


class _FakeEncryption:
    """
    Provide the FakeEncryption test fixture or double with explicit deterministic behavior.

    Example:
        Exercise FakeEncryption through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py
    """
    def __init__(self, encrypted=()):
        """
        Initialize the FakeEncryption test double.

        Example:
            Exercise FakeEncryption.init through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


        :param encrypted: Value supplied for encrypted in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self.encrypted = set(encrypted)

    def is_encrypted(self, uri):
        """
        Perform the is encrypted test-helper operation with deterministic inputs.

        Example:
            Exercise FakeEncryption.is encrypted through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


        :param uri: Value supplied for uri in the focused test operation.
        :return: True when the tested condition is satisfied; otherwise False.
        """
        return uri in self.encrypted


class _FakeCoverReader:
    """
    Provide the FakeCoverReader test fixture or double with explicit deterministic behavior.

    Example:
        Exercise FakeCoverReader through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py
    """
    def __init__(self, *, encrypted=(), payloads=None, extract_raises=False, write_spine=False):
        """
        Initialize the FakeCoverReader test double.

        Example:
            Exercise FakeCoverReader.init through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


        :param encrypted: Value supplied for encrypted in the focused test operation.
        :param payloads: Value supplied for payloads in the focused test operation.
        :param extract_raises: Value supplied for extract raises in the focused test
            operation.
        :param write_spine: Value supplied for write spine in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self.encryption_meta = _FakeEncryption(encrypted)
        self.payloads = payloads or {}
        self.archive = SimpleNamespace(extractall=self._extractall)
        self.extract_raises = extract_raises
        self.write_spine = write_spine

    def read_bytes(self, name):
        """
        Perform the read bytes test-helper operation with deterministic inputs.

        Example:
            Exercise FakeCoverReader.read bytes through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


        :param name: Value supplied for name in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        if name not in self.payloads:
            raise KeyError(name)
        return self.payloads[name]

    def _extractall(self, path):
        """
        Perform the extractall test-helper operation with deterministic inputs.

        Example:
            Exercise FakeCoverReader.extractall through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


        :param path: Value supplied for path in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        if self.extract_raises:
            raise RuntimeError("extract failed")
        if self.write_spine:
            spine = Path(path) / "chap.xhtml"
            spine.write_text("<html><body>cover</body></html>", encoding="utf-8")


class _TellBrokenBytes(io.BytesIO):
    """
    Provide the TellBrokenBytes test fixture or double with explicit deterministic behavior.

    Example:
        Exercise TellBrokenBytes through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py
    """
    def tell(self):
        """
        Perform the tell test-helper operation with deterministic inputs.

        Example:
            Exercise TellBrokenBytes.tell through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        raise OSError("tell unavailable")


class _RestoreBrokenBytes(io.BytesIO):
    """
    Provide the RestoreBrokenBytes test fixture or double with explicit deterministic behavior.

    Example:
        Exercise RestoreBrokenBytes through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py
    """
    def seek(self, pos, whence=os.SEEK_SET):
        """
        Perform the seek test-helper operation with deterministic inputs.

        Example:
            Exercise RestoreBrokenBytes.seek through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


        :param pos: Value supplied for pos in the focused test operation.
        :param whence: Value supplied for whence in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        if getattr(self, "_break_restore", False) and pos != 0:
            raise OSError("restore unavailable")
        return super().seek(pos, whence)


class _FakeOCFReader(epub.OCFReader):
    """
    Provide the FakeOCFReader test fixture or double with explicit deterministic behavior.

    Example:
        Exercise FakeOCFReader through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py
    """
    def __init__(self, files):
        """
        Initialize the FakeOCFReader test double.

        Example:
            Exercise FakeOCFReader.init through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


        :param files: Value supplied for files in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self.files = files
        super().__init__()

    def open(self, name, *_args, **_kwargs):
        """
        Perform the open test-helper operation with deterministic inputs.

        Example:
            Exercise FakeOCFReader.open through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


        :param name: Value supplied for name in the focused test operation.
        :param _args: Value supplied for args in the focused test operation.
        :param _kwargs: Value supplied for kwargs in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        if name not in self.files:
            raise KeyError(name)
        return io.BytesIO(epub._ensure_bytes(self.files[name]))


class _FakeOPF:
    """
    Provide the FakeOPF test fixture or double with explicit deterministic behavior.

    Example:
        Exercise FakeOPF through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py
    """
    raw_languages = ["de"]

    def __init__(self):
        """
        Initialize the FakeOPF test double.

        Example:
            Exercise FakeOPF.init through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


        :return: None; the function records state or raises through its assertions.
        """
        self.smart_updates = []
        self.identifiers = {"old": "keep"}
        self.application_id = None
        self.timestamp = None

    def smart_update(self, mi, apply_null=False):
        """
        Perform the smart update test-helper operation with deterministic inputs.

        Example:
            Exercise FakeOPF.smart update through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


        :param mi: Value supplied for mi in the focused test operation.
        :param apply_null: Value supplied for apply null in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.smart_updates.append((mi, apply_null))

    def get_identifiers(self):
        """
        Return identifiers from deterministic test state.

        Example:
            Exercise FakeOPF.get identifiers through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return dict(self.identifiers)

    def set_identifiers(self, identifiers):
        """
        Perform the set identifiers test-helper operation with deterministic inputs.

        Example:
            Exercise FakeOPF.set identifiers through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


        :param identifiers: Value supplied for identifiers in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.identifiers = identifiers


def _container_xml(opf_path="OEBPS/content.opf", media_type="application/oebps-package+xml") -> bytes:
    """
    Perform the container xml test-helper operation with deterministic inputs.

    Example:
        Exercise container xml through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


    :param opf_path: Value supplied for opf path in the focused test operation.
    :param media_type: Value supplied for media type in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
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
    """
    Perform the opf payload test-helper operation with deterministic inputs.

    Example:
        Exercise opf payload through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


    :return: The deterministic value, row, identity or collection described above.
    """
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
    """
    Perform the epub bytes test-helper operation with deterministic inputs.

    Example:
        Exercise epub bytes through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


    :param opf_path: Value supplied for opf path in the focused test operation.
    :param include_cover: Value supplied for include cover in the focused test
        operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as zf:
        zf.writestr("mimetype", "application/epub+zip")
        zf.writestr("META-INF/container.xml", _container_xml(opf_path=opf_path))
        zf.writestr(opf_path, _opf_payload())
        if include_cover:
            zf.writestr("OEBPS/Images/cover.png", b"\x89PNG\r\n\x1a\ninline-cover")
    return out.getvalue()


def test_epub_private_helpers_cover_payloads_and_type_edges(tmp_path: Path, monkeypatch) -> None:
    """
    Verify epub private helpers cover payloads and type edges.

    Example:
        Exercise test epub private helpers cover payloads and type edges through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify epub container encryption and ocf reader edges.

    Example:
        Exercise test epub container encryption and ocf reader edges through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify epub get metadata inline zip cover liuxin and error paths.

    Example:
        Exercise test epub get metadata inline zip cover liuxin and error paths through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify epub cover helpers render and encryption edges.

    Example:
        Exercise test epub cover helpers render and encryption edges through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
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
    """
    Verify epub update metadata and set metadata fake writer.

    Example:
        Exercise test epub update metadata and set metadata fake writer through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
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
        """
        Provide the FakeWriterReader test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test epub update metadata and set metadata fake writer.FakeWriterReader through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py
        """
        opf_path = "OEBPS/content.opf"
        container = {epub.OPF.MIMETYPE: "OEBPS/content.opf"}

        def __init__(self):
            """
            Initialize the FakeWriterReader test double.

            Example:
                Exercise test epub update metadata and set metadata fake writer.FakeWriterReader.init through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


            :return: None; the function records state or raises through its assertions.
            """
            self.encryption_meta = _FakeEncryption()
            self.archive = object()

        def read_bytes(self, name):
            """
            Perform the read bytes test-helper operation with deterministic inputs.

            Example:
                Exercise test epub update metadata and set metadata fake writer.FakeWriterReader.read bytes through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_epub_edge_cases.py


            :param name: Value supplied for name in the focused test operation.
            :return: The deterministic value, row, identity or collection described above.
            """
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
