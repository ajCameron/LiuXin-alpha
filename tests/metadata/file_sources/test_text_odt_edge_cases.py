"""
Exercise text and ODT metadata edge cases.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test text odt edge cases through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_text_odt_edge_cases.py
"""
from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from LiuXin_alpha.metadata.utils import calibreMetaInformation
from LiuXin_alpha.utils.libraries.liuxin_etree import etree


def _values(raw):
    """
    Perform the values test-helper operation with deterministic inputs.

    Example:
        Exercise values through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_text_odt_edge_cases.py


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


class _BadIterable:
    """
    Provide the BadIterable test fixture or double with explicit deterministic behavior.

    Example:
        Exercise BadIterable through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_text_odt_edge_cases.py
    """
    def __iter__(self):
        """
        Perform the iter test-helper operation with deterministic inputs.

        Example:
            Exercise BadIterable.iter through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_text_odt_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        raise TypeError("cannot iterate")


class _TextReadStream(io.StringIO):
    """
    Provide the TextReadStream test fixture or double with explicit deterministic behavior.

    Example:
        Exercise TextReadStream through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_text_odt_edge_cases.py
    """
    name = "string-stream.txtz"


class _TellSeekBroken:
    """
    Provide the TellSeekBroken test fixture or double with explicit deterministic behavior.

    Example:
        Exercise TellSeekBroken through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_text_odt_edge_cases.py
    """
    name = "broken.txtz"

    def __init__(self, payload):
        """
        Initialize the TellSeekBroken test double.

        Example:
            Exercise TellSeekBroken.init through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_text_odt_edge_cases.py


        :param payload: Value supplied for payload in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self.payload = payload

    def tell(self):
        """
        Perform the tell test-helper operation with deterministic inputs.

        Example:
            Exercise TellSeekBroken.tell through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_text_odt_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        raise OSError("tell unavailable")

    def seek(self, _pos):
        """
        Perform the seek test-helper operation with deterministic inputs.

        Example:
            Exercise TellSeekBroken.seek through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_text_odt_edge_cases.py


        :param _pos: Value supplied for pos in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise OSError("seek unavailable")

    def read(self):
        """
        Perform the read test-helper operation with deterministic inputs.

        Example:
            Exercise TellSeekBroken.read through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_text_odt_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self.payload


class _AuthorsSetterFallback:
    """
    Provide the AuthorsSetterFallback test fixture or double with explicit deterministic behavior.

    Example:
        Exercise AuthorsSetterFallback through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_text_odt_edge_cases.py
    """
    def __init__(self):
        """
        Initialize the AuthorsSetterFallback test double.

        Example:
            Exercise AuthorsSetterFallback.init through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_text_odt_edge_cases.py


        :return: None; the function records state or raises through its assertions.
        """
        self.values = []
        self.calls = 0

    @property
    def authors(self):
        """
        Perform the authors test-helper operation with deterministic inputs.

        Example:
            Exercise AuthorsSetterFallback.authors through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_text_odt_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self.values

    @authors.setter
    def authors(self, value):
        """
        Perform the authors test-helper operation with deterministic inputs.

        Example:
            Exercise AuthorsSetterFallback.authors through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_text_odt_edge_cases.py


        :param value: Value stored, compared or projected by the operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.calls += 1
        if isinstance(value, list):
            raise RuntimeError("list assignment unavailable")
        self.values.append(value)


def _zip_bytes(entries: dict[str, bytes]) -> bytes:
    """
    Perform the zip bytes test-helper operation with deterministic inputs.

    Example:
        Exercise zip bytes through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_text_odt_edge_cases.py


    :param entries: Value supplied for entries in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as zf:
        for name, payload in entries.items():
            zf.writestr(name, payload)
    return out.getvalue()


def _content_xml_with_cover(href: str = "Pictures/cover.png", frame_name: str = "opf.cover") -> bytes:
    """
    Perform the content xml with cover test-helper operation with deterministic inputs.

    Example:
        Exercise content xml with cover through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_text_odt_edge_cases.py


    :param href: Value supplied for href in the focused test operation.
    :param frame_name: Value supplied for frame name in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    return f"""<?xml version="1.0" encoding="utf-8"?>
<office:document-content
    xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0"
    xmlns:draw="urn:oasis:names:tc:opendocument:xmlns:drawing:1.0"
    xmlns:xlink="http://www.w3.org/1999/xlink">
  <office:body>
    <draw:frame draw:name="{frame_name}">
      <draw:image xlink:href="{href}"/>
    </draw:frame>
  </office:body>
</office:document-content>
""".encode()


def test_txt_private_parsers_and_source_edges(tmp_path: Path) -> None:
    """
    Verify txt private parsers and source edges.

    Example:
        Exercise test txt private parsers and source edges through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_text_odt_edge_cases.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.txt as txt

    assert txt._source_name(tmp_path / "名字.txt").endswith("名字.txt")
    assert txt._source_name("plain.txt") == "plain.txt"
    assert txt._source_name(SimpleNamespace(name="stream.txt")) == "stream.txt"
    assert txt._default_metadata("/tmp/Fallback Name.txt").title == "Fallback Name"
    assert txt._decode_head("UTF16 Title".encode("utf-16")) == "UTF16 Title"
    assert txt._sanitize_text("A\r\nB\rC\x00D") == "A\nB\nCD"
    assert txt._clean_field(" --  Title ;: ") == "Title"
    assert txt._parse_gutenberg(["No project here"]) == (None, None)
    assert txt._parse_legacy_block("Legacy Title\n\n\nLegacy Author\nBody") == ("Legacy Title", "Legacy Author")
    assert txt._parse_title_and_byline(["Chapter 1", "Part 2", "Real Title by Same Line Author"]) == (
        "Real Title",
        "Same Line Author",
    )

    mi = calibreMetaInformation("Title", ["Unknown"])
    txt._set_authors(mi, "by Renée & 李白")
    assert mi.authors == ["Renée", "李白"]

    assert txt.get_metadata(b"Bytes Title\n\n\nby Bytes Author\n").title == "Bytes Title"
    assert txt.get_metadata(bytearray(b"Bytearray Title\n\n\nby Bytearray Author\n")).title == "Bytearray Title"
    assert txt.get_metadata(object()).title == "Unknown"


def test_txtz_private_helpers_fallbacks_and_reader_edges(monkeypatch, tmp_path: Path) -> None:
    """
    Verify txtz private helpers fallbacks and reader edges.

    Example:
        Exercise test txtz private helpers fallbacks and reader edges through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_text_odt_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.txtz as txtz

    assert txtz._values(None) == []
    assert txtz._values({"k": "v"}) == ["k"]
    assert txtz._values(_BadIterable())[0].__class__ is _BadIterable
    assert txtz._first(["one", "two"]) == "one"
    assert txtz._title_is_unknown(SimpleNamespace(title=" Unknown "))
    assert txtz._authors_are_unknown(SimpleNamespace(authors=["Unknown"]))
    assert not txtz._authors_are_unknown(SimpleNamespace(authors=["Alice"]))

    md = calibreMetaInformation("Title", ["Unknown"])
    txtz._clear_default_authors(md)
    assert md.authors == []
    fallback = _AuthorsSetterFallback()
    txtz._set_authors(fallback, ["Alice", "", "Bob"])
    assert fallback.values == ["Alice", "Bob"]

    path = tmp_path / "source.txtz"
    path.write_bytes(_zip_bytes({"index.txt": b"Title\n\n\nby Author\n"}))
    assert txtz._source_name(path).endswith("source.txtz")
    assert txtz._read_source_bytes(path)[0].startswith(b"PK")
    assert txtz._read_source_bytes(path.read_bytes())[0].startswith(b"PK")

    text_stream = _TextReadStream("plain text")
    text_stream.seek(3)
    assert txtz._read_source_bytes(text_stream) == (b"plain text", "string-stream.txtz")
    assert text_stream.tell() == 3
    assert txtz._read_source_bytes(_TellSeekBroken("fallback")) == (b"fallback", "broken.txtz")
    with pytest.raises(TypeError):
        txtz._read_source_bytes(object())

    assert txtz._txt_member_key("./nested/book.txt") < txtz._txt_member_key("./nested/z.txt")
    from LiuXin_alpha.utils.libraries.calibre_zipfile import ZipFile

    with ZipFile(io.BytesIO(_zip_bytes({"nested/z.txt": b"z", "index.txt": b"i", "cover.webp": b"c"})), "r") as zf:
        assert txtz._find_txt_member(zf) == "index.txt"
        assert txtz._find_cover_member(zf) == "cover.webp"
    with ZipFile(io.BytesIO(_zip_bytes({"image.png": b"i"})), "r") as zf:
        assert txtz._find_txt_member(zf) is None

    events = []
    monkeypatch.setattr(txtz.default_log, "log_exception", lambda *args, **_kwargs: events.append(args))
    md = calibreMetaInformation("Unknown", ["Unknown"])
    txtz._fallback_from_txt_member(io.BytesIO(b"not zip"), md, extract_cover=True)
    assert events and "TXTZ fallback" in str(events[0][0])

    txt_md = calibreMetaInformation("Fallback Title", ["Fallback Author"])
    monkeypatch.setattr(txtz, "extz_get_metadata", lambda _target, extract_cover=True: calibreMetaInformation("Unknown", ["Unknown"]))
    monkeypatch.setattr(txtz, "txt_get_metadata", lambda _stream: txt_md)
    result = txtz.get_metadata(path, extract_cover=False)
    assert result.title == "Fallback Title"
    assert result.authors == ["Fallback Author"]
    monkeypatch.setattr(txtz, "extz_set_metadata", lambda target, mi: ("set", target, mi.title))
    assert txtz.set_metadata("target", calibreMetaInformation("Set Title", ["A"])) == ("set", "target", "Set Title")


def test_odt_helper_edges_cover_fallback_and_errors(monkeypatch, tmp_path: Path) -> None:
    """
    Verify odt helper edges cover fallback and errors.

    Example:
        Exercise test odt helper edges cover fallback and errors through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_text_odt_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.odt as odt

    assert odt._normalize_text(None) == ""
    assert odt._normalize_text("  café\t世界  ") == "café 世界"
    assert odt._split_tags("a; β,γ") == ["a", "β", "γ"]
    assert odt._stable_dedupe(["a", "b", "a"]) == ["a", "b"]
    assert odt._parse_series_index("7,5") == 7.5
    assert odt._parse_series_index("bad") is None
    assert odt._parse_bool("yes")
    assert not odt._parse_bool("off", default=True)
    assert odt._parse_bool("maybe", default=True)
    assert odt._get_source_title(tmp_path / "Fallback.odt") == "Fallback"
    assert odt._read_source_bytes(b"raw") == b"raw"
    with pytest.raises(TypeError):
        odt._read_source_bytes(object())

    root = etree.fromstring(
        f"""<office:document-meta
          xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0"
          xmlns:meta="{odt.METANS}" xmlns:dc="{odt.DCNS}">
          <office:meta>
            <meta:user-defined meta:name="opf.series"> Série Ω </meta:user-defined>
            <dc:title> Title </dc:title>
          </office:meta>
        </office:document-meta>""".encode()
    )
    assert odt._read_user_defined(root) == {"opf.series": "Série Ω"}
    assert odt._first_ns_text(root, odt.DCNS, "title") == "Title"
    assert list(odt._iter_ns_text(root, odt.DCNS, "missing")) == []
    assert odt._parse_xml_bytes(b"<root><child/></root>").tag == "root"

    raw_odt = _zip_bytes({"content.xml": _content_xml_with_cover(), "Pictures/cover.png": b"cover-bytes"})
    mi = calibreMetaInformation("Title", ["Author"])
    monkeypatch.setattr(odt, "od_load", lambda _stream: (_ for _ in ()).throw(RuntimeError("odf load fail")))
    monkeypatch.setattr(odt, "identify", lambda _raw: ("png", 200, 200))
    odt._extract_cover(raw_odt, mi, opf_meta=True, extract_cover=True)
    assert mi.cover == "Pictures/cover.png"
    assert mi.odf_cover_frame == "opf.cover"
    assert mi.cover_data == ("png", b"cover-bytes")

    mi = calibreMetaInformation("Title", ["Author"])
    odt._extract_cover(b"not zip", mi, opf_meta=False, extract_cover=True)
    assert getattr(mi, "cover_data", None) == (None, None)

    monkeypatch.setattr(odt, "canonicalize_lang", lambda _lang: (_ for _ in ()).throw(RuntimeError("lang fail")), raising=False)
    odt._set_language(mi, "zz")
    assert mi.language == "zz"


def test_odt_beta_helper_edges_cover_fallback_and_errors(monkeypatch, tmp_path: Path) -> None:
    """
    Verify odt beta helper edges cover fallback and errors.

    Example:
        Exercise test odt beta helper edges cover fallback and errors through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_text_odt_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.file_sources.odt_beta as beta

    assert beta._normalize(None) == ""
    assert beta._normalize("  café\t世界  ") == "café 世界"
    assert beta._source_title(tmp_path / "Fallback.odt") == "Fallback"
    assert beta._read_source_bytes(bytearray(b"raw")) == b"raw"
    with pytest.raises(TypeError):
        beta._read_source_bytes(object())
    assert beta._parse_series_index("9,25") == 9.25
    assert beta._parse_series_index("bad") is None
    assert beta._parse_bool("true")
    assert not beta._parse_bool("no", default=True)
    assert beta._fmt_from_href("cover.jpeg") == "jpg"
    assert beta._fmt_from_href("cover.unknown") is None

    root = etree.fromstring(
        f"""<office:document-meta
          xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0"
          xmlns:meta="{beta.METANS}" xmlns:dc="{beta.DCNS}">
          <office:meta>
            <meta:user-defined meta:name="opf.series"> Série Ω </meta:user-defined>
            <dc:title> Title </dc:title>
          </office:meta>
        </office:document-meta>""".encode()
    )
    assert beta._read_user_defined(root) == {"opf.series": "Série Ω"}
    assert beta._first_ns_text(root, beta.DCNS, "title") == "Title"
    assert list(beta._iter_ns_text(root, beta.DCNS, "missing")) == []
    assert beta._parse_xml(b"<root><child/></root>").tag == "root"

    monkeypatch.setattr(beta, "identify", lambda _raw: (None, 0, 0))
    monkeypatch.setattr(beta, "_identify_data", lambda _raw: (123, 456, "PNG"))
    assert beta._image_meta(b"raw") == ("png", 123, 456)
    monkeypatch.setattr(beta, "_identify_data", lambda _raw: (_ for _ in ()).throw(RuntimeError("identify fail")))
    assert beta._image_meta(b"raw") == (None, 0, 0)

    raw_odt = _zip_bytes({"content.xml": _content_xml_with_cover("Pictures/cover.unknown"), "Pictures/cover.unknown": b"cover-bytes"})
    mi = calibreMetaInformation("Title", ["Author"])
    monkeypatch.setattr(beta, "od_load", lambda _stream: (_ for _ in ()).throw(RuntimeError("odf load fail")))
    monkeypatch.setattr(beta, "_image_meta", lambda _raw: (None, 200, 200))
    with zipfile.ZipFile(io.BytesIO(raw_odt), "r") as zin:
        beta.read_cover(io.BytesIO(raw_odt), zin, mi, opfmeta=True, extract_cover=True)
    assert mi.cover == "Pictures/cover.unknown"
    assert mi.cover_data == ("jpeg", b"cover-bytes")

    mi = calibreMetaInformation("Title", ["Author"])
    with zipfile.ZipFile(io.BytesIO(_zip_bytes({"content.xml": b"<broken", "Pictures/cover.png": b"cover"})), "r") as zin:
        beta.read_cover(io.BytesIO(_zip_bytes({})), zin, mi, opfmeta=False, extract_cover=True)
    assert getattr(mi, "cover_data", None) == (None, None)
