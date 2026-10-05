"""
Exercise OPF namespaces, identifiers, refinements, covers and malformed packages.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test opf edge cases through its owning regression module::

        python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py
"""
from __future__ import annotations

import io
from pathlib import Path

import pytest

from LiuXin_alpha.metadata.file_sources import opf
from LiuXin_alpha.utils.libraries.liuxin_etree import etree


def _values(raw):
    """
    Perform the values test-helper operation with deterministic inputs.

    Example:
        Exercise values through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


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


def _first_mapping_value(raw, default=None):
    """
    Perform the first mapping value test-helper operation with deterministic inputs.

    Example:
        Exercise first mapping value through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


    :param raw: Value supplied for raw in the focused test operation.
    :param default: Value supplied for default in the focused test operation.
    :return: The deterministic value, row, identity or collection described above.
    """
    if isinstance(raw, dict):
        try:
            return next(iter(raw.values()))
        except StopIteration:
            return default
    return raw if raw is not None else default


class _NamedBytes(io.BytesIO):
    """
    Provide the NamedBytes test fixture or double with explicit deterministic behavior.

    Example:
        Exercise NamedBytes through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py
    """
    def __init__(self, payload: bytes, name: str = "unicode fixture.opf"):
        """
        Initialize the NamedBytes test double.

        Example:
            Exercise NamedBytes.init through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :param payload: Value supplied for payload in the focused test operation.
        :param name: Value supplied for name in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        super().__init__(payload)
        self.name = name


class _TextStream(io.StringIO):
    """
    Provide the TextStream test fixture or double with explicit deterministic behavior.

    Example:
        Exercise TextStream through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py
    """
    def __init__(self, payload: str, name: str = "text-stream.opf"):
        """
        Initialize the TextStream test double.

        Example:
            Exercise TextStream.init through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :param payload: Value supplied for payload in the focused test operation.
        :param name: Value supplied for name in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        super().__init__(payload)
        self.name = name


class _TellSeekBroken:
    """
    Provide the TellSeekBroken test fixture or double with explicit deterministic behavior.

    Example:
        Exercise TellSeekBroken through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py
    """
    name = "broken-stream.opf"

    def __init__(self, payload):
        """
        Initialize the TellSeekBroken test double.

        Example:
            Exercise TellSeekBroken.init through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :param payload: Value supplied for payload in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self._payload = payload

    def tell(self):
        """
        Perform the tell test-helper operation with deterministic inputs.

        Example:
            Exercise TellSeekBroken.tell through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        raise OSError("tell unavailable")

    def seek(self, _pos):
        """
        Perform the seek test-helper operation with deterministic inputs.

        Example:
            Exercise TellSeekBroken.seek through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :param _pos: Value supplied for pos in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise OSError("seek unavailable")

    def read(self):
        """
        Perform the read test-helper operation with deterministic inputs.

        Example:
            Exercise TellSeekBroken.read through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self._payload


class _ExplodingIdentifiers:
    """
    Provide the ExplodingIdentifiers test fixture or double with explicit deterministic behavior.

    Example:
        Exercise ExplodingIdentifiers through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py
    """
    def get_identifiers(self):
        """
        Return identifiers from deterministic test state.

        Example:
            Exercise ExplodingIdentifiers.get identifiers through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        raise RuntimeError("identifier backend unavailable")


class _IdentifierCarrier:
    """
    Provide the IdentifierCarrier test fixture or double with explicit deterministic behavior.

    Example:
        Exercise IdentifierCarrier through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py
    """
    def __init__(
        self,
        *,
        title=None,
        authors=None,
        language=None,
        comments=None,
        publisher=None,
        tags=None,
        series=None,
        series_index=None,
        title_sort=None,
        isbn=None,
        identifiers=None,
        set_raises=False,
    ):
        """
        Initialize the IdentifierCarrier test double.

        Example:
            Exercise IdentifierCarrier.init through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :param title: Value supplied for title in the focused test operation.
        :param authors: Value supplied for authors in the focused test operation.
        :param language: Value supplied for language in the focused test operation.
        :param comments: Value supplied for comments in the focused test operation.
        :param publisher: Value supplied for publisher in the focused test operation.
        :param tags: Value supplied for tags in the focused test operation.
        :param series: Value supplied for series in the focused test operation.
        :param series_index: Value supplied for series index in the focused test operation.
        :param title_sort: Value supplied for title sort in the focused test operation.
        :param isbn: Value supplied for isbn in the focused test operation.
        :param identifiers: Value supplied for identifiers in the focused test operation.
        :param set_raises: Value supplied for set raises in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self.title = title
        self.authors = authors
        self.language = language
        self.comments = comments
        self.publisher = publisher
        self.tags = tags
        self.series = series
        self.series_index = series_index
        self.title_sort = title_sort
        self.isbn = isbn
        self._identifiers = identifiers
        self._set_raises = set_raises

    def get_identifiers(self):
        """
        Return identifiers from deterministic test state.

        Example:
            Exercise IdentifierCarrier.get identifiers through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self._identifiers

    def set_identifiers(self, identifiers):
        """
        Perform the set identifiers test-helper operation with deterministic inputs.

        Example:
            Exercise IdentifierCarrier.set identifiers through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :param identifiers: Value supplied for identifiers in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        if self._set_raises:
            raise RuntimeError("cannot set identifiers")
        self._identifiers = identifiers


class _BadIterable:
    """
    Provide the BadIterable test fixture or double with explicit deterministic behavior.

    Example:
        Exercise BadIterable through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py
    """
    def __iter__(self):
        """
        Perform the iter test-helper operation with deterministic inputs.

        Example:
            Exercise BadIterable.iter through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        raise RuntimeError("cannot iterate")


class _RestoreSeekBroken(io.BytesIO):
    """
    Provide the RestoreSeekBroken test fixture or double with explicit deterministic behavior.

    Example:
        Exercise RestoreSeekBroken through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py
    """
    def seek(self, pos, whence=0):
        """
        Perform the seek test-helper operation with deterministic inputs.

        Example:
            Exercise RestoreSeekBroken.seek through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :param pos: Value supplied for pos in the focused test operation.
        :param whence: Value supplied for whence in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        if getattr(self, "_break_restore", False) and pos != 0:
            raise OSError("restore unavailable")
        return super().seek(pos, whence)


class _SetIdentifierRaises:
    """
    Provide the SetIdentifierRaises test fixture or double with explicit deterministic behavior.

    Example:
        Exercise SetIdentifierRaises through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py
    """
    def __init__(self):
        """
        Initialize the SetIdentifierRaises test double.

        Example:
            Exercise SetIdentifierRaises.init through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :return: None; the function records state or raises through its assertions.
        """
        self.title = "Raising Identifier Setter"
        self.authors = ["Identifier Author"]
        self.identifiers = {}

    def set_identifier(self, _typ, _val):
        """
        Perform the set identifier test-helper operation with deterministic inputs.

        Example:
            Exercise SetIdentifierRaises.set identifier through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :param _typ: Value supplied for typ in the focused test operation.
        :param _val: Value supplied for val in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise RuntimeError("single setter unavailable")

    def set_identifiers(self, identifiers):
        """
        Perform the set identifiers test-helper operation with deterministic inputs.

        Example:
            Exercise SetIdentifierRaises.set identifiers through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :param identifiers: Value supplied for identifiers in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self.identifiers = identifiers


class _FakeLiuxinMetadata:
    """
    Provide the FakeLiuxinMetadata test fixture or double with explicit deterministic behavior.

    Example:
        Exercise FakeLiuxinMetadata through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py
    """
    def __init__(self, series=None, fail_isbn=False):
        """
        Initialize the FakeLiuxinMetadata test double.

        Example:
            Exercise FakeLiuxinMetadata.init through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :param series: Value supplied for series in the focused test operation.
        :param fail_isbn: Value supplied for fail isbn in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self.title = "Fake Title"
        self.authors = ["Fake Author"]
        self.language = None
        self.comments = None
        self.publisher = None
        self.tags = None
        self.series = series
        self.series_index = None
        self.title_sort = None
        self.calibre_series_index = None
        self._identifiers = {}
        self._fail_isbn = fail_isbn

    def __setattr__(self, name, value):
        """
        Perform the setattr test-helper operation with deterministic inputs.

        Example:
            Exercise FakeLiuxinMetadata.setattr through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :param name: Value supplied for name in the focused test operation.
        :param value: Value stored, compared or projected by the operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        if name == "isbn" and getattr(self, "_fail_isbn", False):
            raise RuntimeError("isbn assignment unavailable")
        super().__setattr__(name, value)

    def finalize(self):
        """
        Perform the finalize test-helper operation with deterministic inputs.

        Example:
            Exercise FakeLiuxinMetadata.finalize through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return None

    def set_identifiers(self, identifiers):
        """
        Perform the set identifiers test-helper operation with deterministic inputs.

        Example:
            Exercise FakeLiuxinMetadata.set identifiers through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :param identifiers: Value supplied for identifiers in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        self._identifiers = identifiers

    def get_identifiers(self):
        """
        Return identifiers from deterministic test state.

        Example:
            Exercise FakeLiuxinMetadata.get identifiers through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :return: The deterministic value, row, identity or collection described above.
        """
        return self._identifiers


def _package_with_unicode_edges() -> bytes:
    """
    Perform the package with unicode edges test-helper operation with deterministic inputs.

    Example:
        Exercise package with unicode edges through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


    :return: The deterministic value, row, identity or collection described above.
    """
    return """<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf"
         xmlns:dc="http://purl.org/dc/elements/1.1/"
         xmlns:opf="http://www.idpf.org/2007/opf"
         unique-identifier="BookId"
         version="2.0">
  <metadata>
    <dc:title>Καφές — 世界 — café 😀</dc:title>
    <dc:creator>Renée Faßbinder and 李白</dc:creator>
    <dc:language>EL</dc:language>
    <dc:description>Line one

Line two with ZWJ: 👩‍💻 and RTL: שלום</dc:description>
    <dc:publisher>دار النشر</dc:publisher>
    <dc:subject>naïve;δοκιμή,テスト</dc:subject>
    <dc:subject>δοκιμή;résumé</dc:subject>
    <meta name="opf.subject" content="追加;δοκιμή"/>
    <meta name="opf.publisher" content="გამომცემელი"/>
    <meta name="opf.language" content="fr"/>
    <meta name="opf.pubdate" content="2026-05-16"/>
    <meta name="opf.series" content="Σειρά 世界"/>
    <meta name="opf.series_index">7,5</meta>
    <meta name="opf.series_index" content="not-a-number"/>
    <meta name="opf.title_sort" content="Cafe World"/>
    <dc:identifier id="BookId">urn:uuid:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee</dc:identifier>
    <dc:identifier opf:scheme="ISBN">978-0-306-40615-7</dc:identifier>
    <dc:identifier opf:scheme="DOI">10.5555/Unicode.δοκιμή</dc:identifier>
    <dc:identifier>custom-id:値</dc:identifier>
    <dc:date>not-a-date</dc:date>
  </metadata>
  <manifest>
    <item id="chap1" href="text/chap1.xhtml" media-type="application/xhtml+xml"/>
  </manifest>
  <spine>
    <itemref idref="chap1"/>
  </spine>
</package>
""".encode("utf-8")


def test_opf_helper_edges_for_names_normalization_and_stream_bytes(tmp_path: Path) -> None:
    """
    Verify opf helper edges for names normalization and stream bytes.

    Example:
        Exercise test opf helper edges for names normalization and stream bytes through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


    :param tmp_path: Pytest-managed temporary directory for filesystem assertions.
    :return: None; the function records state or raises through its assertions.
    """
    assert opf._local_name(None) == ""
    assert opf._local_name("dc:title") == "title"
    assert opf._normalize(None) == ""
    assert opf._split_tags(" alpha;βeta,  γ ") == ["alpha", "βeta", "γ"]
    assert opf._stable_dedupe(["β", "α", "β", "γ"]) == ["β", "α", "γ"]
    assert opf._source_name(tmp_path / "名字.opf").endswith("名字.opf")
    assert opf._source_name("plain-name.opf") == "plain-name.opf"

    default = opf._default_metadata(str(tmp_path / "Fallback Name.opf"))
    assert default.title == "Fallback Name"

    root = etree.fromstring(b"<root><child /></root>")
    assert opf._is_xml_element(root)
    assert opf._read_target_bytes(root, text=False, file_is_raw_root=True).startswith(b"<root")

    assert opf._read_target_bytes(b"<x/>", text=True, file_is_raw_root=False) == b"<x/>"
    assert opf._read_target_bytes(bytearray(b"<x/>"), text=True, file_is_raw_root=False) == b"<x/>"
    assert opf._read_target_bytes("<x>é</x>", text=True, file_is_raw_root=False) == "<x>é</x>".encode()
    with pytest.raises(TypeError):
        opf._read_target_bytes(object(), text=True, file_is_raw_root=False)

    path = tmp_path / "pathlike.opf"
    path.write_bytes(b"<package/>")
    assert opf._read_target_bytes(path, text=False, file_is_raw_root=False) == b"<package/>"
    assert opf._read_target_bytes(bytearray(b"<bytes/>"), text=False, file_is_raw_root=False) == b"<bytes/>"

    stream = _TextStream("<root>text stream</root>")
    stream.seek(3)
    assert opf._read_target_bytes(stream, text=False, file_is_raw_root=False) == b"<root>text stream</root>"
    assert stream.tell() == 3

    broken = _TellSeekBroken("<root>broken</root>")
    assert opf._read_target_bytes(broken, text=False, file_is_raw_root=False) == b"<root>broken</root>"

    restore_broken = _RestoreSeekBroken(b"<root>restore</root>")
    restore_broken.seek(4)
    restore_broken._break_restore = True
    assert opf._read_target_bytes(restore_broken, text=False, file_is_raw_root=False) == b"<root>restore</root>"
    with pytest.raises(TypeError):
        opf._read_target_bytes(object(), text=False, file_is_raw_root=False)


def test_opf_parse_root_and_metadata_node_selection_edges() -> None:
    """
    Verify opf parse root and metadata node selection edges.

    Example:
        Exercise test opf parse root and metadata node selection edges through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


    :return: None; the function records state or raises through its assertions.
    """
    with pytest.raises(opf.OpfParseError):
        opf._parse_root_from_payload(b"")
    with pytest.raises(opf.OpfParseError):
        opf._parse_root_from_payload(b"\x00\x00\x00")

    recovered = opf._parse_root_from_payload(b"<package><metadata><title>Recovered")
    assert opf._local_name(recovered.tag) == "package"

    wrapper = etree.fromstring(
        b"""<root>
          <dc-metadata><title>Legacy DC Metadata</title></dc-metadata>
          <metadata><title>Preferred Metadata</title></metadata>
        </root>"""
    )
    candidates = opf.simple_get_metadata_node(wrapper)
    assert len(candidates) == 2
    assert opf._best_metadata_root(wrapper, seek_md_node=False) is wrapper
    assert opf._first_text(opf._best_metadata_root(wrapper, seek_md_node=True), {"title"}) == "Preferred Metadata"

    dc_only = etree.fromstring(b"<root><dc-metadata><title>Only DC</title></dc-metadata></root>")
    assert opf._first_text(opf._best_metadata_root(dc_only, seek_md_node=True), {"title"}) == "Only DC"
    no_candidates = etree.fromstring(b"<root><title>No Candidate</title></root>")
    assert opf._best_metadata_root(no_candidates, seek_md_node=True) is no_candidates


def test_opf_extract_generic_metadata_unicode_identifiers_and_overrides(monkeypatch) -> None:
    """
    Verify opf extract generic metadata unicode identifiers and overrides.

    Example:
        Exercise test opf extract generic metadata unicode identifiers and overrides through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    root = etree.fromstring(_package_with_unicode_edges())
    calls: list[str] = []
    original = opf.canonicalize_id_name

    def raising_canonicalize(scheme):
        """
        Perform the raising canonicalize test-helper operation with deterministic inputs.

        Example:
            Exercise test opf extract generic metadata unicode identifiers and overrides.raising canonicalize through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :param scheme: Value supplied for scheme in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        calls.append(str(scheme))
        if str(scheme).lower() == "custom-id":
            raise RuntimeError("unknown identifier namespace")
        return original(scheme)

    monkeypatch.setattr(opf, "canonicalize_id_name", raising_canonicalize)

    mi = opf._extract_generic_metadata_from_root(root, source_name="unicode-edge.opf")

    assert mi.title == "Καφές — 世界 — café 😀"
    assert mi.authors == ["Renée Faßbinder", "李白"]
    assert mi.language == "fr"
    assert mi.publisher == "გამომცემელი"
    assert set(mi.tags) >= {"naïve", "δοκιμή", "テスト", "résumé", "追加"}
    assert mi.series == "Σειρά 世界"
    assert float(mi.series_index) == 7.5
    assert mi.title_sort == "Cafe World"
    assert mi.isbn == "9780306406157"
    assert "Line one" in mi.comments
    assert "DOI" in calls
    assert "custom-id" in calls
    identifiers = mi.get_identifiers()
    assert identifiers["doi"] == "10.5555/Unicode.δοκιμή"
    assert identifiers["custom-id"] == "custom-id:値"

    invalid_pubdate = etree.fromstring(b"<metadata><meta name='opf.pubdate' content='definitely-not-a-date'/></metadata>")
    invalid_mi = opf._extract_generic_metadata_from_root(invalid_pubdate)
    assert invalid_mi.pubdate == "definitely-not-a-date"

    identifier_fallback_root = etree.fromstring(b"<metadata><identifier scheme='custom'>custom:value</identifier></metadata>")
    identifier_fallback = _SetIdentifierRaises()
    monkeypatch.setattr(opf, "_default_metadata", lambda _source_name="": identifier_fallback)
    assert opf._extract_generic_metadata_from_root(identifier_fallback_root) is identifier_fallback
    assert identifier_fallback.identifiers == {"custom": "custom:value"}

    empty_identifier_root = etree.fromstring(b"<metadata><identifier>  </identifier></metadata>")
    opf._extract_generic_metadata_from_root(empty_identifier_root)


def test_opf_safe_identifier_and_merge_edges() -> None:
    """
    Verify opf safe identifier and merge edges.

    Example:
        Exercise test opf safe identifier and merge edges through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


    :return: None; the function records state or raises through its assertions.
    """
    assert opf._iter_values(None) == []
    assert opf._iter_values("tag") == ["tag"]
    assert opf._iter_values(_BadIterable())[0].__class__ is _BadIterable

    assert opf._safe_get_identifiers(None) == {}
    assert opf._safe_get_identifiers(object()) == {}
    assert opf._safe_get_identifiers(_ExplodingIdentifiers()) == {}
    assert opf._safe_get_identifiers(_IdentifierCarrier(identifiers={"DOI": "10.1/x", "": "drop"})) == {
        "DOI": "10.1/x"
    }
    assert opf._safe_get_identifiers(_IdentifierCarrier(identifiers=[("oclc", "123"), ("empty", "")])) == {
        "oclc": "123"
    }
    assert opf._safe_get_identifiers(_IdentifierCarrier(identifiers=_BadIterable())) == {}

    preferred = _IdentifierCarrier(
        title=" ",
        authors=[],
        language="und",
        comments="",
        publisher=None,
        tags=["keep", "shared"],
        series="",
        series_index=None,
        title_sort="",
        isbn="",
        identifiers={"doi": "preferred"},
    )
    fallback = _IdentifierCarrier(
        title="Fallback Title",
        authors=["Alice", "Bob", "Alice"],
        language="es",
        comments="Fallback comments",
        publisher="Fallback Publisher",
        tags=["shared", "new"],
        series="Fallback Series",
        series_index=3,
        title_sort="Fallback Sort",
        isbn="9780306406157",
        identifiers={"isbn": "9780306406157", "doi": "fallback"},
    )

    merged = opf._merge_calibre_metadata(preferred, fallback)
    assert merged.title == "Fallback Title"
    assert merged.authors == ["Alice", "Bob"]
    assert merged.language == "es"
    assert merged.comments == "Fallback comments"
    assert merged.publisher == "Fallback Publisher"
    assert merged.tags == ["keep", "shared", "new"]
    assert merged.series == "Fallback Series"
    assert merged.series_index == 3
    assert merged.title_sort == "Fallback Sort"
    assert merged.isbn == "9780306406157"
    assert merged.get_identifiers() == {"isbn": "9780306406157", "doi": "preferred"}

    assert opf._merge_calibre_metadata(None, fallback) is fallback
    assert opf._merge_calibre_metadata(preferred, None) is preferred

    raising = _IdentifierCarrier(identifiers={}, set_raises=True)
    fallback_ids = _IdentifierCarrier(identifiers={"doi": "10.2/x"})
    assert opf._merge_calibre_metadata(raising, fallback_ids) is raising


def test_opf_to_liuxin_metadata_fallback_and_field_preservation(monkeypatch) -> None:
    """
    Verify opf to liuxin metadata fallback and field preservation.

    Example:
        Exercise test opf to liuxin metadata fallback and field preservation through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    calibre_like = _IdentifierCarrier(
        title="Fallback Convert Title",
        authors=["Åsa", "李白"],
        language="sv",
        comments="Preserve comments",
        publisher="Preserve Publisher",
        tags=["α", "β"],
        series="Preserve Series",
        series_index=4.5,
        title_sort="Convert Title, Fallback",
        isbn="9780306406157",
        identifiers={"doi": "10.1000/xyz"},
    )

    def explode_from_calibre(*_args, **_kwargs):
        """
        Perform the explode from calibre test-helper operation with deterministic inputs.

        Example:
            Exercise test opf to liuxin metadata fallback and field preservation.explode from calibre through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :param _args: Value supplied for args in the focused test operation.
        :param _kwargs: Value supplied for kwargs in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise RuntimeError("conversion failed")

    def explode_finalize(self):
        """
        Perform the explode finalize test-helper operation with deterministic inputs.

        Example:
            Exercise test opf to liuxin metadata fallback and field preservation.explode finalize through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :param self: Value supplied for self in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise RuntimeError("finalize failed")

    monkeypatch.setattr(opf.MetaData, "from_calibre", explode_from_calibre)
    monkeypatch.setattr(opf.MetaData, "finalize", explode_finalize)

    md = opf._to_liuxin_metadata(calibre_like)

    assert md.title == "Fallback Convert Title"
    assert _values(md.authors) == ["Åsa", "李白"]
    assert md.language == "sv"
    assert "Preserve comments" in _values(md.comments)[0]
    assert "Preserve Publisher" in _values(md.publisher)[0]
    assert set(_values(md.tags)) >= {"α", "β"}
    assert _values(md.series) == ["Preserve Series"]
    assert float(_first_mapping_value(md.series_index, 0.0)) == 4.5
    assert md.get_identifiers()["doi"] == {"10.1000/xyz"}
    assert _values(md.isbn) == ["9780306406157"]


def test_opf_to_liuxin_metadata_series_identifier_and_isbn_edges(monkeypatch) -> None:
    """
    Verify opf to liuxin metadata series identifier and isbn edges.

    Example:
        Exercise test opf to liuxin metadata series identifier and isbn edges through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    calibre_like = _IdentifierCarrier(
        title="Series Index Without Series",
        authors=["Author"],
        series=None,
        series_index=8,
        isbn="9780306406157",
        identifiers={"doi": "10.123/fake"},
    )

    fake_with_series = _FakeLiuxinMetadata(series={"Existing Series": object()})
    monkeypatch.setattr(opf.MetaData, "from_calibre", lambda _calibre_md: fake_with_series)
    assert opf._to_liuxin_metadata(calibre_like) is fake_with_series
    assert fake_with_series.series_index == ("Existing Series", 8)

    fake_without_series = _FakeLiuxinMetadata(series=None, fail_isbn=True)
    monkeypatch.setattr(opf.MetaData, "from_calibre", lambda _calibre_md: fake_without_series)
    assert opf._to_liuxin_metadata(calibre_like) is fake_without_series
    assert fake_without_series.calibre_series_index == 8
    assert "isbn" not in fake_without_series.__dict__

    class _IdentifierRaises(_IdentifierCarrier):
        """
        Provide the IdentifierRaises test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test opf to liuxin metadata series identifier and isbn edges.IdentifierRaises through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py
        """
        def get_identifiers(self):
            """
            Return identifiers from deterministic test state.

            Example:
                Exercise test opf to liuxin metadata series identifier and isbn edges.IdentifierRaises.get identifiers through its owning regression module::

                    python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


            :return: The deterministic value, row, identity or collection described above.
            """
            raise RuntimeError("identifier lookup failed")

    fake_identifier_receiver = _FakeLiuxinMetadata()
    monkeypatch.setattr(opf.MetaData, "from_calibre", lambda _calibre_md: fake_identifier_receiver)
    assert opf._to_liuxin_metadata(_IdentifierRaises(title="Title", authors=["Author"], identifiers={})) is fake_identifier_receiver


def test_opf_get_metadata_fallbacks_log_and_return_safe_metadata(monkeypatch) -> None:
    """
    Verify opf get metadata fallbacks log and return safe metadata.

    Example:
        Exercise test opf get metadata fallbacks log and return safe metadata through its owning regression module::

            python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    events: list[tuple[str, str]] = []

    def record_log(message, err, level, *_details):
        """
        Perform the record log test-helper operation with deterministic inputs.

        Example:
            Exercise test opf get metadata fallbacks log and return safe metadata.record log through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :param message: Value supplied for message in the focused test operation.
        :param err: Value supplied for err in the focused test operation.
        :param level: Value supplied for level in the focused test operation.
        :param _details: Value supplied for details in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        events.append((level, message))

    monkeypatch.setattr(opf.default_log, "log_exception", record_log)

    def explode_parser(_root):
        """
        Perform the explode parser test-helper operation with deterministic inputs.

        Example:
            Exercise test opf get metadata fallbacks log and return safe metadata.explode parser through its owning regression module::

                python -m pytest -q tests/metadata/file_sources/test_opf_edge_cases.py


        :param _root: Value supplied for root in the focused test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        raise RuntimeError("canonical parser failed")

    monkeypatch.setattr(opf, "_parse_using_opf_stack", explode_parser)

    md = opf.get_metadata(_NamedBytes(_package_with_unicode_edges(), name="canonical-fallback.opf"))
    assert md.title == "Καφές — 世界 — café 😀"
    assert _values(md.authors) == ["Renée Faßbinder", "李白"]
    assert any(level == "DEBUG" for level, _message in events)

    md_calibre = opf.get_metadata(_NamedBytes(_package_with_unicode_edges()), calibre=True)
    assert md_calibre.title == "Καφές — 世界 — café 😀"

    bad = opf.get_metadata(object(), fallback_on_parse_error=True)
    assert bad.title == "Unknown"
    assert _values(bad.authors) == ["Unknown"]
    assert any(level == "ERROR" for level, _message in events)
