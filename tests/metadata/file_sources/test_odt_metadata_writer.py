"""
Check the ODT metadata writer: what it writes, what it keeps, and what it refuses.

LiuXin had no ODT writer; this one writes ``meta.xml`` as calibre's does. Every
result is read back with LiuXin's reader and with calibre's ported reader.

Example:
    Run these checks::

        python -m pytest -q tests/metadata/file_sources/test_odt_metadata_writer.py
"""

from __future__ import annotations

import datetime
import io
import zipfile
from pathlib import Path
from typing import Any

import pytest
from lxml import etree

from LiuXin_alpha.file_formats.plugin_runtime import (
    get_file_type_metadata,
    set_file_type_metadata,
)
from LiuXin_alpha.file_formats.providers.ported_calibre import ported_reader
from LiuXin_alpha.metadata.book.base import calibreMetaInformation
from LiuXin_alpha.metadata.file_sources.builtin_writers import get_metadata_set_plugins
from LiuXin_alpha.metadata.file_sources.odt import OdtFormatError, set_metadata

OFFICE = "urn:oasis:names:tc:opendocument:xmlns:office:1.0"
META = "urn:oasis:names:tc:opendocument:xmlns:meta:1.0"
DC = "http://purl.org/dc/elements/1.1/"
META_XML = (
    '<?xml version="1.0" encoding="UTF-8"?>\n'
    f'<office:document-meta xmlns:office="{OFFICE}" xmlns:meta="{META}" xmlns:dc="{DC}" office:version="1.2">'
    "<office:meta>{children}</office:meta></office:document-meta>"
)
CONTENT_XML = (
    f'<office:document-content xmlns:office="{OFFICE}"><office:body><office:text/></office:body>'
    "</office:document-content>"
)


def _odt(children: str = "", *, meta_xml: str | None = None) -> bytes:
    """
    Build a small ODT: an uncompressed ``mimetype`` first, then content, a picture and metadata.

    Example:
        >>> zipfile.ZipFile(io.BytesIO(_odt())).namelist()[0]
        'mimetype'


    :param children: The elements of ``office:meta``.
    :param meta_xml: The whole ``meta.xml`` in their place; ``None`` to build it.
    :return: The package.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as package:
        package.writestr(
            zipfile.ZipInfo("mimetype"), "application/vnd.oasis.opendocument.text"
        )
        package.writestr("content.xml", CONTENT_XML, compress_type=zipfile.ZIP_DEFLATED)
        package.writestr(
            zipfile.ZipInfo("Pictures/1.png"), b"\x89PNG\r\n\x1a\n" + bytes(range(256))
        )
        package.writestr(
            "meta.xml",
            META_XML.format(children=children) if meta_xml is None else meta_xml,
            compress_type=zipfile.ZIP_DEFLATED,
        )
        package.writestr(
            "META-INF/manifest.xml", "<manifest/>", compress_type=zipfile.ZIP_DEFLATED
        )
    return buffer.getvalue()


def _full_record() -> Any:
    """
    Build a record with every field the ODT writer stores.

    Example:
        >>> _full_record().series_index
        2.5


    :return: The record.
    """
    mi = calibreMetaInformation("Écrit — A Title", ["Ann Writer", "Jürgen Ölmann"])
    mi.title_sort = "Title, A"
    mi.author_sort = "Writer, Ann & Ölmann, Jürgen"
    mi.tags = ["alpha", "beta gamma"]
    mi.languages = ["fra"]
    mi.pubdate = datetime.datetime(2020, 5, 17, 12, 0, tzinfo=datetime.UTC)
    mi.publisher = "Éditions X"
    mi.series = "Saga"
    mi.series_index = 2.5
    mi.set_identifiers({"isbn": "9780441013593", "goodreads": "234225"})
    mi.rating = 8
    mi.comments = "Some comments & more <here>"
    return mi


def _fields(mi: Any) -> dict[str, Any]:
    """
    Take the fields the ODT writer stores, comparably across both readers.

    Example:
        >>> _fields(_full_record())["languages"]
        ['fra']


    :param mi: A record from either reader, or one to write.
    :return: The fields.
    """
    return {
        "title": mi.title,
        "authors": list(mi.authors),
        "tags": sorted(mi.tags or []),
        "languages": list(mi.languages or []),
        "pubdate": str(mi.pubdate)[:10] if mi.pubdate else None,
        "publisher": mi.publisher,
        "series": mi.series,
        "series_index": float(mi.series_index) if mi.series else None,
        "identifiers": dict(mi.identifiers or {}),
        "rating": mi.rating,
        "comments": mi.comments,
        "title_sort": mi.title_sort,
        "author_sort": mi.author_sort,
    }


def _write(data: bytes, mi: Any, errors: list[str] | None = None) -> bytes:
    """
    Write a record into an ODT through the writer plugin, as the application does.

    Example:
        >>> b"Dune" in _write(_odt(), calibreMetaInformation("Dune", []))
        True


    :param data: The ODT.
    :param mi: The record.
    :param errors: Collects the errors the writer reports.
    :return: The rewritten ODT.
    """
    stream = io.BytesIO(data)
    report = (
        None
        if errors is None
        else (lambda _mi, _ftype, trace: errors.append(str(trace)))
    )
    set_file_type_metadata(stream, mi, "odt", report_error=report)
    return stream.getvalue()


def _both(data: bytes) -> tuple[dict[str, Any], dict[str, Any]]:
    """
    Read an ODT with LiuXin's reader and with calibre's.

    Example:
        >>> _both(_odt())[1]["title"]
        'Unknown'


    :param data: The ODT.
    :return: The fields LiuXin's reader gives, and those calibre's gives.
    """
    ours = get_file_type_metadata(io.BytesIO(data), "odt")
    calibre = ported_reader("odt")(io.BytesIO(data), extract_cover=False)
    return _fields(ours), _fields(calibre)


def _members(data: bytes) -> list[tuple[str, int, bytes]]:
    """
    List a package's members in order, with their compression and contents.

    Example:
        >>> [m[:2] for m in _members(_odt())][:1]
        [('mimetype', 0)]


    :param data: The package.
    :return: Name, compression method and bytes of each member.
    """
    with zipfile.ZipFile(io.BytesIO(data)) as package:
        return [
            (i.filename, i.compress_type, package.read(i.filename))
            for i in package.infolist()
        ]


def _meta(data: bytes) -> Any:
    """
    Parse an ODT's ``meta.xml``, strictly.

    Example:
        >>> _meta(_odt()).tag.endswith("document-meta")
        True


    :param data: The ODT.
    :return: Its root element.
    """
    with zipfile.ZipFile(io.BytesIO(data)) as package:
        raw = package.read("meta.xml")
    return etree.fromstring(
        raw, etree.XMLParser(resolve_entities=False, no_network=True)
    )


def test_the_odt_writer_is_registered() -> None:
    """
    An ODT writer plugin exists, for ODT alone.

    Example:
        >>> test_the_odt_writer_is_registered()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    plugins = {plugin.__name__: plugin for plugin in get_metadata_set_plugins()}
    assert plugins["ODTMetadataWriter"].file_types == frozenset({"odt"})


def test_every_field_reads_back_in_both_readers() -> None:
    """
    Write every field twice; LiuXin's reader and calibre's read back what was written.

    Example:
        >>> test_every_field_reads_back_in_both_readers()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    data = _odt()
    for _ in range(2):
        errors: list[str] = []
        data = _write(data, _full_record(), errors)
        assert not errors
    ours, calibre = _both(data)
    assert ours == calibre == _fields(_full_record())


def test_only_meta_xml_changes() -> None:
    """
    Every other member keeps its bytes, compression and place; ``mimetype`` stays first and stored.

    Example:
        >>> test_only_meta_xml_changes()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    original = _odt("<dc:title>Old</dc:title>")
    written = _write(_write(original, _full_record()), _full_record())
    before, after = _members(original), _members(written)
    assert [m[0] for m in after] == [m[0] for m in before]
    assert after[0][:2] == ("mimetype", zipfile.ZIP_STORED)
    assert [m for m in after if m[0] != "meta.xml"] == [
        m for m in before if m[0] != "meta.xml"
    ]


def test_fields_the_record_leaves_null_are_kept() -> None:
    """
    A title-only record changes the title, and the elements and properties the file had stay.

    Example:
        >>> test_fields_the_record_leaves_null_are_kept()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    original = _odt(
        "<dc:title>Old</dc:title><meta:initial-creator>Cy Writer</meta:initial-creator>"
        "<dc:creator>Cy Writer</dc:creator><dc:description>About it</dc:description>"
        "<meta:keyword>one, two</meta:keyword><dc:language>de</dc:language>"
        '<meta:user-defined meta:name="opf.metadata" meta:value-type="boolean">true</meta:user-defined>'
        '<meta:user-defined meta:name="opf.publisher">Old Press</meta:user-defined>'
        '<meta:user-defined meta:name="opf.series">Old Saga</meta:user-defined>'
        '<meta:user-defined meta:name="opf.seriesindex">3.0</meta:user-defined>'
    )
    ours_before, calibre_before = _both(original)
    ours, calibre = _both(_write(original, calibreMetaInformation("New Title", [])))
    assert ours == {**ours_before, "title": "New Title"}
    assert calibre == {**calibre_before, "title": "New Title"}
    assert calibre["authors"] == ["Cy Writer"] and calibre["series"] == "Old Saga"


def test_properties_a_reader_prefers_are_replaced() -> None:
    """
    Remove the old properties that a reader reads in place of a written field, in any letter case.

    LiuXin's reader takes ``opf.title``, ``opf.subject`` and ``opf.series_index`` over
    what calibre writes. Both readers take ``opf.language`` over ``dc:language``, and
    calibre's merges ``meta:keywords`` into the tags. A write must not be hidden behind them.

    Example:
        >>> test_properties_a_reader_prefers_are_replaced()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    original = _odt(
        "<dc:title>Old</dc:title><meta:keywords>stale</meta:keywords>"
        '<meta:user-defined meta:name="Opf.metadata" meta:value-type="boolean">true</meta:user-defined>'
        '<meta:user-defined meta:name="Opf.title">Old</meta:user-defined>'
        '<meta:user-defined meta:name="opf.subject">stale</meta:user-defined>'
        '<meta:user-defined meta:name="Opf.language">de</meta:user-defined>'
        '<meta:user-defined meta:name="Opf.isbn">9780306406157</meta:user-defined>'
        '<meta:user-defined meta:name="opf.series">Old Saga</meta:user-defined>'
        '<meta:user-defined meta:name="opf.series_index">9</meta:user-defined>'
    )
    written = _write(original, _full_record())
    ours, calibre = _both(written)
    assert ours == calibre == _fields(_full_record())
    names = [
        (x.get(f"{{{META}}}name") or "").lower()
        for x in _meta(written).iter(f"{{{META}}}user-defined")
    ]
    assert names.count("opf.metadata") == 1
    assert not {
        "opf.title",
        "opf.subject",
        "opf.language",
        "opf.isbn",
        "opf.series_index",
    } & set(names)
    assert not list(_meta(written).iter(f"{{{META}}}keywords"))


def test_tags_the_subject_supplies_stay_in_the_subject() -> None:
    """
    Keep each reader's tags as they were when a record repeats the tags read from the file.

    LiuXin's reader counts the document's Subject as tags; calibre's does not. A
    title-only write through the service starts from LiuXin's reading, Subject included.
    Copying that into the keywords would give calibre a tag it never had.

    Example:
        >>> test_tags_the_subject_supplies_stay_in_the_subject()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    original = _odt(
        "<dc:title>Old</dc:title><dc:subject>Fiction; Sea</dc:subject><meta:keyword>whales</meta:keyword>"
    )
    ours_before, calibre_before = _both(original)
    assert ours_before["tags"] == ["Fiction", "Sea", "whales"] and calibre_before[
        "tags"
    ] == ["whales"]

    mi = calibreMetaInformation("New", [])
    mi.tags = ["Fiction", "Sea", "whales"]
    ours, calibre = _both(_write(original, mi))
    assert (
        ours["tags"] == ours_before["tags"]
        and calibre["tags"] == calibre_before["tags"]
    )

    mi.tags = ["Fiction", "Sea"]
    written = _write(original, mi)
    assert not list(_meta(written).iter(f"{{{META}}}keyword"))
    assert [x.text for x in _meta(written).iter(f"{{{DC}}}subject")] == ["Fiction; Sea"]
    assert _both(written)[1]["tags"] == []


def test_a_title_only_write_through_the_service_changes_only_the_title(
    tmp_path: Path,
) -> None:
    """
    The service fills omitted fields from LiuXin's reading of the file; each reader still reads what it read.

    Fields whose value is what LiuXin already reads are not written, so values LiuXin
    takes from places calibre ignores are not copied where calibre would read them.

    Example:
        >>> test_a_title_only_write_through_the_service_changes_only_the_title(Path("."))  # doctest: +SKIP


    :param tmp_path: Holds the book.
    :return: None; the assertions are the check.
    """
    from types import SimpleNamespace

    from LiuXin_alpha.core.program_services import metadata as service

    book = tmp_path / "book.odt"
    # LiuXin's reader takes the creator, the last-saved date, the Subject and plain
    # series properties where calibre's takes the initial creator or nothing.
    book.write_bytes(
        _odt(
            "<dc:title>Old</dc:title><meta:initial-creator>Ann Writer</meta:initial-creator>"
            "<dc:creator>Cy Writer</dc:creator><dc:date>2017-04-08T03:49:24</dc:date>"
            "<dc:subject>Fiction</dc:subject><meta:keyword>whales, sea</meta:keyword><dc:language>de</dc:language>"
            '<meta:user-defined meta:name="opf.metadata" meta:value-type="boolean">true</meta:user-defined>'
            '<meta:user-defined meta:name="opf.publisher">Old Press</meta:user-defined>'
            '<meta:user-defined meta:name="series">Plain Saga</meta:user-defined>'
            '<meta:user-defined meta:name="series_index">2</meta:user-defined>'
        )
    )
    ours_before, calibre_before = _both(book.read_bytes())
    receipt = service.metadata_file_write(
        None,  # type: ignore[arg-type]
        SimpleNamespace(payload={"path": str(book), "metadata": {"title": "After"}}),
    )
    assert receipt["updated"] is True
    ours, calibre = _both(book.read_bytes())
    assert ours == {**ours_before, "title": "After"}
    assert calibre == {**calibre_before, "title": "After"}
    assert calibre["authors"] == ["Ann Writer"] and ours["authors"] == ["Cy Writer"]
    assert calibre["pubdate"] is None and calibre["series"] is None


def test_hostile_text_is_written_as_well_formed_xml() -> None:
    """
    Characters XML cannot hold are dropped; markup characters, astral and right-to-left text are kept.

    Example:
        >>> test_hostile_text_is_written_as_well_formed_xml()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    nul, bell, surrogate = chr(0), chr(7), chr(0xD800)
    title = f"A{nul}B{bell}C{surrogate}D <&> 🙂 שלום"
    mi = calibreMetaInformation(title, [f"Ann{nul} Writer"])
    mi.comments = "x" * 100_000 + "]]>"
    mi.tags = [f"t{surrogate}ag", "<b>"]
    errors: list[str] = []
    written = _write(_odt(), mi, errors)
    assert not errors
    _meta(written)
    ours, calibre = _both(written)
    for fields in (ours, calibre):
        assert fields["title"] == "ABCD <&> 🙂 שלום"
        assert fields["authors"] == ["Ann Writer"]
        assert fields["comments"] == "x" * 100_000 + "]]>"
        assert fields["tags"] == ["<b>", "tag"]


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(b"not a zip file", id="not-a-zip"),
        pytest.param(_odt().replace(b"meta.xml", b"meta.xmx"), id="no-meta-xml"),
        pytest.param(_odt(meta_xml="<office:document-meta"), id="malformed-meta-xml"),
        pytest.param(
            _odt(meta_xml=f'<office:document-meta xmlns:office="{OFFICE}"/>'),
            id="no-office-meta",
        ),
    ],
)
def test_a_file_that_cannot_be_written_is_refused_unchanged(data: bytes) -> None:
    """
    Refuse with a clear error, leaving the stream as it was; the plugin reports the error.

    Example:
        >>> test_a_file_that_cannot_be_written_is_refused_unchanged(b"")  # doctest: +SKIP


    :param data: The file.
    :return: None; the assertions are the check.
    """
    stream = io.BytesIO(data)
    with pytest.raises(OdtFormatError):
        set_metadata(stream, calibreMetaInformation("T", ["A"]))
    assert stream.getvalue() == data
    errors: list[str] = []
    assert _write(data, calibreMetaInformation("T", ["A"]), errors) == data
    assert errors


def test_entities_in_meta_xml_are_not_expanded(tmp_path: Path) -> None:
    """
    A ``meta.xml`` that declares an external entity is written without reading what it points to.

    Example:
        >>> test_entities_in_meta_xml_are_not_expanded(Path("."))  # doctest: +SKIP


    :param tmp_path: Holds the file the entity names.
    :return: None; the assertions are the check.
    """
    secret = tmp_path / "secret.txt"
    secret.write_text("SECRET-CONTENTS")
    meta_xml = (
        f'<?xml version="1.0"?><!DOCTYPE d [<!ENTITY x SYSTEM "file://{secret}">]>'
        f'<office:document-meta xmlns:office="{OFFICE}" xmlns:dc="{DC}">'
        "<office:meta><dc:description>&x;</dc:description></office:meta></office:document-meta>"
    )
    written = _write(_odt(meta_xml=meta_xml), calibreMetaInformation("T", ["A"]))
    assert b"SECRET-CONTENTS" not in written
    assert b"SECRET-CONTENTS" not in zipfile.ZipFile(io.BytesIO(written)).read(
        "meta.xml"
    )


def test_liuxin_reads_what_calibre_s_writer_writes() -> None:
    """
    Read the identifiers and rating calibre's own ODT writer stores, which LiuXin's reader ignored.

    calibre's ``meta.xml`` writer is run on its own: the ported module's ZIP
    helper, ``safe_replace``, is not ported.

    Example:
        >>> test_liuxin_reads_what_calibre_s_writer_writes()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    from LiuXin_alpha.metadata.upstream_calibre.odt import (
        _set_metadata as calibre_meta_xml,
    )

    raw = zipfile.ZipFile(io.BytesIO(_odt())).read("meta.xml")
    ours, calibre = _both(
        _odt(meta_xml=calibre_meta_xml(raw, _full_record()).decode("utf-8"))
    )
    assert (
        ours["identifiers"]
        == calibre["identifiers"]
        == {"isbn": "9780441013593", "goodreads": "234225"}
    )
    assert ours["rating"] == calibre["rating"] == 8


def test_the_data_repository_odts_keep_everything_but_meta_xml(
    md_test_files_by_ext: dict[str, list[Path]],
) -> None:
    """
    Write every field into each ODT in ``LiuXin_alpha_data``, twice; check the package and both readers.

    LiuXin's reader also counts the document's Subject (``dc:subject``) as a tag,
    which calibre's does not, so its tags are the written ones plus the Subject.

    Example:
        >>> test_the_data_repository_odts_keep_everything_but_meta_xml({})  # doctest: +SKIP


    :param md_test_files_by_ext: The data repository's books, by extension.
    :return: None; the assertions are the check.
    """
    paths = md_test_files_by_ext.get("odt", [])
    if not paths:
        pytest.skip("no ODT books in LiuXin_alpha_data")
    for path in paths:
        original = path.read_bytes()
        written = _write(_write(original, _full_record()), _full_record())
        before, after = _members(original), _members(written)
        assert [m for m in after if m[0] != "meta.xml"] == [
            m for m in before if m[0] != "meta.xml"
        ], path.name
        assert [m[0] for m in after] == [m[0] for m in before], path.name
        subjects = sorted(
            {
                part.strip()
                for x in _meta(written).iter(f"{{{DC}}}subject")
                for part in (x.text or "").split(",")
                if part.strip()
            }
        )
        ours, calibre = _both(written)
        expected = _fields(_full_record())
        assert calibre == expected, path.name
        assert ours == {**expected, "tags": sorted({*expected["tags"], *subjects})}, (
            path.name
        )
    assert len(paths) >= 7


def test_a_title_only_service_write_changes_only_the_title_in_the_data_repository_odts(
    md_test_files_by_ext: dict[str, list[Path]], tmp_path: Path
) -> None:
    """
    Write a title through the service into each ODT in ``LiuXin_alpha_data``; both readers see only that change.

    Example:
        >>> test_a_title_only_service_write_changes_only_the_title_in_the_data_repository_odts({}, Path("."))  # doctest: +SKIP


    :param md_test_files_by_ext: The data repository's books, by extension.
    :param tmp_path: Holds the copies written.
    :return: None; the assertions are the check.
    """
    from types import SimpleNamespace

    from LiuXin_alpha.core.program_services import metadata as service

    paths = md_test_files_by_ext.get("odt", [])
    if not paths:
        pytest.skip("no ODT books in LiuXin_alpha_data")
    for path in paths:
        book = tmp_path / path.name
        book.write_bytes(path.read_bytes())
        ours_before, calibre_before = _both(book.read_bytes())
        receipt = service.metadata_file_write(
            None,  # type: ignore[arg-type]
            SimpleNamespace(
                payload={"path": str(book), "metadata": {"title": "After"}}
            ),
        )
        assert receipt["updated"] is True, path.name
        ours, calibre = _both(book.read_bytes())
        assert ours == {**ours_before, "title": "After"}, path.name
        assert calibre == {**calibre_before, "title": "After"}, path.name
