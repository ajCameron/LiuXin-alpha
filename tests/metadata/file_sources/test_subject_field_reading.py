"""
Check the ``read_subject_fields_as`` preference: a document's Subject field as subjects, or as calibre reads it.

The fields are HTML's ``<meta name="subject">``, RTF's ``\\subject``, PDF's Info
``/Subject`` and ODT's ``dc:subject``. See
``LiuXin_alpha.metadata.file_sources.subject_fields``.

Example:
    Run these checks::

        python -m pytest -q tests/metadata/file_sources/test_subject_field_reading.py
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from LiuXin_alpha.configuration_defaults import GLOBAL_DEFAULTS
from LiuXin_alpha.file_formats.pdf.objects import PdfDocument
from LiuXin_alpha.file_formats.plugin_runtime import (
    get_file_type_metadata,
    set_file_type_metadata,
)
from LiuXin_alpha.file_formats.providers.ported_calibre import ported_reader
from LiuXin_alpha.metadata.book.base import calibreMetadata, calibreMetaInformation
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData,
)
from LiuXin_alpha.metadata.file_sources import pdf, subject_fields
from LiuXin_alpha.metadata.file_sources.builtin_writers import get_metadata_set_plugins
from LiuXin_alpha.metadata.file_sources.html import get_metadata_ as read_html
from tests.support.subject_reading import subject_reading

PDF_CORPUS = Path(__file__).resolve().parents[2] / "fixtures" / "pdf_metadata"
RTF = rb"{\rtf1\ansi{\info{\title Old}{\subject History, Modern; Sea}{\doccomm About it}{\category to-read}}Hello}"
HTML = (
    b'<html><head><title>T</title><meta name="subject" content="History, Modern; Sea">'
    b'<meta name="tags" content="to-read"></head></html>'
)
OFFICE = "urn:oasis:names:tc:opendocument:xmlns:office:1.0"
META = "urn:oasis:names:tc:opendocument:xmlns:meta:1.0"
DC = "http://purl.org/dc/elements/1.1/"


def _values(raw: Any) -> list[str]:
    """
    List a field's values, from either kind of record (LiuXin's keeps some fields as dicts).

    Example:
        >>> _values({"a": None}), _values("x"), _values(None)
        (['a'], ['x'], [])


    :param raw: The field.
    :return: Its values.
    """
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw]
    return [str(x) for x in raw]


def _subjects(mi: Any) -> list[str]:
    """
    Take a record's subjects, whichever kind of record.

    Example:
        >>> _subjects(calibreMetadata("T"))
        []


    :param mi: The record.
    :return: The subjects.
    """
    return subject_fields.subjects_of(mi)


def _record() -> Any:
    """
    Make a record with comments, a tag and two subjects, one holding a comma.

    Example:
        >>> _record().subjects
        ['History, Modern', 'Sea']


    :return: The record.
    """
    mi = calibreMetaInformation("A Title", ["Ann Writer"])
    mi.comments = "About it"
    mi.tags = ["to-read"]
    mi.subjects = ["History, Modern", "Sea"]
    return mi


def _write(data: bytes, ftype: str, mi: Any) -> bytes:
    """
    Write a record into a file through the runtime, as the application does, failing on any reported error.

    Example:
        >>> _write(RTF, "rtf", _record()).startswith(rb"{\\rtf1")
        True


    :param data: The file.
    :param ftype: Its type.
    :param mi: The record.
    :return: The written file.
    """
    errors: list[str] = []
    stream = io.BytesIO(data)
    set_file_type_metadata(
        stream,
        mi,
        ftype,
        report_error=lambda _mi, _ftype, trace: errors.append(str(trace)),
    )
    assert not errors, errors
    return stream.getvalue()


def test_the_default_reads_subject_fields_as_subjects() -> None:
    """
    The shipped default is ``subjects``; an unknown setting falls back to it.

    Example:
        >>> test_the_default_reads_subject_fields_as_subjects()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    assert GLOBAL_DEFAULTS[subject_fields.SUBJECT_FIELDS_PREFERENCE] == "subjects"
    assert subject_fields.DEFAULT_SUBJECT_FIELD_READING == "subjects"
    for setting, reading in (
        ("subjects", "subjects"),
        ("calibre", "calibre"),
        ("sideways", "subjects"),
    ):
        with subject_reading(setting):
            assert subject_fields.subject_field_reading() == reading


def test_subjects_are_set_and_taken_on_either_kind_of_record() -> None:
    """
    calibre's form keeps subjects in ``subjects``; LiuXin's record in ``subject``.

    Example:
        >>> test_subjects_are_set_and_taken_on_either_kind_of_record()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    calibre = calibreMetadata("T")
    liuxin = CalibreLikeLiuXinBookMetaData("T", ["A"])
    for record in (calibre, liuxin):
        subject_fields.set_subjects(record, ["Sea", " "])
        assert subject_fields.subjects_of(record) == ["Sea"]
    assert list(liuxin.subject) == ["Sea"]
    assert (
        subject_fields.join_subjects(subject_fields.split_subjects("a, b;c"))
        == "a, b; c"
    )


def test_writer_plugins_follow_the_preference() -> None:
    """
    The RTF, PDF and ODT writers take the subjects apart only when the Subject holds them.

    Example:
        >>> test_writer_plugins_follow_the_preference()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    plugins = {p.__name__: p for p in get_metadata_set_plugins()}
    for name in ("RTFMetadataWriter", "PDFMetadataWriter", "ODTMetadataWriter"):
        plugin = plugins[name](None)
        with subject_reading("subjects"):
            assert plugin.writes_subjects, name
        with subject_reading("calibre"):
            assert not plugin.writes_subjects, name


@pytest.mark.parametrize(
    ("reading", "tags", "subjects"),
    [
        ("subjects", ["to-read"], ["History, Modern", "Sea"]),
        ("calibre", ["to-read", "History", "Modern", "Sea"], []),
    ],
)
def test_html_reads_its_subject_by_the_preference(
    reading: str, tags: list[str], subjects: list[str]
) -> None:
    """
    HTML's ``<meta name="subject">``: subject headings, or tags as calibre reads them.

    Example:
        >>> test_html_reads_its_subject_by_the_preference("subjects", [], [])  # doctest: +SKIP


    :param reading: The preference.
    :param tags: The tags expected.
    :param subjects: The subjects expected.
    :return: None; the assertions are the check.
    """
    with subject_reading(reading):
        mi = read_html(HTML)
    assert _values(mi.tags) == tags
    assert _subjects(mi) == subjects


@pytest.mark.parametrize(
    ("reading", "comments", "subjects"),
    [
        ("subjects", ["About it"], ["History, Modern", "Sea"]),
        ("calibre", ["About it"], []),
    ],
)
def test_rtf_reads_its_subject_by_the_preference(
    reading: str, comments: list[str], subjects: list[str]
) -> None:
    """
    RTF's ``\\subject``: subject headings, or the comments as calibre reads it; ``\\doccomm`` is the comments.

    Example:
        >>> test_rtf_reads_its_subject_by_the_preference("subjects", [], [])  # doctest: +SKIP


    :param reading: The preference.
    :param comments: The comments expected.
    :param subjects: The subjects expected.
    :return: None; the assertions are the check.
    """
    with subject_reading(reading):
        mi = get_file_type_metadata(io.BytesIO(RTF), "rtf")
    assert _values(mi.comments) == comments
    assert _subjects(mi) == subjects
    with subject_reading("calibre"):
        only_subject = get_file_type_metadata(
            io.BytesIO(rb"{\rtf1{\info{\subject About}}x}"), "rtf"
        )
    assert _values(only_subject.comments) == ["About"]


def test_rtf_writes_subjects_and_comments_by_the_preference() -> None:
    """
    Subjects into ``\\subject`` and the comments into ``\\doccomm``; or, as calibre, the comments into ``\\subject``.

    Example:
        >>> test_rtf_writes_subjects_and_comments_by_the_preference()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    original = rb"{\rtf1\ansi{\info{\title Old}{\subject Old}}Hello}"
    with subject_reading("subjects"):
        written = _write(original, "rtf", _record())
        assert (
            rb"{\subject History, Modern; Sea}" in written
            and rb"{\doccomm About it}" in written
        )
        mi = get_file_type_metadata(io.BytesIO(written), "rtf")
    assert _subjects(mi) == ["History, Modern", "Sea"] and _values(mi.comments) == [
        "About it"
    ]
    assert _values(mi.tags) == ["to-read"]

    with subject_reading("calibre"):
        written = _write(original, "rtf", _record())
        assert rb"{\subject About it}" in written and rb"doccomm" not in written
        mi = get_file_type_metadata(io.BytesIO(written), "rtf")
    assert _values(mi.comments) == ["About it"] and not _subjects(mi)
    assert ported_reader("rtf")(io.BytesIO(written)).comments == "About it"


@pytest.mark.parametrize("merge", ["newest", "calibre", "union"])
def test_pdf_reads_its_info_subject_as_subjects_whatever_the_merge_policy(
    merge: str,
) -> None:
    """
    By default the Info ``/Subject`` holds subjects under every merge policy, and is neither a tag nor the description.

    Example:
        >>> test_pdf_reads_its_info_subject_as_subjects_whatever_the_merge_policy("newest")  # doctest: +SKIP


    :param merge: The merge policy.
    :return: None; the assertions are the check.
    """
    data = (PDF_CORPUS / "plain.pdf").read_bytes()
    with subject_reading("subjects"):
        mi = pdf.get_metadata(io.BytesIO(data), merge=merge)
    assert _subjects(mi) == ["Corpus subject"]
    assert "corpus subject" not in {t.lower() for t in _values(mi.tags)}
    assert "Corpus subject" not in _values(mi.comments)


@pytest.mark.parametrize("fixture", ["plain", "xmp_and_info"])
def test_pdf_writes_subjects_into_the_info_subject_and_keeps_the_comments_in_the_packet(
    fixture: str,
) -> None:
    """
    Subjects into ``/Subject``; the comments into ``dc:description``, a packet being added where there was none.

    Both LiuXin's reader and calibre's find the comments.

    Example:
        >>> test_pdf_writes_subjects_into_the_info_subject_and_keeps_the_comments_in_the_packet("plain")  # doctest: +SKIP


    :param fixture: A corpus file, without and with a packet.
    :return: None; the assertions are the check.
    """
    with subject_reading("subjects"):
        written = _write((PDF_CORPUS / f"{fixture}.pdf").read_bytes(), "pdf", _record())
        mi = get_file_type_metadata(io.BytesIO(written), "pdf")
    found = PdfDocument(written).metadata()
    assert found.info.get("Subject") == "History, Modern; Sea"
    assert found.xmp is not None and b"About it" in found.xmp
    assert _subjects(mi) == ["History, Modern", "Sea"]
    assert _values(mi.comments) == ["About it"]
    assert ported_reader("pdf")(io.BytesIO(written)).comments == "About it"


def test_pdf_writes_as_calibre_under_calibre_s_reading() -> None:
    """
    Leave ``/Subject`` as it is, put the subjects among the keywords and the comments in the packet.

    calibre's writer never writes the Info Subject, and always writes a packet. Read
    back, the packet is as new as the Info dictionary, so its tags win; LiuXin's
    earlier ``union`` rule keeps the Subject among them.

    Example:
        >>> test_pdf_writes_as_calibre_under_calibre_s_reading()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    with subject_reading("calibre"):
        written = _write((PDF_CORPUS / "plain.pdf").read_bytes(), "pdf", _record())
        read = {merge: pdf.get_metadata(io.BytesIO(written), merge=merge) for merge in ("newest", "union")}
    found = PdfDocument(written).metadata()
    assert found.info.get("Subject") == "Corpus subject"
    assert found.info.get("Keywords") == "to-read, History, Modern, Sea"
    assert found.xmp is not None and b"About it" in found.xmp
    assert list(read["newest"].comments) == ["About it"]
    assert "corpus subject" not in {str(tag).lower() for tag in read["newest"].tags}
    assert "corpus subject" in {str(tag).lower() for tag in read["union"].tags}


@pytest.mark.parametrize("fixture", ["broken_xref", "leading_junk"])
def test_a_damaged_pdf_given_a_packet_is_still_rewritten_whole(fixture: str) -> None:
    """
    A new packet would let an appended update read back over damaged data; the file is rewritten whole, as before.

    Example:
        >>> test_a_damaged_pdf_given_a_packet_is_still_rewritten_whole("broken_xref")  # doctest: +SKIP


    :param fixture: A damaged corpus file without a packet.
    :return: None; the assertions are the check.
    """
    from LiuXin_alpha.metadata.file_sources.pdf_write import write_metadata

    with subject_reading("subjects"):
        update = pdf._metadata_update(_record())
    assert update.subject_holds_subjects and update.comments
    _payload, report = write_metadata(
        (PDF_CORPUS / f"{fixture}.pdf").read_bytes(), update
    )
    assert report.rewritten
    assert report.xmp == "updated"


def _odt() -> bytes:
    """
    Build a small ODT whose Subject and keywords both hold something.

    Example:
        >>> zipfile.ZipFile(io.BytesIO(_odt())).namelist()[0]
        'mimetype'


    :return: The package.
    """
    meta = (
        f'<office:document-meta xmlns:office="{OFFICE}" xmlns:meta="{META}" xmlns:dc="{DC}">'
        "<office:meta><dc:title>Old</dc:title><dc:subject>Fiction; Sea</dc:subject>"
        "<meta:keyword>whales</meta:keyword></office:meta></office:document-meta>"
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as package:
        package.writestr(
            zipfile.ZipInfo("mimetype"), "application/vnd.oasis.opendocument.text"
        )
        package.writestr(
            "content.xml", f'<office:document-content xmlns:office="{OFFICE}"/>'
        )
        package.writestr("meta.xml", meta)
    return buffer.getvalue()


def test_odt_under_calibre_s_reading_ignores_its_subject_and_keeps_subjects_among_the_keywords() -> (
    None
):
    """
    calibre does not read ODT's Subject; under its reading the subjects are written among the keywords.

    Example:
        >>> test_odt_under_calibre_s_reading_ignores_its_subject_and_keeps_subjects_among_the_keywords()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    with subject_reading("calibre"):
        read = get_file_type_metadata(io.BytesIO(_odt()), "odt")
        mi = calibreMetaInformation("New", [])
        mi.subjects = ["Sea stories"]
        written = _write(_odt(), "odt", mi)
        after = get_file_type_metadata(io.BytesIO(written), "odt")
    assert not _subjects(read) and _values(read.tags) == ["whales"]
    meta = zipfile.ZipFile(io.BytesIO(written)).read("meta.xml")
    assert b"Fiction; Sea</dc:subject>" in meta
    # The record has no tags of its own, so its subjects become the keywords.
    assert _values(after.tags) == ["Sea stories"]
    with subject_reading("subjects"):
        assert _subjects(get_file_type_metadata(io.BytesIO(written), "odt")) == [
            "Fiction",
            "Sea",
        ]


@pytest.mark.parametrize("reading", ["subjects", "calibre"])
@pytest.mark.parametrize(("ftype", "data"), [("rtf", RTF), ("pdf", None)])
def test_a_title_only_write_through_the_service_changes_only_the_title(
    reading: str, ftype: str, data: bytes | None, tmp_path: Path
) -> None:
    """
    The service fills omitted fields from LiuXin's reading; under either reading the rest reads as before.

    Example:
        >>> test_a_title_only_write_through_the_service_changes_only_the_title("subjects", "rtf", RTF, Path("."))  # doctest: +SKIP


    :param reading: The preference.
    :param ftype: The format.
    :param data: The book, or ``None`` for the PDF corpus file.
    :param tmp_path: Holds the book.
    :return: None; the assertions are the check.
    """
    from LiuXin_alpha.core.program_services import metadata as service

    book = tmp_path / f"book.{ftype}"
    book.write_bytes(
        data if data is not None else (PDF_CORPUS / "xmp_and_info.pdf").read_bytes()
    )

    def fields() -> tuple[list[str], list[str], list[str], list[str]]:
        """
        Read the book's comments, tags, subjects and title.

        Example:
            >>> fields()  # doctest: +SKIP


        :return: The four fields.
        """
        mi = get_file_type_metadata(io.BytesIO(book.read_bytes()), ftype)
        return (
            _values(mi.comments),
            sorted(_values(mi.tags)),
            _subjects(mi),
            _values(mi.title),
        )

    with subject_reading(reading):
        comments, tags, subjects, _title = fields()
        receipt = service.metadata_file_write(
            None,  # type: ignore[arg-type]
            SimpleNamespace(
                payload={"path": str(book), "metadata": {"title": "After"}}
            ),
        )
        assert receipt["updated"] is True
        assert fields() == (comments, tags, subjects, ["After"])
