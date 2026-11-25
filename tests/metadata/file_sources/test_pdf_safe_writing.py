"""
Check that writing PDF metadata keeps everything else the file holds, or says what it could not keep.

Writing needs pypdf (the ``pdf`` extra); without it these checks are skipped. Where
poppler's ``pdfinfo`` is installed, every written file is also read with it.

Example:
    Run these checks::

        python -m pytest -q tests/metadata/file_sources/test_pdf_safe_writing.py
"""

from __future__ import annotations

import io
import json
import shutil
import subprocess
import warnings
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

from LiuXin_alpha.file_formats.pdf.objects import (
    PdfDocument,
    minimal_pdf,
    read_metadata,
)
from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import (
    CalibreLikeLiuXinBookMetaData as MetaData,
)
from LiuXin_alpha.metadata.file_sources import pdf, pdf_merge, pdf_write
from LiuXin_alpha.metadata.file_sources.pdf_write import (
    MetadataUpdate,
    PdfWriteError,
    XmpNotEditable,
    pdf_date,
    updated_packet,
    write_metadata,
)
from tests.support.subject_reading import subject_reading

pypdf = pytest.importorskip("pypdf")

CORPUS = Path(__file__).resolve().parents[2] / "fixtures" / "pdf_metadata"
CASES: dict[str, Any] = json.loads((CORPUS / "cases.json").read_text(encoding="utf-8"))
ENCRYPTED = sorted(name for name in CASES if name.startswith(("aes", "rc4")))
#: Files whose cross-reference data an appended update cannot chain on to.
DAMAGED = {"broken_xref", "leading_junk"}
MOMENT = datetime(2026, 11, 19, 23, 0, tzinfo=UTC)
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
PACKET = f'<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="{RDF}"><rdf:Description/></rdf:RDF></x:xmpmeta>'


def _corpus(name: str) -> bytes:
    """
    Read a corpus file.

    Example:
        >>> _corpus("plain").startswith(b"%PDF")
        True


    :param name: The case.
    :return: The file's bytes.
    """
    return (CORPUS / f"{name}.pdf").read_bytes()


def _metadata() -> MetaData:
    """
    Make the metadata every test writes.

    Example:
        >>> _metadata().title
        'Nouveau — Title'


    :return: The metadata.
    """
    mi = MetaData()
    mi.title = "Nouveau — Title"
    mi.authors = ["Ann Writer", "Bob Two"]
    mi.tags = ["t1", "t2"]
    mi.comments = "New description"
    return mi


def _write(data: bytes) -> tuple[bytes, Any]:
    """
    Write :func:`_metadata` into a file's bytes.

    Example:
        >>> _write(_corpus("plain"))[1].rewritten
        False


    :param data: The file.
    :return: The new file and the report.
    """
    stream = io.BytesIO(data)
    report = pdf.set_metadata(stream, _metadata())
    return stream.getvalue(), report


def _poppler_title(data: bytes, tmp_path: Path) -> str | None:
    """
    Read a file's title with poppler's ``pdfinfo``.

    Example:
        Read a title, if poppler is installed::

            title = _poppler_title(data, tmp_path)


    :param data: The file.
    :param tmp_path: A folder to write it to.
    :return: The title, or ``None`` when poppler finds none.
    """
    path = tmp_path / "written.pdf"
    path.write_bytes(data)
    out = subprocess.run(
        ["pdfinfo", "-enc", "UTF-8", str(path)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    return next(
        (
            line.split(":", 1)[1].strip()
            for line in out.stdout.splitlines()
            if line.startswith("Title:")
        ),
        None,
    )


@pytest.mark.parametrize("name", sorted(set(CASES) - set(ENCRYPTED) - DAMAGED))
def test_the_update_is_appended_after_the_original_bytes(
    name: str, tmp_path: Path
) -> None:
    """
    An intact file keeps its bytes; the update follows them, and every reader finds it.

    Example:
        >>> test_the_update_is_appended_after_the_original_bytes("plain", Path("/tmp"))  # doctest: +SKIP


    :param name: The corpus case.
    :param tmp_path: A folder for poppler's copy.
    :return: None after the file is checked.
    """
    original = _corpus(name)
    written, report = _write(original)
    assert written.startswith(original) and len(written) > len(original)
    assert not report.rewritten and report.warnings == ()
    document = PdfDocument(written)
    found = document.metadata()
    assert not document.repaired
    assert found.info["Title"] == "Nouveau — Title"
    assert found.info["Author"] == "Ann Writer & Bob Two"
    assert found.info["ModDate"].startswith("D:")
    assert (pypdf.PdfReader(io.BytesIO(written)).metadata or {})[
        "/Title"
    ] == "Nouveau — Title"
    if shutil.which("pdfinfo"):
        assert _poppler_title(written, tmp_path) == "Nouveau — Title"


def test_document_structure_survives() -> None:
    """
    Outlines, attachments and the XMP packet are still there; the old writer copied only the pages.

    Example:
        >>> test_document_structure_survives()


    :return: None after the structure is checked.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        writer = pypdf.PdfWriter(
            clone_from=pypdf.PdfReader(CORPUS / "xmp_and_info.pdf")
        )
        writer.add_outline_item("Chapter One", 0)
        writer.add_attachment("note.txt", b"attached bytes")
        out = io.BytesIO()
        writer.write(out)
    written, report = _write(out.getvalue())
    reader = pypdf.PdfReader(io.BytesIO(written))
    assert sorted(reader.trailer["/Root"].keys()) == [
        "/Metadata",
        "/Names",
        "/Outlines",
        "/Pages",
        "/Type",
    ]
    assert [item.title for item in reader.outline] == ["Chapter One"]
    assert reader.attachments == {"note.txt": [b"attached bytes"]}
    assert report.xmp == "updated"


def test_the_earlier_revision_is_still_there() -> None:
    """
    The original bytes are untouched, so the earlier revision, and any signature over it, survives.

    Example:
        >>> test_the_earlier_revision_is_still_there()


    :return: None after the revision is read.
    """
    original = _corpus("xmp_and_info")
    written, _report = _write(original)
    assert read_metadata(written[: len(original)]) == read_metadata(original)


def test_every_policy_reads_what_was_written() -> None:
    """
    The packet is updated with the Info dictionary, so no merge policy finds stale values.

    Example:
        >>> test_every_policy_reads_what_was_written()


    :return: None after each policy is checked.
    """
    written, report = _write(_corpus("xmp_and_info"))
    assert report.xmp == "updated"
    for policy in ("newest", "calibre", "union"):
        md = pdf.get_metadata(io.BytesIO(written), merge=policy)
        assert md.title == "Nouveau — Title", policy
        assert list(md.authors) == ["Ann Writer", "Bob Two"], policy
        assert list(md.comments) == ["New description"], policy
    for policy in ("newest", "calibre"):
        assert sorted(pdf.get_metadata(io.BytesIO(written), merge=policy).tags) == [
            "t1",
            "t2",
        ]
    # The packet's other properties stay: its publisher, DOI and creation date.
    packet = read_metadata(written).xmp or b""
    for kept in (
        b"XMP Publisher",
        b"10.4242/corpus",
        b"2019-06-01T12:00:00Z",
        b"<?xpacket end",
    ):
        assert kept in packet


@pytest.mark.parametrize("name", sorted(DAMAGED))
def test_damaged_files_are_rewritten_whole_with_a_warning(
    name: str, tmp_path: Path, monkeypatch: Any
) -> None:
    """
    An update appended to damaged cross-reference data would not read back, so the document is cloned.

    Example:
        Run under pytest, which supplies the fixtures::

            python -m pytest -q tests/metadata/file_sources/test_pdf_safe_writing.py -k damaged


    :param name: The corpus case.
    :param tmp_path: A folder for poppler's copy.
    :param monkeypatch: Captures the log.
    :return: None after the file and the warning are checked.
    """
    logged: list[tuple[Any, ...]] = []
    monkeypatch.setattr(
        pdf.default_log, "log_variables", lambda *args: logged.append(args)
    )
    written, report = _write(_corpus(name))
    assert report.rewritten
    assert "rewritten whole" in report.warnings[0]
    assert any(
        args[1] == "WARNING" and "rewritten whole" in str(args) for args in logged
    )
    document = PdfDocument(written)
    assert document.metadata().info["Title"] == "Nouveau — Title"
    assert not document.repaired
    if shutil.which("pdfinfo"):
        assert _poppler_title(written, tmp_path) == "Nouveau — Title"


@pytest.mark.parametrize("name", ENCRYPTED)
def test_encrypted_files_are_refused_and_left_unchanged(name: str) -> None:
    """
    pypdf's updates to encrypted files do not read back reliably, so none is written.

    Example:
        >>> test_encrypted_files_are_refused_and_left_unchanged("rc4_40_no_password")


    :param name: The corpus case.
    :return: None after the refusal is checked.
    """
    original = _corpus(name)
    stream = io.BytesIO(original)
    with pytest.raises(PdfWriteError, match="encrypted"):
        pdf.set_metadata(stream, _metadata())
    assert stream.getvalue() == original


def _with_packet(packet: bytes, *, stream_entries: bytes = b"") -> bytes:
    """
    Build a PDF whose catalog holds the given XMP stream.

    Example:
        >>> b"/Metadata 3 0 R" in _with_packet(b"<x/>")
        True


    :param packet: The stream's bytes, as stored.
    :param stream_entries: More entries for the stream's dictionary, such as a filter.
    :return: The file's bytes.
    """
    stream = b"<< /Type /Metadata /Subtype /XML /Length %d %s>>\nstream\n" % (
        len(packet),
        stream_entries,
    )
    return minimal_pdf(
        b"<< /Title (Old) >>",
        extra={
            3: stream + packet + b"\nendstream",
            4: b"<< /Type /Pages /Kids [] /Count 0 >>",
        },
        catalog=b"<< /Type /Catalog /Pages 4 0 R /Metadata 3 0 R >>",
    )


@pytest.mark.parametrize(
    ("packet", "entries"),
    [
        (b"<not xml", b""),
        # Valid XMP, but pypdf re-encodes only Flate streams.
        (PACKET.encode().hex().encode() + b">", b"/Filter /ASCIIHexDecode "),
    ],
    ids=["malformed", "unsupported-filter"],
)
def test_a_packet_that_cannot_be_edited_is_left_with_a_warning(
    packet: bytes, entries: bytes
) -> None:
    """
    The Info dictionary is still written, and the report says the packet was not.

    Example:
        >>> test_a_packet_that_cannot_be_edited_is_left_with_a_warning(b"<not xml", b"")


    :param packet: The stored stream.
    :param entries: Its extra dictionary entries.
    :return: None after the report is checked.
    """
    written, report = _write(_with_packet(packet, stream_entries=entries))
    assert report.xmp == "unchanged" and not report.rewritten
    assert "XMP packet was left unchanged" in report.warnings[0]
    assert read_metadata(written).info["Title"] == "Nouveau — Title"


def test_a_file_without_a_packet_gets_none_unless_there_are_comments() -> None:
    """
    No packet is added to a file that had none, unless there are comments to keep in it.

    The comments go into the packet's ``dc:description`` alone, under either reading
    of the Info Subject, so a file without a packet gets one for them, as calibre
    always writes one. See also ``test_subject_field_reading.py``.

    Example:
        >>> test_a_file_without_a_packet_gets_none_unless_there_are_comments()


    :return: None after the reports are checked.
    """
    plain = MetaData()
    plain.title = "Nouveau — Title"
    for reading in ("calibre", "subjects"):
        with subject_reading(reading):
            stream = io.BytesIO(_corpus("plain"))
            report = pdf.set_metadata(stream, plain)
            assert report.xmp == "none"
            assert read_metadata(stream.getvalue()).xmp is None
            written, report = _write(_corpus("plain"))
            assert report.xmp == "updated"
            assert b"New description" in read_metadata(written).xmp


def test_a_path_is_appended_to_in_place(tmp_path: Path) -> None:
    """
    Given a path, the file is opened and the update appended to it.

    Example:
        >>> test_a_path_is_appended_to_in_place(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: The folder.
    :return: None after the file is checked.
    """
    path = tmp_path / "book.pdf"
    original = _corpus("object_streams")
    path.write_bytes(original)
    report = pdf.set_metadata(path, _metadata())
    assert not report.rewritten
    assert path.read_bytes().startswith(original)
    assert pdf.get_metadata(path).title == "Nouveau — Title"


def test_a_damaged_file_on_disk_is_replaced_whole_and_keeps_its_permissions(
    tmp_path: Path,
) -> None:
    """
    A rewrite goes to a new file beside the old one, renamed into place; nothing is left over.

    Example:
        >>> test_a_damaged_file_on_disk_is_replaced_whole_and_keeps_its_permissions(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: The folder.
    :return: None after the file and folder are checked.
    """
    folder = tmp_path / "books"
    folder.mkdir()
    path = folder / "book.pdf"
    path.write_bytes(_corpus("broken_xref"))
    path.chmod(0o640)
    report = pdf.set_metadata(path, _metadata())
    assert report.rewritten
    assert pdf.get_metadata(path).title == "Nouveau — Title"
    assert path.stat().st_mode & 0o777 == 0o640
    assert [entry.name for entry in folder.iterdir()] == ["book.pdf"]


def test_a_failed_rewrite_leaves_the_file_untouched(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    If the new file cannot be put in place, the old one is as it was and the new one is gone.

    Example:
        >>> test_a_failed_rewrite_leaves_the_file_untouched(Path("/tmp"), None)  # doctest: +SKIP


    :param tmp_path: The folder.
    :param monkeypatch: Makes the rename fail.
    :return: None after the file and folder are checked.
    """
    folder = tmp_path / "books"
    folder.mkdir()
    path = folder / "book.pdf"
    original = _corpus("broken_xref")
    path.write_bytes(original)

    def refuse(_source: str, _target: str) -> None:
        """
        Fail as a full or read-only disk would.

        Example:
            >>> refuse("a", "b")  # doctest: +SKIP


        :param _source: The new file.
        :param _target: The old one.
        :return: Never returns.
        :raises OSError: Always.
        """
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(pdf_write.os, "replace", refuse)
    with pytest.raises(PdfWriteError, match="untouched"):
        pdf.set_metadata(path, _metadata())
    assert path.read_bytes() == original
    assert [entry.name for entry in folder.iterdir()] == ["book.pdf"]


def test_a_failed_append_is_cut_off_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    If an appended update cannot be completed, the file goes back to its old length.

    Example:
        >>> test_a_failed_append_is_cut_off_again(Path("/tmp"), None)  # doctest: +SKIP


    :param tmp_path: The folder.
    :param monkeypatch: Makes the write stop part-way, then fail.
    :return: None after the file is checked.
    """
    path = tmp_path / "book.pdf"
    original = _corpus("plain")
    path.write_bytes(original)
    real_write = pdf_write.os.write
    calls: list[int] = []

    def partly(descriptor: int, data: Any) -> int:
        """
        Write a few bytes, then fail as a full disk would.

        Example:
            >>> partly(1, b"")  # doctest: +SKIP


        :param descriptor: The file.
        :param data: The bytes.
        :return: How many were written, the first time.
        :raises OSError: The second time.
        """
        calls.append(len(data))
        if len(calls) > 1:
            raise OSError(28, "No space left on device")
        return real_write(descriptor, bytes(data[:7]))

    monkeypatch.setattr(pdf_write.os, "write", partly)
    with pytest.raises(PdfWriteError, match="cut off again"):
        pdf.set_metadata(path, _metadata())
    assert path.read_bytes() == original


def test_a_file_that_changes_meanwhile_is_not_written(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """
    If another program changes the file while the update is prepared, nothing is written.

    Example:
        >>> test_a_file_that_changes_meanwhile_is_not_written(Path("/tmp"), None)  # doctest: +SKIP


    :param tmp_path: The folder.
    :param monkeypatch: Changes the file during the write.
    :return: None after the file is checked.
    """
    path = tmp_path / "book.pdf"
    path.write_bytes(_corpus("plain"))
    real = pdf_write.write_metadata

    def meanwhile(original: bytes, update: MetadataUpdate, **options: Any) -> Any:
        """
        Prepare the update, while another program appends to the file.

        Example:
            >>> meanwhile(b"", MetadataUpdate())  # doctest: +SKIP


        :param original: The file.
        :param update: The fields.
        :param options: The other arguments.
        :return: The prepared update.
        """
        with path.open("ab") as other:
            other.write(b"% another program's change\n")
        return real(original, update, **options)

    monkeypatch.setattr(pdf_write, "write_metadata", meanwhile)
    with pytest.raises(PdfWriteError, match="changed"):
        pdf.set_metadata(path, _metadata())
    assert path.read_bytes() == _corpus("plain") + b"% another program's change\n"


def test_a_symbolic_link_is_followed_and_kept(tmp_path: Path) -> None:
    """
    Given a link, the file it names is written, and the link stays a link, even for a rewrite.

    Example:
        >>> test_a_symbolic_link_is_followed_and_kept(Path("/tmp"))  # doctest: +SKIP


    :param tmp_path: The folder.
    :return: None after the link and file are checked.
    """
    target = tmp_path / "book.pdf"
    target.write_bytes(_corpus("broken_xref"))
    link = tmp_path / "link.pdf"
    try:
        link.symlink_to(target)
    except OSError:
        pytest.skip("this system cannot make symbolic links here")
    pdf.set_metadata(link, _metadata())
    assert link.is_symlink()
    assert pdf.get_metadata(target).title == "Nouveau — Title"


class _FailingStream(io.BytesIO):
    """
    A stream whose writes fail after the first few bytes, as a full disk would.

    Example:
        >>> _FailingStream(b"x").getvalue()
        b'x'
    """

    def __init__(self: _FailingStream, data: bytes) -> None:
        """
        Hold the data, and allow writes until one is attempted.

        Example:
            >>> _FailingStream(b"x").fail
            False


        :param data: The stream's bytes.
        :return: None.
        """
        super().__init__(data)
        self.fail = False

    def write(self: _FailingStream, data: Any) -> int:
        """
        Write a few bytes and fail, the first time; then write normally, so a rollback works.

        Example:
            >>> _FailingStream(b"").write(b"")  # doctest: +SKIP


        :param data: The bytes.
        :return: How many were written.
        :raises OSError: The first time.
        """
        if not self.fail:
            self.fail = True
            super().write(bytes(data[:7]))
            raise OSError(28, "No space left on device")
        return super().write(data)


@pytest.mark.parametrize("name", ["plain", "broken_xref"])
def test_a_failed_write_to_a_stream_puts_the_original_back(name: str) -> None:
    """
    After a failed write, whether appended or whole, a stream holds its original bytes again.

    Example:
        >>> test_a_failed_write_to_a_stream_puts_the_original_back("plain")


    :param name: The case: one appended to, one rewritten.
    :return: None after the stream is checked.
    """
    original = _corpus(name)
    stream = _FailingStream(original)
    with pytest.raises(PdfWriteError, match="put back"):
        pdf.set_metadata(stream, _metadata())
    assert stream.getvalue() == original


def test_the_stream_position_is_restored() -> None:
    """
    The caller's stream is left where it was.

    Example:
        >>> test_the_stream_position_is_restored()


    :return: None after the position is checked.
    """
    stream = io.BytesIO(_corpus("plain"))
    stream.seek(5)
    pdf.set_metadata(stream, _metadata())
    assert stream.tell() == 5


def test_existing_info_entries_are_kept() -> None:
    """
    Entries the metadata does not set stay as the file had them.

    Example:
        >>> test_existing_info_entries_are_kept()


    :return: None after the entries are checked.
    """
    info = read_metadata(_write(_corpus("plain"))[0]).info
    assert info["Producer"] == "Corpus Producer"
    assert info["Creator"] == "Corpus Creator"


def test_the_moment_is_written_to_both_sources() -> None:
    """
    The Info ``ModDate`` and the packet's dates are the same moment.

    Example:
        >>> test_the_moment_is_written_to_both_sources()


    :return: None after the dates are checked.
    """
    payload, _report = write_metadata(
        _corpus("xmp_and_info"), MetadataUpdate(title="T"), moment=MOMENT
    )
    found = read_metadata(payload)
    assert found.info["ModDate"] == pdf_date(MOMENT) == "D:20261119230000+00'00'"
    assert b"<xmp:MetadataDate>2026-11-19T23:00:00+00:00</xmp:MetadataDate>" in (
        found.xmp or b""
    )


def test_updated_packet_replaces_attribute_properties_and_keeps_languages() -> None:
    """
    A property written as an attribute is replaced; a title's other languages stay.

    Example:
        >>> test_updated_packet_replaces_attribute_properties_and_keeps_languages()


    :return: None after the packet is checked.
    """
    packet = (
        f'<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="{RDF}">'
        '<rdf:Description xmlns:pdf="http://ns.adobe.com/pdf/1.3/" pdf:Producer="Old Producer"/>'
        '<rdf:Description xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:title><rdf:Alt>'
        '<rdf:li xml:lang="fr">Titre</rdf:li><rdf:li xml:lang="x-default">Old</rdf:li>'
        "</rdf:Alt></dc:title></rdf:Description></rdf:RDF></x:xmpmeta>"
    ).encode()
    new = updated_packet(
        packet, MetadataUpdate(title="New", producer="New Producer"), MOMENT
    )
    parsed = pdf.xmp_to_dict(new, attributes=True)
    assert parsed["dc"]["title"] == {"x-default": "New", "fr": "Titre"}
    assert parsed["pdf"]["Producer"] == "New Producer"
    assert b"Old" not in new


@pytest.mark.parametrize(
    ("packet", "reason"),
    [
        (b"<not xml", "not well-formed"),
        (b"<x:xmpmeta xmlns:x='adobe:ns:meta/'/>", "no rdf:RDF"),
        (b'<!DOCTYPE x [<!ENTITY e "e">]><x>&e;</x>', "document type"),
        (b" " * (4 << 20) + b"<x/>", "larger than"),
    ],
    ids=["malformed", "no-rdf", "doctype", "too-large"],
)
def test_updated_packet_refuses_what_it_cannot_edit_safely(
    packet: bytes, reason: str
) -> None:
    """
    A packet that is not XMP, declares entities, or is too large is not edited.

    Example:
        >>> test_updated_packet_refuses_what_it_cannot_edit_safely(b"<not xml", "not well-formed")


    :param packet: The packet.
    :param reason: Part of the expected refusal.
    :return: None after the refusal is checked.
    """
    with pytest.raises(XmpNotEditable, match=reason):
        updated_packet(packet, MetadataUpdate(title="T"), MOMENT)


def test_authors_survive_a_round_trip() -> None:
    """
    Authors are joined as the reader splits them, so each comes back separately.

    Example:
        >>> test_authors_survive_a_round_trip()


    :return: None after the authors are checked.
    """
    mi = MetaData()
    mi.authors = ["Ann Writer", "Smith & Sons", "Carol"]
    stream = io.BytesIO(_corpus("plain"))
    pdf.set_metadata(stream, mi)
    info = read_metadata(stream.getvalue()).info
    assert info["Author"] == "Ann Writer & Smith && Sons & Carol"
    assert pdf_merge.info_fields(info).authors == (
        "Ann Writer",
        "Smith & Sons",
        "Carol",
    )
    assert list(pdf.get_metadata(io.BytesIO(stream.getvalue())).authors) == [
        "Ann Writer",
        "Smith & Sons",
        "Carol",
    ]
