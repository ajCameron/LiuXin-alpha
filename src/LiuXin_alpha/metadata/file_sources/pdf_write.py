"""
Write metadata into a PDF without losing anything else the file holds.

The update is appended to the file as an incremental update, the way PDF editors save:
the original bytes stay as they were, so outlines, forms and attachments are kept, the
earlier revision can still be recovered, and the bytes a digital signature covers do
not change. The Info
dictionary gets the new values and a new ``ModDate``. When the file has an XMP packet,
the same fields and ``xmp:MetadataDate`` are updated in it too, so no reader finds new
Info beside stale XMP, whichever way it combines the two.

Every result is read back with LiuXin's own PDF reader before it is returned. An
appended update to a file whose cross-reference data is damaged chains on to the
damage, and other readers may then not find the catalog at all; when the read-back
needs a repair scan, the whole document is cloned into a new file instead, and the
report says so. Encrypted files are refused.

A failure part-way through writing leaves the file as it was. On disk
(:func:`write_file`), an appended update that fails is cut off again, and a whole
rewrite goes to a new file beside the old one that replaces it only once complete,
so the old file is never half-overwritten. A stream (:func:`write_stream`) cannot be
swapped like that, so after a failure its original bytes are put back.

pypdf comes from the ``pdf`` extra.

Example:
    >>> pdf_date(datetime(2026, 11, 19, 23, 5, tzinfo=UTC))
    "D:20261119230500+00'00'"
"""

from __future__ import annotations

import contextlib
import io
import os
import shutil
import tempfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from typing import Any

from lxml import etree  # type: ignore[import-untyped]

from LiuXin_alpha.metadata.file_sources.subject_fields import join_subjects
from LiuXin_alpha.metadata.utils import authors_to_string

__all__ = [
    "MetadataUpdate",
    "PdfWriteError",
    "PdfWriteReport",
    "XmpNotEditable",
    "pdf_date",
    "updated_packet",
    "write_file",
    "write_metadata",
    "write_stream",
    "xmp_date",
]

RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
DC = "http://purl.org/dc/elements/1.1/"
XMP = "http://ns.adobe.com/xap/1.0/"
PDF = "http://ns.adobe.com/pdf/1.3/"
XML_LANG = "{http://www.w3.org/XML/1998/namespace}lang"
_PREFIXES = {RDF: "rdf", DC: "dc", XMP: "xmp", PDF: "pdf"}
#: The largest packet edited; LiuXin's reader reads no larger.
XMP_LIMIT = 4 << 20


class PdfWriteError(RuntimeError):
    """
    Signal that metadata could not be written without harming the file, which is left as it was.

    Example:
        >>> issubclass(PdfWriteError, RuntimeError)
        True
    """


class XmpNotEditable(ValueError):
    """
    Signal that an XMP packet cannot be edited safely, so it is left as it is.

    Example:
        >>> str(XmpNotEditable("no rdf:RDF element"))
        'no rdf:RDF element'
    """


@dataclass(frozen=True)
class MetadataUpdate:
    """
    The fields to write; a field left empty keeps whatever the file says.

    Example:
        >>> MetadataUpdate(title="T", authors=("Ann", "Bob & Co")).info()
        {'/Title': 'T', '/Author': 'Ann & Bob && Co'}
    """

    title: str | None = None
    authors: tuple[str, ...] = ()
    comments: str | None = None
    tags: tuple[str, ...] = ()
    producer: str | None = None
    publisher: str | None = None
    #: Info entries with no XMP counterpart, such as ``/Series``.
    extra: Mapping[str, str] = field(default_factory=dict)
    #: Subject headings, for the ``/Subject`` when it holds them.
    subjects: tuple[str, ...] = ()
    #: Whether ``/Subject`` holds the subjects (the ``read_subject_fields_as``
    #: preference's default) rather than the comments; the comments then go only into
    #: the packet's ``dc:description``.
    subject_holds_subjects: bool = False

    def info(self: MetadataUpdate) -> dict[str, str]:
        """
        Give the Info dictionary entries, mapped as LiuXin's reader reads them back.

        The authors are joined with `` & `` (an ampersand in a name is doubled). The
        ``Subject`` holds the subjects, joined by semicolons, when
        ``subject_holds_subjects`` says so; otherwise it holds the comments, as the XMP
        specification maps it.

        Example:
            >>> MetadataUpdate(comments="About", tags=("a", "b")).info()
            {'/Subject': 'About', '/Keywords': 'a, b'}
            >>> MetadataUpdate(comments="About", subjects=("Sea",), subject_holds_subjects=True).info()
            {'/Subject': 'Sea'}


        :return: The entries, by name with its slash.
        """
        out: dict[str, str] = {}
        if self.title:
            out["/Title"] = self.title
        if self.authors:
            out["/Author"] = authors_to_string(self.authors)
        if self.subject_holds_subjects:
            if self.subjects:
                out["/Subject"] = join_subjects(self.subjects)
        elif self.comments:
            out["/Subject"] = self.comments
        if self.tags:
            out["/Keywords"] = ", ".join(self.tags)
        if self.producer:
            out["/Producer"] = self.producer
        if self.publisher:
            out["/Publisher"] = self.publisher
        out.update(self.extra)
        return out


@dataclass(frozen=True)
class PdfWriteReport:
    """
    What a write did.

    Example:
        >>> PdfWriteReport(rewritten=False, xmp="updated").warnings
        ()
    """

    #: ``False`` when the update was appended; ``True`` when the document was cloned whole.
    rewritten: bool
    #: ``updated``, ``none`` (the file has no packet) or ``unchanged`` (see the warnings).
    xmp: str
    warnings: tuple[str, ...] = ()


def pdf_date(moment: datetime) -> str:
    """
    Write a time as a PDF date.

    Example:
        >>> pdf_date(datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))
        "D:20260102030405+00'00'"


    :param moment: The time, with a zone.
    :return: The date.
    """
    offset = moment.utcoffset() or timedelta(0)
    minutes = int(offset.total_seconds()) // 60
    sign = "-" if minutes < 0 else "+"
    hours, minutes = divmod(abs(minutes), 60)
    return moment.strftime("D:%Y%m%d%H%M%S") + f"{sign}{hours:02d}'{minutes:02d}'"


def xmp_date(moment: datetime) -> str:
    """
    Write a time as an XMP date.

    Example:
        >>> xmp_date(datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))
        '2026-01-02T03:04:05+00:00'


    :param moment: The time, with a zone.
    :return: The date.
    """
    return moment.isoformat(timespec="seconds")


def _element(parent: Any, namespace: str, name: str) -> Any:
    """
    Add an element, declaring its namespace's usual prefix only where it is not already in scope.

    Example:
        >>> root = etree.fromstring(f'<rdf:RDF xmlns:rdf="{RDF}"/>')
        >>> element = _element(root, DC, "title")
        >>> element.prefix, element.nsmap["dc"] == DC
        ('dc', True)


    :param parent: The element to add to.
    :param namespace: The new element's namespace.
    :param name: Its local name.
    :return: The element.
    """
    qualified = f"{{{namespace}}}{name}"
    if namespace in parent.nsmap.values():
        return etree.SubElement(parent, qualified)
    prefix = _PREFIXES.get(namespace)
    nsmap = {prefix: namespace} if prefix and prefix not in parent.nsmap else None
    return etree.SubElement(parent, qualified, nsmap=nsmap)


def _replace(
    descriptions: Sequence[Any], namespace: str, name: str, fill: Callable[[Any], None]
) -> None:
    """
    Replace a top-level property, written as an element or an attribute, with a new element.

    The new element takes the place of the first old one, or ends the first description.

    Example:
        >>> rdf = etree.fromstring(f'<rdf:RDF xmlns:rdf="{RDF}"><rdf:Description xmlns:pdf="{PDF}" pdf:Producer="Old"/></rdf:RDF>')
        >>> _replace(rdf, PDF, "Producer", lambda element: setattr(element, "text", "New"))
        >>> etree.tostring(rdf[0]).decode().count("New"), etree.tostring(rdf[0]).decode().count("Old")
        (1, 0)


    :param descriptions: The packet's ``rdf:Description`` elements.
    :param namespace: The property's namespace.
    :param name: Its local name.
    :param fill: Gives the new element its content.
    :return: None.
    """
    qualified = f"{{{namespace}}}{name}"
    place: tuple[Any, int] | None = None
    tail: str | None = None
    for description in descriptions:
        description.attrib.pop(qualified, None)
        for child in description.findall(qualified):
            if place is None:
                place, tail = (description, description.index(child)), child.tail
            description.remove(child)
    parent, index = (
        place if place is not None else (descriptions[0], len(descriptions[0]))
    )
    element = _element(parent, namespace, name)
    parent.remove(element)
    parent.insert(index, element)
    element.tail = tail
    fill(element)


def _set_alternative(
    descriptions: Sequence[Any], namespace: str, name: str, text: str | None
) -> None:
    """
    Set the default of a language alternative, keeping its other languages.

    Example:
        >>> rdf = etree.fromstring(
        ...     f'<rdf:RDF xmlns:rdf="{RDF}"><rdf:Description xmlns:dc="{DC}"><dc:title><rdf:Alt>'
        ...     '<rdf:li xml:lang="x-default">Old</rdf:li><rdf:li xml:lang="fr">Vieux</rdf:li>'
        ...     "</rdf:Alt></dc:title></rdf:Description></rdf:RDF>"
        ... )
        >>> _set_alternative(rdf, DC, "title", "New")
        >>> [li.text for li in rdf.iter(f"{{{RDF}}}li")]
        ['New', 'Vieux']


    :param descriptions: The packet's ``rdf:Description`` elements.
    :param namespace: The property's namespace.
    :param name: Its local name.
    :param text: The default text; ``None`` leaves the property as it is.
    :return: None.
    """
    if not text:
        return
    qualified = f"{{{namespace}}}{name}"
    for description in descriptions:
        for existing in description.findall(qualified):
            alt = existing.find(f"{{{RDF}}}Alt")
            if alt is None:
                continue
            for item in alt.findall(f"{{{RDF}}}li"):
                if item.get(XML_LANG) == "x-default":
                    alt.remove(item)
            item = etree.Element(f"{{{RDF}}}li")
            item.set(XML_LANG, "x-default")
            item.text = text
            alt.insert(0, item)
            for duplicate in description.findall(qualified):
                if duplicate is not existing:
                    description.remove(duplicate)
            for other in descriptions:
                other.attrib.pop(qualified, None)
                if other is not description:
                    for duplicate in other.findall(qualified):
                        other.remove(duplicate)
            return

    def fill(element: Any) -> None:
        """
        Write a new alternative holding the default only.

        Example:
            >>> fill(element)  # doctest: +SKIP


        :param element: The property's element.
        :return: None.
        """
        item = etree.SubElement(
            etree.SubElement(element, f"{{{RDF}}}Alt"), f"{{{RDF}}}li"
        )
        item.set(XML_LANG, "x-default")
        item.text = text

    _replace(descriptions, namespace, name, fill)


def _set_list(
    descriptions: Sequence[Any],
    namespace: str,
    name: str,
    kind: str,
    items: Sequence[str],
) -> None:
    """
    Set an ordered (``Seq``) or unordered (``Bag``) list property.

    Example:
        >>> rdf = etree.fromstring(f'<rdf:RDF xmlns:rdf="{RDF}"><rdf:Description/></rdf:RDF>')
        >>> _set_list(rdf, DC, "creator", "Seq", ["Ann", "Bob"])
        >>> [li.text for li in rdf.iter(f"{{{RDF}}}li")]
        ['Ann', 'Bob']


    :param descriptions: The packet's ``rdf:Description`` elements.
    :param namespace: The property's namespace.
    :param name: Its local name.
    :param kind: ``Seq`` or ``Bag``.
    :param items: The items; none leaves the property as it is.
    :return: None.
    """
    if not items:
        return

    def fill(element: Any) -> None:
        """
        Write the list.

        Example:
            >>> fill(element)  # doctest: +SKIP


        :param element: The property's element.
        :return: None.
        """
        container = etree.SubElement(element, f"{{{RDF}}}{kind}")
        for text in items:
            etree.SubElement(container, f"{{{RDF}}}li").text = text

    _replace(descriptions, namespace, name, fill)


def _set_text(
    descriptions: Sequence[Any], namespace: str, name: str, text: str | None
) -> None:
    """
    Set a simple text property.

    Example:
        >>> rdf = etree.fromstring(f'<rdf:RDF xmlns:rdf="{RDF}"><rdf:Description/></rdf:RDF>')
        >>> _set_text(rdf, PDF, "Keywords", "a, b")
        >>> rdf[0][0].text
        'a, b'


    :param descriptions: The packet's ``rdf:Description`` elements.
    :param namespace: The property's namespace.
    :param name: Its local name.
    :param text: The text; ``None`` leaves the property as it is.
    :return: None.
    """
    if not text:
        return

    def fill(element: Any) -> None:
        """
        Write the text.

        Example:
            >>> fill(element)  # doctest: +SKIP


        :param element: The property's element.
        :return: None.
        """
        element.text = text

    _replace(descriptions, namespace, name, fill)


def updated_packet(packet: bytes, update: MetadataUpdate, moment: datetime) -> bytes:
    """
    Write the update's fields into an XMP packet, keeping every other property.

    ``dc:title`` and ``dc:description`` keep their other languages; ``dc:creator``,
    ``dc:subject``, ``dc:publisher``, ``pdf:Keywords`` and ``pdf:Producer`` are
    replaced; ``xmp:MetadataDate`` and ``xmp:ModifyDate`` become the moment.

    Example:
        >>> packet = f'<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="{RDF}"/></x:xmpmeta>'.encode()
        >>> new = updated_packet(packet, MetadataUpdate(title="T"), datetime(2026, 1, 1, tzinfo=UTC))
        >>> b'<rdf:li xml:lang="x-default">T</rdf:li>' in new, b"2026-01-01T00:00:00+00:00</xmp:MetadataDate>" in new
        (True, True)


    :param packet: The packet.
    :param update: The fields to write.
    :param moment: When the metadata changed.
    :return: The new packet.
    :raises XmpNotEditable: The packet is not well-formed XMP that can be edited safely.
    """
    if len(packet) > XMP_LIMIT:
        raise XmpNotEditable(f"the packet is larger than {XMP_LIMIT} bytes")
    parser = etree.XMLParser(
        resolve_entities=False, no_network=True, load_dtd=False, huge_tree=False
    )
    try:
        root = etree.fromstring(packet, parser=parser)
    except etree.XMLSyntaxError as error:
        raise XmpNotEditable(f"the packet is not well-formed XML: {error}") from error
    if root.getroottree().docinfo.doctype:
        raise XmpNotEditable("the packet declares a document type")
    rdf = root if root.tag == f"{{{RDF}}}RDF" else root.find(f".//{{{RDF}}}RDF")
    if rdf is None:
        raise XmpNotEditable("the packet has no rdf:RDF element")
    descriptions = rdf.findall(f"{{{RDF}}}Description")
    if not descriptions:
        description = etree.SubElement(rdf, f"{{{RDF}}}Description")
        description.set(f"{{{RDF}}}about", "")
        descriptions = [description]
    _set_alternative(descriptions, DC, "title", update.title)
    _set_list(descriptions, DC, "creator", "Seq", update.authors)
    _set_alternative(descriptions, DC, "description", update.comments)
    _set_list(descriptions, DC, "subject", "Bag", update.tags)
    _set_list(
        descriptions,
        DC,
        "publisher",
        "Bag",
        [update.publisher] if update.publisher else [],
    )
    _set_text(
        descriptions, PDF, "Keywords", ", ".join(update.tags) if update.tags else None
    )
    _set_text(descriptions, PDF, "Producer", update.producer)
    _set_text(descriptions, XMP, "ModifyDate", xmp_date(moment))
    _set_text(descriptions, XMP, "MetadataDate", xmp_date(moment))
    result: bytes = etree.tostring(root.getroottree(), encoding="utf-8")
    return result


@dataclass
class _Built:
    """
    One attempt's result.

    Example:
        >>> _Built(b"%PDF-1.4").warnings
        []
    """

    payload: bytes
    #: The packet written, or ``None`` when none was.
    packet: bytes | None = None
    #: Whether the file has a packet, written or not.
    has_packet: bool = False
    warnings: list[str] = field(default_factory=list)


#: An XMP packet with nothing in it, for a file that has none.
EMPTY_PACKET = (
    b'<?xpacket begin="\xef\xbb\xbf" id="W5M0MpCehiHzreSzNTczkc9d"?>\n'
    b'<x:xmpmeta xmlns:x="adobe:ns:meta/"><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">'
    b'<rdf:Description rdf:about=""/></rdf:RDF></x:xmpmeta>\n<?xpacket end="w"?>'
)


def _add_packet(writer: Any) -> Any:
    """
    Give a document an empty XMP packet, as its catalog's ``/Metadata``.

    Example:
        Give a pypdf writer a packet to fill::

            reference = _add_packet(writer)


    :param writer: A pypdf ``PdfWriter``.
    :return: The reference to the new stream.
    """
    from pypdf.generic import DecodedStreamObject, NameObject

    stream = DecodedStreamObject()
    stream.set_data(EMPTY_PACKET)
    stream[NameObject("/Type")] = NameObject("/Metadata")
    stream[NameObject("/Subtype")] = NameObject("/XML")
    reference = writer._add_object(stream)
    writer.root_object[NameObject("/Metadata")] = reference
    return reference


def _build(
    original: bytes, update: MetadataUpdate, moment: datetime, *, append: bool
) -> _Built:
    """
    Write the update with pypdf, as an appended update or as a clone of the whole document.

    Example:
        Append an update to a file's bytes::

            built = _build(original, update, moment, append=True)


    :param original: The file.
    :param update: The fields to write.
    :param moment: When the metadata changed.
    :param append: Append an incremental update, rather than clone the document.
    :return: The new file, the packet written, and warnings.
    """
    from pypdf import PdfReader, PdfWriter

    reader = PdfReader(io.BytesIO(original), strict=False)
    writer = (
        PdfWriter(reader, incremental=True) if append else PdfWriter(clone_from=reader)
    )
    writer.add_metadata({**update.info(), "/ModDate": pdf_date(moment)})
    built = _Built(b"")
    reference = writer.root_object.get("/Metadata")
    if reference is None and update.subject_holds_subjects and update.comments:
        # The Subject holds subjects, so the comments live only in the packet: give
        # the file one rather than lose them.
        reference = _add_packet(writer)
    if reference is not None:
        built.has_packet = True
        try:
            stream = reference.get_object()
            packet = updated_packet(stream.get_data(), update, moment)
            stream.set_data(packet)
            built.packet = packet
        except Exception as error:  # pypdf raises several kinds for streams it cannot decode or re-encode
            built.warnings.append(
                f"The XMP packet was left unchanged ({error}), so it may disagree with the Info "
                "dictionary; readers that prefer XMP may show the old values."
            )
    out = io.BytesIO()
    writer.write(out)
    built.payload = out.getvalue()
    return built


def _appends_cleanly(original: bytes, update: MetadataUpdate, moment: datetime) -> bool:
    """
    Tell whether an appended update without a new XMP packet reads back from a file.

    A file without a packet gets one when the comments have nowhere else to go, which
    rewrites the catalog in the appended update. That can hide damaged cross-reference
    data from the read-back check, so whether to append is decided as it would be
    without the packet.

    Example:
        Decide whether to append::

            if not _appends_cleanly(original, update, moment): ...


    :param original: The file.
    :param update: The fields to write.
    :param moment: When the metadata changed.
    :return: True when such an update appends and reads back.
    """
    plain = replace(update, comments=None)
    try:
        built = _build(original, plain, moment, append=True)
    except Exception:  # pypdf raises many kinds for files it cannot parse
        return False
    return built.payload.startswith(original) and _reads_back(built.payload, plain, moment, built.packet)


def _reads_back(
    payload: bytes, update: MetadataUpdate, moment: datetime, packet: bytes | None
) -> bool:
    """
    Tell whether LiuXin's reader finds what was written, through cross-reference data alone.

    Example:
        Check a new file::

            assert _reads_back(payload, update, moment, packet)


    :param payload: The new file.
    :param update: The fields written.
    :param moment: The ``ModDate`` written.
    :param packet: The XMP packet written, if one was.
    :return: Whether everything reads back without a repair scan.
    """
    from LiuXin_alpha.file_formats.pdf.errors import PdfReadError
    from LiuXin_alpha.file_formats.pdf.objects import PdfDocument

    try:
        document = PdfDocument(payload)
        found = document.metadata()
    except PdfReadError:
        return False
    if document.repaired:
        return False
    expected = {name.lstrip("/"): value for name, value in update.info().items()}
    expected["ModDate"] = pdf_date(moment)
    if any(found.info.get(name) != value for name, value in expected.items()):
        return False
    return packet is None or found.xmp == packet


def write_metadata(
    original: bytes, update: MetadataUpdate, *, moment: datetime | None = None
) -> tuple[bytes, PdfWriteReport]:
    """
    Write metadata into a PDF, appending an update where that reads back cleanly.

    Example:
        Write a title into a file's bytes::

            payload, report = write_metadata(original, MetadataUpdate(title="New"))
            assert payload.startswith(original) or report.rewritten


    :param original: The file.
    :param update: The fields to write.
    :param moment: When the metadata changed; now, by default.
    :return: The new file, which begins with the original unless the report says it was
        rewritten, and the report.
    :raises PdfWriteError: The file is encrypted, pypdf cannot read it, or no result reads back.
    """
    from pypdf import PdfReader

    moment = moment or datetime.now(UTC).replace(microsecond=0)
    try:
        encrypted = PdfReader(io.BytesIO(original), strict=False).is_encrypted
    except Exception as error:  # pypdf raises many kinds for files it cannot parse
        raise PdfWriteError(f"pypdf cannot read the PDF: {error}") from error
    if encrypted:
        raise PdfWriteError(
            "LiuXin does not write metadata into encrypted PDFs: pypdf's updates to them do not "
            "read back reliably. The file is unchanged."
        )
    attempts = (True, False)
    if update.subject_holds_subjects and update.comments and not _appends_cleanly(original, update, moment):
        # A new packet puts the catalog into the appended update, which can then read
        # back even over damaged cross-reference data. Decide as the update would
        # without one: rewrite whole a file that an appended update does not suit.
        attempts = (False,)
    for append in attempts:
        try:
            built = _build(original, update, moment, append=append)
        except Exception as error:  # as above; nothing has been written yet
            if append:
                continue
            raise PdfWriteError(f"pypdf could not write the PDF: {error}") from error
        if append and not built.payload.startswith(original):
            continue
        if not _reads_back(built.payload, update, moment, built.packet):
            continue
        if not append:
            built.warnings.insert(
                0,
                "The PDF was rewritten whole: an update appended to it would not read back, as its "
                "cross-reference data is damaged. Its pages and document structure are kept, but not "
                "its original bytes, and any digital signature no longer verifies.",
            )
        state = (
            "updated"
            if built.packet is not None
            else "unchanged"
            if built.has_packet
            else "none"
        )
        report = PdfWriteReport(
            rewritten=not append, xmp=state, warnings=tuple(built.warnings)
        )
        return built.payload, report
    raise PdfWriteError(
        "LiuXin could not write metadata that reads back from this PDF. The file is unchanged."
    )


def _same_file(before: os.stat_result, now: os.stat_result) -> bool:
    """
    Tell whether a file is unchanged since it was read, by its size and modification time.

    Example:
        >>> _same_file(os.stat(__file__), os.stat(__file__))
        True


    :param before: Its status when it was read.
    :param now: Its status now.
    :return: Whether both match.
    """
    return (before.st_size, before.st_mtime_ns) == (now.st_size, now.st_mtime_ns)


def _changed_meanwhile() -> PdfWriteError:
    """
    Build the error for a file that changed while its update was being prepared.

    Example:
        >>> "changed" in str(_changed_meanwhile())
        True


    :return: The error.
    """
    return PdfWriteError(
        "The PDF changed while its metadata was being prepared, so nothing was written; "
        "it was left as it now is."
    )


def _append_to_file(
    target: str, before: os.stat_result, length: int, addition: bytes
) -> None:
    """
    Append an update to a file, cutting it back to its old length if the write fails.

    Example:
        Append an update to a file read as ``original``::

            _append_to_file(target, before, len(original), payload[len(original):])


    :param target: The file's real path.
    :param before: Its status when it was read.
    :param length: Its length then, where the update begins.
    :param addition: The update.
    :return: None.
    :raises PdfWriteError: The file changed meanwhile, or the write failed and was undone.
    """
    descriptor = os.open(target, os.O_RDWR | getattr(os, "O_BINARY", 0))
    try:
        if not _same_file(before, os.fstat(descriptor)):
            raise _changed_meanwhile()
        try:
            view = memoryview(addition)
            os.lseek(descriptor, length, os.SEEK_SET)
            while view:
                view = view[os.write(descriptor, view) :]
            os.fsync(descriptor)
        except BaseException as error:
            with contextlib.suppress(OSError):
                os.ftruncate(descriptor, length)
                os.fsync(descriptor)
            if isinstance(error, OSError):
                raise PdfWriteError(
                    f"Writing the update failed ({error}); it was cut off again, so the PDF "
                    "is as it was."
                ) from error
            raise
    finally:
        os.close(descriptor)


def _replace_file(target: str, before: os.stat_result, payload: bytes) -> None:
    """
    Replace a file by writing the new one beside it and renaming it into place once complete.

    The new file takes the old one's permissions. It is a new file, so other hard links
    to the old one keep the old version.

    Example:
        Rewrite a file read as ``original``::

            _replace_file(target, before, payload)


    :param target: The file's real path.
    :param before: Its status when it was read.
    :param payload: The new file.
    :return: None.
    :raises PdfWriteError: The file changed meanwhile, or the new file could not be
        written; the old one is untouched either way.
    """
    directory, name = os.path.split(target)
    descriptor, staged = tempfile.mkstemp(
        dir=directory or ".", prefix=f".{name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        shutil.copymode(target, staged)
        if not _same_file(before, os.stat(target)):
            raise _changed_meanwhile()
        os.replace(staged, target)
    except BaseException as error:
        with contextlib.suppress(OSError):
            os.unlink(staged)
        if isinstance(error, OSError):
            raise PdfWriteError(
                f"Writing the new PDF failed ({error}); the old one is untouched."
            ) from error
        raise
    with contextlib.suppress(OSError, AttributeError):
        folder = os.open(directory or ".", os.O_RDONLY)
        try:
            os.fsync(folder)
        finally:
            os.close(folder)


def write_file(
    path: str | os.PathLike[str],
    update: MetadataUpdate,
    *,
    moment: datetime | None = None,
) -> PdfWriteReport:
    """
    Write metadata into a PDF on disk, so that a failure part-way leaves it as it was.

    A symbolic link is followed, and the file it names is written.

    Example:
        Retitle a file::

            report = write_file("book.pdf", MetadataUpdate(title="New"))


    :param path: The file.
    :param update: The fields to write.
    :param moment: When the metadata changed; now, by default.
    :return: The report.
    :raises PdfWriteError: As for :func:`write_metadata`; also when the file changed while
        the update was prepared, or the write failed. The file is as it was.
    """
    target = os.path.realpath(path)
    with open(target, "rb") as handle:
        before = os.fstat(handle.fileno())
        original = handle.read()
    payload, report = write_metadata(original, update, moment=moment)
    if report.rewritten:
        _replace_file(target, before, payload)
    else:
        _append_to_file(target, before, len(original), payload[len(original) :])
    return report


def write_stream(
    stream: Any, update: MetadataUpdate, *, moment: datetime | None = None
) -> PdfWriteReport:
    """
    Write metadata into a PDF held in a readable, writable, seekable binary stream.

    The stream is read from its start. If the write fails, the original bytes are put
    back before the error is raised. The stream's position afterwards is unspecified.

    Example:
        Retitle a PDF held in memory::

            stream = io.BytesIO(original)
            report = write_stream(stream, MetadataUpdate(title="New"))


    :param stream: The stream.
    :param update: The fields to write.
    :param moment: When the metadata changed; now, by default.
    :return: The report.
    :raises PdfWriteError: As for :func:`write_metadata`, or the write failed and the
        original bytes were put back.
    """
    stream.seek(0)
    original = stream.read()
    payload, report = write_metadata(original, update, moment=moment)
    start = 0 if report.rewritten else len(original)
    try:
        stream.seek(start)
        stream.truncate()
        stream.write(payload[start:])
        stream.flush()
        with contextlib.suppress(AttributeError, OSError, io.UnsupportedOperation):
            os.fsync(stream.fileno())
    except BaseException as error:
        with contextlib.suppress(Exception):
            stream.seek(start)
            stream.truncate()
            stream.write(original[start:])
            stream.flush()
        if isinstance(error, (OSError, ValueError)):
            raise PdfWriteError(
                f"Writing the PDF failed ({error}); its original bytes were put back."
            ) from error
        raise
    return report
