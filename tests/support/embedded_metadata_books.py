"""
Build tiny but real EPUB and FB2 books whose embedded metadata tests control.

The books are valid enough for LiuXin's metadata readers: an EPUB with a
container, OPF and one chapter, and an FB2 with a title-info block. Bodies are
placeholders; only the metadata matters to the ingest planner.
"""

from __future__ import annotations

import io
import zipfile
from collections.abc import Sequence
from xml.sax.saxutils import escape

_CONTAINER = (
    '<?xml version="1.0"?>'
    '<container version="1.0" xmlns="urn:oasis:names:tc:opendocument:xmlns:container">'
    '<rootfiles><rootfile full-path="content.opf" media-type="application/oebps-package+xml"/>'
    "</rootfiles></container>"
)
_CHAPTER = (
    '<?xml version="1.0" encoding="utf-8"?>'
    '<html xmlns="http://www.w3.org/1999/xhtml"><head><title>c</title></head>'
    "<body><p>Text.</p></body></html>"
)


def epub_bytes(
    title: str,
    authors: Sequence[str] = (),
    *,
    language: str | None = "en",
    date: str | None = None,
    publisher: str | None = None,
    identifiers: Sequence[tuple[str, str]] = (),
    tags: Sequence[str] = (),
    series: str | None = None,
    series_index: float | None = None,
    description: str | None = None,
) -> bytes:
    """
    Return an EPUB 2 book carrying the given metadata.

    Example:
        >>> epub_bytes("A", ["B"])[:2]
        b'PK'


    :param title: ``dc:title``.
    :param authors: ``dc:creator`` names with the ``aut`` role.
    :param language: ``dc:language``, or None to omit it.
    :param date: ``dc:date`` text, or None.
    :param publisher: ``dc:publisher``, or None.
    :param identifiers: ``(scheme, value)`` pairs written as ``dc:identifier``.
    :param tags: ``dc:subject`` values.
    :param series: calibre series name, or None.
    :param series_index: calibre series index, used with ``series``.
    :param description: ``dc:description`` text, or None.
    :return: Zipped EPUB bytes.
    """
    metadata = [f"<dc:title>{escape(title)}</dc:title>"]
    metadata += [
        f'<dc:creator opf:role="aut">{escape(name)}</dc:creator>' for name in authors
    ]
    metadata += [f"<dc:subject>{escape(tag)}</dc:subject>" for tag in tags]
    metadata += [
        f'<dc:identifier opf:scheme="{escape(scheme)}">{escape(value)}</dc:identifier>'
        for scheme, value in identifiers
    ]
    metadata.append(
        '<dc:identifier id="bookid">urn:uuid:00000000-0000-4000-8000-000000000001</dc:identifier>'
    )
    if language:
        metadata.append(f"<dc:language>{escape(language)}</dc:language>")
    if date:
        metadata.append(f"<dc:date>{escape(date)}</dc:date>")
    if publisher:
        metadata.append(f"<dc:publisher>{escape(publisher)}</dc:publisher>")
    if description:
        metadata.append(f"<dc:description>{escape(description)}</dc:description>")
    if series:
        metadata.append(f'<meta name="calibre:series" content="{escape(series)}"/>')
        if series_index is not None:
            metadata.append(
                f'<meta name="calibre:series_index" content="{series_index}"/>'
            )
    opf = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="bookid">'
        '<metadata xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:opf="http://www.idpf.org/2007/opf">'
        + "".join(metadata)
        + '</metadata><manifest><item id="c" href="c.xhtml" media-type="application/xhtml+xml"/>'
        '</manifest><spine><itemref idref="c"/></spine></package>'
    )
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(zipfile.ZipInfo("mimetype"), "application/epub+zip")
        archive.writestr("META-INF/container.xml", _CONTAINER, zipfile.ZIP_DEFLATED)
        archive.writestr("content.opf", opf, zipfile.ZIP_DEFLATED)
        archive.writestr("c.xhtml", _CHAPTER, zipfile.ZIP_DEFLATED)
    return buffer.getvalue()


def fb2_bytes(
    title: str, authors: Sequence[tuple[str, str]] = (), *, language: str = "en"
) -> bytes:
    """
    Return an FB2 book with a title, authors and language.

    Example:
        >>> fb2_bytes("A", [("Ann", "Bee")]).startswith(b"<?xml")
        True


    :param title: ``book-title``.
    :param authors: ``(first name, last name)`` pairs.
    :param language: ``lang`` value.
    :return: FB2 XML bytes.
    """
    people = "".join(
        f"<author><first-name>{escape(first)}</first-name><last-name>{escape(last)}</last-name></author>"
        for first, last in authors
    )
    text = (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<FictionBook xmlns="http://www.gribuser.ru/xml/fictionbook/2.0">'
        f"<description><title-info><genre>prose</genre>{people}"
        f"<book-title>{escape(title)}</book-title><lang>{escape(language)}</lang></title-info>"
        "</description><body><section><p>Text.</p></section></body></FictionBook>"
    )
    return text.encode("utf-8")


__all__ = ["epub_bytes", "fb2_bytes"]
