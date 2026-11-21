"""
Check that the readers of OPF 2 containers return every field the package carries.

EPUB, HTMLZ and TXTZ all keep their metadata in an OPF 2 package. These readers
used to return only the title, authors and identifiers, in both the calibre and
the LiuXin forms, so a book's publisher, subjects, language, description, rights
and series were dropped as it was read.
"""

from __future__ import annotations

import io
import zipfile
from pathlib import Path

import pytest

from LiuXin_alpha.metadata.file_sources import epub, extz

PACKAGE = """<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="id">
 <metadata xmlns:dc="http://purl.org/dc/elements/1.1/"
           xmlns:opf="http://www.idpf.org/2007/opf">
  <dc:title>Retention Test</dc:title>
  <dc:creator opf:role="aut" opf:file-as="Doe, Jane">Jane Doe</dc:creator>
  <dc:identifier id="id" opf:scheme="ISBN">9780000000002</dc:identifier>
  <dc:language>de</dc:language>
  <dc:publisher>Pub Co</dc:publisher>
  <dc:subject>Fiction</dc:subject>
  <dc:description>A blurb.</dc:description>
  <dc:rights>CC-BY</dc:rights>
  <meta name="calibre:series" content="Saga"/>
  <meta name="calibre:series_index" content="3"/>
 </metadata>
 <manifest><item id="c" href="c.html" media-type="application/xhtml+xml"/></manifest>
 <spine><itemref idref="c"/></spine>
</package>
"""

CONTAINER_XML = (
    '<?xml version="1.0"?><container version="1.0" '
    'xmlns="urn:oasis:names:tc:opendocument:xmlns:container"><rootfiles>'
    '<rootfile full-path="content.opf" media-type="application/oebps-package+xml"/>'
    "</rootfiles></container>"
)

CHAPTER = (
    '<?xml version="1.0" encoding="utf-8"?><html xmlns="http://www.w3.org/1999/xhtml">'
    "<head><title>c</title></head><body><p>Text.</p></body></html>"
)


def zip_bytes(members: dict[str, str]) -> bytes:
    """
    Pack text members into a ZIP archive, storing the first one uncompressed.

    Example:
        >>> zip_bytes({"mimetype": "application/epub+zip"})[30:38]
        b'mimetype'


    :param members: Member names mapped to their text, in archive order.
    :return: The archive's bytes.
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        for position, (name, text) in enumerate(members.items()):
            if position == 0:
                archive.writestr(zipfile.ZipInfo(name), text)
            else:
                archive.writestr(name, text)
    return buffer.getvalue()


def epub_bytes() -> bytes:
    """
    Build an EPUB 2 whose package carries the sample metadata.

    Example:
        >>> epub_bytes()[:2]
        b'PK'


    :return: The EPUB's bytes.
    """
    return zip_bytes(
        {
            "mimetype": "application/epub+zip",
            "META-INF/container.xml": CONTAINER_XML,
            "content.opf": PACKAGE,
            "c.html": CHAPTER,
        }
    )


def test_epub_calibre_metadata_keeps_the_package_fields() -> None:
    """
    Return the publisher, subjects, language and the rest in calibre form.

    Example:
        >>> test_epub_calibre_metadata_keeps_the_package_fields()


    :return: None; the assertions are the check.
    """
    mi = epub.get_metadata(io.BytesIO(epub_bytes()), calibre_metadata=True)

    assert mi.title == "Retention Test"
    assert mi.authors == ["Jane Doe"]
    assert mi.author_sort == "Doe, Jane"
    assert mi.publisher == "Pub Co"
    assert mi.tags == ["Fiction"]
    assert mi.languages == ["deu"]
    assert mi.comments == "A blurb."
    assert mi.rights == "CC-BY"
    assert (mi.series, mi.series_index) == ("Saga", 3.0)


def test_epub_liuxin_metadata_keeps_the_package_fields() -> None:
    """
    Return the same fields in the LiuXin container.

    Example:
        >>> test_epub_liuxin_metadata_keeps_the_package_fields()


    :return: None; the assertions are the check.
    """
    md = epub.get_metadata(io.BytesIO(epub_bytes()), calibre_metadata=False)

    assert list(md.authors) == ["Jane Doe"]
    assert md.creator_sort == "Doe, Jane"
    assert list(md.publisher) == ["Pub Co"]
    assert list(md.tags) == ["Fiction"]
    assert md.languages == ["deu"]
    assert list(md.comments) == ["A blurb."]
    assert md.rights == "CC-BY"
    assert dict(md.series_index) == {"Saga": 3.0}


@pytest.mark.parametrize(
    ("suffix", "text_member", "text"),
    [("htmlz", "index.html", CHAPTER), ("txtz", "index.txt", "Text.\n")],
    ids=["htmlz", "txtz"],
)
def test_extz_metadata_keeps_the_package_fields(
    tmp_path: Path, suffix: str, text_member: str, text: str
) -> None:
    """
    Return the embedded package's fields from HTMLZ and TXTZ archives.

    Example:
        Run the parametrized cases with pytest::

            python -m pytest -q tests/metadata/file_sources/test_opf2_container_field_retention.py


    :param tmp_path: Directory for the sample archive.
    :param suffix: The archive's extension.
    :param text_member: The name of the archive's text member.
    :param text: The text member's content.
    :return: None; the assertions are the check.
    """
    path = tmp_path / f"sample.{suffix}"
    path.write_bytes(zip_bytes({text_member: text, "metadata.opf": PACKAGE}))

    md = extz.get_metadata(str(path))

    assert list(md.authors) == ["Jane Doe"]
    assert list(md.publisher) == ["Pub Co"]
    assert list(md.tags) == ["Fiction"]
    assert md.languages == ["deu"]
    assert list(md.comments) == ["A blurb."]
    assert md.rights == "CC-BY"
    assert dict(md.series_index) == {"Saga": 3.0}
