"""
Check that an OPF 2 package hands every field it carries to its book metadata.

``OPF.to_book_metadata()`` once built its result from the title and authors alone,
so publisher, subjects, language, description, dates, rights and series were lost
by every caller: the EPUB, HTMLZ, TXTZ, LIT, MOBI and ZIP metadata readers, and the
conversion pipeline, which builds the OEB book's metadata from it. Upstream calibre
copies every field from the OPF; these tests hold the fork to that.
"""

from __future__ import annotations

import logging
from datetime import datetime

from LiuXin_alpha.file_formats.oeb.base import OEBBook
from LiuXin_alpha.file_formats.oeb.reader import OEBReader
from LiuXin_alpha.file_formats.opf.opf2 import OPF
from LiuXin_alpha.utils.libraries.liuxin_etree import etree

OPF2_PACKAGE = """<?xml version='1.0' encoding='utf-8'?>
<package xmlns="http://www.idpf.org/2007/opf" version="2.0" unique-identifier="id">
 <metadata xmlns:dc="http://purl.org/dc/elements/1.1/"
           xmlns:opf="http://www.idpf.org/2007/opf">
  <dc:title>Field Test</dc:title>
  <dc:creator opf:role="aut" opf:file-as="Doe, Jane">Jane Doe</dc:creator>
  <dc:identifier id="id" opf:scheme="ISBN">9780000000002</dc:identifier>
  <dc:language>fr</dc:language>
  <dc:publisher>Pub Co</dc:publisher>
  <dc:subject>Fiction</dc:subject>
  <dc:subject>Sea; Ships</dc:subject>
  <dc:description>A blurb.</dc:description>
  <dc:date>2001-02-03</dc:date>
  <dc:rights>CC-BY</dc:rights>
  <meta name="calibre:series" content="Saga"/>
  <meta name="calibre:series_index" content="2"/>
  <meta name="calibre:rating" content="8"/>
  <meta name="calibre:title_sort" content="Field Test, The"/>
 </metadata>
 <manifest><item id="c" href="c.html" media-type="application/xhtml+xml"/></manifest>
 <spine><itemref idref="c"/></spine>
</package>
"""


def opf2_package() -> OPF:
    """
    Parse the sample OPF 2 package without reading a table of contents.

    Example:
        >>> opf2_package().title
        'Field Test'


    :return: The parsed package.
    """
    root = etree.fromstring(OPF2_PACKAGE.encode("utf-8"))
    return OPF(None, preparsed_opf=root, read_toc=False)


def test_calibre_book_metadata_keeps_every_package_field() -> None:
    """
    Copy the descriptive fields as well as the title, authors and identifiers.

    Example:
        >>> test_calibre_book_metadata_keeps_every_package_field()


    :return: None; the assertions are the check.
    """
    mi = opf2_package().to_book_metadata()

    assert mi.title == "Field Test"
    assert mi.authors == ["Jane Doe"]
    assert mi.author_sort == "Doe, Jane"
    assert mi.get_identifiers() == {"isbn": "9780000000002"}
    assert mi.publisher == "Pub Co"
    assert mi.tags == ["Fiction", "Sea; Ships"]
    assert mi.languages == ["fra"]
    assert mi.comments == "A blurb."
    assert isinstance(mi.pubdate, datetime)
    assert (mi.pubdate.year, mi.pubdate.month, mi.pubdate.day) == (2001, 2, 3)
    assert mi.rights == "CC-BY"
    assert (mi.series, mi.series_index) == ("Saga", 2.0)
    assert mi.rating == 8
    assert mi.title_sort == "Field Test, The"


def test_liuxin_book_metadata_keeps_every_package_field() -> None:
    """
    Carry the same fields into the LiuXin container when ``calibre=False``.

    Example:
        >>> test_liuxin_book_metadata_keeps_every_package_field()


    :return: None; the assertions are the check.
    """
    md = opf2_package().to_book_metadata(calibre=False)

    assert list(md.authors) == ["Jane Doe"]
    assert list(md.publisher) == ["Pub Co"]
    assert list(md.tags) == ["Fiction", "Sea; Ships"]
    assert md.languages == ["fra"]
    assert list(md.comments) == ["A blurb."]
    assert list(md.series) == ["Saga"]
    assert dict(md.series_index) == {"Saga": 2.0}
    assert md.ratings["calibre"] == 8
    assert md.title_sort == "Field Test, The"
    assert md.rights == "CC-BY"
    assert md.to_calibre().publisher == "Pub Co"


def test_book_metadata_leaves_the_package_structure_in_place() -> None:
    """
    Restore the manifest, spine, guide and TOC that are kept out of the copy.

    Example:
        >>> test_book_metadata_leaves_the_package_structure_in_place()


    :return: None; the assertions are the check.
    """
    opf = opf2_package()
    manifest, spine, guide, toc = opf.manifest, opf.spine, opf.guide, opf.toc

    opf.to_book_metadata()

    assert opf.manifest is manifest
    assert opf.spine is spine
    assert opf.guide is guide
    assert opf.toc is toc
    assert [item.path for item in opf.spine] == [item.path for item in spine]


def test_conversion_reads_the_book_metadata_from_an_opf2_package() -> None:
    """
    Give the OEB book the package's metadata, and its language, not the machine's.

    The conversion pipeline reads an input's OPF through ``OEBReader``, so a field
    the book metadata drops is missing from every converted file.

    Example:
        >>> test_conversion_reads_the_book_metadata_from_an_opf2_package()


    :return: None; the assertions are the check.
    """
    log = logging.getLogger("liuxin.tests.opf2-book-metadata")
    oeb = OEBBook(log, html_preprocessor=None)
    reader = OEBReader()
    reader.oeb = oeb
    reader.logger = reader.log = oeb.logger

    reader._metadata_from_opf(etree.fromstring(OPF2_PACKAGE.encode("utf-8")))

    def values(term: str) -> list[str]:
        """
        Return one metadata term's values as text.

        Example:
            >>> values("title")
            ['Field Test']


        :param term: The OEB metadata term.
        :return: Each value as a string.
        """
        return [str(item) for item in oeb.metadata[term]]

    assert values("title") == ["Field Test"]
    assert values("publisher") == ["Pub Co"]
    assert values("subject") == ["Fiction", "Sea; Ships"]
    assert values("language") == ["fra"]
    assert values("description") == ["A blurb."]
    assert values("rights") == ["CC-BY"]
    assert values("date")[0].startswith("2001-02-03")
