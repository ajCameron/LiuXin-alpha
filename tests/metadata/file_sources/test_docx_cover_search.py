"""
Check that the DOCX cover search runs in one pass and reads each image once.

calibre's search, which LiuXin's reader copied, is the XPath
``linear_time.DOCX_FIRST_IMAGES``: the first image in each drawing. libxml2
merges its results in quadratic time or worse, so 64,000 drawings took 8 s and
a 100 KB book can hold a million; and the reader read the image again for each
drawing that named it. ``linear_time.first_images`` finds the same images in one
pass, and the reader reads each image once.
"""

from __future__ import annotations

import io
import random
import struct
import time
import zipfile
import zlib

from lxml import etree

from LiuXin_alpha.file_formats.docx.container import DOCX
from LiuXin_alpha.metadata.file_sources.docx import get_metadata
from LiuXin_alpha.utils.linear_time import DOCX_FIRST_IMAGES, first_images

W = "http://schemas.openxmlformats.org/wordprocessingml/2006/main"
A = "http://schemas.openxmlformats.org/drawingml/2006/main"
R = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
PACKAGE = "http://schemas.openxmlformats.org/package/2006/relationships"


def png(width: int, height: int, padding: int = 0) -> bytes:
    """
    Make a PNG whose header gives a size, padded with a text chunk.

    Example:
        >>> png(1, 1)[:8]
        b'\\x89PNG\\r\\n\\x1a\\n'


    :param width: The width the header gives.
    :param height: The height the header gives.
    :param padding: Bytes of text chunk, to make the file larger.
    :return: The PNG's bytes.
    """

    def chunk(kind: bytes, data: bytes) -> bytes:
        """
        Frame one PNG chunk.

        Example:
            See ``png``.


        :param kind: The chunk type.
        :param data: Its data.
        :return: The chunk.
        """
        return struct.pack(">I", len(data)) + kind + data + struct.pack(">I", zlib.crc32(kind + data))

    header = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)
    return b"\x89PNG\r\n\x1a\n" + chunk(b"IHDR", header) + chunk(b"tEXt", b"pad\0" + bytes(padding)) + chunk(b"IEND", b"")


def docx_book(drawings: list[str], images: dict[str, bytes]) -> bytes:
    """
    Make a DOCX whose body holds one drawing per entry, each naming an image by relationship ID.

    Example:
        >>> docx_book(["rId1"], {"rId1": png(1, 1)})[:2]
        b'PK'


    :param drawings: The relationship ID each drawing's ``a:blip`` names, in order.
    :param images: The image for each relationship ID.
    :return: The book's bytes.
    """
    body = "".join(f'<w:r><w:drawing><a:blip r:embed="{rid}"/></w:drawing></w:r>' for rid in drawings)
    document = f'<w:document xmlns:w="{W}" xmlns:a="{A}" xmlns:r="{R}"><w:body><w:p>{body}</w:p></w:body></w:document>'
    types = (
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/><Default Extension="png" ContentType="image/png"/>'
        '<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.'
        'wordprocessingml.document.main+xml"/>'
        '<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>'
        "</Types>"
    )
    package = (
        f'<Relationships xmlns="{PACKAGE}">'
        f'<Relationship Id="rId1" Type="{R}/officeDocument" Target="word/document.xml"/>'
        f'<Relationship Id="rId2" Type="{PACKAGE}/metadata/core-properties" Target="docProps/core.xml"/>'
        "</Relationships>"
    )
    links = "".join(
        f'<Relationship Id="{rid}" Type="{R}/image" Target="media/{rid}.png"/>' for rid in images
    )
    core = (
        '<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" '
        'xmlns:dc="http://purl.org/dc/elements/1.1/"><dc:title>Cover Search</dc:title></cp:coreProperties>'
    )
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", types)
        archive.writestr("_rels/.rels", package)
        archive.writestr("word/_rels/document.xml.rels", f'<Relationships xmlns="{PACKAGE}">{links}</Relationships>')
        archive.writestr("docProps/core.xml", core)
        archive.writestr("word/document.xml", document)
        for rid, data in images.items():
            archive.writestr(f"word/media/{rid}.png", data, compress_type=zipfile.ZIP_STORED)
    return out.getvalue()


def random_document(chooser: random.Random) -> etree._Element:
    """
    Make a document of nested drawings, images with and without their attribute, comments and other elements.

    ``x:`` is another prefix for the ``w`` namespace, which ``name()`` does not match.

    Example:
        >>> random_document(random.Random(1)).tag
        '{urn:w}document'


    :param chooser: The random source.
    :return: The document's root.
    """
    kinds = ["w:drawing", "w:pict", "a:blip", "a:blip+", "v:imagedata", "v:imagedata+", "a:blip#", "w:r", "x:drawing"]

    def build(depth: int) -> str:
        """
        Make a run of random elements, nested up to depth 5.

        Example:
            See ``random_document``.


        :param depth: How deep this run sits.
        :return: The markup.
        """
        parts = []
        for _ in range(chooser.randint(0, 4)):
            kind = chooser.choice([*kinds, "comment"])
            if kind == "comment":
                parts.append("<!--c-->")
                continue
            attrs = {"a:blip+": ' r:embed="e"', "v:imagedata+": ' r:id="i"', "a:blip#": ' r:id="i"'}.get(kind, "")
            name = kind.rstrip("+#")
            inner = build(depth + 1) if depth < 5 else ""
            parts.append(f"<{name}{attrs}>{inner}</{name}>")
        return "".join(parts)

    namespaces = 'xmlns:w="urn:w" xmlns:x="urn:w" xmlns:a="urn:a" xmlns:v="urn:v" xmlns:r="urn:r"'
    return etree.fromstring(f"<w:document {namespaces}>{build(0)}</w:document>")


def test_first_images_finds_what_the_xpath_finds() -> None:
    """
    Give the XPath's images, in its order, on 5,000 random documents.

    Example:
        >>> test_first_images_finds_what_the_xpath_finds()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    xpath = etree.XPath(DOCX_FIRST_IMAGES, namespaces={"r": "urn:r"})
    chooser = random.Random(7)
    for _ in range(5_000):
        document = random_document(chooser)
        assert first_images(document, "urn:r") == xpath(document), etree.tostring(document)


def test_first_images_is_quick_on_a_million_drawings() -> None:
    """
    Find the images of a million drawings in seconds, where the XPath would take hours.

    Example:
        >>> test_first_images_is_quick_on_a_million_drawings()  # doctest: +SKIP


    :return: None; the assertions are the check.
    """
    body = '<w:r><w:drawing><a:blip r:embed="rId9"/></w:drawing></w:r>' * 1_000_000
    document = etree.fromstring(f'<w:document xmlns:w="{W}" xmlns:a="{A}" xmlns:r="{R}">{body}</w:document>')
    started = time.monotonic()
    assert len(first_images(document, R)) == 1_000_000
    assert time.monotonic() - started < 20


def test_the_reader_reads_each_image_once_and_still_finds_the_cover(monkeypatch: object) -> None:
    """
    Read a wide image named by 50,000 drawings once, then take the portrait image after it as the cover.

    Example:
        >>> test_the_reader_reads_each_image_once_and_still_finds_the_cover(None)  # doctest: +SKIP


    :param monkeypatch: Counts the reads.
    :return: None; the assertions are the check.
    """
    reads: list[str] = []
    original = DOCX.read

    def counting(self: DOCX, name: str) -> bytes:
        """
        Count a read and do it.

        Example:
            See the test.


        :param self: The container.
        :param name: The member.
        :return: Its bytes.
        """
        if name.endswith(".png"):
            reads.append(name)
        return original(self, name)

    monkeypatch.setattr(DOCX, "read", counting)  # type: ignore[attr-defined]
    cover = png(400, 500)
    book = docx_book(["rIdWide"] * 50_000 + ["rIdCover"], {"rIdWide": png(4000, 10, 100_000), "rIdCover": cover})
    started = time.monotonic()
    mi = get_metadata(io.BytesIO(book))
    assert time.monotonic() - started < 10
    assert mi.title == "Cover Search"
    assert list(mi.cover_data) == [("png", cover)]  # LiuXin's containers hold values as ordered keys
    assert reads == ["word/media/rIdWide.png", "word/media/rIdCover.png"]
