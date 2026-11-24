"""
Check that LiuXin's PDF reader takes a scanned first page's JPEG as the cover, and nothing else.

Without poppler nothing in LiuXin draws a page, so the reader had no cover for any
PDF. A scanned book's first page is usually one JPEG over the whole page, which can
be taken as it is.

Example:
    Run the checks::

        python -m pytest -q tests/file_formats/pdf/test_pdf_scanned_cover.py
"""

from __future__ import annotations

import io
import zlib
from pathlib import Path

import pytest

from LiuXin_alpha.file_formats.pdf.objects import PdfDocument, minimal_pdf
from LiuXin_alpha.file_formats.pdf.scanned_cover import first_page_jpeg, scanned_pdf
from LiuXin_alpha.metadata.file_sources import pdf

JPEG = b"\xff\xd8\xff\xe0 a scanned page \xff\xd9"
CORPUS = Path(__file__).resolve().parents[2] / "fixtures" / "pdf_metadata"


def cover_of(data: bytes) -> bytes | None:
    """
    Look for the cover in a PDF's bytes.

    Example:
        >>> cover_of(scanned_pdf(JPEG)) == JPEG
        True


    :param data: The PDF.
    :return: The JPEG found, or ``None``.
    """
    return first_page_jpeg(PdfDocument(data))


def test_a_scanned_first_page_gives_its_jpeg_as_the_cover() -> None:
    """
    Read the JPEG a first page draws over the whole page as the book's cover.

    Example:
        >>> test_a_scanned_first_page_gives_its_jpeg_as_the_cover()


    :return: None; the assertions are the check.
    """
    data = scanned_pdf(JPEG)
    assert cover_of(data) == JPEG
    md = pdf.get_metadata(io.BytesIO(data))
    assert md.title == "Scan"
    assert md.to_calibre().cover_data == ("jpeg", JPEG)


@pytest.mark.parametrize(
    "content",
    [
        b"q 612 0 0 792 0 0 cm /Im0 Do Q",
        b"q 2 0 0 2 0 0 cm q 306 0 0 396 0 0 cm /Im0 Do Q Q",
        b"BT /F1 1 Tf (text with \\) /Im0 Do) Tj ET q 612 0 0 792 0 0 cm /Im0 Do Q",
        b"BI /W 1 /H 1 /CS /G /BPC 8 ID \x00 Do EI q 612 0 0 792 0 0 cm /Im0 Do Q",
        b"3 Tr BT (OCR text layer) Tj ET q 620 0 0 800 -4 -4 cm /Im0 Do Q",
    ],
    ids=["plain", "nested-matrices", "string-with-Do", "inline-image", "bleed-and-ocr-text"],
)
def test_the_page_may_be_drawn_in_several_ways(content: bytes) -> None:
    """
    Find the one image however the matrix is built, past strings and inline images.

    Example:
        >>> test_the_page_may_be_drawn_in_several_ways(b"q 612 0 0 792 0 0 cm /Im0 Do Q")


    :param content: The page's content stream.
    :return: None; the assertions are the check.
    """
    assert cover_of(scanned_pdf(JPEG, content=content)) == JPEG


@pytest.mark.parametrize(
    ("content", "kwargs"),
    [
        (b"q 306 0 0 396 0 0 cm /Im0 Do Q", {}),
        (b"q 0 792 -612 0 612 0 cm /Im0 Do Q", {}),
        (b"q -612 0 0 792 612 0 cm /Im0 Do Q", {}),
        (b"q 612 0 0 792 0 0 cm /Im0 Do Q q 100 0 0 100 0 0 cm /Im1 Do Q", {"extra_image": True}),
        (b"BT /F1 12 Tf (born digital) Tj ET", {}),
        (b"q 612 0 0 792 0 0 cm /Im0 Do Q", {"width": 100, "height": 130}),
    ],
    ids=["half-the-page", "rotated", "mirrored", "two-images", "no-image", "too-few-pixels"],
)
def test_other_first_pages_give_no_cover(content: bytes, kwargs: dict[str, object]) -> None:
    """
    Give no cover where the first page is not one upright image over the page.

    Example:
        >>> test_other_first_pages_give_no_cover(b"BT ET", {})


    :param content: The page's content stream.
    :param kwargs: More for :func:`scanned_pdf`.
    :return: None; the assertions are the check.
    """
    assert cover_of(scanned_pdf(JPEG, content=content, **kwargs)) is None


def build(*, image: bytes = b"", page: bytes = b"", filters: bytes = b"/DCTDecode", subtype: bytes = b"/Image") -> bytes:
    """
    Write a scanned one-page PDF with changes to its image or page dictionary, its offsets correct.

    Example:
        >>> cover_of(build()) == JPEG
        True


    :param image: More entries for the image's dictionary.
    :param page: More entries for the page's dictionary.
    :param filters: The image's filter or filters.
    :param subtype: The XObject's subtype.
    :return: The PDF's bytes.
    """
    content = b"q 612 0 0 792 0 0 cm /Im0 Do Q"
    return minimal_pdf(
        b"<< >>",
        catalog=b"<< /Type /Catalog /Pages 3 0 R >>",
        extra={
            3: b"<< /Type /Pages /Kids [4 0 R] /Count 1 >>",
            4: b"<< /Type /Page /MediaBox [0 0 612 792] %s /Resources << /XObject << /Im0 5 0 R >> >> /Contents 6 0 R >>"
            % page,
            5: b"<< /Type /XObject /Subtype %s /Width 1224 /Height 1584 %s /BitsPerComponent 8 /Filter %s /Length %d >>"
            b"\nstream\n%s\nendstream" % (subtype, image or b"/ColorSpace /DeviceRGB", filters, len(JPEG), JPEG),
            6: b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        },
    )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"image": b"/ColorSpace /DeviceCMYK"},
        {"image": b"/ColorSpace /DeviceRGB /Decode [1 0 1 0 1 0]"},
        {"filters": b"/JPXDecode"},
        {"page": b"/Rotate 90"},
        {"subtype": b"/Form"},
    ],
    ids=["cmyk", "decode-array", "jpeg-2000", "page-rotated", "not-an-image"],
)
def test_images_that_would_not_show_as_their_jpeg_give_no_cover(kwargs: dict[str, bytes]) -> None:
    """
    Leave alone a JPEG that the page shows differently, or a stream that is not a JPEG image.

    Example:
        >>> test_images_that_would_not_show_as_their_jpeg_give_no_cover({"page": b"/Rotate 90"})


    :param kwargs: What to change in the scanned PDF.
    :return: None; the assertions are the check.
    """
    assert cover_of(build()) == JPEG
    data = build(**kwargs)
    document = PdfDocument(data)
    assert first_page_jpeg(document) is None
    assert not document.repaired


def test_a_jpeg_behind_another_filter_is_decoded_to_the_jpeg() -> None:
    """
    Undo a Flate filter in front of the JPEG, keeping the JPEG itself.

    Example:
        >>> test_a_jpeg_behind_another_filter_is_decoded_to_the_jpeg()


    :return: None; the assertions are the check.
    """
    packed = zlib.compress(JPEG)
    image = (
        b"<< /Type /XObject /Subtype /Image /Width 1224 /Height 1584 /ColorSpace /DeviceGray "
        b"/BitsPerComponent 8 /Filter [/FlateDecode /DCTDecode] /Length %d >>\nstream\n%s\nendstream"
        % (len(packed), packed)
    )
    content = b"q 612 0 0 792 0 0 cm /Im0 Do Q"
    data = minimal_pdf(
        b"<< >>",
        catalog=b"<< /Type /Catalog /Pages 3 0 R >>",
        extra={
            3: b"<< /Type /Pages /Kids [4 0 R] /Count 1 >>",
            4: b"<< /Type /Page /MediaBox [0 0 612 792] /Resources << /XObject << /Im0 5 0 R >> >> /Contents 6 0 R >>",
            5: image,
            6: b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        },
    )
    assert cover_of(data) == JPEG


def test_a_damaged_page_tree_costs_only_the_cover() -> None:
    """
    Read the metadata of a file whose page tree is broken, without a cover.

    Example:
        >>> test_a_damaged_page_tree_costs_only_the_cover()


    :return: None; the assertions are the check.
    """
    data = scanned_pdf(JPEG).replace(b"/Kids [4 0 R]", b"/Kids [40 0 R]")
    md = pdf.get_metadata(io.BytesIO(data))
    assert md.title == "Scan"
    assert not md.cover_data


@pytest.mark.parametrize("path", sorted(CORPUS.glob("*.pdf")), ids=lambda p: p.stem)
def test_the_born_digital_corpus_gives_no_cover(path: Path) -> None:
    """
    Give no cover for the corpus files, none of which is a scan; damaged ones still read.

    Example:
        >>> test_the_born_digital_corpus_gives_no_cover(CORPUS / "plain.pdf")


    :param path: A corpus file.
    :return: None; the assertions are the check.
    """
    md = pdf.get_metadata(io.BytesIO(path.read_bytes()), fallback_on_parse_error=True)
    assert not md.cover_data
