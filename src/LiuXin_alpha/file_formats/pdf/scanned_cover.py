"""
Find a scanned book's cover on its first page, without drawing the page.

A scanned PDF's first page is usually one JPEG drawn over the whole page. Without
poppler nothing in LiuXin can draw a page, but such an image can be taken as it is:
the JPEG the scanner wrote. :func:`first_page_jpeg` takes it when the first page
draws exactly one image, upright, over most of the page, and the image is a JPEG in
grey or RGB. Anything else gives no cover, rather than a wrong one.

Only the first page's own content streams are read, within the document's limits
and at most :data:`CONTENT_BYTES` of them.

Example:
    >>> from LiuXin_alpha.file_formats.pdf.objects import PdfDocument
    >>> first_page_jpeg(PdfDocument(scanned_pdf(b"\\xff\\xd8 page \\xff\\xd9")))
    b'\\xff\\xd8 page \\xff\\xd9'
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.file_formats.pdf.syntax import PdfName, PdfStream

if TYPE_CHECKING:
    from LiuXin_alpha.file_formats.pdf.objects import PdfDocument

__all__ = ["CONTENT_BYTES", "MIN_COVERAGE", "MIN_PIXELS", "first_page_jpeg", "scanned_pdf"]

#: The share of the page the image must cover.
MIN_COVERAGE = 0.85
#: The fewest pixels along each side of the image.
MIN_PIXELS = 150
#: The most bytes of the first page's content read.
CONTENT_BYTES = 1 << 20
#: The most images a page may draw before it is not taken for a scan.
_MAX_DRAWS = 16
#: The deepest page tree followed to the first page.
_MAX_DEPTH = 32

_TOKEN = re.compile(
    rb"""
    (?P<space>[\x00\t\n\x0c\r ]+|%[^\r\n]*)
    | (?P<number>[+-]?(?:\d+\.?\d*|\.\d+))
    | (?P<name>/[^\x00\t\n\x0c\r ()<>\[\]{}/%]*)
    | (?P<string>\()
    | (?P<other><<|>>|<[^>]*>|[\[\]{}])
    | (?P<op>[^\x00\t\n\x0c\r ()<>\[\]{}/%]+)
    """,
    re.VERBOSE,
)
_INLINE_END = re.compile(rb"[\x00\t\n\x0c\r ]EI(?=[\x00\t\n\x0c\r ]|$)")
_GREY_OR_RGB = frozenset({"DeviceGray", "DeviceRGB", "CalGray", "CalRGB", "G", "RGB"})

Matrix = tuple[float, float, float, float, float, float]
_IDENTITY: Matrix = (1.0, 0.0, 0.0, 1.0, 0.0, 0.0)


def first_page_jpeg(document: PdfDocument) -> bytes | None:
    """
    Take the JPEG a scanned first page draws over the whole page.

    Example:
        See the module.


    :param document: The open document.
    :return: The JPEG's bytes, or ``None`` when the first page is not such a scan.
    :raises PdfReadError: The document is damaged where the page is, or a limit is reached.
    """
    page, inherited = _first_page(document)
    if page is None:
        return None
    area = _page_area(document, page, inherited)
    rotate = document.resolve(page.get("Rotate", inherited.get("Rotate", 0)))
    if not area or (isinstance(rotate, int) and rotate % 360):
        return None
    resources = document.resolve(page.get("Resources", inherited.get("Resources")))
    xobjects = document.resolve(resources.get("XObject")) if isinstance(resources, Mapping) else None
    if not isinstance(xobjects, Mapping):
        return None
    images = []
    for name, matrix in _draws(_content(document, page)):
        stream = document.resolve(xobjects.get(name))
        if isinstance(stream, PdfStream) and document.resolve(stream.dictionary.get("Subtype")) == "Image":
            images.append((stream, matrix))
    if len(images) != 1:
        return None
    stream, (a, b, c, d, _e, _f) = images[0]
    if b or c or a <= 0 or d <= 0 or a * d < MIN_COVERAGE * area:
        return None
    if not _plain_image(document, stream.dictionary):
        return None
    return document.jpeg_data(stream)


def _first_page(document: PdfDocument) -> tuple[Mapping[str, Any] | None, dict[str, Any]]:
    """
    Follow the page tree to its first page, keeping what the page inherits.

    Example:
        >>> from LiuXin_alpha.file_formats.pdf.objects import PdfDocument
        >>> page, inherited = _first_page(PdfDocument(scanned_pdf(b"\\xff\\xd8")))
        >>> page["Type"], sorted(inherited)
        ('Page', ['MediaBox'])


    :param document: The open document.
    :return: The page, or ``None``, and the inheritable entries of the nodes above it.
    """
    root = document.resolve(document.trailer.get("Root"))
    node = document.resolve(root.get("Pages")) if isinstance(root, Mapping) else None
    inherited: dict[str, Any] = {}
    for _ in range(_MAX_DEPTH):
        if not isinstance(node, Mapping):
            return None, inherited
        kids = document.resolve(node.get("Kids"))
        if node.get("Type") == "Page" or not isinstance(kids, list):
            return node, inherited
        for key in ("Resources", "MediaBox", "CropBox", "Rotate"):
            if key in node:
                inherited[key] = node[key]
        node = document.resolve(kids[0]) if kids else None
    return None, inherited


def _page_area(document: PdfDocument, page: Mapping[str, Any], inherited: Mapping[str, Any]) -> float:
    """
    Measure the page's visible area: its crop box, else its media box.

    Example:
        >>> from LiuXin_alpha.file_formats.pdf.objects import PdfDocument
        >>> _page_area(PdfDocument(scanned_pdf(b"\\xff\\xd8")), {"MediaBox": [0, 0, 600, 800]}, {})
        480000.0


    :param document: The open document.
    :param page: The page.
    :param inherited: What it inherits.
    :return: The area in square points, or zero when the box cannot be read.
    """
    for key in ("CropBox", "MediaBox"):
        box = document.resolve(page.get(key, inherited.get(key)))
        if isinstance(box, list) and len(box) == 4:
            corners = [document.resolve(value) for value in box]
            if all(isinstance(value, (int, float)) for value in corners):
                return abs(float(corners[2]) - float(corners[0])) * abs(float(corners[3]) - float(corners[1]))
    return 0.0


def _content(document: PdfDocument, page: Mapping[str, Any]) -> bytes:
    """
    Decode the page's content streams, joined, up to :data:`CONTENT_BYTES`.

    Example:
        >>> from LiuXin_alpha.file_formats.pdf.objects import PdfDocument
        >>> _content(PdfDocument(scanned_pdf(b"\\xff\\xd8")), {})
        b''


    :param document: The open document.
    :param page: The page.
    :return: The content.
    :raises PdfReadError: A stream is damaged, or a limit is reached.
    """
    contents = document.resolve(page.get("Contents"))
    streams = contents if isinstance(contents, list) else [contents]
    out = bytearray()
    for item in streams[:64]:
        stream = document.resolve(item)
        if isinstance(stream, PdfStream):
            out += document._decode(stream) + b"\n"  # noqa: SLF001 - the reader's own decoding, limits and all
        if len(out) > CONTENT_BYTES:
            return bytes(out[:CONTENT_BYTES])
    return bytes(out)


def _draws(content: bytes) -> Iterator[tuple[str, Matrix]]:
    """
    List the XObjects a content stream draws, with the matrix in force for each.

    Only ``q``, ``Q``, ``cm`` and ``Do`` matter here; inline images are skipped. It
    stops after :data:`_MAX_DRAWS`.

    Example:
        >>> list(_draws(b"q 600 0 0 800 0 0 cm /Im0 Do Q"))
        [('Im0', (600.0, 0.0, 0.0, 800.0, 0.0, 0.0))]


    :param content: The page's content.
    :return: ``(name, matrix)`` pairs, in drawing order.
    """
    matrix, saved, operands, drawn = _IDENTITY, [], [], 0
    position, end = 0, len(content)
    while position < end and drawn < _MAX_DRAWS:
        token = _TOKEN.match(content, position)
        if token is None:
            position += 1
            continue
        position = token.end()
        kind, value = token.lastgroup, token.group()
        if kind == "space":
            continue
        if kind == "string":
            position = _skip_string(content, position)
            operands.clear()
        elif kind in ("number", "name"):
            operands.append(float(value) if kind == "number" else PdfName(value[1:].decode("latin-1")))
        elif kind == "op":
            if value == b"Do" and operands and isinstance(operands[-1], PdfName):
                drawn += 1
                yield str(operands[-1]), matrix
            matrix, position = _operate(value, operands, matrix, saved, content, position)
            operands.clear()


def _operate(op: bytes, operands: list[Any], matrix: Matrix, saved: list[Matrix], content: bytes, position: int) -> tuple[Matrix, int]:
    """
    Apply a graphics-state operator to the matrix, or skip an inline image's data.

    Example:
        >>> _operate(b"cm", [2.0, 0.0, 0.0, 2.0, 5.0, 5.0], _IDENTITY, [], b"", 0)
        ((2.0, 0.0, 0.0, 2.0, 5.0, 5.0), 0)


    :param op: The operator.
    :param operands: Its operands.
    :param matrix: The current matrix.
    :param saved: The saved matrices, which ``q`` and ``Q`` push and pop.
    :param content: The content, for an inline image.
    :param position: Where reading resumes.
    :return: The new matrix, and where reading resumes.
    """
    if op == b"q" and len(saved) < 64:
        saved.append(matrix)
    elif op == b"Q" and saved:
        matrix = saved.pop()
    elif op == b"cm" and len(operands) >= 6 and all(isinstance(v, float) for v in operands[-6:]):
        matrix = _multiply(tuple(operands[-6:]), matrix)  # type: ignore[arg-type]
    elif op == b"ID":
        found = _INLINE_END.search(content, position + 1)
        position = found.end() if found else len(content)
    return matrix, position


def _multiply(first: Matrix, second: Matrix) -> Matrix:
    """
    Multiply two PDF matrices, ``first`` applied before ``second``.

    Example:
        >>> _multiply((2.0, 0.0, 0.0, 2.0, 0.0, 0.0), (1.0, 0.0, 0.0, 1.0, 10.0, 20.0))
        (2.0, 0.0, 0.0, 2.0, 10.0, 20.0)


    :param first: The matrix ``cm`` gives.
    :param second: The matrix in force.
    :return: The new matrix in force.
    """
    a, b, c, d, e, f = first
    A, B, C, D, E, F = second  # noqa: N806 - the matrix's usual names
    return (
        a * A + b * C,
        a * B + b * D,
        c * A + d * C,
        c * B + d * D,
        e * A + f * C + E,
        e * B + f * D + F,
    )


def _skip_string(content: bytes, position: int) -> int:
    """
    Find the end of a literal string, its nested parentheses and escapes included.

    Example:
        >>> _skip_string(b"(a (b) \\\\) c) Tj", 1)
        12


    :param content: The content.
    :param position: Just after the opening parenthesis.
    :return: Just after the closing one, or the end of the content.
    """
    depth = 1
    while position < len(content) and depth:
        byte = content[position]
        if byte == 0x5C:  # backslash: the next byte is escaped
            position += 1
        elif byte == 0x28:
            depth += 1
        elif byte == 0x29:
            depth -= 1
        position += 1
    return position


def _plain_image(document: PdfDocument, dictionary: Mapping[str, Any]) -> bool:
    """
    Tell whether an image shows as its JPEG does: large enough, grey or RGB, with no decode array or mask.

    CMYK JPEGs are left alone: many are stored inverted, which a ``/Decode`` array or
    an Adobe marker undoes, and a lifted file could show as a negative.

    Example:
        >>> from LiuXin_alpha.file_formats.pdf.objects import PdfDocument
        >>> document = PdfDocument(scanned_pdf(b"\\xff\\xd8"))
        >>> _plain_image(document, {"Width": 600, "Height": 800, "ColorSpace": PdfName("DeviceRGB")})
        True
        >>> _plain_image(document, {"Width": 600, "Height": 800, "ColorSpace": PdfName("DeviceCMYK")})
        False


    :param document: The open document.
    :param dictionary: The image's dictionary.
    :return: Whether to take it.
    """
    width, height = (document.resolve(dictionary.get(key)) for key in ("Width", "Height"))
    if not (isinstance(width, int) and isinstance(height, int) and min(width, height) >= MIN_PIXELS):
        return False
    if any(key in dictionary for key in ("Decode", "ImageMask", "Mask", "D", "IM")):
        return False
    space = document.resolve(dictionary.get("ColorSpace", dictionary.get("CS")))
    if isinstance(space, list) and space and document.resolve(space[0]) == "ICCBased":
        profile = document.resolve(space[1]) if len(space) > 1 else None
        return isinstance(profile, PdfStream) and document.resolve(profile.dictionary.get("N")) in (1, 3)
    return isinstance(space, str) and str(space) in _GREY_OR_RGB


def scanned_pdf(jpeg: bytes, *, content: bytes = b"q 612 0 0 792 0 0 cm /Im0 Do Q", width: int = 1224, height: int = 1584, extra_image: bool = False) -> bytes:
    """
    Write a one-page PDF that draws a JPEG, as a scanner would, for examples and tests.

    Example:
        >>> scanned_pdf(b"\\xff\\xd8").startswith(b"%PDF-1.4")
        True


    :param jpeg: The image's bytes.
    :param content: The page's content stream.
    :param width: The image's width in pixels.
    :param height: The image's height in pixels.
    :param extra_image: Add a second image, ``/Im1``, to the page's resources.
    :return: The PDF's bytes.
    """
    from LiuXin_alpha.file_formats.pdf.objects import minimal_pdf

    xobjects = b"/Im0 5 0 R" + (b" /Im1 5 0 R" if extra_image else b"")
    image = b"<< /Type /XObject /Subtype /Image /Width %d /Height %d /ColorSpace /DeviceRGB " % (width, height)
    image += b"/BitsPerComponent 8 /Filter /DCTDecode /Length %d >>\nstream\n%s\nendstream" % (len(jpeg), jpeg)
    return minimal_pdf(
        b"<< /Title (Scan) >>",
        catalog=b"<< /Type /Catalog /Pages 3 0 R >>",
        extra={
            3: b"<< /Type /Pages /Kids [4 0 R] /Count 1 /MediaBox [0 0 612 792] >>",
            4: b"<< /Type /Page /Parent 3 0 R /Resources << /XObject << %s >> >> /Contents 6 0 R >>" % xobjects,
            5: image,
            6: b"<< /Length %d >>\nstream\n%s\nendstream" % (len(content), content),
        },
    )
