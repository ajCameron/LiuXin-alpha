# -*- coding: utf-8 -*-

"""
Render PDF output through the headless backend.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise headless writer through a consuming regression::

        python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
"""

from __future__ import annotations

import typing as _typing

import re
import textwrap
from pathlib import Path

from lxml import html

from LiuXin_alpha.file_formats.pdf.render.common import PAPER_SIZES, Name, String
from LiuXin_alpha.file_formats.pdf.render.serialize import PDFStream

__license__ = "GPL v3"
__copyright__ = "2026, LiuXin contributors"
__docformat__ = "restructuredtext en"


def _parse_custom_size(raw: _typing.Any) -> tuple[_typing.Any, ...] | None:
    """
    Parse custom size under the format's safety and compatibility rules.

    Example:
        Exercise  parse custom size through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


    :param raw: Value supplied for raw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not raw:
        return None
    width, sep, height = str(raw).partition("x")
    if not sep:
        return None
    try:
        return float(width), float(height)
    except Exception:
        return None


def _unit_to_points(unit: _typing.Any) -> _typing.Any:
    """
    Perform the unit to points operation under explicit file-format and conversion rules.

    Example:
        Exercise  unit to points through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


    :param unit: Value supplied for unit under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    unit = (unit or "inch").lower()
    return {
        "point": 1.0,
        "inch": 72.0,
        "millimeter": 72.0 / 25.4,
        "centimeter": 72.0 / 2.54,
        "pica": 12.0,
        "didot": (0.375 * (72.0 / 2.54)) * 0.1,
        "cicero": 12.0 * ((0.375 * (72.0 / 2.54)) * 0.1),
        "devicepixel": 1.0,
    }.get(unit, 72.0)


def _page_size_points(opts: _typing.Any) -> tuple[_typing.Any, ...]:
    """
    Perform the page size points operation under explicit file-format and conversion rules.

    Example:
        Exercise  page size points through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


    :param opts: Value supplied for opts under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    custom = _parse_custom_size(getattr(opts, "custom_size", None))
    if custom is not None:
        scale = _unit_to_points(getattr(opts, "unit", "inch"))
        width, height = custom[0] * scale, custom[1] * scale
    else:
        name = str(getattr(opts, "paper_size", "letter")).lower()
        width, height = PAPER_SIZES.get(name, PAPER_SIZES["letter"])
    if str(getattr(opts, "orientation", "portrait")).lower() == "landscape":
        width, height = height, width
    return float(width), float(height)


def _normalize_text(raw: _typing.Any) -> _typing.Any:
    # Collapse whitespace but preserve paragraph boundaries.
    """
    Normalize text under the format's safety and compatibility rules.

    Example:
        Exercise  normalize text through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


    :param raw: Value supplied for raw under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    out = re.sub(r"[ \t\r\f\v]+", " ", raw)
    out = re.sub(r"\n{3,}", "\n\n", out)
    return out.strip()


def _extract_blocks(path: _typing.Any) -> _typing.Any:
    """
    Extract blocks under the format's safety and compatibility rules.

    Example:
        Exercise  extract blocks through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    data = Path(path).read_bytes()
    try:
        root = html.fromstring(data)
    except Exception:
        text = data.decode("utf-8", "replace")
        return [text] if text.strip() else []

    for bad in root.xpath("//script|//style"):
        parent = bad.getparent()
        if parent is not None:
            parent.remove(bad)

    blocks = []
    for elem in root.xpath("//h1|//h2|//h3|//h4|//h5|//h6|//p|//li|//blockquote|//pre|//div"):
        txt = _normalize_text("".join(elem.itertext()))
        if txt:
            blocks.append(txt)

    if not blocks:
        txt = _normalize_text("".join(root.itertext()))
        if txt:
            blocks.append(txt)
    return blocks


class HeadlessPDFWriter(object):
    """
    Provide the headlesspdfwriter contract for validated ebook processing.

    Example:
        Exercise HeadlessPDFWriter through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, opts: _typing.Any, log: _typing.Any, cover_data: _typing.Any = None, toc: _typing.Any = None) -> None:
        """
        Initialize and validate the headlesspdfwriter state.

        Example:
            Exercise HeadlessPDFWriter.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param cover_data: Value supplied for cover data under the utility contract.
        :param toc: Value supplied for toc under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.opts = opts
        self.log = log
        self.cover_data = cover_data
        self.toc = toc

    def _draw_line(self: _typing.Self, pdf: _typing.Any, font_name: _typing.Any, font_size: _typing.Any, x: _typing.Any, y: _typing.Any, text: _typing.Any) -> None:
        """
        Perform the draw line operation under explicit file-format and conversion rules.

        Example:
            Exercise HeadlessPDFWriter. draw line through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param pdf: Value supplied for pdf under the utility contract.
        :param font_name: Value supplied for font name under the utility contract.
        :param font_size: Value supplied for font size under the utility contract.
        :param x: Value supplied for x under the utility contract.
        :param y: Value supplied for y under the utility contract.
        :param text: Text parsed, normalized or rendered.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pdf.current_page.write("BT ")
        pdf.serialize(Name(font_name))
        pdf.current_page.write(" %s Tf " % int(font_size))
        pdf.current_page.write("%.2f %.2f Td " % (x, y))
        pdf.serialize(String(text))
        pdf.current_page.write_line(" Tj ET")

    def _draw_page_number(self: _typing.Self, pdf: _typing.Any, font_name: _typing.Any, page_num: _typing.Any, page_width: _typing.Any, bottom_margin: _typing.Any) -> None:
        """
        Perform the draw page number operation under explicit file-format and conversion rules.

        Example:
            Exercise HeadlessPDFWriter. draw page number through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param pdf: Value supplied for pdf under the utility contract.
        :param font_name: Value supplied for font name under the utility contract.
        :param page_num: Value supplied for page num under the utility contract.
        :param page_width: Value supplied for page width under the utility contract.
        :param bottom_margin: Value supplied for bottom margin under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not getattr(self.opts, "pdf_page_numbers", False):
            return
        self._draw_line(
            pdf,
            font_name,
            9,
            max(24.0, page_width / 2.0 - 20.0),
            max(14.0, bottom_margin / 2.0),
            str(page_num),
        )

    def dump(self: _typing.Self, items: _typing.Any, out_stream: _typing.Any, pdf_metadata: _typing.Any) -> None:
        """
        Perform the dump operation under explicit file-format and conversion rules.

        Example:
            Exercise HeadlessPDFWriter.dump through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param items: Value supplied for items under the utility contract.
        :param out_stream: Value supplied for out stream under the utility contract.
        :param pdf_metadata: Value supplied for pdf metadata under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        page_width, page_height = _page_size_points(self.opts)
        margin_left = float(getattr(self.opts, "margin_left", 36) or 36)
        margin_right = float(getattr(self.opts, "margin_right", 36) or 36)
        margin_top = float(getattr(self.opts, "margin_top", 36) or 36)
        margin_bottom = float(getattr(self.opts, "margin_bottom", 36) or 36)
        font_size = float(getattr(self.opts, "pdf_default_font_size", 12) or 12)
        line_height = max(12.0, font_size * 1.35)
        usable_width = max(72.0, page_width - margin_left - margin_right)
        max_chars = max(20, int(usable_width / max(font_size * 0.52, 1.0)))

        compress = not bool(getattr(self.opts, "uncompressed_pdf", False))
        mark_links = bool(getattr(self.opts, "pdf_mark_links", False))
        pdf = PDFStream(out_stream, (page_width, page_height), compress=compress, mark_links=mark_links, debug=self.log.debug)
        pdf.set_metadata(title=pdf_metadata.title, author=pdf_metadata.author, tags=pdf_metadata.tags)

        font_ref = pdf.font_manager.add_standard_font("Helvetica")
        font_name = pdf.current_page.add_font(font_ref)

        page_num = 1
        y = page_height - margin_top
        min_y = margin_bottom + line_height

        def maybe_page_break() -> None:
            """
            Perform the maybe page break operation under explicit file-format and conversion rules.

            Example:
                Exercise HeadlessPDFWriter.dump.maybe page break through a consuming regression::

                    python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            nonlocal y, page_num, font_name
            if y < min_y:
                self._draw_page_number(pdf, font_name, page_num, page_width, margin_bottom)
                pdf.end_page()
                font_name = pdf.current_page.add_font(font_ref)
                page_num += 1
                y = page_height - margin_top

        for item in items:
            blocks = _extract_blocks(item)
            self.log.debug("Headless PDF fallback rendering %s (%d blocks)" % (item, len(blocks)))
            for block in blocks:
                for line in textwrap.wrap(block, width=max_chars, break_long_words=True, break_on_hyphens=False):
                    maybe_page_break()
                    self._draw_line(pdf, font_name, font_size, margin_left, y, line)
                    y -= line_height
                y -= line_height * 0.5
                maybe_page_break()
            y -= line_height
            maybe_page_break()

        self._draw_page_number(pdf, font_name, page_num, page_width, margin_bottom)
        pdf.end()
