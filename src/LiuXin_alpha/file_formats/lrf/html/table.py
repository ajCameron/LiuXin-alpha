"""
Lay out HTML table structure within LRF page constraints.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise table through a consuming regression::

        python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
"""
from __future__ import print_function
from __future__ import annotations

import typing as _typing

import math
import sys
import re

from LiuXin_alpha.file_formats.lrf.fonts import get_font
from LiuXin_alpha.file_formats.lrf.pylrs.pylrs import (
    TextBlock,
    Text,
    CR,
    Span,
    CharButton,
    Plot,
    Paragraph,
    LrsTextTag,
)

# P2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import memory_range
from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"


def ceil(num: _typing.Any) -> _typing.Any:
    """
    Perform the ceil operation under explicit file-format and conversion rules.

    Example:
        Exercise ceil through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


    :param num: Value supplied for num under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return int(math.ceil(num))


def print_xml(elem: _typing.Any) -> None:
    """
    Perform the print xml operation under explicit file-format and conversion rules.

    Example:
        Exercise print xml through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


    :param elem: Value supplied for elem under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.lrf.pylrs.pylrs import ElementWriter

    elem = elem.toElement("utf8")
    ew = ElementWriter(elem, sourceEncoding="utf8")
    ew.write(sys.stdout)
    print()


def cattrs(base: _typing.Any, extra: _typing.Any) -> _typing.Any:
    """
    Perform the cattrs operation under explicit file-format and conversion rules.

    Example:
        Exercise cattrs through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


    :param base: Value supplied for base under the utility contract.
    :param extra: Value supplied for extra under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    new = base.copy()
    new.update(extra)
    return new


def tokens(tb: _typing.Any) -> _typing.Iterator[_typing.Any]:
    """
    Return the next token. A token is : 1. A string a block of text that has the same style

    Example:
        Exercise tokens through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


    :param tb: Value supplied for tb under the utility contract.
    :return: An iterator yielding the normalized values described above.
    """

    def process_element(x: _typing.Any, attrs: _typing.Any) -> _typing.Iterator[_typing.Any]:
        """
        Perform the process element operation under explicit file-format and conversion rules.

        Example:
            Exercise tokens.process element through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param x: Value supplied for x under the utility contract.
        :param attrs: Value supplied for attrs under the utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        if isinstance(x, CR):
            yield 2, None
        elif isinstance(x, Text):
            yield x.text, cattrs(attrs, {})
        elif isinstance(x, six_string_types):
            yield x, cattrs(attrs, {})
        elif isinstance(x, (CharButton, LrsTextTag)):
            if x.contents:
                if hasattr(x.contents[0], "text"):
                    yield x.contents[0].text, cattrs(attrs, {})
                elif hasattr(x.contents[0], "attrs"):
                    for z in process_element(x.contents[0], x.contents[0].attrs):
                        yield z
        elif isinstance(x, Plot):
            yield x, None
        elif isinstance(x, Span):
            attrs = cattrs(attrs, x.attrs)
            for y in x.contents:
                for z in process_element(y, attrs):
                    yield z

    for i in tb.contents:
        if isinstance(i, CR):
            yield 1, None
        elif isinstance(i, Paragraph):
            for j in i.contents:
                attrs = {}
                if hasattr(j, "attrs"):
                    attrs = j.attrs
                for k in process_element(j, attrs):
                    yield k


class Cell(object):
    """
    Provide the cell contract for validated ebook processing.

    Example:
        Exercise Cell through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
    """
    def __init__(self: _typing.Self, conv: _typing.Any, tag: _typing.Any, css: _typing.Any) -> None:
        """
        Initialize and validate the cell state.

        Example:
            Exercise Cell.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param conv: Value supplied for conv under the utility contract.
        :param tag: Value supplied for tag under the utility contract.
        :param css: Value supplied for css under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.conv = conv
        self.tag = tag
        self.css = css
        self.text_blocks = []
        self.pwidth = -1.0
        if ("width" in tag) and "%" in tag["width"]:
            try:
                self.pwidth = float(tag["width"].replace("%", ""))
            except ValueError:
                pass
        if ("width" in css) and "%" in css["width"]:
            try:
                self.pwidth = float(css["width"].replace("%", ""))
            except ValueError:
                pass
        if self.pwidth > 100:
            self.pwidth = -1
        self.rowspan = self.colspan = 1
        try:
            self.colspan = int(tag["colspan"]) if ("colspan" in tag) else 1
            self.rowspan = int(tag["rowspan"]) if ("rowspan" in tag) else 1
        except:
            pass

        pp = conv.current_page
        conv.book.allow_new_page = False
        conv.current_page = conv.book.create_page()
        conv.parse_tag(tag, css)
        conv.end_current_block()
        for item in conv.current_page.contents:
            if isinstance(item, TextBlock):
                self.text_blocks.append(item)
        conv.current_page = pp
        conv.book.allow_new_page = True
        if not self.text_blocks:
            tb = conv.book.create_text_block()
            tb.Paragraph(" ")
            self.text_blocks.append(tb)
        for tb in self.text_blocks:
            tb.parent = None
            tb.objId = 0
            # Needed as we have to eventually change this BlockStyle's width and
            # height attributes. This blockstyle may be shared with other
            # elements, so doing that causes havoc.
            tb.blockStyle = conv.book.create_block_style()
            ts = conv.book.create_text_style(**tb.textStyle.attrs)
            ts.attrs["parindent"] = 0
            tb.textStyle = ts
            if ts.attrs["align"] == "foot":
                if isinstance(tb.contents[-1], Paragraph):
                    tb.contents[-1].append(" ")

    def pts_to_pixels(self: _typing.Self, pts: _typing.Any) -> _typing.Any:
        """
        Perform the pts to pixels operation under explicit file-format and conversion rules.

        Example:
            Exercise Cell.pts to pixels through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param pts: Value supplied for pts under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        pts = int(pts)
        return ceil((float(self.conv.profile.dpi) / 72.0) * (pts / 10.0))

    def minimum_width(self: _typing.Self) -> _typing.Any:
        """
        Perform the minimum width operation under explicit file-format and conversion rules.

        Example:
            Exercise Cell.minimum width through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return max([self.minimum_tb_width(tb) for tb in self.text_blocks])

    def minimum_tb_width(self: _typing.Self, tb: _typing.Any) -> _typing.Any:
        """
        Perform the minimum tb width operation under explicit file-format and conversion rules.

        Example:
            Exercise Cell.minimum tb width through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param tb: Value supplied for tb under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ts = tb.textStyle.attrs
        default_font = get_font(ts["fontfacename"], self.pts_to_pixels(ts["fontsize"]))
        parindent = self.pts_to_pixels(ts["parindent"])
        mwidth = 0
        for token, attrs in tokens(tb):
            font = default_font
            if isinstance(token, int):  # Handle para and line breaks
                continue
            if isinstance(token, Plot):
                return self.pts_to_pixels(token.xsize)
            ff = attrs.get("fontfacename", ts["fontfacename"])
            fs = attrs.get("fontsize", ts["fontsize"])
            if (ff, fs) != (ts["fontfacename"], ts["fontsize"]):
                font = get_font(ff, self.pts_to_pixels(fs))
            if not token.strip():
                continue
            word = token.split()
            word = word[0] if word else ""
            width = font.getsize(word)[0]
            if width > mwidth:
                mwidth = width
        return parindent + mwidth + 2

    def text_block_size(self: _typing.Self, tb: _typing.Any, maxwidth: _typing.Any = sys.maxsize, debug: bool = False) -> tuple[_typing.Any, ...]:
        """
        Perform the text block size operation under explicit file-format and conversion rules.

        Example:
            Exercise Cell.text block size through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param tb: Value supplied for tb under the utility contract.
        :param maxwidth: Value supplied for maxwidth under the utility contract.
        :param debug: Value supplied for debug under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ts = tb.textStyle.attrs
        default_font = get_font(ts["fontfacename"], self.pts_to_pixels(ts["fontsize"]))
        parindent = self.pts_to_pixels(ts["parindent"])
        top, bottom, left, right = 0, 0, parindent, parindent

        def add_word(width: _typing.Any, height: _typing.Any, left: _typing.Any, right: _typing.Any, top: _typing.Any, bottom: _typing.Any, ls: _typing.Any, ws: _typing.Any) -> tuple[_typing.Any, ...]:
            """
            Perform the add word operation under explicit file-format and conversion rules.

            Example:
                Exercise Cell.text block size.add word through a consuming regression::

                    python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


            :param width: Value supplied for width under the utility contract.
            :param height: Value supplied for height under the utility contract.
            :param left: Value supplied for left under the utility contract.
            :param right: Value supplied for right under the utility contract.
            :param top: Value supplied for top under the utility contract.
            :param bottom: Value supplied for bottom under the utility contract.
            :param ls: Value supplied for ls under the utility contract.
            :param ws: Value supplied for ws under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if left + width > maxwidth:
                left = width + ws
                top += ls
                bottom = top + ls if top + ls > bottom else bottom
            else:
                left += width + ws
                right = left if left > right else right
                bottom = top + ls if top + ls > bottom else bottom
            return left, right, top, bottom

        for token, attrs in tokens(tb):
            if attrs == None:
                attrs = {}
            font = default_font
            ls = self.pts_to_pixels(attrs.get("baselineskip", ts["baselineskip"])) + self.pts_to_pixels(
                attrs.get("linespace", ts["linespace"])
            )
            ws = self.pts_to_pixels(attrs.get("wordspace", ts["wordspace"]))
            if isinstance(token, int):  # Handle para and line breaks
                if top != bottom:  # Previous element not a line break
                    top = bottom
                else:
                    top += ls
                    bottom += ls
                left = parindent if token == 1 else 0
                continue
            if isinstance(token, Plot):
                width, height = self.pts_to_pixels(token.xsize), self.pts_to_pixels(token.ysize)
                left, right, top, bottom = add_word(width, height, left, right, top, bottom, height, ws)
                continue
            ff = attrs.get("fontfacename", ts["fontfacename"])
            fs = attrs.get("fontsize", ts["fontsize"])
            if (ff, fs) != (ts["fontfacename"], ts["fontsize"]):
                font = get_font(ff, self.pts_to_pixels(fs))
            for word in token.split():
                width, height = font.getsize(word)
                left, right, top, bottom = add_word(width, height, left, right, top, bottom, ls, ws)
        return right + 3 + max(parindent, 10), bottom

    def text_block_preferred_width(self: _typing.Self, tb: _typing.Any, debug: bool = False) -> _typing.Any:
        """
        Perform the text block preferred width operation under explicit file-format and conversion rules.

        Example:
            Exercise Cell.text block preferred width through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param tb: Value supplied for tb under the utility contract.
        :param debug: Value supplied for debug under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.text_block_size(tb, sys.maxsize, debug=debug)[0]

    def preferred_width(self: _typing.Self, debug: bool = False) -> _typing.Any:
        """
        Perform the preferred width operation under explicit file-format and conversion rules.

        Example:
            Exercise Cell.preferred width through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param debug: Value supplied for debug under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return ceil(max([self.text_block_preferred_width(i, debug=debug) for i in self.text_blocks]))

    def height(self: _typing.Self, width: _typing.Any) -> _typing.Any:
        """
        Perform the height operation under explicit file-format and conversion rules.

        Example:
            Exercise Cell.height through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param width: Value supplied for width under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return sum([self.text_block_size(i, width)[1] for i in self.text_blocks])


class Row(object):
    """
    Provide the row contract for validated ebook processing.

    Example:
        Exercise Row through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
    """
    def __init__(self: _typing.Self, conv: _typing.Any, row: _typing.Any, css: _typing.Any, colpad: _typing.Any) -> None:
        """
        Initialize and validate the row state.

        Example:
            Exercise Row.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param conv: Value supplied for conv under the utility contract.
        :param row: Value supplied for row under the utility contract.
        :param css: Value supplied for css under the utility contract.
        :param colpad: Value supplied for colpad under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.cells = []
        self.colpad = colpad
        cells = row.findAll(re.compile("td|th", re.IGNORECASE))
        self.targets = []
        for cell in cells:
            ccss = conv.tag_css(cell, css)[0]
            self.cells.append(Cell(conv, cell, ccss))
        for a in row.findAll(id=True) + row.findAll(name=True):
            name = a["name"] if "name" in a else a["id"] if "id" in a else None
            if name is not None:
                self.targets.append(name.replace("#", ""))

    def number_of_cells(self: _typing.Self) -> _typing.Any:
        """
        Number of cells in this row. Respects colspan

        Example:
            Exercise Row.number of cells through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = 0
        for cell in self.cells:
            ans += cell.colspan
        return ans

    def height(self: _typing.Self, widths: _typing.Any) -> _typing.Any:
        """
        Perform the height operation under explicit file-format and conversion rules.

        Example:
            Exercise Row.height through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param widths: Value supplied for widths under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        i, heights = 0, []
        for cell in self.cells:
            width = sum(widths[i : i + cell.colspan])
            heights.append(cell.height(width))
            i += cell.colspan
        if not heights:
            return 0
        return max(heights)

    def cell_from_index(self: _typing.Self, col: _typing.Any) -> _typing.Any:
        """
        Perform the cell from index operation under explicit file-format and conversion rules.

        Example:
            Exercise Row.cell from index through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param col: Value supplied for col under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        i = -1
        cell = None
        for cell in self.cells:
            for k in range(0, cell.colspan):
                if i == col:
                    break
                i += 1
            if i == col:
                break
        return cell

    def minimum_width(self: _typing.Self, col: _typing.Any) -> _typing.Any:
        """
        Perform the minimum width operation under explicit file-format and conversion rules.

        Example:
            Exercise Row.minimum width through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param col: Value supplied for col under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        cell = self.cell_from_index(col)
        if not cell:
            return 0
        return cell.minimum_width()

    def preferred_width(self: _typing.Self, col: _typing.Any) -> _typing.Any:
        """
        Perform the preferred width operation under explicit file-format and conversion rules.

        Example:
            Exercise Row.preferred width through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param col: Value supplied for col under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        cell = self.cell_from_index(col)
        if not cell:
            return 0
        return 0 if cell.colspan > 1 else cell.preferred_width()

    def width_percent(self: _typing.Self, col: _typing.Any) -> _typing.Any:
        """
        Perform the width percent operation under explicit file-format and conversion rules.

        Example:
            Exercise Row.width percent through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param col: Value supplied for col under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        cell = self.cell_from_index(col)
        if not cell:
            return -1
        return -1 if cell.colspan > 1 else cell.pwidth

    def cell_iterator(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the cell iterator operation under explicit file-format and conversion rules.

        Example:
            Exercise Row.cell iterator through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: An iterator yielding the normalized values described above.
        """
        for c in self.cells:
            yield c


class Table(object):
    """
    Provide the table contract for validated ebook processing.

    Example:
        Exercise Table through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
    """
    def __init__(self: _typing.Self, conv: _typing.Any, table: _typing.Any, css: _typing.Any, rowpad: int = 10, colpad: int = 10) -> None:
        """
        Initialize and validate the table state.

        Example:
            Exercise Table.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param conv: Value supplied for conv under the utility contract.
        :param table: Value supplied for table under the utility contract.
        :param css: Value supplied for css under the utility contract.
        :param rowpad: Value supplied for rowpad under the utility contract.
        :param colpad: Value supplied for colpad under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.rows = []
        self.conv = conv
        self.rowpad = rowpad
        self.colpad = colpad
        rows = table.findAll("tr")
        conv.in_table = True
        for row in rows:
            rcss = conv.tag_css(row, css)[0]
            self.rows.append(Row(conv, row, rcss, colpad))
        conv.in_table = False

    def number_of_columns(self: _typing.Self) -> _typing.Any:
        """
        Perform the number of columns operation under explicit file-format and conversion rules.

        Example:
            Exercise Table.number of columns through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        col_max = 0
        for row in self.rows:
            col_max = row.number_of_cells() if row.number_of_cells() > col_max else col_max
        return col_max

    def number_or_rows(self: _typing.Self) -> _typing.Any:
        """
        Perform the number or rows operation under explicit file-format and conversion rules.

        Example:
            Exercise Table.number or rows through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return len(self.rows)

    def height(self: _typing.Self, maxwidth: _typing.Any) -> _typing.Any:
        """
        Return row heights + self.rowpad

        Example:
            Exercise Table.height through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param maxwidth: Value supplied for maxwidth under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        widths = self.get_widths(maxwidth)
        return sum([row.height(widths) + self.rowpad for row in self.rows]) - self.rowpad

    def minimum_width(self: _typing.Self, col: _typing.Any) -> _typing.Any:
        """
        Perform the minimum width operation under explicit file-format and conversion rules.

        Example:
            Exercise Table.minimum width through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param col: Value supplied for col under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return max([row.minimum_width(col) for row in self.rows])

    def width_percent(self: _typing.Self, col: _typing.Any) -> _typing.Any:
        """
        Perform the width percent operation under explicit file-format and conversion rules.

        Example:
            Exercise Table.width percent through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param col: Value supplied for col under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return max([row.width_percent(col) for row in self.rows])

    def get_widths(self: _typing.Self, maxwidth: _typing.Any) -> _typing.Any:
        """
        Return widths of columns + self.colpad

        Example:
            Exercise Table.get widths through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param maxwidth: Value supplied for maxwidth under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        rows, cols = self.number_or_rows(), self.number_of_columns()
        widths = range(cols)
        for c in range(cols):
            cell_widths = [0 for i in range(rows)]
            for r in range(rows):
                try:
                    cell_widths[r] = self.rows[r].preferred_width(c)
                except IndexError:
                    continue
            widths[c] = max(cell_widths)

        min_widths = [self.minimum_width(i) + 10 for i in memory_range(cols)]
        for i in memory_range(len(widths)):
            wp = self.width_percent(i)
            if wp >= 0.0:
                widths[i] = max(
                    min_widths[i],
                    ceil((wp / 100.0) * (maxwidth - (cols - 1) * self.colpad)),
                )

        itercount = 0

        while sum(widths) > maxwidth - ((len(widths) - 1) * self.colpad) and itercount < 100:
            for i in range(cols):
                widths[i] = (
                    ceil((95.0 / 100.0) * widths[i]) if ceil((95.0 / 100.0) * widths[i]) >= min_widths[i] else widths[i]
                )
            itercount += 1

        return [i + self.colpad for i in widths]

    def blocks(self: _typing.Self, maxwidth: _typing.Any, maxheight: _typing.Any) -> _typing.Iterator[_typing.Any]:
        """
        Perform the blocks operation under explicit file-format and conversion rules.

        Example:
            Exercise Table.blocks through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param maxwidth: Value supplied for maxwidth under the utility contract.
        :param maxheight: Value supplied for maxheight under the utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        rows, cols = self.number_or_rows(), self.number_of_columns()
        cellmatrix = [[None for c in range(cols)] for r in range(rows)]
        rowpos = [0 for i in range(rows)]
        for r in range(rows):
            nc = self.rows[r].cell_iterator()
            try:
                while True:
                    cell = nc.next()
                    cellmatrix[r][rowpos[r]] = cell
                    rowpos[r] += cell.colspan
                    for k in range(1, cell.rowspan):
                        try:
                            rowpos[r + k] += 1
                        except IndexError:
                            break
            except StopIteration:  # No more cells in this row
                continue

        widths = self.get_widths(maxwidth)
        heights = [row.height(widths) for row in self.rows]

        xpos = [sum(widths[:i]) for i in range(cols)]
        delta = maxwidth - sum(widths)
        if delta < 0:
            delta = 0
        for r in range(len(cellmatrix)):
            yield None, 0, heights[r], 0, self.rows[r].targets
            for c in range(len(cellmatrix[r])):
                cell = cellmatrix[r][c]
                if not cell:
                    continue
                width = sum(widths[c : c + cell.colspan]) - self.colpad * cell.colspan
                sypos = 0
                for tb in cell.text_blocks:
                    tb.blockStyle = self.conv.book.create_block_style(
                        blockwidth=width,
                        blockheight=cell.text_block_size(tb, width)[1],
                        blockrule="horz-fixed",
                    )

                    yield tb, xpos[c], sypos, delta, None
                    sypos += tb.blockStyle.attrs["blockheight"]
