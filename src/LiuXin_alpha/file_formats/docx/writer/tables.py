#!/usr/bin/env python2
# vim:fileencoding=utf-8

"""
Translate normalized HTML tables into DOCX grids, cells, spans and borders.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise tables through a consuming regression::

        python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

from collections import namedtuple

from LiuXin_alpha.file_formats.docx.writer.styles import (
    read_css_block_borders as rcbb,
    border_edges,
)
from LiuXin_alpha.file_formats.docx.writer.utils import convert_color

# Py2/Py3
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import memory_range

__license__ = "GPL v3"
__copyright__ = "2015, Kovid Goyal <kovid at kovidgoyal.net>"


class Dummy(object):
    """
    Provide the dummy contract for validated ebook processing.

    Example:
        Exercise Dummy through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
    """
    pass


Border = namedtuple("Border", "css_style style width color level")
border_style_weight = {
    x: 100 - i for i, x in enumerate(("double", "solid", "dashed", "dotted", "ridge", "outset", "groove", "inset"))
}


class SpannedCell(object):
    """
    Provide the spannedcell contract for validated ebook processing.

    Example:
        Exercise SpannedCell through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
    """
    def __init__(self: _typing.Self, spanning_cell: _typing.Any, horizontal: bool = True) -> None:
        """
        Initialize and validate the spannedcell state.

        Example:
            Exercise SpannedCell.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param spanning_cell: Value supplied for spanning cell under the utility contract.
        :param horizontal: Value supplied for horizontal under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.spanning_cell = spanning_cell
        self.horizontal = horizontal
        self.row_span = self.col_span = 1

    def resolve_borders(self: _typing.Self) -> None:
        """
        Perform the resolve borders operation under explicit file-format and conversion rules.

        Example:
            Exercise SpannedCell.resolve borders through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pass

    def serialize(self: _typing.Self, tr: _typing.Any, makeelement: _typing.Any) -> None:
        """
        Perform the serialize operation under explicit file-format and conversion rules.

        Example:
            Exercise SpannedCell.serialize through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param tr: Value supplied for tr under the utility contract.
        :param makeelement: Value supplied for makeelement under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        tc = makeelement(tr, "w:tc")
        tc_pr = makeelement(tc, "w:tcPr")
        makeelement(tc_pr, "w:%sMerge" % ("h" if self.horizontal else "v"), w_val="continue")
        makeelement(tc, "w:p")


def read_css_block_borders(self: _typing.Any, css: _typing.Any) -> None:
    """
    Read css block borders under the format's safety and compatibility rules.

    Example:
        Exercise read css block borders through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


    :param self: Value supplied for self under the utility contract.
    :param css: Value supplied for css under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    obj = Dummy()
    rcbb(obj, css, store_css_style=True)
    for edge in border_edges:
        setattr(
            self,
            "border_" + edge,
            Border(
                getattr(obj, "border_%s_css_style" % edge),
                getattr(obj, "border_%s_style" % edge),
                getattr(obj, "border_%s_width" % edge),
                getattr(obj, "border_%s_color" % edge),
                self.BLEVEL,
            ),
        )
        setattr(self, "padding_" + edge, getattr(obj, "padding_" + edge))


def as_percent(x: _typing.Any) -> _typing.Any:
    """
    Perform the as percent operation under explicit file-format and conversion rules.

    Example:
        Exercise as percent through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


    :param x: Value supplied for x under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if x and x.endswith("%"):
        try:
            return float(x.rstrip("%"))
        except Exception:
            pass


def convert_width(tag_style: _typing.Any) -> tuple[_typing.Any, ...]:
    """
    Convert width under the format's safety and compatibility rules.

    Example:
        Exercise convert width through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


    :param tag_style: Value supplied for tag style under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if tag_style is not None:
        w = tag_style._get("width")
        wp = as_percent(w)
        if w == "auto":
            return "auto", 0
        elif wp is not None:
            return "pct", int(wp * 50)
        else:
            try:
                return "dxa", int(float(tag_style["width"]) * 20)
            except Exception:
                pass
    return "auto", 0


class Cell(object):

    """
    Provide the cell contract for validated ebook processing.

    Example:
        Exercise Cell through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
    """
    BLEVEL = 2

    def __init__(self: _typing.Self, row: _typing.Any, html_tag: _typing.Any, tag_style: _typing.Any) -> None:
        """
        Initialize and validate the cell state.

        Example:
            Exercise Cell.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param row: Value supplied for row under the utility contract.
        :param html_tag: Value supplied for html tag under the utility contract.
        :param tag_style: Value supplied for tag style under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.row = row
        self.table = self.row.table
        self.html_tag = html_tag
        try:
            self.row_span = max(0, int(html_tag.get("rowspan", 1)))
        except Exception:
            self.row_span = 1
        try:
            self.col_span = max(0, int(html_tag.get("colspan", 1)))
        except Exception:
            self.col_span = 1
        self.valign = {"top": "top", "bottom": "bottom", "middle": "center"}.get(tag_style._get("vertical-align"))
        self.items = []
        self.width = convert_width(tag_style)
        self.background_color = None if tag_style is None else convert_color(tag_style.backgroundColor)
        read_css_block_borders(self, tag_style)

    def add_block(self: _typing.Self, block: _typing.Any) -> None:
        """
        Perform the add block operation under explicit file-format and conversion rules.

        Example:
            Exercise Cell.add block through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param block: Value supplied for block under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.items.append(block)
        block.parent_items = self.items

    def add_table(self: _typing.Self, table: _typing.Any) -> _typing.Any:
        """
        Perform the add table operation under explicit file-format and conversion rules.

        Example:
            Exercise Cell.add table through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param table: Value supplied for table under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.items.append(table)
        return table

    def serialize(self: _typing.Self, parent: _typing.Any, makeelement: _typing.Any) -> None:
        """
        Perform the serialize operation under explicit file-format and conversion rules.

        Example:
            Exercise Cell.serialize through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :param makeelement: Value supplied for makeelement under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        tc = makeelement(parent, "w:tc")
        tc_pr = makeelement(tc, "w:tcPr")
        makeelement(tc_pr, "w:tcW", w_type=self.width[0], w_w=str(self.width[1]))
        # For some reason, Word 2007 refuses to honor <w:shd> at the table or row
        # level, despite what the specs say, so we inherit and apply at the
        # cell level
        bc = self.background_color or self.row.background_color or self.row.table.background_color
        if bc:
            makeelement(tc_pr, "w:shd", w_val="clear", w_color="auto", w_fill=bc)

        b = makeelement(tc_pr, "w:tcBorders", append=False)
        for edge, border in iteritems(self.borders):
            if border is not None and border.width > 0 and border.style != "none":
                makeelement(
                    b,
                    "w:" + edge,
                    w_val=border.style,
                    w_sz=str(border.width),
                    w_color=border.color,
                )
        if len(b) > 0:
            tc_pr.append(b)

        m = makeelement(tc_pr, "w:tcMar", append=False)
        for edge in border_edges:
            padding = getattr(self, "padding_" + edge)
            if (
                edge in {"top", "bottom"}
                or (edge == "left" and self is self.row.first_cell)
                or (edge == "right" and self is self.row.last_cell)
            ):
                padding += getattr(self.row, "padding_" + edge)
            if padding > 0:
                makeelement(m, "w:" + edge, w_type="dxa", w_w=str(int(padding * 20)))
        if len(m) > 0:
            tc_pr.append(m)

        if self.valign is not None:
            makeelement(tc_pr, "w:vAlign", w_val=self.valign)

        if self.row_span > 1:
            makeelement(tc_pr, "w:vMerge", w_val="restart")
        if self.col_span > 1:
            makeelement(tc_pr, "w:hMerge", w_val="restart")

        item = None
        for item in self.items:
            item.serialize(tc)
        if item is None or isinstance(item, Table):
            # Word 2007 requires the last element in a table cell to be a paragraph
            makeelement(tc, "w:p")

    def applicable_borders(self: _typing.Self, edge: _typing.Any) -> _typing.Any:
        """
        Perform the applicable borders operation under explicit file-format and conversion rules.

        Example:
            Exercise Cell.applicable borders through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param edge: Value supplied for edge under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if edge == "left":
            items = {self.table, self.row, self} if self.row.first_cell is self else {self}
        elif edge == "top":
            items = ({self.table} if self.table.first_row is self.row else set()) | {
                self,
                self.row,
            }
        elif edge == "right":
            items = {self.table, self, self.row} if self.row.last_cell is self else {self}
        elif edge == "bottom":
            items = ({self.table} if self.table.last_row is self.row else set()) | {
                self,
                self.row,
            }
        return {getattr(x, "border_" + edge) for x in items}

    def resolve_border(self: _typing.Self, edge: _typing.Any) -> _typing.Any:
        # In Word cell borders override table borders, and Word ignores row
        # borders, so we consolidate all borders as cell borders
        # In HTML the priority is as described here:
        # http://www.w3.org/TR/CSS21/tables.html#border-conflict-resolution
        """
        Perform the resolve border operation under explicit file-format and conversion rules.

        Example:
            Exercise Cell.resolve border through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param edge: Value supplied for edge under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        neighbor = self.neighbor(edge)
        borders = self.applicable_borders(edge)
        if neighbor is not None:
            nedge = {
                "left": "right",
                "top": "bottom",
                "right": "left",
                "bottom": "top",
            }[edge]
            borders |= neighbor.applicable_borders(nedge)

        for b in borders:
            if b.css_style == "hidden":
                return None

        def weight(local_border: _typing.Any) -> tuple[_typing.Any, ...]:
            """
            Perform the weight operation under explicit file-format and conversion rules.

            Example:
                Exercise Cell.resolve border.weight through a consuming regression::

                    python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


            :param local_border: Value supplied for local border under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return (
                0 if local_border.css_style == "none" else 1,
                local_border.width,
                border_style_weight.get(local_border.css_style, 0),
                local_border.level,
            )

        border = sorted(borders, key=weight)[-1]
        return border

    def resolve_borders(self: _typing.Self) -> None:
        """
        Perform the resolve borders operation under explicit file-format and conversion rules.

        Example:
            Exercise Cell.resolve borders through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.borders = {edge: self.resolve_border(edge) for edge in border_edges}

    def neighbor(self: _typing.Self, edge: _typing.Any) -> _typing.Any:
        """
        Perform the neighbor operation under explicit file-format and conversion rules.

        Example:
            Exercise Cell.neighbor through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param edge: Value supplied for edge under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        idx = self.row.cells.index(self)
        ans = None
        if edge == "left":
            ans = self.row.cells[idx - 1] if idx > 0 else None
        elif edge == "right":
            ans = self.row.cells[idx + 1] if (idx + 1) < len(self.row.cells) else None
        elif edge == "top":
            ridx = self.table.rows.index(self.row)
            if ridx > 0 and idx < len(self.table.rows[ridx - 1].cells):
                ans = self.table.rows[ridx - 1].cells[idx]
        elif edge == "bottom":
            ridx = self.table.rows.index(self.row)
            if ridx + 1 < len(self.table.rows) and idx < len(self.table.rows[ridx + 1].cells):
                ans = self.table.rows[ridx + 1].cells[idx]
        return getattr(ans, "spanning_cell", ans)


class Row(object):

    """
    Provide the row contract for validated ebook processing.

    Example:
        Exercise Row through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
    """
    BLEVEL = 1

    def __init__(self: _typing.Self, table: _typing.Any, html_tag: _typing.Any, tag_style: _typing.Any = None) -> None:
        """
        Initialize and validate the row state.

        Example:
            Exercise Row.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param table: Value supplied for table under the utility contract.
        :param html_tag: Value supplied for html tag under the utility contract.
        :param tag_style: Value supplied for tag style under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.table = table
        self.html_tag = html_tag
        self.cells = []
        self.current_cell = None
        self.background_color = None if tag_style is None else convert_color(tag_style.backgroundColor)
        read_css_block_borders(self, tag_style)

    @property
    def first_cell(self: _typing.Self) -> _typing.Any:
        """
        Perform the first cell operation under explicit file-format and conversion rules.

        Example:
            Exercise Row.first cell through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.cells[0] if self.cells else None

    @property
    def last_cell(self: _typing.Self) -> _typing.Any:
        """
        Perform the last cell operation under explicit file-format and conversion rules.

        Example:
            Exercise Row.last cell through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.cells[-1] if self.cells else None

    def start_new_cell(self: _typing.Self, html_tag: _typing.Any, tag_style: _typing.Any) -> None:
        """
        Perform the start new cell operation under explicit file-format and conversion rules.

        Example:
            Exercise Row.start new cell through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param html_tag: Value supplied for html tag under the utility contract.
        :param tag_style: Value supplied for tag style under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.current_cell = Cell(self, html_tag, tag_style)

    def finish_tag(self: _typing.Self, html_tag: _typing.Any) -> None:
        """
        Perform the finish tag operation under explicit file-format and conversion rules.

        Example:
            Exercise Row.finish tag through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param html_tag: Value supplied for html tag under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.current_cell is not None:
            if html_tag is self.current_cell.html_tag:
                self.cells.append(self.current_cell)
                self.current_cell = None

    def add_block(self: _typing.Self, block: _typing.Any) -> None:
        """
        Perform the add block operation under explicit file-format and conversion rules.

        Example:
            Exercise Row.add block through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param block: Value supplied for block under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.current_cell.add_block(block)

    def add_table(self: _typing.Self, table: _typing.Any) -> _typing.Any:
        """
        Perform the add table operation under explicit file-format and conversion rules.

        Example:
            Exercise Row.add table through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param table: Value supplied for table under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.current_cell.add_table(table)

    def serialize(self: _typing.Self, parent: _typing.Any, makeelement: _typing.Any) -> None:
        """
        Perform the serialize operation under explicit file-format and conversion rules.

        Example:
            Exercise Row.serialize through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :param makeelement: Value supplied for makeelement under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        tr = makeelement(parent, "w:tr")
        for cell in self.cells:
            cell.serialize(tr, makeelement)


class Table(object):

    """
    Provide the table contract for validated ebook processing.

    Example:
        Exercise Table through a consuming regression::

            python -m pytest -q tests/file_formats/docx/test_docx_modernized.py
    """
    BLEVEL = 0

    def __init__(self: _typing.Self, namespace: _typing.Any, html_tag: _typing.Any, tag_style: _typing.Any = None) -> None:
        """
        Initialize and validate the table state.

        Example:
            Exercise Table.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param namespace: Value supplied for namespace under the utility contract.
        :param html_tag: Value supplied for html tag under the utility contract.
        :param tag_style: Value supplied for tag style under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.namespace = namespace
        self.html_tag = html_tag
        self.rows = []
        self.current_row = None
        self.width = convert_width(tag_style)
        self.background_color = None if tag_style is None else convert_color(tag_style.backgroundColor)
        self.jc = None
        self.float = None
        self.margin_left = self.margin_right = self.margin_top = self.margin_bottom = None
        if tag_style is not None:
            ml, mr = tag_style._get("margin-left"), tag_style.get("margin-right")
            if ml == "auto":
                self.jc = "center" if mr == "auto" else "right"
            self.float = tag_style["float"]
            for edge in border_edges:
                setattr(self, "margin_" + edge, tag_style["margin-" + edge])
        read_css_block_borders(self, tag_style)

    @property
    def first_row(self: _typing.Self) -> _typing.Any:
        """
        Perform the first row operation under explicit file-format and conversion rules.

        Example:
            Exercise Table.first row through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.rows[0] if self.rows else None

    @property
    def last_row(self: _typing.Self) -> _typing.Any:
        """
        Perform the last row operation under explicit file-format and conversion rules.

        Example:
            Exercise Table.last row through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.rows[-1] if self.rows else None

    def finish_tag(self: _typing.Self, html_tag: _typing.Any) -> _typing.Any:
        """
        Perform the finish tag operation under explicit file-format and conversion rules.

        Example:
            Exercise Table.finish tag through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param html_tag: Value supplied for html tag under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.current_row is not None:
            self.current_row.finish_tag(html_tag)
            if self.current_row.html_tag is html_tag:
                self.rows.append(self.current_row)
                self.current_row = None
        table_ended = self.html_tag is html_tag
        if table_ended:
            self.expand_spanned_cells()
            for row in self.rows:
                for cell in row.cells:
                    cell.resolve_borders()
        return table_ended

    def expand_spanned_cells(self: _typing.Self) -> None:
        # Expand horizontally
        """
        Perform the expand spanned cells operation under explicit file-format and conversion rules.

        Example:
            Exercise Table.expand spanned cells through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for row in self.rows:
            for cell in tuple(row.cells):
                idx = row.cells.index(cell)
                if cell.col_span > 1 and (cell is row.cells[-1] or not isinstance(row.cells[idx + 1], SpannedCell)):
                    row.cells[idx : idx + 1] = [cell] + [
                        SpannedCell(cell, horizontal=True) for i in memory_range(1, cell.col_span)
                    ]

        # Expand vertically
        for r, row in enumerate(self.rows):
            for idx, cell in enumerate(row.cells):
                if cell.row_span > 1:
                    for nrow in self.rows[r + 1 :]:
                        sc = SpannedCell(cell, horizontal=False)
                        try:
                            tcell = nrow.cells[idx]
                        except Exception:
                            tcell = None
                        if tcell is None:
                            nrow.cells.extend(
                                [
                                    SpannedCell(nrow.cells[-1], horizontal=True)
                                    for i in memory_range(idx - len(nrow.cells))
                                ]
                            )
                            nrow.cells.append(sc)
                        else:
                            if isinstance(tcell, SpannedCell):
                                # Conflict between rowspan and colspan
                                break
                            else:
                                nrow.cells.insert(idx, sc)

    def start_new_row(self: _typing.Self, html_tag: _typing.Any, html_style: _typing.Any) -> None:
        """
        Perform the start new row operation under explicit file-format and conversion rules.

        Example:
            Exercise Table.start new row through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param html_tag: Value supplied for html tag under the utility contract.
        :param html_style: Value supplied for html style under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.current_row is not None:
            self.rows.append(self.current_row)
        self.current_row = Row(self, html_tag, html_style)

    def start_new_cell(self: _typing.Self, html_tag: _typing.Any, html_style: _typing.Any) -> None:
        """
        Perform the start new cell operation under explicit file-format and conversion rules.

        Example:
            Exercise Table.start new cell through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param html_tag: Value supplied for html tag under the utility contract.
        :param html_style: Value supplied for html style under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.current_row is None:
            self.start_new_row(html_tag, None)
        self.current_row.start_new_cell(html_tag, html_style)

    def add_block(self: _typing.Self, block: _typing.Any) -> None:
        """
        Perform the add block operation under explicit file-format and conversion rules.

        Example:
            Exercise Table.add block through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param block: Value supplied for block under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.current_row.add_block(block)

    def add_table(self: _typing.Self, table: _typing.Any) -> _typing.Any:
        """
        Perform the add table operation under explicit file-format and conversion rules.

        Example:
            Exercise Table.add table through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param table: Value supplied for table under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.current_row.add_table(table)

    def serialize(self: _typing.Self, parent: _typing.Any) -> None:
        """
        Perform the serialize operation under explicit file-format and conversion rules.

        Example:
            Exercise Table.serialize through a consuming regression::

                python -m pytest -q tests/file_formats/docx/test_docx_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        makeelement = self.namespace.makeelement
        rows = [r for r in self.rows if r.cells]
        if not rows:
            return
        tbl = makeelement(parent, "w:tbl")
        tbl_pr = makeelement(tbl, "w:tblPr")
        makeelement(tbl_pr, "w:tblW", w_type=self.width[0], w_w=str(self.width[1]))
        if self.float in {"left", "right"}:
            kw = {
                "w_vertAnchor": "text",
                "w_horzAnchor": "text",
                "w_tblpXSpec": self.float,
            }
            for edge in border_edges:
                val = getattr(self, "margin_" + edge) or 0
                if {self.float, edge} == {"left", "right"}:
                    val = max(val, 2)
                kw["w_" + edge + "FromText"] = str(max(0, int(val * 20)))
            makeelement(tbl_pr, "w:tblpPr", **kw)
        if self.jc is not None:
            makeelement(tbl_pr, "w:jc", w_val=self.jc)
        for row in rows:
            row.serialize(tbl, makeelement)
