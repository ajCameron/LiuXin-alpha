#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Reconstruct reading order, paragraphs and images from PDF layout data.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise reflow through a consuming regression::

        python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
"""
from __future__ import with_statement
from __future__ import annotations

import typing as _typing

import sys
import os
from functools import cmp_to_key

from lxml import etree

from LiuXin_alpha.utils.libraries.liuxin_six import six_cmp
from LiuXin_alpha.utils.libraries.liuxin_six import memory_range

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class Font(object):
    """
    Provide the font contract for validated ebook processing.

    Example:
        Exercise Font through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, spec: _typing.Any) -> None:
        """
        Initialize and validate the font state.

        Example:
            Exercise Font.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param spec: Value supplied for spec under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.id = spec.get("id")
        self.size = float(spec.get("size"))
        self.color = spec.get("color")
        self.family = spec.get("family")


class Element(object):
    """
    Provide the element contract for validated ebook processing.

    Example:
        Exercise Element through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the element state.

        Example:
            Exercise Element.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.starts_block = None
        self.block_style = None

    def __eq__(self: _typing.Self, other: _typing.Any) -> bool:
        """
        Perform the eq operation under explicit file-format and conversion rules.

        Example:
            Exercise Element.  eq   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param other: Value supplied for other under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.id == other.id

    def __hash__(self: _typing.Self) -> _typing.Any:
        """
        Perform the hash operation under explicit file-format and conversion rules.

        Example:
            Exercise Element.  hash   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return hash(self.id)


class Image(Element):
    """
    Provide the image contract for validated ebook processing.

    Example:
        Exercise Image through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, img: _typing.Any, opts: _typing.Any, log: _typing.Any, idc: _typing.Any) -> None:
        """
        Initialize and validate the image state.

        Example:
            Exercise Image.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param img: Value supplied for img under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param idc: Value supplied for idc under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        Element.__init__(self)
        self.opts, self.log = opts, log
        self.id = idc.next()
        self.top, self.left, self.width, self.height, self.iwidth, self.iheight = map(
            float,
            map(img.get, ("top", "left", "rwidth", "rheight", "iwidth", "iheight")),
        )
        self.src = img.get("src")
        self.bottom = self.top + self.height
        self.right = self.left + self.width

    def to_html(self: _typing.Self) -> _typing.Any:
        """
        Perform the to html operation under explicit file-format and conversion rules.

        Example:
            Exercise Image.to html through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return '<img src="%s" width="%dpx" height="%dpx"/>' % (
            self.src,
            int(self.width),
            int(self.height),
        )

    def dump(self: _typing.Self, f: _typing.Any) -> None:
        """
        Perform the dump operation under explicit file-format and conversion rules.

        Example:
            Exercise Image.dump through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param f: Value supplied for f under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        f.write(self.to_html())
        f.write("\n")


class Text(Element):
    """
    Provide the text contract for validated ebook processing.

    Example:
        Exercise Text through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, text: _typing.Any, font_map: _typing.Any, opts: _typing.Any, log: _typing.Any, idc: _typing.Any) -> None:
        """
        Initialize and validate the text state.

        Example:
            Exercise Text.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param text: Text parsed, normalized or rendered.
        :param font_map: Value supplied for font map under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param idc: Value supplied for idc under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        Element.__init__(self)
        self.id = idc.next()
        self.opts, self.log = opts, log
        self.font_map = font_map
        self.top, self.left, self.width, self.height = map(float, map(text.get, ("top", "left", "width", "height")))
        self.bottom = self.top + self.height
        self.right = self.left + self.width
        self.font = self.font_map[text.get("font")]
        self.font_size = self.font.size
        self.color = self.font.color
        self.font_family = self.font.family

        text.tail = ""
        self.text_as_string = etree.tostring(text, method="text", encoding="unicode")
        self.raw = text.text if text.text else ""
        for x in text.iterchildren():
            self.raw += etree.tostring(x, method="xml", encoding="unicode")
        self.average_character_width = self.width / len(self.text_as_string)

    def coalesce(self: _typing.Self, other: _typing.Any, page_number: _typing.Any) -> None:
        """
        Perform the coalesce operation under explicit file-format and conversion rules.

        Example:
            Exercise Text.coalesce through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param other: Value supplied for other under the utility contract.
        :param page_number: Value supplied for page number under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.opts.verbose > 2:
            self.log.debug(
                "Coalescing %r with %r on page %d" % (self.text_as_string, other.text_as_string, page_number)
            )
        self.top = min(self.top, other.top)
        self.right = other.right
        self.width = self.right - self.left
        self.bottom = max(self.bottom, other.bottom)
        self.height = self.bottom - self.top
        self.font_size = max(self.font_size, other.font_size)
        self.font = other.font if self.font_size == other.font_size else other.font
        self.text_as_string += other.text_as_string
        self.raw += other.raw
        self.average_character_width = (self.average_character_width + other.average_character_width) / 2.0

    def to_html(self: _typing.Self) -> _typing.Any:
        """
        Perform the to html operation under explicit file-format and conversion rules.

        Example:
            Exercise Text.to html through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.raw

    def dump(self: _typing.Self, f: _typing.Any) -> None:
        """
        Perform the dump operation under explicit file-format and conversion rules.

        Example:
            Exercise Text.dump through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param f: Value supplied for f under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        f.write(self.to_html().encode("utf-8"))
        f.write("\n")


class FontSizeStats(dict):
    """
    Provide the fontsizestats contract for validated ebook processing.

    Example:
        Exercise FontSizeStats through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, stats: _typing.Any) -> None:
        """
        Initialize and validate the fontsizestats state.

        Example:
            Exercise FontSizeStats.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param stats: Value supplied for stats under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        total = float(sum(stats.values()))
        self.most_common_size, self.chars_at_most_common_size = -1, 0

        for sz, chars in stats.items():
            if chars >= self.chars_at_most_common_size:
                self.most_common_size, self.chars_at_most_common_size = sz, chars
            self[sz] = chars / total


class Interval(object):
    """
    Provide the interval contract for validated ebook processing.

    Example:
        Exercise Interval through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, left: _typing.Any, right: _typing.Any) -> None:
        """
        Initialize and validate the interval state.

        Example:
            Exercise Interval.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param left: Value supplied for left under the utility contract.
        :param right: Value supplied for right under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.left, self.right = left, right
        self.width = right - left

    def intersection(self: _typing.Self, other: _typing.Any) -> _typing.Any:
        """
        Perform the intersection operation under explicit file-format and conversion rules.

        Example:
            Exercise Interval.intersection through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param other: Value supplied for other under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        left = max(self.left, other.left)
        right = min(self.right, other.right)
        return Interval(left, right)

    def centered_in(self: _typing.Self, parent: _typing.Any) -> bool:
        """
        Perform the centered in operation under explicit file-format and conversion rules.

        Example:
            Exercise Interval.centered in through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param parent: Value supplied for parent under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        left = abs(self.left - parent.left)
        right = abs(self.right - parent.right)
        return abs(left - right) < 3

    def __nonzero__(self: _typing.Self) -> bool:
        """
        Perform the nonzero operation under explicit file-format and conversion rules.

        Example:
            Exercise Interval.  nonzero   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: True when the documented condition holds; otherwise False.
        """
        return self.width > 0

    def __eq__(self: _typing.Self, other: _typing.Any) -> bool:
        """
        Perform the eq operation under explicit file-format and conversion rules.

        Example:
            Exercise Interval.  eq   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param other: Value supplied for other under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.left == other.left and self.right == other.right

    def __hash__(self: _typing.Self) -> _typing.Any:
        """
        Perform the hash operation under explicit file-format and conversion rules.

        Example:
            Exercise Interval.  hash   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return hash("(%f,%f)" % (self.left, self.right))


class Column(object):

    # A column contains an element is the element bulges out to
    # the left or the right by at most HFUZZ*col width.
    """
    Provide the column contract for validated ebook processing.

    Example:
        Exercise Column through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    HFUZZ = 0.2

    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the column state.

        Example:
            Exercise Column.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        self.left = self.right = self.top = self.bottom = 0
        self.width = self.height = 0
        self.elements = []
        self.average_line_separation = 0

    def add(self: _typing.Self, elem: _typing.Any) -> None:
        """
        Perform the add operation under explicit file-format and conversion rules.

        Example:
            Exercise Column.add through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param elem: Value supplied for elem under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if elem in self.elements:
            return
        self.elements.append(elem)
        self._post_add()

    def prepend(self: _typing.Self, elem: _typing.Any) -> None:
        """
        Perform the prepend operation under explicit file-format and conversion rules.

        Example:
            Exercise Column.prepend through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param elem: Value supplied for elem under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if elem in self.elements:
            return
        self.elements.insert(0, elem)
        self._post_add()

    def _post_add(self: _typing.Self) -> None:
        """
        Perform the post add operation under explicit file-format and conversion rules.

        Example:
            Exercise Column. post add through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.elements.sort(key=cmp_to_key(lambda ele_1, ele_2: six_cmp(ele_1.bottom, ele_2.bottom)))
        self.top = self.elements[0].top
        self.bottom = self.elements[-1].bottom
        self.left, self.right = sys.maxsize, 0
        for x in self:
            self.left = min(self.left, x.left)
            self.right = max(self.right, x.right)
        self.width, self.height = self.right - self.left, self.bottom - self.top

    def __iter__(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the iter operation under explicit file-format and conversion rules.

        Example:
            Exercise Column.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: An iterator yielding the normalized values described above.
        """
        for x in self.elements:
            yield x

    def __len__(self: _typing.Self) -> _typing.Any:
        """
        Perform the len operation under explicit file-format and conversion rules.

        Example:
            Exercise Column.  len   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return len(self.elements)

    def contains(self: _typing.Self, elem: _typing.Any) -> bool:
        """
        Perform the contains operation under explicit file-format and conversion rules.

        Example:
            Exercise Column.contains through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param elem: Value supplied for elem under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return elem.left > self.left - self.HFUZZ * self.width and elem.right < self.right + self.HFUZZ * self.width

    def collect_stats(self: _typing.Self) -> None:
        """
        Perform the collect stats operation under explicit file-format and conversion rules.

        Example:
            Exercise Column.collect stats through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if len(self.elements) > 1:
            gaps = [self.elements[i + 1].top - self.elements[i].bottom for i in range(0, len(self.elements) - 1)]
            self.average_line_separation = sum(gaps) / len(gaps)
        for i, elem in enumerate(self.elements):
            left_margin = elem.left - self.left
            elem.indent_fraction = left_margin / self.width
            elem.width_fraction = elem.width / self.width
            if i == 0:
                elem.top_gap_ratio = None
            else:
                elem.top_gap_ratio = (self.elements[i - 1].bottom - elem.top) / self.average_line_separation

    def previous_element(self: _typing.Self, idx: _typing.Any) -> _typing.Any:
        """
        Perform the previous element operation under explicit file-format and conversion rules.

        Example:
            Exercise Column.previous element through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param idx: Value supplied for idx under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if idx == 0:
            return None
        return self.elements[idx - 1]

    def dump(self: _typing.Self, f: _typing.Any, num: _typing.Any) -> None:
        """
        Perform the dump operation under explicit file-format and conversion rules.

        Example:
            Exercise Column.dump through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param f: Value supplied for f under the utility contract.
        :param num: Value supplied for num under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        f.write("******** Column %d\n\n" % num)
        for elem in self.elements:
            elem.dump(f)


class Box(list):
    """
    Provide the box contract for validated ebook processing.

    Example:
        Exercise Box through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, type: str = "p") -> None:
        """
        Initialize and validate the box state.

        Example:
            Exercise Box.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param type: Value supplied for type under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.tag = type

    def to_html(self: _typing.Self) -> _typing.Any:
        """
        Perform the to html operation under explicit file-format and conversion rules.

        Example:
            Exercise Box.to html through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = ["<%s>" % self.tag]
        for elem in self:
            if isinstance(elem, int):
                ans.append('<a name="page_%d"/>' % elem)
            else:
                ans.append(elem.to_html() + " ")
        ans.append("</%s>" % self.tag)
        return ans


class ImageBox(Box):
    """
    Provide the imagebox contract for validated ebook processing.

    Example:
        Exercise ImageBox through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, img: _typing.Any) -> None:
        """
        Initialize and validate the imagebox state.

        Example:
            Exercise ImageBox.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param img: Value supplied for img under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        Box.__init__(self)
        self.img = img

    def to_html(self: _typing.Self) -> _typing.Any:
        """
        Perform the to html operation under explicit file-format and conversion rules.

        Example:
            Exercise ImageBox.to html through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = list(['<div style="text-align:center">'])
        ans.append(self.img.to_html())
        if len(self) > 0:
            ans.append("<br/>")
            for elem in self:
                if isinstance(elem, int):
                    ans.append('<a name="page_%d"/>' % elem)
                else:
                    ans.append(elem.to_html() + " ")
        ans.append("</div>")
        return ans


class Region(object):
    """
    Provide the region contract for validated ebook processing.

    Example:
        Exercise Region through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, opts: _typing.Any, log: _typing.Any) -> None:
        """
        Initialize and validate the region state.

        Example:
            Exercise Region.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.opts, self.log = opts, log
        self.columns = []
        self.top = self.bottom = self.left = self.right = self.width = self.height = 0

    def add(self: _typing.Self, columns: _typing.Any) -> None:
        """
        Perform the add operation under explicit file-format and conversion rules.

        Example:
            Exercise Region.add through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param columns: Value supplied for columns under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not self.columns:
            for x in sorted(columns, key=cmp_to_key(lambda col_1, col_2: six_cmp(col_1.left, col_2.left))):
                self.columns.append(x)
        else:
            for i in range(len(columns)):
                for elem in columns[i]:
                    self.columns[i].add(elem)

    def contains(self: _typing.Self, columns: _typing.Any) -> bool:
        # TODO: handle unbalanced columns
        """
        Perform the contains operation under explicit file-format and conversion rules.

        Example:
            Exercise Region.contains through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param columns: Value supplied for columns under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not self.columns:
            return True
        if len(columns) != len(self.columns):
            return False
        for i in range(len(columns)):
            c1, c2 = self.columns[i], columns[i]
            x1 = Interval(c1.left, c1.right)
            x2 = Interval(c2.left, c2.right)
            intersection = x1.intersection(x2)
            base = min(x1.width, x2.width)
            if intersection.width / base < 0.6:
                return False
        return True

    def is_empty(self: _typing.Self) -> bool:
        """
        Return whether is empty holds for the supplied ebook data.

        Example:
            Exercise Region.is empty through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: True when the documented condition holds; otherwise False.
        """
        return len(self.columns) == 0

    def line_count(self: _typing.Self) -> _typing.Any:
        """
        Perform the line count operation under explicit file-format and conversion rules.

        Example:
            Exercise Region.line count through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        max_lines = 0
        for c in self.columns:
            max_lines = max(max_lines, len(c))
        return max_lines

    def is_small(self: _typing.Self) -> bool:
        """
        Return whether is small holds for the supplied ebook data.

        Example:
            Exercise Region.is small through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: True when the documented condition holds; otherwise False.
        """
        return self.line_count < 3

    def absorb(self: _typing.Self, singleton: _typing.Any) -> None:
        """
        Perform the absorb operation under explicit file-format and conversion rules.

        Example:
            Exercise Region.absorb through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param singleton: Value supplied for singleton under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        def most_suitable_column(local_elem: _typing.Any) -> _typing.Any:
            """
            Perform the most suitable column operation under explicit file-format and conversion rules.

            Example:
                Exercise Region.absorb.most suitable column through a consuming regression::

                    python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


            :param local_elem: Value supplied for local elem under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            mc, mw = None, 0
            for col in self.columns:
                i = Interval(col.left, col.right)
                e = Interval(local_elem.left, local_elem.right)
                w = i.intersection(e).width
                if w > mw:
                    mc, mw = col, w
            if mc is None:
                self.log.warn("No suitable column for singleton", local_elem.to_html())
                mc = self.columns[0]
            return mc

        for c in singleton.columns:
            for elem in c:
                c = most_suitable_column(elem)
                if self.opts.verbose > 3:
                    idx = self.columns.index(c)
                    self.log.debug("Absorbing singleton %s into column" % elem.to_html(), idx)
                c.add(elem)

    def collect_stats(self: _typing.Self) -> None:
        """
        Perform the collect stats operation under explicit file-format and conversion rules.

        Example:
            Exercise Region.collect stats through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for column in self.columns:
            column.collect_stats()
        self.average_line_separation = sum([x.average_line_separation for x in self.columns]) / float(len(self.columns))

    def __iter__(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        """
        Perform the iter operation under explicit file-format and conversion rules.

        Example:
            Exercise Region.  iter   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: An iterator yielding the normalized values described above.
        """
        for x in self.columns:
            yield x

    def absorb_regions(self: _typing.Self, regions: _typing.Any, at: _typing.Any) -> None:
        """
        Perform the absorb regions operation under explicit file-format and conversion rules.

        Example:
            Exercise Region.absorb regions through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param regions: Value supplied for regions under the utility contract.
        :param at: Value supplied for at under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for region in regions:
            self.absorb_region(region, at)

    def absorb_region(self: _typing.Self, region: _typing.Any, at: _typing.Any) -> None:
        """
        Perform the absorb region operation under explicit file-format and conversion rules.

        Example:
            Exercise Region.absorb region through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param region: Value supplied for region under the utility contract.
        :param at: Value supplied for at under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if len(region.columns) <= len(self.columns):
            for i in range(len(region.columns)):
                src, dest = region.columns[i], self.columns[i]
                if at != "bottom":
                    src = reversed(list(iter(src)))
                for elem in src:
                    func = dest.add if at == "bottom" else dest.prepend
                    func(elem)

        else:
            col_map = {}
            for i, col in enumerate(region.columns):
                max_overlap, max_overlap_index = 0, 0
                for j, dcol in enumerate(self.columns):
                    sint = Interval(col.left, col.right)
                    dint = Interval(dcol.left, dcol.right)
                    width = sint.intersection(dint).width
                    if width > max_overlap:
                        max_overlap = width
                        max_overlap_index = j
                col_map[i] = max_overlap_index
            lines = max(map(len, region.columns))
            if at == "bottom":
                lines = range(lines)
            else:
                lines = range(lines - 1, -1, -1)
            for i in lines:
                for j, src in enumerate(region.columns):
                    dest = self.columns[col_map[j]]
                    if i < len(src):
                        func = dest.add if at == "bottom" else dest.prepend
                        func(src.elements[i])

    def dump(self: _typing.Self, f: _typing.Any) -> None:
        """
        Perform the dump operation under explicit file-format and conversion rules.

        Example:
            Exercise Region.dump through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param f: Value supplied for f under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        f.write("############################################################\n")
        f.write("########## Region (%d columns) ###############\n" % len(self.columns))
        f.write("############################################################\n\n")
        for i, col in enumerate(self.columns):
            col.dump(f, i)

    def linearize(self: _typing.Self) -> None:
        """
        Perform the linearize operation under explicit file-format and conversion rules.

        Example:
            Exercise Region.linearize through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.elements = []
        for x in self.columns:
            self.elements.extend(x)
        self.boxes = [Box()]
        for i, elem in enumerate(self.elements):
            if isinstance(elem, Image):
                self.boxes.append(ImageBox(elem))
                img = Interval(elem.left, elem.right)
                for j in range(i + 1, len(self.elements)):
                    t = self.elements[j]
                    if not isinstance(t, Text):
                        break
                    ti = Interval(t.left, t.right)
                    if not ti.centered_in(img):
                        break
                    self.boxes[-1].append(t)
                self.boxes.append(Box())
            else:
                is_indented = False
                if i + 1 < len(self.elements):
                    indent_diff = elem.indent_fraction - self.elements[i + 1].indent_fraction
                    if indent_diff > 0.05:
                        is_indented = True
                if elem.top_gap_ratio > 1.2 or is_indented:
                    self.boxes.append(Box())
                self.boxes[-1].append(elem)


class Page(object):

    # Fraction of a character width that two strings have to be apart,
    # for them to be considered part of the same text fragment
    """
    Provide the page contract for validated ebook processing.

    Example:
        Exercise Page through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    COALESCE_FACTOR = 0.5

    # Fraction of text height that two strings' bottoms can differ by
    # for them to be considered to be part of the same text fragment
    LINE_FACTOR = 0.4

    # Multiplies the average line height when determining row height
    # of a particular element to detect columns.
    YFUZZ = 1.5

    def __init__(self: _typing.Self, page: _typing.Any, font_map: _typing.Any, opts: _typing.Any, log: _typing.Any, idc: _typing.Any) -> None:
        """
        Initialize and validate the page state.

        Example:
            Exercise Page.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param page: Value supplied for page under the utility contract.
        :param font_map: Value supplied for font map under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :param idc: Value supplied for idc under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.opts, self.log = opts, log
        self.font_map = font_map
        self.number = int(page.get("number"))
        self.width, self.height = map(float, map(page.get, ("width", "height")))
        self.id = "page%d" % self.number

        self.texts = []
        self.left_margin, self.right_margin = self.width, 0

        for text in page.xpath("descendant::text"):
            self.texts.append(Text(text, self.font_map, self.opts, self.log, idc))
            text = self.texts[-1]
            self.left_margin = min(text.left, self.left_margin)
            self.right_margin = max(text.right, self.right_margin)

        self.textwidth = self.right_margin - self.left_margin

        self.font_size_stats = {}
        self.average_text_height = 0
        for t in self.texts:
            if t.font_size not in self.font_size_stats:
                self.font_size_stats[t.font_size] = 0
            self.font_size_stats[t.font_size] += len(t.text_as_string)
            self.average_text_height += t.height
        if len(self.texts):
            self.average_text_height /= len(self.texts)

        self.font_size_stats = FontSizeStats(self.font_size_stats)

        self.coalesce_fragments()

        self.elements = list(self.texts)
        for img in page.xpath("descendant::img"):
            self.elements.append(Image(img, self.opts, self.log, idc))
        self.elements.sort(key=cmp_to_key(lambda ele_1, ele_2: six_cmp(ele_1.top, ele_2.top)))

    def coalesce_fragments(self: _typing.Self) -> None:
        """
        Perform the coalesce fragments operation under explicit file-format and conversion rules.

        Example:
            Exercise Page.coalesce fragments through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        def find_match(frag: _typing.Any) -> _typing.Any:
            """
            Find match under the format's safety and compatibility rules.

            Example:
                Exercise Page.coalesce fragments.find match through a consuming regression::

                    python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


            :param frag: Value supplied for frag under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            for t in self.texts:
                hdelta = t.left - frag.right
                hoverlap = self.COALESCE_FACTOR * frag.average_character_width
                if (
                    t is not frag
                    and -hoverlap < hdelta < hoverlap
                    and abs(t.bottom - frag.bottom) < self.LINE_FACTOR * frag.height
                ):
                    return t

        match_found = True
        while match_found:
            match_found, match = False, None
            for frag in self.texts:
                match = find_match(frag)
                if match is not None:
                    match_found = True
                    frag.coalesce(match, self.number)
                    break
            if match is not None:
                self.texts.remove(match)

    def first_pass(self: _typing.Self) -> None:
        """
        Sort page into regions and columns

        Example:
            Exercise Page.first pass through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.regions = []
        if not self.elements:
            return
        for i, x in enumerate(self.elements):
            x.idx = i
        current_region = Region(self.opts, self.log)
        processed = set([])
        for x in self.elements:
            if x in processed:
                continue
            elems = set(self.find_elements_in_row_of(x))
            columns = self.sort_into_columns(x, elems)
            processed.update(elems)
            if not current_region.contains(columns):
                self.regions.append(current_region)
                current_region = Region(self.opts, self.log)
            current_region.add(columns)
        if not current_region.is_empty:
            self.regions.append(current_region)

        if self.opts.verbose > 2:
            self.debug_dir = "page-%d" % self.number
            os.mkdir(self.debug_dir)
            self.dump_regions("pre-coalesce")

        self.coalesce_regions()
        self.dump_regions("post-coalesce")

    def dump_regions(self: _typing.Self, fname: _typing.Any) -> None:
        """
        Perform the dump regions operation under explicit file-format and conversion rules.

        Example:
            Exercise Page.dump regions through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param fname: Value supplied for fname under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        fname = "regions-" + fname + ".txt"
        with open(os.path.join(self.debug_dir, fname), "wb") as f:
            f.write("Page #%d\n\n" % self.number)
            for region in self.regions:
                region.dump(f)

    def coalesce_regions(self: _typing.Self) -> None:
        # find contiguous sets of small regions
        # absorb into a neighboring region (prefer the one with number of cols
        # closer to the avg number of cols in the set, if equal use larger
        # region)
        """
        Perform the coalesce regions operation under explicit file-format and conversion rules.

        Example:
            Exercise Page.coalesce regions through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        found = True
        absorbed = set([])
        processed = set([])
        while found:
            found = False
            for i, region in enumerate(self.regions):

                if region in absorbed:
                    continue

                if region.is_small and region not in processed:
                    found = True
                    processed.add(region)
                    regions = [region]
                    end = i + 1
                    for j in range(i + 1, len(self.regions)):
                        end = j
                        if self.regions[j].is_small:
                            regions.append(self.regions[j])
                        else:
                            break
                    prev_region = None if i == 0 else i - 1
                    next_region = end if end < len(self.regions) and self.regions[end] not in regions else None
                    absorb_at = "bottom"
                    if prev_region is None and next_region is not None:
                        absorb_into = next_region
                        absorb_at = "top"
                    elif next_region is None and prev_region is not None:
                        absorb_into = prev_region
                    elif prev_region is None and next_region is None:
                        if len(regions) > 1:
                            absorb_into = i
                            regions = regions[1:]
                        else:
                            absorb_into = None
                    else:
                        absorb_into = prev_region
                        if self.regions[next_region].line_count >= self.regions[prev_region].line_count:
                            avg_column_count = sum([len(r.columns) for r in regions]) / float(len(regions))
                            if self.regions[next_region].line_count > self.regions[prev_region].line_count or abs(
                                avg_column_count - len(self.regions[prev_region].columns)
                            ) > abs(avg_column_count - len(self.regions[next_region].columns)):
                                absorb_into = next_region
                                absorb_at = "top"

                    if absorb_into is not None:
                        self.regions[absorb_into].absorb_regions(regions, absorb_at)
                        absorbed.update(regions)

        for region in absorbed:
            self.regions.remove(region)

    def sort_into_columns(self: _typing.Self, elem: _typing.Any, neighbors: _typing.Any) -> _typing.Any:
        """
        Perform the sort into columns operation under explicit file-format and conversion rules.

        Example:
            Exercise Page.sort into columns through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param elem: Value supplied for elem under the utility contract.
        :param neighbors: Value supplied for neighbors under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        neighbors.add(elem)
        neighbors = sorted(neighbors, key=cmp_to_key(lambda col_1, col_2: six_cmp(col_1.left, col_2.left)))
        if self.opts.verbose > 3:
            self.log.debug("Neighbors:", [x.to_html() for x in neighbors])
        columns = [Column()]
        columns[0].add(elem)
        for x in neighbors:
            added = False
            for c in columns:
                if c.contains(x):
                    c.add(x)
                    added = True
                    break
            if not added:
                columns.append(Column())
                columns[-1].add(x)
                columns.sort(key=cmp_to_key(lambda col_1, col_2: six_cmp(col_1.left, col_2.left)))
        return columns

    def find_elements_in_row_of(self: _typing.Self, x: _typing.Any) -> _typing.Iterator[_typing.Any]:
        """
        Find elements in row of under the format's safety and compatibility rules.

        Example:
            Exercise Page.find elements in row of through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param x: Value supplied for x under the utility contract.
        :return: An iterator yielding the normalized values described above.
        """
        interval = Interval(x.top, x.top + self.YFUZZ * self.average_text_height)
        h_interval = Interval(x.left, x.right)
        for y in self.elements[x.idx : x.idx + 15]:
            if y is not x:
                y_interval = Interval(y.top, y.bottom)
                x_interval = Interval(y.left, y.right)
                if (
                    interval.intersection(y_interval).width > 0.5 * self.average_text_height
                    and x_interval.intersection(h_interval).width <= 0
                ):
                    yield y

    def second_pass(self: _typing.Self) -> None:
        """
        Locate paragraph boundaries in each column

        Example:
            Exercise Page.second pass through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for region in self.regions:
            region.collect_stats()
            region.linearize()


class PDFDocument(object):
    """
    Provide the pdfdocument contract for validated ebook processing.

    Example:
        Exercise PDFDocument through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, xml: _typing.Any, opts: _typing.Any, log: _typing.Any) -> None:
        """
        Initialize and validate the pdfdocument state.

        Example:
            Exercise PDFDocument.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param xml: Value supplied for xml under the utility contract.
        :param opts: Value supplied for opts under the utility contract.
        :param log: Value supplied for log under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.opts, self.log = opts, log
        parser = etree.XMLParser(recover=True)
        self.root = etree.fromstring(xml, parser=parser)
        idc = iter(memory_range(sys.maxsize))

        self.fonts = []
        self.font_map = {}

        for spec in self.root.xpath("//font"):
            self.fonts.append(Font(spec))
            self.font_map[self.fonts[-1].id] = self.fonts[-1]

        self.pages = []
        self.page_map = {}

        for page in self.root.xpath("//page"):
            page = Page(page, self.font_map, opts, log, idc)
            self.page_map[page.id] = page
            self.pages.append(page)

        self.collect_font_statistics()

        for page in self.pages:
            page.document_font_stats = self.font_size_stats
            page.first_pass()
            page.second_pass()

        self.linearize()
        self.render()

    def collect_font_statistics(self: _typing.Self) -> None:
        """
        Perform the collect font statistics operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFDocument.collect font statistics through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.font_size_stats = {}
        for p in self.pages:
            for sz in p.font_size_stats:
                chars = p.font_size_stats[sz]
                if sz not in self.font_size_stats:
                    self.font_size_stats[sz] = 0
                self.font_size_stats[sz] += chars

        self.font_size_stats = FontSizeStats(self.font_size_stats)

    def linearize(self: _typing.Self) -> None:
        """
        Perform the linearize operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFDocument.linearize through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.elements = []
        last_region = last_block = None
        for page in self.pages:
            page_number_inserted = False
            for region in page.regions:
                merge_first_block = (
                    last_region is not None
                    and len(last_region.columns) == len(region.columns)
                    and not hasattr(last_block, "img")
                )
                for i, block in enumerate(region.boxes):
                    if merge_first_block:
                        merge_first_block = False
                        if not page_number_inserted:
                            last_block.append(page.number)
                            page_number_inserted = True
                        for elem in block:
                            last_block.append(elem)
                    else:
                        if not page_number_inserted:
                            block.insert(0, page.number)
                            page_number_inserted = True
                        self.elements.append(block)
                    last_block = block
                last_region = region

    def render(self: _typing.Self) -> None:
        """
        Perform the render operation under explicit file-format and conversion rules.

        Example:
            Exercise PDFDocument.render through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        html = [
            '<?xml version="1.0" encoding="UTF-8"?>',
            '<html xmlns="http://www.w3.org/1999/xhtml">',
            "<head>",
            "<title>PDF Reflow conversion</title>",
            "</head>",
            "<body>",
            "<div>",
        ]
        for elem in self.elements:
            html.extend(elem.to_html())
        html += ["</body>", "</html>"]
        raw = ("\n".join(html)).replace("</strong><strong>", "")
        with open("index.html", "wb") as f:
            f.write(raw.encode("utf-8"))
