#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

"""
Translate drawing operations into PDF graphics commands.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise graphics through a consuming regression::

        python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

from math import sqrt
from collections import namedtuple

try:
    from PyQt5.Qt import QBrush, QPen, Qt, QPointF, QTransform, QPaintEngine, QImage
    _HAS_QT = True
except Exception:
    QBrush = QPen = Qt = QPointF = QTransform = QPaintEngine = QImage = None
    _HAS_QT = False

from LiuXin_alpha.file_formats.pdf.render.common import (
    Name,
    Array,
    fmtnum,
    Stream,
    Dictionary,
)
from LiuXin_alpha.file_formats.pdf.render.serialize import Path
from LiuXin_alpha.file_formats.pdf.render.gradients import LinearGradientPattern

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def _require_qt() -> None:
    """
    Perform the require qt operation under explicit file-format and conversion rules.

    Example:
        Exercise  require qt through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if not _HAS_QT:
        raise RuntimeError("PyQt5 is required for PDF graphics rendering.")


def convert_path(path: _typing.Any) -> _typing.Any:  # {{{
    """
    Convert path under the format's safety and compatibility rules.

    Example:
        Exercise convert path through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    p = Path()
    i = 0
    while i < path.elementCount():
        elem = path.elementAt(i)
        em = (elem.x, elem.y)
        i += 1
        if elem.isMoveTo():
            p.move_to(*em)
        elif elem.isLineTo():
            p.line_to(*em)
        elif elem.isCurveTo():
            added = False
            if path.elementCount() > i + 1:
                c1, c2 = path.elementAt(i), path.elementAt(i + 1)
                if c1.type == path.CurveToDataElement and c2.type == path.CurveToDataElement:
                    i += 2
                    p.curve_to(em[0], em[1], c1.x, c1.y, c2.x, c2.y)
                    added = True
            if not added:
                raise ValueError("Invalid curve to operation")
    return p


# }}}

Brush = namedtuple("Brush", "origin brush color")


class TilingPattern(Stream):
    """
    Provide the tilingpattern contract for validated ebook processing.

    Example:
        Exercise TilingPattern through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, cache_key: _typing.Any, matrix: _typing.Any, w: int = 8, h: int = 8, paint_type: int = 2, compress: bool = False) -> None:
        """
        Initialize and validate the tilingpattern state.

        Example:
            Exercise TilingPattern.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param cache_key: Value supplied for cache key under the utility contract.
        :param matrix: Value supplied for matrix under the utility contract.
        :param w: Value supplied for w under the utility contract.
        :param h: Value supplied for h under the utility contract.
        :param paint_type: Value supplied for paint type under the utility contract.
        :param compress: Value supplied for compress under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        Stream.__init__(self, compress=compress)
        self.paint_type = paint_type
        self.w, self.h = w, h
        self.matrix = (
            matrix.m11(),
            matrix.m12(),
            matrix.m21(),
            matrix.m22(),
            matrix.dx(),
            matrix.dy(),
        )
        self.resources = Dictionary()
        self.cache_key = (self.__class__.__name__, cache_key, self.matrix)

    def add_extra_keys(self: _typing.Self, d: _typing.Any) -> None:
        """
        Add supported metadata keys to the PDF information dictionary.

        Example:
            Exercise TilingPattern.add extra keys through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param d: Value supplied for d under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        d["Type"] = Name("Pattern")
        d["PatternType"] = 1
        d["PaintType"] = self.paint_type
        d["TilingType"] = 1
        d["BBox"] = Array([0, 0, self.w, self.h])
        d["XStep"] = self.w
        d["YStep"] = self.h
        d["Matrix"] = Array(self.matrix)
        d["Resources"] = self.resources


class QtPattern(TilingPattern):

    """
    Provide the qtpattern contract for validated ebook processing.

    Example:
        Exercise QtPattern through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    qt_patterns = (  # {{{
        "0 J\n" "6 w\n" "[] 0 d\n" "4 0 m\n" "4 8 l\n" "0 4 m\n" "8 4 l\n" "S\n",  # Dense1Pattern
        "0 J\n"
        "2 w\n"
        "[6 2] 1 d\n"
        "0 0 m\n"
        "0 8 l\n"
        "8 0 m\n"
        "8 8 l\n"
        "S\n"
        "[] 0 d\n"
        "2 0 m\n"
        "2 8 l\n"
        "6 0 m\n"
        "6 8 l\n"
        "S\n"
        "[6 2] -3 d\n"
        "4 0 m\n"
        "4 8 l\n"
        "S\n",  # Dense2Pattern
        "0 J\n"
        "2 w\n"
        "[6 2] 1 d\n"
        "0 0 m\n"
        "0 8 l\n"
        "8 0 m\n"
        "8 8 l\n"
        "S\n"
        "[2 2] -1 d\n"
        "2 0 m\n"
        "2 8 l\n"
        "6 0 m\n"
        "6 8 l\n"
        "S\n"
        "[6 2] -3 d\n"
        "4 0 m\n"
        "4 8 l\n"
        "S\n",  # Dense3Pattern
        "0 J\n"
        "2 w\n"
        "[2 2] 1 d\n"
        "0 0 m\n"
        "0 8 l\n"
        "8 0 m\n"
        "8 8 l\n"
        "S\n"
        "[2 2] -1 d\n"
        "2 0 m\n"
        "2 8 l\n"
        "6 0 m\n"
        "6 8 l\n"
        "S\n"
        "[2 2] 1 d\n"
        "4 0 m\n"
        "4 8 l\n"
        "S\n",  # Dense4Pattern
        "0 J\n"
        "2 w\n"
        "[2 6] -1 d\n"
        "0 0 m\n"
        "0 8 l\n"
        "8 0 m\n"
        "8 8 l\n"
        "S\n"
        "[2 2] 1 d\n"
        "2 0 m\n"
        "2 8 l\n"
        "6 0 m\n"
        "6 8 l\n"
        "S\n"
        "[2 6] 3 d\n"
        "4 0 m\n"
        "4 8 l\n"
        "S\n",  # Dense5Pattern
        "0 J\n"
        "2 w\n"
        "[2 6] -1 d\n"
        "0 0 m\n"
        "0 8 l\n"
        "8 0 m\n"
        "8 8 l\n"
        "S\n"
        "[2 6] 3 d\n"
        "4 0 m\n"
        "4 8 l\n"
        "S\n",  # Dense6Pattern
        "0 J\n" "2 w\n" "[2 6] -1 d\n" "0 0 m\n" "0 8 l\n" "8 0 m\n" "8 8 l\n" "S\n",  # Dense7Pattern
        "1 w\n" "0 4 m\n" "8 4 l\n" "S\n",  # HorPattern
        "1 w\n" "4 0 m\n" "4 8 l\n" "S\n",  # VerPattern
        "1 w\n" "4 0 m\n" "4 8 l\n" "0 4 m\n" "8 4 l\n" "S\n",  # CrossPattern
        "1 w\n" "-1 5 m\n" "5 -1 l\n" "3 9 m\n" "9 3 l\n" "S\n",  # BDiagPattern
        "1 w\n" "-1 3 m\n" "5 9 l\n" "3 -1 m\n" "9 5 l\n" "S\n",  # FDiagPattern
        "1 w\n"
        "-1 3 m\n"
        "5 9 l\n"
        "3 -1 m\n"
        "9 5 l\n"
        "-1 5 m\n"
        "5 -1 l\n"
        "3 9 m\n"
        "9 3 l\n"
        "S\n",  # DiagCrossPattern
    )  # }}}

    def __init__(self: _typing.Self, pattern_num: _typing.Any, matrix: _typing.Any) -> None:
        """
        Initialize and validate the qtpattern state.

        Example:
            Exercise QtPattern.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param pattern_num: Value supplied for pattern num under the utility contract.
        :param matrix: Value supplied for matrix under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        super(QtPattern, self).__init__(pattern_num, matrix)
        self.write(self.qt_patterns[pattern_num - 2])


class TexturePattern(TilingPattern):
    """
    Provide the texturepattern contract for validated ebook processing.

    Example:
        Exercise TexturePattern through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, pixmap: _typing.Any, matrix: _typing.Any, pdf: _typing.Any, clone: _typing.Any = None) -> None:
        """
        Initialize and validate the texturepattern state.

        Example:
            Exercise TexturePattern.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param pixmap: Value supplied for pixmap under the utility contract.
        :param matrix: Value supplied for matrix under the utility contract.
        :param pdf: Value supplied for pdf under the utility contract.
        :param clone: Value supplied for clone under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        _require_qt()
        if clone is None:
            image = pixmap.toImage()
            cache_key = pixmap.cacheKey()
            imgref = pdf.add_image(image, cache_key)
            paint_type = 2 if image.format() in {QImage.Format_MonoLSB, QImage.Format_Mono} else 1
            super(TexturePattern, self).__init__(
                cache_key,
                matrix,
                w=image.width(),
                h=image.height(),
                paint_type=paint_type,
            )
            m = (self.w, 0, 0, -self.h, 0, self.h)
            self.resources["XObject"] = Dictionary({"Texture": imgref})
            self.write_line("%s cm /Texture Do" % (" ".join(map(fmtnum, m))))
        else:
            super(TexturePattern, self).__init__(
                clone.cache_key[1],
                matrix,
                w=clone.w,
                h=clone.h,
                paint_type=clone.paint_type,
            )
            self.resources["XObject"] = Dictionary(clone.resources["XObject"])
            self.write(clone.getvalue())


class GraphicsState(object):

    """
    Provide the graphicsstate contract for validated ebook processing.

    Example:
        Exercise GraphicsState through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    FIELDS = (
        "fill",
        "stroke",
        "opacity",
        "transform",
        "brush_origin",
        "clip_updated",
        "do_fill",
        "do_stroke",
    )

    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the graphicsstate state.

        Example:
            Exercise GraphicsState.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        _require_qt()
        self.fill = QBrush(Qt.white)
        self.stroke = QPen()
        self.opacity = 1.0
        self.transform = QTransform()
        self.brush_origin = QPointF()
        self.clip_updated = False
        self.do_fill = False
        self.do_stroke = True
        self.qt_pattern_cache = {}

    def __eq__(self: _typing.Self, other: _typing.Any) -> bool:
        """
        Perform the eq operation under explicit file-format and conversion rules.

        Example:
            Exercise GraphicsState.  eq   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param other: Value supplied for other under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        for x in self.FIELDS:
            if getattr(other, x) != getattr(self, x):
                return False
        return True

    def copy(self: _typing.Self) -> _typing.Any:
        """
        Perform the copy operation under explicit file-format and conversion rules.

        Example:
            Exercise GraphicsState.copy through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = GraphicsState()
        ans.fill = QBrush(self.fill)
        ans.stroke = QPen(self.stroke)
        ans.opacity = self.opacity
        ans.transform = self.transform * QTransform()
        ans.brush_origin = QPointF(self.brush_origin)
        ans.clip_updated = self.clip_updated
        ans.do_fill, ans.do_stroke = self.do_fill, self.do_stroke
        return ans


class Graphics(object):
    """
    Provide the graphics contract for validated ebook processing.

    Example:
        Exercise Graphics through a consuming regression::

            python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py
    """
    def __init__(self: _typing.Self, page_width_px: _typing.Any, page_height_px: _typing.Any) -> None:
        """
        Initialize and validate the graphics state.

        Example:
            Exercise Graphics.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param page_width_px: Value supplied for page width px under the utility contract.
        :param page_height_px: Value supplied for page height px under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        _require_qt()
        self.base_state = GraphicsState()
        self.current_state = GraphicsState()
        self.pending_state = None
        self.page_width_px, self.page_height_px = (page_width_px, page_height_px)

    def begin(self: _typing.Self, pdf: _typing.Any) -> None:
        """
        Perform the begin operation under explicit file-format and conversion rules.

        Example:
            Exercise Graphics.begin through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param pdf: Value supplied for pdf under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.pdf = pdf

    def update_state(self: _typing.Self, state: _typing.Any, painter: _typing.Any) -> None:
        """
        Perform the update state operation under explicit file-format and conversion rules.

        Example:
            Exercise Graphics.update state through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param state: Value supplied for state under the utility contract.
        :param painter: Value supplied for painter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        flags = state.state()
        if self.pending_state is None:
            self.pending_state = self.current_state.copy()

        s = self.pending_state

        if flags & QPaintEngine.DirtyTransform:
            s.transform = state.transform()

        if flags & QPaintEngine.DirtyBrushOrigin:
            s.brush_origin = state.brushOrigin()

        if flags & QPaintEngine.DirtyBrush:
            s.fill = state.brush()

        if flags & QPaintEngine.DirtyPen:
            s.stroke = state.pen()

        if flags & QPaintEngine.DirtyOpacity:
            s.opacity = state.opacity()

        if flags & QPaintEngine.DirtyClipPath or flags & QPaintEngine.DirtyClipRegion:
            s.clip_updated = True

    def reset(self: _typing.Self) -> None:
        """
        Perform the reset operation under explicit file-format and conversion rules.

        Example:
            Exercise Graphics.reset through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.current_state = GraphicsState()
        self.pending_state = None

    def __call__(self: _typing.Self, pdf_system: _typing.Any, painter: _typing.Any) -> None:
        # Apply the currently pending state to the PDF
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise Graphics.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param pdf_system: Value supplied for pdf system under the utility contract.
        :param painter: Value supplied for painter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if self.pending_state is None:
            return

        pdf_state = self.current_state
        ps = self.pending_state
        pdf = self.pdf

        if ps.transform != pdf_state.transform or ps.clip_updated:
            pdf.restore_stack()
            pdf.save_stack()
            pdf_state = self.base_state

        if pdf_state.transform != ps.transform:
            pdf.transform(ps.transform)

        if pdf_state.opacity != ps.opacity or pdf_state.stroke != ps.stroke:
            self.apply_stroke(ps, pdf_system, painter)

        if pdf_state.opacity != ps.opacity or pdf_state.fill != ps.fill or pdf_state.brush_origin != ps.brush_origin:
            self.apply_fill(ps, pdf_system, painter)

        if ps.clip_updated:
            ps.clip_updated = False
            path = painter.clipPath()
            if not path.isEmpty():
                p = convert_path(path)
                fill_rule = {Qt.OddEvenFill: "evenodd", Qt.WindingFill: "winding"}[path.fillRule()]
                pdf.add_clip(p, fill_rule=fill_rule)

        self.current_state = self.pending_state
        self.pending_state = None

    def convert_brush(self: _typing.Self, brush: _typing.Any, brush_origin: _typing.Any, global_opacity: _typing.Any, pdf_system: _typing.Any, qt_system: _typing.Any) -> tuple[_typing.Any, ...]:
        # Convert a QBrush to PDF operators
        """
        Convert brush under the format's safety and compatibility rules.

        Example:
            Exercise Graphics.convert brush through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param brush: Value supplied for brush under the utility contract.
        :param brush_origin: Value supplied for brush origin under the utility contract.
        :param global_opacity: Value supplied for global opacity under the utility contract.
        :param pdf_system: Value supplied for pdf system under the utility contract.
        :param qt_system: Value supplied for qt system under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        style = brush.style()
        pdf = self.pdf

        pattern = color = pat = None
        opacity = global_opacity
        do_fill = True

        matrix = QTransform.fromTranslate(brush_origin.x(), brush_origin.y()) * pdf_system * qt_system.inverted()[0]
        vals = list(brush.color().getRgbF())
        self.brushobj = None

        if style <= Qt.DiagCrossPattern:
            opacity *= vals[-1]
            color = vals[:3]

            if style > Qt.SolidPattern:
                pat = QtPattern(style, matrix)

        elif style == Qt.TexturePattern:
            pat = TexturePattern(brush.texture(), matrix, pdf)
            if pat.paint_type == 2:
                opacity *= vals[-1]
                color = vals[:3]

        elif style == Qt.LinearGradientPattern:
            pat = LinearGradientPattern(brush, matrix, pdf, self.page_width_px, self.page_height_px)
            opacity *= pat.const_opacity
        # TODO: Add support for radial/conical gradient fills

        if opacity < 1e-4 or style == Qt.NoBrush:
            do_fill = False
        self.brushobj = Brush(brush_origin, pat, color)

        if pat is not None:
            pattern = pdf.add_pattern(pat)
        return color, opacity, pattern, do_fill

    def apply_stroke(self: _typing.Self, state: _typing.Any, pdf_system: _typing.Any, painter: _typing.Any) -> None:
        # TODO: Support miter limit by using QPainterPathStroker
        """
        Perform the apply stroke operation under explicit file-format and conversion rules.

        Example:
            Exercise Graphics.apply stroke through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param state: Value supplied for state under the utility contract.
        :param pdf_system: Value supplied for pdf system under the utility contract.
        :param painter: Value supplied for painter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pen = state.stroke
        self.pending_state.do_stroke = True
        pdf = self.pdf

        # Width
        w = pen.widthF()
        if pen.isCosmetic():
            t = painter.transform()
            try:
                w /= sqrt(t.m11() ** 2 + t.m22() ** 2)
            except ZeroDivisionError:
                pass
        pdf.serialize(w)
        pdf.current_page.write(" w ")

        # Line cap
        cap = {Qt.FlatCap: 0, Qt.RoundCap: 1, Qt.SquareCap: 2}.get(pen.capStyle(), 0)
        pdf.current_page.write("%d J " % cap)

        # Line join
        join = {Qt.MiterJoin: 0, Qt.RoundJoin: 1, Qt.BevelJoin: 2}.get(pen.joinStyle(), 0)
        pdf.current_page.write("%d j " % join)

        # Dash pattern
        ps = {
            Qt.DashLine: [3],
            Qt.DotLine: [1, 2],
            Qt.DashDotLine: [3, 2, 1, 2],
            Qt.DashDotDotLine: [3, 2, 1, 2, 1, 2],
        }.get(pen.style(), [])
        if ps:
            pdf.serialize(Array(ps))
            pdf.current_page.write(" 0 d ")

        # Stroke fill
        color, opacity, pattern, self.pending_state.do_stroke = self.convert_brush(
            pen.brush(),
            state.brush_origin,
            state.opacity,
            pdf_system,
            painter.transform(),
        )
        self.pdf.apply_stroke(color, pattern, opacity)
        if pen.style() == Qt.NoPen:
            self.pending_state.do_stroke = False

    def apply_fill(self: _typing.Self, state: _typing.Any, pdf_system: _typing.Any, painter: _typing.Any) -> None:
        """
        Perform the apply fill operation under explicit file-format and conversion rules.

        Example:
            Exercise Graphics.apply fill through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param state: Value supplied for state under the utility contract.
        :param pdf_system: Value supplied for pdf system under the utility contract.
        :param painter: Value supplied for painter under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.pending_state.do_fill = True
        color, opacity, pattern, self.pending_state.do_fill = self.convert_brush(
            state.fill,
            state.brush_origin,
            state.opacity,
            pdf_system,
            painter.transform(),
        )
        self.pdf.apply_fill(color, pattern, opacity)
        self.last_fill = self.brushobj

    def __enter__(self: _typing.Self) -> None:
        """
        Implement the conversion resource's enter lifecycle operation.

        Example:
            Exercise Graphics.  enter   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.pdf.save_stack()

    def __exit__(self: _typing.Self, *args: _typing.Any) -> None:
        """
        Implement the conversion resource's exit lifecycle operation.

        Example:
            Exercise Graphics.  exit   through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.pdf.restore_stack()

    def resolve_fill(self: _typing.Self, rect: _typing.Any, pdf_system: _typing.Any, qt_system: _typing.Any) -> None:
        """
        Qt's paint system does not update brushOrigin when using TexturePatterns and it also uses TexturePatterns to emulate gradients, leading to brokenness. So this method allows the paint engine to update the brush origin before painting an object. While not perfect, this is better than nothing. The problem is that if the rect being filled has a border, then QtWebKit generates an image of the rect size - border but fills the full rect, and there's no way for the paint engine to know that and adjust the brush origin.

        Example:
            Exercise Graphics.resolve fill through a consuming regression::

                python -m pytest -q tests/file_formats/pdf/test_pdf_modernized.py


        :param rect: Value supplied for rect under the utility contract.
        :param pdf_system: Value supplied for pdf system under the utility contract.
        :param qt_system: Value supplied for qt system under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if not hasattr(self, "last_fill") or not self.current_state.do_fill:
            return

        if isinstance(self.last_fill.brush, TexturePattern):
            tl = rect.topLeft()
            if tl == self.last_fill.origin:
                return

            matrix = QTransform.fromTranslate(tl.x(), tl.y()) * pdf_system * qt_system.inverted()[0]

            pat = TexturePattern(None, matrix, self.pdf, clone=self.last_fill.brush)
            pattern = self.pdf.add_pattern(pat)
            self.pdf.apply_fill(self.last_fill.color, pattern)
