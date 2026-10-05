#!/usr/bin/env  python

"""
Render complex table content as an image for LRF output.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise table as image through a consuming regression::

        python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
"""
from __future__ import annotations

import typing as _typing

import atexit
import os
import shutil
import tempfile

try:
    from PyQt5.Qt import (
        QUrl,
        QApplication,
        QSize,
        QEventLoop,
        QPainter,
        QImage,
        QObject,
        Qt,
    )
    from PyQt5.QtWebKitWidgets import QWebPage

    _QT_IMPORT_ERROR = None
except Exception as err:
    QUrl = QApplication = QSize = QEventLoop = QPainter = QImage = Qt = QWebPage = None
    QObject = object
    _QT_IMPORT_ERROR = err

# Py2.Py3 compatability layer
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal kovid@kovidgoyal.net"
__docformat__ = "restructuredtext en"


class HTMLTableRenderer(QObject):
    """
    Provide the htmltablerenderer contract for validated ebook processing.

    Example:
        Exercise HTMLTableRenderer through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py
    """
    def __init__(self: _typing.Self, html: _typing.Any, base_dir: _typing.Any, width: _typing.Any, height: _typing.Any, dpi: _typing.Any, factor: _typing.Any) -> None:
        """
        `width, height`: page width and height in pixels `base_dir`: The directory in which the HTML file that contains the table resides

        Example:
            Exercise HTMLTableRenderer.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param html: Value supplied for html under the utility contract.
        :param base_dir: Value supplied for base dir under the utility contract.
        :param width: Value supplied for width under the utility contract.
        :param height: Value supplied for height under the utility contract.
        :param dpi: Value supplied for dpi under the utility contract.
        :param factor: Value supplied for factor under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        if _QT_IMPORT_ERROR is not None:
            raise RuntimeError("PyQt5 with QtWebKit is required to render HTML tables as images") from _QT_IMPORT_ERROR
        QObject.__init__(self)

        self.app = None
        self.width, self.height, self.dpi = width, height, dpi
        self.base_dir = base_dir
        self.images = []
        self.tdir = tempfile.mkdtemp(prefix="calibre_render_table")
        self.loop = QEventLoop()
        self.page = QWebPage()
        self.page.loadFinished.connect(self.render_html)
        self.page.mainFrame().setTextSizeMultiplier(factor)
        self.page.mainFrame().setHtml(html, QUrl("file:" + os.path.abspath(self.base_dir)))

    def render_html(self: _typing.Self, ok: _typing.Any) -> None:
        """
        Perform the render html operation under explicit file-format and conversion rules.

        Example:
            Exercise HTMLTableRenderer.render html through a consuming regression::

                python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


        :param ok: Value supplied for ok under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        try:
            if not ok:
                return
            cwidth, cheight = (
                self.page.mainFrame().contentsSize().width(),
                self.page.mainFrame().contentsSize().height(),
            )
            self.page.setViewportSize(QSize(cwidth, cheight))
            factor = float(self.width) / cwidth if cwidth > self.width else 1
            cutoff_height = int(self.height / factor) - 3
            image = QImage(self.page.viewportSize(), QImage.Format_ARGB32)
            image.setDotsPerMeterX(self.dpi * (100 / 2.54))
            image.setDotsPerMeterY(self.dpi * (100 / 2.54))
            painter = QPainter(image)
            self.page.mainFrame().render(painter)
            painter.end()
            cheight = image.height()
            cwidth = image.width()
            pos = 0
            while pos < cheight:
                img = image.copy(0, pos, cwidth, min(cheight - pos, cutoff_height))
                pos += cutoff_height - 20
                if cwidth > self.width:
                    img = img.scaledToWidth(self.width, Qt.SmoothTransform)
                f = os.path.join(self.tdir, "%d.png" % pos)
                img.save(f)
                self.images.append((f, img.width(), img.height()))
        finally:
            QApplication.quit()


def render_table(soup: _typing.Any, table: _typing.Any, css: _typing.Any, base_dir: _typing.Any, width: _typing.Any, height: _typing.Any, dpi: _typing.Any, factor: float = 1.0) -> _typing.Any:
    """
    Perform the render table operation under explicit file-format and conversion rules.

    Example:
        Exercise render table through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


    :param soup: Value supplied for soup under the utility contract.
    :param table: Value supplied for table under the utility contract.
    :param css: Value supplied for css under the utility contract.
    :param base_dir: Value supplied for base dir under the utility contract.
    :param width: Value supplied for width under the utility contract.
    :param height: Value supplied for height under the utility contract.
    :param dpi: Value supplied for dpi under the utility contract.
    :param factor: Value supplied for factor under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    head = ""
    for e in soup.findAll(["link", "style"]):
        head += six_unicode(e) + "\n\n"
    style = ""
    for key, val in css.items():
        style += key + ":%s;" % val
    html = """\
<html>
    <head>
        %s
    </head>
    <body style="width: %dpx; background: white">
        <style type="text/css">
            table {%s}
        </style>
        %s
    </body>
</html>
    """ % (
        head,
        width - 10,
        style,
        six_unicode(table),
    )
    images, tdir = do_render(html, base_dir, width, height, dpi, factor)
    atexit.register(shutil.rmtree, tdir)
    return images


def do_render(html: _typing.Any, base_dir: _typing.Any, width: _typing.Any, height: _typing.Any, dpi: _typing.Any, factor: _typing.Any) -> tuple[_typing.Any, ...]:
    """
    Perform the do render operation under explicit file-format and conversion rules.

    Example:
        Exercise do render through a consuming regression::

            python -m pytest -q tests/file_formats/lrf/test_lrf_modernized.py


    :param html: Value supplied for html under the utility contract.
    :param base_dir: Value supplied for base dir under the utility contract.
    :param width: Value supplied for width under the utility contract.
    :param height: Value supplied for height under the utility contract.
    :param dpi: Value supplied for dpi under the utility contract.
    :param factor: Value supplied for factor under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        from LiuXin_alpha.surfaces.gui2 import is_ok_to_use_qt
    except Exception:
        is_ok_to_use_qt = lambda: False

    if not is_ok_to_use_qt():
        raise RuntimeError("Qt is unavailable in this environment")
    tr = HTMLTableRenderer(html, base_dir, width, height, dpi, factor)
    tr.loop.exec_()
    return tr.images, tr.tdir
