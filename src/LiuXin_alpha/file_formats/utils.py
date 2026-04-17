
"""
Utils for file conversion and generation.
"""

import traceback
import os
import re

from typing import Optional, Union, LiteralString

from LiuXin_alpha.utils.storage.local import CurrentDir
from LiuXin_alpha.utils.logging import prints
from LiuXin_alpha.utils.resources import P
from LiuXin_alpha.utils.resources import I

from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.file_formats import ParserError

__license__ = "GPL v3"
__copyright__ = "2008, Kovid Goyal <kovid at kovidgoyal.net>"





class HTMLRenderer:
    """
    HTML renderer class to generate covers from HTML.
    """
    def __init__(self, page, loop):
        self.page, self.loop = page, loop
        self.data = ""
        self.exception = self.tb = None

    def __call__(self, ok):
        from PyQt5.Qt import QImage, QPainter, QByteArray, QBuffer

        try:
            if not ok:
                raise RuntimeError("Rendering of HTML failed.")
            de = self.page.mainFrame().documentElement()
            pe = de.findFirst("parsererror")
            if not pe.isNull():
                raise ParserError(pe.toPlainText())
            image = QImage(self.page.viewportSize(), QImage.Format_ARGB32)
            image.setDotsPerMeterX(96 * (100 / 2.54))
            image.setDotsPerMeterY(96 * (100 / 2.54))
            painter = QPainter(image)
            self.page.mainFrame().render(painter)
            painter.end()
            ba = QByteArray()
            buf = QBuffer(ba)
            buf.open(QBuffer.WriteOnly)
            image.save(buf, "JPEG")
            self.data = str(ba.data())
        except Exception as e:
            self.exception = e
            self.traceback = traceback.format_exc()
        finally:
            self.loop.exit(0)


def return_raster_image(path: Union[str, LiteralString, bytes]) -> Optional[bytes]:
    """
    Return a rasterized image - or return None if the given path doesn't point to one.

    :param path:
    :return:
    """
    from LiuXin_alpha.utils.image_tools.imghdr import what

    if os.access(path, os.R_OK):
        with open(path, "rb") as f:
            raw = f.read()
        if what(None, raw) not in (None, "svg"):
            return raw


def extract_cover_from_embedded_svg(html, base, log) -> Optional[bytes]:
    from lxml import etree
    from LiuXin_alpha.file_formats.oeb.base import XPath, SVG, XLINK

    root = etree.fromstring(html)

    svg = XPath("//svg:svg")(root)
    if len(svg) == 1 and len(svg[0]) == 1 and svg[0][0].tag == SVG("image"):
        image = svg[0][0]
        href = image.get(XLINK("href"), None)
        if href:
            path = os.path.join(base, *href.split("/"))
            return return_raster_image(path)


def extract_calibre_cover(raw, base, log) -> Optional[bytes]:
    """
    Extract a cover from a html tree.

    :param raw:
    :param base:
    :param log:
    :return:
    """
    from LiuXin_alpha.utils.libraries.BeautifulSoup import BeautifulSoup

    soup = BeautifulSoup(raw)
    matches = soup.find(name=["h1", "h2", "h3", "h4", "h5", "h6", "p", "span", "font", "br"])
    images = soup.findAll("img")
    if matches is None and len(images) == 1 and images[0].get("alt", "") == "cover":
        img = images[0]
        img = os.path.join(base, *img["src"].split("/"))
        cover = return_raster_image(img)
        return cover

    # Look for a simple cover, i.e. a body with no text and only one <img> tag
    if matches is None:
        body = soup.find("body")
        if body is not None:
            text = "".join(map(six_unicode, body.findAll(text=True)))
            if text.strip():
                # Body has text, abort
                return
            images = body.findAll("img", src=True)
            if 0 < len(images) < 2:
                img = os.path.join(base, *images[0]["src"].split("/"))
                return return_raster_image(img)


def render_html_svg_workaround(path_to_html, log, width=590, height=750):
    """
    Render html data (which might or might not include an svg file) as a image.

    This is what's used to generate a cover for a book when an actual image can't be extracted - the first page of the
    book is rendered and that#s returned.
    :param path_to_html: The path to the html to preform the render wity
    :param log:
    :param width: The width of the output
    :param height: The height of the output
    :return:
    """
    from LiuXin_alpha.file_formats.oeb.base import SVG_NS

    with open(path_to_html, "rb") as f:
        raw = f.read()
    data = None

    # Look for a svg image in the raw data
    if SVG_NS.encode("utf-8") in raw:
        try:
            data = extract_cover_from_embedded_svg(raw, os.path.dirname(path_to_html), log)
        except:
            pass

    if data is None:
        try:
            data = extract_calibre_cover(raw, os.path.dirname(path_to_html), log)
        except:
            pass

    # Todo: Install https://github.com/AdamN/python-webkit2png as a fallback for when PyQt isn't installed at all
    if data is None:
        try:
            from LiuXin_alpha.surfaces.gui2 import is_ok_to_use_qt
        except Exception:
            is_ok_to_use_qt = lambda: False

        if is_ok_to_use_qt():
            data = render_html_data(path_to_html, width, height)
        else:
            try:
                from LiuXin_alpha.utils.ipc.simple_worker import fork_job, WorkerError
            except ModuleNotFoundError:
                fork_job = WorkerError = None

            if fork_job is not None:
                try:
                    result = fork_job(
                        "LiuXin_alpha.file_formats.utils",
                        "render_html_data",
                        (path_to_html, width, height),
                        no_output=True,
                    )
                    data = result["result"]
                except WorkerError as err:
                    prints(err.orig_tb)
                except Exception:
                    traceback.print_exc()

    return data


def render_html_data(path_to_html, width, height):
    renderer = render_html(path_to_html, width, height)
    return getattr(renderer, "data", None)


def render_html(path_to_html, width=590, height=750, as_xhtml=True):
    try:
        from PyQt5.QtWebKitWidgets import QWebPage
        from PyQt5.Qt import QEventLoop, QPalette, Qt, QUrl, QSize
    except Exception:
        return None
    try:
        from LiuXin_alpha.surfaces.gui2 import is_ok_to_use_qt
    except Exception:
        return None

    if not is_ok_to_use_qt():
        return None
    path_to_html = os.path.abspath(path_to_html)
    with CurrentDir(os.path.dirname(path_to_html)):
        page = QWebPage()
        settings = page.settings()
        settings.setAttribute(settings.PluginsEnabled, False)
        pal = page.palette()
        pal.setBrush(QPalette.Background, Qt.white)
        page.setPalette(pal)
        page.setViewportSize(QSize(width, height))
        page.mainFrame().setScrollBarPolicy(Qt.Vertical, Qt.ScrollBarAlwaysOff)
        page.mainFrame().setScrollBarPolicy(Qt.Horizontal, Qt.ScrollBarAlwaysOff)
        loop = QEventLoop()
        renderer = HTMLRenderer(page, loop)
        page.loadFinished.connect(renderer, type=Qt.QueuedConnection)
        if as_xhtml:
            with open(path_to_html, "rb") as f:
                page.mainFrame().setContent(f.read(), "application/xhtml+xml", QUrl.fromLocalFile(path_to_html))
        else:
            page.mainFrame().load(QUrl.fromLocalFile(path_to_html))
        loop.exec_()
    renderer.loop = renderer.page = None
    page.loadFinished.disconnect()
    del page
    del loop
    if isinstance(renderer.exception, ParserError) and as_xhtml:
        return render_html(path_to_html, width=width, height=height, as_xhtml=False)
    return renderer


def check_ebook_format(stream, current_guess):
    ans = current_guess
    if current_guess.lower() in ("prc", "mobi", "azw", "azw1", "azw3"):
        stream.seek(0)
        if stream.read(3) == b"TPZ":
            ans = "tpz"
        stream.seek(0)
    return ans


def normalize(x):
    """
    Brings a unicode string into normal form.
    There may be multiple different ways of representing a unicode string which are human readable as the same -
    however they will differ on a bytes level. Normalization brings a unicode string into a form suitable for
    comparison.
    :param x:
    :return:
    """
    if isinstance(x, str):
        import unicodedata

        x = unicodedata.normalize("NFC", x)
    return x


def calibre_cover(
    title, author_string, series_string=None, output_format="jpg", title_size=46, author_size=36, logo_path=None
):
    """
    Generate a custom cover file for your books.
    :param title: Title for the book
    :param author_string: The author (creators) string to appear on the cover
    :param series_string: The series of the work
    :param output_format: IGNORED - Currently only 'jpg' is used
    :param title_size: The font size of the title
    :param author_size: Font size of the creator
    :param logo_path: Replacement logo instead of the default
    :return:
    """
    # Initial normalization
    title = normalize(title)
    author_string = normalize(author_string)
    series_string = normalize(series_string)

    # Import resources
    from LiuXin_alpha.utils.wrappers.magick.draw import create_cover_page, TextLine
    import regex

    # Determine how the text is going to appear
    pat = regex.compile(r"\p{Cf}+", flags=regex.VERSION1)  # remove non-printing chars like the soft hyphen
    text = pat.sub("", title + author_string + (series_string or ""))
    font_path = P("fonts/liberation/LiberationSerif-Bold.ttf")
    from LiuXin_alpha.utils.fonts.utils import get_font_for_text

    font = open(font_path, "rb").read()
    c = get_font_for_text(text, font)
    cleanup = False
    if c is not None and c != font:
        from LiuXin_alpha.utils.ptempfiles import PersistentTemporaryFile

        pt = PersistentTemporaryFile(".ttf")
        pt.write(c)
        pt.close()
        font_path = pt.name
        cleanup = True

    # Build the final encoded lines to go to the cover creator
    lines = [
        TextLine(pat.sub("", title), title_size, font_path=font_path),
        TextLine(pat.sub("", author_string), author_size, font_path=font_path),
    ]
    if series_string:
        lines.append(TextLine(pat.sub("", series_string), author_size, font_path=font_path))

    if logo_path is None:
        logo_path = I("library.png")
    try:
        return create_cover_page(
            lines, logo_path, output_format="jpg", texture_opacity=0.3, texture_data=I("cover_texture.png", data=True)
        )
    finally:
        if cleanup:
            os.remove(font_path)


UNIT_RE = re.compile(r"^(-*[0-9]*[.]?[0-9]*)\s*(%|em|ex|en|px|mm|cm|in|pt|pc|rem)$")


def unit_convert(value, base, font, dpi, body_font_size=12):
    "Return value in pts"
    if isinstance(value, (int, float)):
        return value
    try:
        return float(value) * 72.0 / dpi
    except:
        pass
    result = value
    m = UNIT_RE.match(value)
    if m is not None and m.group(1):
        value = float(m.group(1))
        unit = m.group(2)
        if unit == "%":
            result = (value / 100.0) * base
        elif unit == "px":
            result = value * 72.0 / dpi
        elif unit == "in":
            result = value * 72.0
        elif unit == "pt":
            result = value
        elif unit == "em":
            result = value * font
        elif unit in ("ex", "en"):
            # This is a hack for ex since we have no way to know
            # the x-height of the font
            font = font
            result = value * font * 0.5
        elif unit == "pc":
            result = value * 12.0
        elif unit == "mm":
            result = value * 2.8346456693
        elif unit == "cm":
            result = value * 28.346456693
        elif unit == "rem":
            result = value * body_font_size
    return result


def generate_masthead(title, output_path=None, width=600, height=60):
    from LiuXin_alpha.file_formats.conversion.config import load_defaults

    recs = load_defaults("mobi_output")
    masthead_font_family = recs.get("masthead_font", None)
    from LiuXin_alpha.file_formats.covers import generate_masthead

    return generate_masthead(
        title, output_path=output_path, width=width, height=height, font_family=masthead_font_family
    )


def escape_xpath_attr(value):
    if '"' in value:
        if "'" in value:
            parts = re.split('("+)', value)
            ans = []
            for x in parts:
                if x:
                    q = "'" if '"' in x else '"'
                    ans.append(q + x + q)
            return "concat(%s)" % ", ".join(ans)
        else:
            return "'%s'" % value
    return '"%s"' % value


def parse_css_length(value):
    try:
        m = UNIT_RE.match(value)
    except TypeError:
        return None, None
    if m is not None and m.group(1):
        value = float(m.group(1))
        unit = m.group(2)
        return value, unit.lower()
    return None, None
