from __future__ import with_statement
from __future__ import annotations

import typing as _typing

# SVG are not supported by a number of the ebook standard that are supported - so need a way of rendering SVG images
# which might be present in the input into something which can be used in the output. Thus this rasterizer.
# For a safe version which doesn't need to use PyQt - see rasterize_safe.py

"""
SVG rasterization transform.
"""

import os
import re

from lxml import etree
try:
    from PyQt5.Qt import (
        Qt,
        QByteArray,
        QBuffer,
        QIODevice,
        QColor,
        QImage,
        QPainter,
        QSvgRenderer,
    )

    _QT_IMPORT_ERROR = None
except Exception as err:
    Qt = QByteArray = QBuffer = QIODevice = QColor = QImage = QPainter = QSvgRenderer = None
    _QT_IMPORT_ERROR = err

from LiuXin_alpha.file_formats.oeb.base import XHTML, XLINK
from LiuXin_alpha.file_formats.oeb.base import SVG_MIME, PNG_MIME
from LiuXin_alpha.file_formats.oeb.base import xml2str, xpath
from LiuXin_alpha.file_formats.oeb.base import urlnormalize
from LiuXin_alpha.file_formats.oeb.stylizer import Stylizer

from LiuXin_alpha.utils.image_tools.imghdr import what
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.libraries.liuxin_six import six_urldefrag as urldefrag
from LiuXin_alpha.utils.ptempfiles import PersistentTemporaryFile

__license__ = "GPL v3"
__copyright__ = "2008, Marshall T. Vandegrift <llasram@gmail.com>"

IMAGE_TAGS = {XHTML("img"), XHTML("object")}
KEEP_ATTRS = {"class", "style", "width", "height", "align"}


class Unavailable(Exception):
    pass


# Todo: Check safe to use qt by spinning it off into a different threas - see if it crashes
class SVGRasterizer(object):
    def __init__(self: _typing.Self) -> None:
        if _QT_IMPORT_ERROR is not None:
            raise Unavailable("PyQt5 is unavailable for SVG rasterization")
        from LiuXin_alpha.surfaces.gui2 import must_use_qt

        must_use_qt()

    @classmethod
    def config(cls: type[_typing.Self], cfg: _typing.Any) -> _typing.Any:
        return cfg

    @classmethod
    def generate(cls: type[_typing.Self], opts: _typing.Any) -> _typing.Any:
        return cls()

    def __call__(self: _typing.Self, oeb: _typing.Any, context: _typing.Any) -> None:
        oeb.logger.info("Rasterizing SVG images...")
        self.temp_files = []
        self.stylizer_cache = {}
        self.oeb = oeb
        self.opts = context
        self.profile = context.dest
        self.images = {}
        self.dataize_manifest()
        self.rasterize_spine()
        self.rasterize_cover()
        for pt in self.temp_files:
            try:
                os.remove(pt)
            except:
                pass

    def rasterize_svg(self: _typing.Self, elem: _typing.Any, width: int = 0, height: int = 0, format: str = "PNG") -> _typing.Any:
        view_box = elem.get("viewBox", elem.get("viewbox", None))
        sizes = None
        logger = self.oeb.logger

        if view_box is not None:
            try:
                box = [float(x) for x in filter(None, re.split("[, ]", view_box))]
                sizes = [box[2] - box[0], box[3] - box[1]]
            except (TypeError, ValueError, IndexError):
                logger.warn('SVG image has invalid viewBox="%s", ignoring the viewBox' % view_box)
            else:
                for image in elem.xpath(
                    'descendant::*[local-name()="image" and ' '@height and contains(@height, "%")]'
                ):
                    logger.info("Found SVG image height in %, trying to convert...")
                    try:
                        h = float(image.get("height").replace("%", "")) / 100.0
                        image.set("height", str(h * sizes[1]))
                    except:
                        logger.exception("Failed to convert percentage height:", image.get("height"))

        data = QByteArray(xml2str(elem, with_tail=False))
        svg = QSvgRenderer(data)
        size = svg.defaultSize()

        # If the size is 100 x 100 - and we're in a view box - scale the image to the size of the view box
        if size.width() == 100 and size.height() == 100 and sizes:
            size.setWidth(sizes[0])
            size.setHeight(sizes[1])
        if width or height:
            size.scale(width, height, Qt.KeepAspectRatio)
        logger.info("Rasterizing %r to %dx%d" % (elem, size.width(), size.height()))
        image = QImage(size, QImage.Format_ARGB32_Premultiplied)
        image.fill(QColor("white").rgb())
        painter = QPainter(image)
        svg.render(painter)
        painter.end()
        array = QByteArray()
        qbuffer = QBuffer(array)
        qbuffer.open(QIODevice.WriteOnly)
        image.save(qbuffer, format)
        return str(array)

    def dataize_manifest(self: _typing.Self) -> None:
        for item in self.oeb.manifest.values():
            if item.media_type == SVG_MIME and item.data is not None:
                self.dataize_svg(item)

    def dataize_svg(self: _typing.Self, item: _typing.Any, svg: _typing.Any = None) -> _typing.Any:
        if svg is None:
            svg = item.data
        hrefs = self.oeb.manifest.hrefs
        for elem in xpath(svg, "//svg:*[@xl:href]"):
            href = urlnormalize(elem.attrib[XLINK("href")])
            path = urldefrag(href)[0]
            if not path:
                continue
            abshref = item.abshref(path)
            if abshref not in hrefs:
                continue
            linkee = hrefs[abshref]
            data = str(linkee)
            ext = what(None, data) or "jpg"
            with PersistentTemporaryFile(suffix="." + ext) as pt:
                pt.write(data)
                self.temp_files.append(pt.name)
            elem.attrib[XLINK("href")] = pt.name
        return svg

    def stylizer(self: _typing.Self, item: _typing.Any) -> _typing.Any:
        ans = self.stylizer_cache.get(item, None)
        if ans is None:
            ans = Stylizer(item.data, item.href, self.oeb, self.opts, self.profile)
            self.stylizer_cache[item] = ans
        return ans

    def rasterize_spine(self: _typing.Self) -> None:
        for item in self.oeb.spine:
            self.rasterize_item(item)

    def rasterize_item(self: _typing.Self, item: _typing.Any) -> None:
        html = item.data
        hrefs = self.oeb.manifest.hrefs
        for elem in xpath(html, "//h:img[@src]"):
            src = urlnormalize(elem.attrib["src"])
            image = hrefs.get(item.abshref(src), None)
            if image and image.media_type == SVG_MIME:
                style = self.stylizer(item).style(elem)
                self.rasterize_external(elem, style, item, image)
        for elem in xpath(html, '//h:object[@type="%s" and @data]' % SVG_MIME):
            data = urlnormalize(elem.attrib["data"])
            image = hrefs.get(item.abshref(data), None)
            if image and image.media_type == SVG_MIME:
                style = self.stylizer(item).style(elem)
                self.rasterize_external(elem, style, item, image)
        for elem in xpath(html, "//svg:svg"):
            style = self.stylizer(item).style(elem)
            self.rasterize_inline(elem, style, item)

    def rasterize_inline(self: _typing.Self, elem: _typing.Any, style: _typing.Any, item: _typing.Any) -> None:
        width = style["width"]
        height = style["height"]
        width = (width / 72) * self.profile.dpi
        height = (height / 72) * self.profile.dpi
        elem = self.dataize_svg(item, elem)
        data = self.rasterize_svg(elem, width, height)
        manifest = self.oeb.manifest
        href = os.path.splitext(item.href)[0] + ".png"
        item_id, href = manifest.generate(item.id, href)
        manifest.add(item_id, href, PNG_MIME, data=data)
        img = etree.Element(XHTML("img"), src=item.relhref(href))
        elem.getparent().replace(elem, img)
        for prop in ("width", "height"):
            if prop in elem.attrib:
                img.attrib[prop] = elem.attrib[prop]

    def rasterize_external(self: _typing.Self, elem: _typing.Any, style: _typing.Any, item: _typing.Any, svgitem: _typing.Any) -> None:
        width = style["width"]
        height = style["height"]
        width = (width / 72) * self.profile.dpi
        height = (height / 72) * self.profile.dpi
        data = QByteArray(str(svgitem))
        svg = QSvgRenderer(data)
        size = svg.defaultSize()
        size.scale(width, height, Qt.KeepAspectRatio)
        key = (svgitem.href, size.width(), size.height())
        if key in self.images:
            href = self.images[key]
        else:
            logger = self.oeb.logger
            logger.info("Rasterizing %r to %dx%d" % (svgitem.href, size.width(), size.height()))
            image = QImage(size, QImage.Format_ARGB32_Premultiplied)
            image.fill(QColor("white").rgb())
            painter = QPainter(image)
            svg.render(painter)
            painter.end()
            array = QByteArray()
            qbuffer = QBuffer(array)
            qbuffer.open(QIODevice.WriteOnly)
            image.save(qbuffer, "PNG")
            data = str(array)
            manifest = self.oeb.manifest
            href = os.path.splitext(svgitem.href)[0] + ".png"
            item_id, href = manifest.generate(svgitem.id, href)
            manifest.add(item_id, href, PNG_MIME, data=data)
            self.images[key] = href
        elem.tag = XHTML("img")
        for attr in elem.attrib:
            if attr not in KEEP_ATTRS:
                del elem.attrib[attr]
        elem.attrib["src"] = item.relhref(href)
        if elem.text:
            elem.attrib["alt"] = elem.text
            elem.text = None
        for child in elem:
            elem.remove(child)

    def rasterize_cover(self: _typing.Self) -> None:
        covers = self.oeb.metadata.cover
        if not covers:
            return
        if six_unicode(covers[0]) not in self.oeb.manifest.ids:
            self.oeb.logger.warn("Cover not in manifest, skipping.")
            self.oeb.metadata.clear("cover")
            return
        cover = self.oeb.manifest.ids[six_unicode(covers[0])]
        if not cover.media_type == SVG_MIME:
            return
        width = (self.profile.width / 72) * self.profile.dpi
        height = (self.profile.height / 72) * self.profile.dpi
        data = self.rasterize_svg(cover.data, width, height)
        href = os.path.splitext(cover.href)[0] + ".png"
        cover_id, href = self.oeb.manifest.generate(cover.id, href)
        self.oeb.manifest.add(cover_id, href, PNG_MIME, data=data)
        covers[0].value = cover_id
