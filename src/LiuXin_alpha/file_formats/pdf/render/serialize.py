#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:fdm=marker:ai

from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import hashlib

from LiuXin_alpha.utils.libraries.liuxin_six import long

try:
    from PyQt5.Qt import QBuffer, QByteArray, QImage, Qt, QColor, qRgba, QPainter
    _HAS_QT = True
except Exception:
    QBuffer = QByteArray = QImage = Qt = QColor = qRgba = QPainter = None
    _HAS_QT = False

from LiuXin_alpha.file_formats.pdf.render.common import (
    Reference,
    EOL,
    serialize,
    Stream,
    Dictionary,
    String,
    Name,
    Array,
    fmtnum,
)
from LiuXin_alpha.file_formats.pdf.render.fonts import FontManager
from LiuXin_alpha.file_formats.pdf.render.links import Links

from LiuXin_alpha.constants import __appname__, __version__
from LiuXin_alpha.utils.date import utcnow
from LiuXin_alpha.utils.libraries.liuxin_six import six_map
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid at kovidgoyal.net>"
__docformat__ = "restructuredtext en"

PDFVER = b"%PDF-1.4"  # 1.4 is needed for XMP metadata


def _require_qt() -> None:
    if not _HAS_QT:
        raise RuntimeError("PyQt5 is required for PDF serialization.")


class IndirectObjects(object):
    def __init__(self: _typing.Self) -> None:
        self._list = []
        self._map = {}
        self._offsets = []

    def __len__(self: _typing.Self) -> _typing.Any:
        return len(self._list)

    def add(self: _typing.Self, o: _typing.Any) -> _typing.Any:
        self._list.append(o)
        ref = Reference(len(self._list), o)
        self._map[id(o)] = ref
        self._offsets.append(None)
        return ref

    def commit(self: _typing.Self, ref: _typing.Any, stream: _typing.Any) -> None:
        self.write_obj(stream, ref.num, ref.obj)

    def write_obj(self: _typing.Self, stream: _typing.Any, num: _typing.Any, obj: _typing.Any) -> None:
        stream.write(EOL)
        self._offsets[num - 1] = stream.tell()
        stream.write("%d 0 obj" % num)
        stream.write(EOL)
        serialize(obj, stream)
        if stream.last_char != EOL:
            stream.write(EOL)
        stream.write("endobj")
        stream.write(EOL)

    def __getitem__(self: _typing.Self, o: _typing.Any) -> _typing.Any:
        try:
            return self._map[id(self._list[o] if isinstance(o, int) else o)]
        except (KeyError, IndexError):
            raise KeyError("The object %r was not found" % o)

    def pdf_serialize(self: _typing.Self, stream: _typing.Any) -> None:
        for i, obj in enumerate(self._list):
            offset = self._offsets[i]
            if offset is None:
                self.write_obj(stream, i + 1, obj)

    def write_xref(self: _typing.Self, stream: _typing.Any) -> _typing.Any:
        self.xref_offset = stream.tell()
        stream.write(b"xref" + EOL)
        stream.write("0 %d" % (1 + len(self._offsets)))
        stream.write(EOL)
        stream.write("%010d 65535 f " % 0)
        stream.write(EOL)

        for offset in self._offsets:
            line = "%010d 00000 n " % offset
            stream.write(line.encode("ascii") + EOL)
        return self.xref_offset


class Page(Stream):
    def __init__(self: _typing.Self, parentref: _typing.Any, *args: _typing.Any, **kwargs: _typing.Any) -> None:
        super(Page, self).__init__(*args, **kwargs)
        self.page_dict = Dictionary(
            {
                "Type": Name("Page"),
                "Parent": parentref,
            }
        )
        self.opacities = {}
        self.fonts = {}
        self.xobjects = {}
        self.patterns = {}

    def set_opacity(self: _typing.Self, opref: _typing.Any) -> None:
        if opref not in self.opacities:
            self.opacities[opref] = "Opa%d" % len(self.opacities)
        name = self.opacities[opref]
        serialize(Name(name), self)
        self.write(b" gs ")

    def add_font(self: _typing.Self, fontref: _typing.Any) -> _typing.Any:
        if fontref not in self.fonts:
            self.fonts[fontref] = "F%d" % len(self.fonts)
        return self.fonts[fontref]

    def add_image(self: _typing.Self, imgref: _typing.Any) -> _typing.Any:
        if imgref not in self.xobjects:
            self.xobjects[imgref] = "Image%d" % len(self.xobjects)
        return self.xobjects[imgref]

    def add_pattern(self: _typing.Self, patternref: _typing.Any) -> _typing.Any:
        if patternref not in self.patterns:
            self.patterns[patternref] = "Pat%d" % len(self.patterns)
        return self.patterns[patternref]

    def add_resources(self: _typing.Self) -> None:
        r = Dictionary()
        if self.opacities:
            extgs = Dictionary()
            for opref, name in iteritems(self.opacities):
                extgs[name] = opref
            r["ExtGState"] = extgs
        if self.fonts:
            fonts = Dictionary()
            for ref, name in iteritems(self.fonts):
                fonts[name] = ref
            r["Font"] = fonts
        if self.xobjects:
            xobjects = Dictionary()
            for ref, name in iteritems(self.xobjects):
                xobjects[name] = ref
            r["XObject"] = xobjects
        if self.patterns:
            r["ColorSpace"] = Dictionary({"PCSp": Array([Name("Pattern"), Name("DeviceRGB")])})
            patterns = Dictionary()
            for ref, name in iteritems(self.patterns):
                patterns[name] = ref
            r["Pattern"] = patterns
        if r:
            self.page_dict["Resources"] = r

    def end(self: _typing.Self, objects: _typing.Any, stream: _typing.Any) -> _typing.Any:
        contents = objects.add(self)
        objects.commit(contents, stream)
        self.page_dict["Contents"] = contents
        self.add_resources()
        ret = objects.add(self.page_dict)
        # objects.commit(ret, stream)
        return ret


class Path(object):
    def __init__(self: _typing.Self) -> None:
        self.ops = []

    def move_to(self: _typing.Self, x: _typing.Any, y: _typing.Any) -> None:
        self.ops.append((x, y, "m"))

    def line_to(self: _typing.Self, x: _typing.Any, y: _typing.Any) -> None:
        self.ops.append((x, y, "l"))

    def curve_to(self: _typing.Self, x1: _typing.Any, y1: _typing.Any, x2: _typing.Any, y2: _typing.Any, x: _typing.Any, y: _typing.Any) -> None:
        self.ops.append((x1, y1, x2, y2, x, y, "c"))

    def close(self: _typing.Self) -> None:
        self.ops.append(("h",))


class Catalog(Dictionary):
    def __init__(self: _typing.Self, pagetree: _typing.Any) -> None:
        super(Catalog, self).__init__({"Type": Name("Catalog"), "Pages": pagetree})


class PageTree(Dictionary):
    def __init__(self: _typing.Self, page_size: _typing.Any) -> None:
        super(PageTree, self).__init__(
            {
                "Type": Name("Pages"),
                "MediaBox": Array([0, 0, page_size[0], page_size[1]]),
                "Kids": Array(),
                "Count": 0,
            }
        )

    def add_page(self: _typing.Self, pageref: _typing.Any) -> None:
        self["Kids"].append(pageref)
        self["Count"] += 1

    def get_ref(self: _typing.Self, num: _typing.Any) -> _typing.Any:
        return self["Kids"][num - 1]

    def get_num(self: _typing.Self, pageref: _typing.Any) -> _typing.Any:
        try:
            return self["Kids"].index(pageref) + 1
        except ValueError:
            return -1


class HashingStream(object):
    def __init__(self: _typing.Self, f: _typing.Any) -> None:
        self.f = f
        self.tell = f.tell
        self.hashobj = hashlib.sha256()
        self.last_char = b""

    def write(self: _typing.Self, raw: _typing.Any) -> None:
        self.write_raw(raw if isinstance(raw, bytes) else raw.encode("ascii"))

    def write_raw(self: _typing.Self, raw: _typing.Any) -> None:
        self.f.write(raw)
        self.hashobj.update(raw)
        if raw:
            self.last_char = raw[-1:]


class Image(Stream):
    def __init__(self: _typing.Self, data: _typing.Any, w: _typing.Any, h: _typing.Any, depth: _typing.Any, mask: _typing.Any, soft_mask: _typing.Any, dct: _typing.Any) -> None:
        Stream.__init__(self)
        self.width, self.height, self.depth = w, h, depth
        self.mask, self.soft_mask = mask, soft_mask
        if dct:
            self.filters.append(Name("DCTDecode"))
        else:
            self.compress = True
        self.write(data)

    def add_extra_keys(self: _typing.Self, d: _typing.Any) -> None:
        d["Type"] = Name("XObject")
        d["Subtype"] = Name("Image")
        d["Width"] = self.width
        d["Height"] = self.height
        if self.depth == 1:
            d["ImageMask"] = True
            d["Decode"] = Array([1, 0])
        else:
            d["BitsPerComponent"] = 8
            d["ColorSpace"] = Name("Device" + ("RGB" if self.depth == 32 else "Gray"))
        if self.mask is not None:
            d["Mask"] = self.mask
        if self.soft_mask is not None:
            d["SMask"] = self.soft_mask


class Metadata(Stream):
    def __init__(self: _typing.Self, mi: _typing.Any) -> None:
        Stream.__init__(self)
        from LiuXin_alpha.metadata.xmp import metadata_to_xmp_packet

        self.write(metadata_to_xmp_packet(mi))

    def add_extra_keys(self: _typing.Self, d: _typing.Any) -> None:
        d["Type"] = Name("Metadata")
        d["Subtype"] = Name("XML")


class PDFStream(object):

    PATH_OPS = {
        # stroke fill   fill-rule
        (False, False, "winding"): "n",
        (False, False, "evenodd"): "n",
        (False, True, "winding"): "f",
        (False, True, "evenodd"): "f*",
        (True, False, "winding"): "S",
        (True, False, "evenodd"): "S",
        (True, True, "winding"): "B",
        (True, True, "evenodd"): "B*",
    }

    def __init__(self: _typing.Self, stream: _typing.Any, page_size: _typing.Any, compress: bool = False, mark_links: bool = False, debug: _typing.Any = print) -> None:
        self.stream = HashingStream(stream)
        self.compress = compress
        self.write_line(PDFVER)
        # Todo: This is a hack - not sure the encoding is utf-8 always. Have to check.
        self.write_line('%íì¦"'.encode())
        creator = "%s %s [http://calibre-ebook.com]" % (__appname__, __version__)
        self.write_line(("%% Created by %s" % creator).encode("utf-8"))
        self.objects = IndirectObjects()
        self.objects.add(PageTree(page_size))
        self.objects.add(Catalog(self.page_tree))
        self.current_page = Page(self.page_tree, compress=self.compress)
        self.info = Dictionary(
            {
                "Creator": String(creator),
                "Producer": String(creator),
                "CreationDate": utcnow(),
            }
        )
        self.stroke_opacities, self.fill_opacities = {}, {}
        self.font_manager = FontManager(self.objects, self.compress)
        self.image_cache = {}
        self.pattern_cache, self.shader_cache = {}, {}
        self.debug = debug
        self.links = Links(self, mark_links, page_size)
        if _HAS_QT:
            i = QImage(1, 1, QImage.Format_ARGB32)
            i.fill(qRgba(0, 0, 0, 255))
            self.alpha_bit = i.constBits().asstring(4).find(b"\xff")
        else:
            # Used only by image embedding paths, which are guarded by _require_qt().
            self.alpha_bit = 3

    @property
    def page_tree(self: _typing.Self) -> _typing.Any:
        return self.objects[0]

    @property
    def catalog(self: _typing.Self) -> _typing.Any:
        return self.objects[1]

    def get_pageref(self: _typing.Self, pagenum: _typing.Any) -> _typing.Any:
        return self.page_tree.obj.get_ref(pagenum)

    def set_metadata(self: _typing.Self, title: _typing.Any = None, author: _typing.Any = None, tags: _typing.Any = None, mi: _typing.Any = None) -> None:
        if title:
            self.info["Title"] = String(title)
        if author:
            self.info["Author"] = String(author)
        if tags:
            self.info["Keywords"] = String(tags)
        if mi is not None:
            self.metadata = self.objects.add(Metadata(mi))
            self.catalog.obj["Metadata"] = self.metadata

    def write_line(self: _typing.Self, byts: bytes = b"") -> None:
        byts = byts if isinstance(byts, bytes) else byts.encode("ascii")
        self.stream.write(byts + EOL)

    def transform(self: _typing.Self, *args: _typing.Any) -> None:
        if len(args) == 1:
            m = args[0]
            vals = [m.m11(), m.m12(), m.m21(), m.m22(), m.dx(), m.dy()]
        else:
            vals = args
        cm = " ".join(six_map(fmtnum, vals))
        self.current_page.write_line(cm + " cm")

    def save_stack(self: _typing.Self) -> None:
        self.current_page.write_line("q")

    def restore_stack(self: _typing.Self) -> None:
        self.current_page.write_line("Q")

    def reset_stack(self: _typing.Self) -> None:
        self.current_page.write_line("Q q")

    def draw_rect(self: _typing.Self, x: _typing.Any, y: _typing.Any, width: _typing.Any, height: _typing.Any, stroke: bool = True, fill: bool = False) -> None:
        self.current_page.write("%s re " % " ".join(six_map(fmtnum, (x, y, width, height))))
        self.current_page.write_line(self.PATH_OPS[(stroke, fill, "winding")])

    def write_path(self: _typing.Self, path: _typing.Any) -> None:
        for i, op in enumerate(path.ops):
            if i != 0:
                self.current_page.write_line()
            for x in op:
                self.current_page.write((fmtnum(x) if isinstance(x, (int, long, float)) else x) + " ")

    def draw_path(self: _typing.Self, path: _typing.Any, stroke: bool = True, fill: bool = False, fill_rule: str = "winding") -> None:
        if not path.ops:
            return
        self.write_path(path)
        self.current_page.write_line(self.PATH_OPS[(stroke, fill, fill_rule)])

    def add_clip(self: _typing.Self, path: _typing.Any, fill_rule: str = "winding") -> None:
        if not path.ops:
            return
        self.write_path(path)
        op = "W" if fill_rule == "winding" else "W*"
        self.current_page.write_line(op + " " + "n")

    def serialize(self: _typing.Self, o: _typing.Any) -> None:
        serialize(o, self.current_page)

    def set_stroke_opacity(self: _typing.Self, opacity: _typing.Any) -> None:
        if opacity not in self.stroke_opacities:
            op = Dictionary({"Type": Name("ExtGState"), "CA": opacity})
            self.stroke_opacities[opacity] = self.objects.add(op)
        self.current_page.set_opacity(self.stroke_opacities[opacity])

    def set_fill_opacity(self: _typing.Self, opacity: _typing.Any) -> None:
        opacity = float(opacity)
        if opacity not in self.fill_opacities:
            op = Dictionary({"Type": Name("ExtGState"), "ca": opacity})
            self.fill_opacities[opacity] = self.objects.add(op)
        self.current_page.set_opacity(self.fill_opacities[opacity])

    def end_page(self: _typing.Self) -> None:
        pageref = self.current_page.end(self.objects, self.stream)
        self.page_tree.obj.add_page(pageref)
        self.current_page = Page(self.page_tree, compress=self.compress)

    def draw_glyph_run(self: _typing.Self, transform: _typing.Any, size: _typing.Any, font_metrics: _typing.Any, glyphs: _typing.Any) -> None:
        glyph_ids = {x[-1] for x in glyphs}
        fontref = self.font_manager.add_font(font_metrics, glyph_ids)
        name = self.current_page.add_font(fontref)
        self.current_page.write(b"BT ")
        serialize(Name(name), self.current_page)
        self.current_page.write(" %s Tf " % fmtnum(size))
        self.current_page.write("%s Tm " % " ".join(six_map(fmtnum, transform)))
        for x, y, glyph_id in glyphs:
            self.current_page.write_raw(("%s %s Td <%04X> Tj " % (fmtnum(x), fmtnum(y), glyph_id)).encode("ascii"))
        self.current_page.write_line(b" ET")

    def get_image(self: _typing.Self, cache_key: _typing.Any) -> _typing.Any:
        return self.image_cache.get(cache_key, None)

    def write_image(self: _typing.Self, data: _typing.Any, w: _typing.Any, h: _typing.Any, depth: _typing.Any, dct: bool = False, mask: _typing.Any = None, soft_mask: _typing.Any = None, cache_key: _typing.Any = None) -> _typing.Any:
        imgobj = Image(data, w, h, depth, mask, soft_mask, dct)
        self.image_cache[cache_key] = r = self.objects.add(imgobj)
        self.objects.commit(r, self.stream)
        return r

    def add_image(self: _typing.Self, img: _typing.Any, cache_key: _typing.Any) -> _typing.Any:
        _require_qt()
        ref = self.get_image(cache_key)
        if ref is not None:
            return ref

        fmt = img.format()
        image = QImage(img)
        if (
            image.depth() == 1
            and img.colorTable().size() == 2
            and img.colorTable().at(0) == QColor(Qt.black).rgba()
            and img.colorTable().at(1) == QColor(Qt.white).rgba()
        ):
            if fmt == QImage.Format_MonoLSB:
                image = image.convertToFormat(QImage.Format_Mono)
            fmt = QImage.Format_Mono
        else:
            if fmt != QImage.Format_RGB32 and fmt != QImage.Format_ARGB32:
                image = image.convertToFormat(QImage.Format_ARGB32)
                fmt = QImage.Format_ARGB32

        w = image.width()
        h = image.height()
        d = image.depth()

        if fmt == QImage.Format_Mono:
            bytes_per_line = (w + 7) >> 3
            data = image.constBits().asstring(bytes_per_line * h)
            return self.write_image(data, w, h, d, cache_key=cache_key)

        has_alpha = False
        soft_mask = None

        tmask = None
        if fmt == QImage.Format_ARGB32:
            tmask = image.constBits().asstring(4 * w * h)[self.alpha_bit :: 4]
            sdata = bytearray(tmask)
            vals = set(sdata)
            vals.discard(255)  # discard opaque pixels
            has_alpha = bool(vals)
            if has_alpha:
                # Blend image onto a white background as otherwise Qt will render
                # transparent pixels as black
                background = QImage(image.size(), QImage.Format_ARGB32_Premultiplied)
                background.fill(Qt.white)
                painter = QPainter(background)
                painter.drawImage(0, 0, image)
                painter.end()
                image = background

        ba = QByteArray()
        buf = QBuffer(ba)
        image.save(buf, "jpeg", 94)
        data = bytes(ba.data())

        if has_alpha:
            soft_mask = self.write_image(tmask, w, h, 8)

        return self.write_image(data, w, h, 32, dct=True, soft_mask=soft_mask, cache_key=cache_key)

    def add_pattern(self: _typing.Self, pattern: _typing.Any) -> _typing.Any:
        if pattern.cache_key not in self.pattern_cache:
            self.pattern_cache[pattern.cache_key] = self.objects.add(pattern)
        return self.current_page.add_pattern(self.pattern_cache[pattern.cache_key])

    def add_shader(self: _typing.Self, shader: _typing.Any) -> _typing.Any:
        if shader.cache_key not in self.shader_cache:
            self.shader_cache[shader.cache_key] = self.objects.add(shader)
        return self.shader_cache[shader.cache_key]

    def draw_image(self: _typing.Self, x: _typing.Any, y: _typing.Any, width: _typing.Any, height: _typing.Any, imgref: _typing.Any) -> None:
        name = self.current_page.add_image(imgref)
        self.current_page.write(
            "q %s 0 0 %s %s %s cm " % (fmtnum(width), fmtnum(-height), fmtnum(x), fmtnum(y + height))
        )
        serialize(Name(name), self.current_page)
        self.current_page.write_line(" Do Q")

    def apply_color_space(self: _typing.Self, color: _typing.Any, pattern: _typing.Any, stroke: bool = False) -> None:
        wl = self.current_page.write_line
        if color is not None and pattern is None:
            wl(" ".join(six_map(fmtnum, color)) + (" RG" if stroke else " rg"))
        elif color is None and pattern is not None:
            wl("/Pattern %s /%s %s" % ("CS" if stroke else "cs", pattern, "SCN" if stroke else "scn"))
        elif color is not None and pattern is not None:
            col = " ".join(six_map(fmtnum, color))
            wl("/PCSp %s %s /%s %s" % ("CS" if stroke else "cs", col, pattern, "SCN" if stroke else "scn"))

    def apply_fill(self: _typing.Self, color: _typing.Any = None, pattern: _typing.Any = None, opacity: _typing.Any = None) -> None:
        if opacity is not None:
            self.set_fill_opacity(opacity)
        self.apply_color_space(color, pattern)

    def apply_stroke(self: _typing.Self, color: _typing.Any = None, pattern: _typing.Any = None, opacity: _typing.Any = None) -> None:
        if opacity is not None:
            self.set_stroke_opacity(opacity)
        self.apply_color_space(color, pattern, stroke=True)

    def end(self: _typing.Self) -> None:
        if self.current_page.getvalue():
            self.end_page()
        self.font_manager.embed_fonts(self.debug)
        inforef = self.objects.add(self.info)
        self.links.add_links()
        self.objects.pdf_serialize(self.stream)
        self.write_line()
        startxref = self.objects.write_xref(self.stream)
        file_id = String(self.stream.hashobj.hexdigest())
        self.write_line("trailer")
        trailer = Dictionary(
            {
                "Root": self.catalog,
                "Size": len(self.objects) + 1,
                "ID": Array([file_id, file_id]),
                "Info": inforef,
            }
        )
        serialize(trailer, self.stream)
        self.write_line("startxref")
        self.write_line("%d" % startxref)
        self.stream.write("%%EOF")
