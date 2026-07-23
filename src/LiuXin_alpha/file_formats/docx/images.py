#!/usr/bin/env python2
# vim:fileencoding=utf-8

from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import os

from lxml.html.builder import IMG, HR

from LiuXin_alpha.constants import iswindows

from LiuXin_alpha.file_formats.docx.names import barename

from LiuXin_alpha.utils.storage.local.filenames import ascii_filename
try:
    from LiuXin_alpha.utils.image_tools.img import resize_to_fit, image_to_data
except Exception:
    from LiuXin_alpha.utils.image_tools.img_fallback import resize_to_fit, image_to_data
from LiuXin_alpha.utils.image_tools.imghdr import what

# Py2/Py3
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import dict_itervalues as itervalues

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


class LinkedImageNotFound(ValueError):
    def __init__(self: _typing.Self, fname: _typing.Any) -> None:
        ValueError.__init__(self, fname)
        self.fname = fname


def emu_to_pt(x: _typing.Any) -> _typing.Any:
    return x / 12700


def pt_to_emu(x: _typing.Any) -> _typing.Any:
    return int(x * 12700)


def get_image_properties(parent: _typing.Any, XPath: _typing.Any, get: _typing.Any) -> tuple[_typing.Any, ...]:
    width = height = None
    for extent in XPath("./wp:extent")(parent):
        try:
            width = emu_to_pt(int(extent.get("cx")))
        except (TypeError, ValueError):
            pass
        try:
            height = emu_to_pt(int(extent.get("cy")))
        except (TypeError, ValueError):
            pass
    ans = {}
    if width is not None:
        ans["width"] = "%.3gpt" % width
    if height is not None:
        ans["height"] = "%.3gpt" % height

    alt = None
    for docPr in XPath("./wp:docPr")(parent):
        x = docPr.get("descr", None)
        if x:
            alt = x
        if docPr.get("hidden", None) in {"true", "on", "1"}:
            ans["display"] = "none"

    return ans, alt


def get_image_margins(elem: _typing.Any) -> _typing.Any:
    ans = {}
    for w, css in iteritems({"L": "left", "T": "top", "R": "right", "B": "bottom"}):
        val = elem.get("dist%s" % w, None)
        if val is not None:
            try:
                val = emu_to_pt(val)
            except (TypeError, ValueError):
                continue
            ans["padding-%s" % css] = "%.3gpt" % val
    return ans


def get_hpos(anchor: _typing.Any, page_width: _typing.Any, XPath: _typing.Any, get: _typing.Any) -> _typing.Any:
    for ph in XPath("./wp:positionH")(anchor):
        rp = ph.get("relativeFrom", None)
        if rp == "leftMargin":
            return 0
        if rp == "rightMargin":
            return 1
        for align in XPath("./wp:align")(ph):
            al = align.text
            if al == "left":
                return 0
            if al == "center":
                return 0.5
            if al == "right":
                return 1
        for po in XPath("./wp:posOffset")(ph):
            try:
                pos = emu_to_pt(int(po.text))
            except (TypeError, ValueError):
                continue
            return pos / page_width

    for sp in XPath("./wp:simplePos")(anchor):
        try:
            x = emu_to_pt(sp.get("x", None))
        except (TypeError, ValueError):
            continue
        return x / page_width

    return 0


class Images(object):
    def __init__(self: _typing.Self, namespace: _typing.Any, log: _typing.Any) -> None:
        self.namespace = namespace
        self.rid_map = {}
        self.used = {}
        self.resized = {}
        self.names = set()
        self.all_images = set()
        self.links = []
        self.log = log

    def __call__(self: _typing.Self, relationships_by_id: _typing.Any) -> None:
        self.rid_map = relationships_by_id

    def read_image_data(self: _typing.Self, fname: _typing.Any, base: _typing.Any = None) -> tuple[_typing.Any, ...]:
        if fname.startswith("file://"):
            src = fname[len("file://") :]
            if iswindows and src and src[0] == "/":
                src = src[1:]
            if not src or not os.path.exists(src):
                raise LinkedImageNotFound(src)
            with open(src, "rb") as rawsrc:
                raw = rawsrc.read()
        else:
            raw = self.docx.read(fname)
        base = base or ascii_filename(fname.rpartition("/")[-1]).replace(" ", "_") or "image"
        ext = what(None, raw) or base.rpartition(".")[-1] or "jpeg"
        if ext == "emf":
            # For an example, see: https://bugs.launchpad.net/bugs/1224849
            self.log("Found an EMF image: %s, trying to extract embedded raster image" % fname)
            from LiuXin_alpha.utils.wmf.emf import emf_unwrap

            try:
                raw = emf_unwrap(raw)
            except Exception as e:
                self.log.exception(
                    "Failed to extract embedded raster image from EMF " "- exception message: {}".format(e.message)
                )
            else:
                ext = "png"
        base = base.rpartition(".")[0]
        if not base:
            base = "image"
        base += "." + ext
        return raw, base

    def unique_name(self: _typing.Self, base: _typing.Any) -> _typing.Any:
        exists = frozenset(itervalues(self.used))
        c = 1
        name = base
        while name in exists:
            n, e = base.rpartition(".")[0::2]
            name = "%s-%d.%s" % (n, c, e)
            c += 1
        return name

    def resize_image(self: _typing.Self, raw: _typing.Any, base: _typing.Any, max_width: _typing.Any, max_height: _typing.Any) -> tuple[_typing.Any, ...]:
        resized, img = resize_to_fit(raw, max_width, max_height)
        if resized:
            base, ext = os.path.splitext(base)
            base += "-%dx%d%s" % (max_width, max_height, ext)
            raw = image_to_data(img, fmt=ext[1:])
        return raw, base, resized

    def generate_filename(self: _typing.Self, rid: _typing.Any, base: _typing.Any = None, rid_map: _typing.Any = None, max_width: _typing.Any = None, max_height: _typing.Any = None) -> _typing.Any:
        rid_map = self.rid_map if rid_map is None else rid_map
        fname = rid_map[rid]
        key = (fname, max_width, max_height)
        ans = self.used.get(key)
        if ans is not None:
            return ans
        raw, base = self.read_image_data(fname, base=base)
        resized = False
        if max_width is not None and max_height is not None:
            raw, base, resized = self.resize_image(raw, base, max_width, max_height)
        name = self.unique_name(base)
        self.used[key] = name
        if max_width is not None and max_height is not None and not resized:
            okey = (fname, None, None)
            if okey in self.used:
                return self.used[okey]
            self.used[okey] = name
        with open(os.path.join(self.dest_dir, name), "wb") as f:
            f.write(raw)
        self.all_images.add("images/" + name)
        return name

    def pic_to_img(self: _typing.Self, pic: _typing.Any, alt: _typing.Any, parent: _typing.Any) -> _typing.Any:
        xpath, get = self.namespace.XPath, self.namespace.get
        link = None
        for hl in xpath("descendant::a:hlinkClick[@r:id]")(parent):
            link = {"id": get(hl, "r:id")}
            tgt = hl.get("tgtFrame", None)
            if tgt:
                link["target"] = tgt
            title = hl.get("tooltip", None)
            if title:
                link["title"] = title

        for pr in xpath("descendant::pic:cNvPr")(pic):
            name = pr.get("name", None)
            if name:
                name = ascii_filename(name).replace(" ", "_")
            alt = pr.get("descr", None)
            for a in xpath("descendant::a:blip[@r:embed or @r:link]")(pic):
                rid = get(a, "r:embed")
                if not rid:
                    rid = get(a, "r:link")
                if rid and rid in self.rid_map:
                    try:
                        src = self.generate_filename(rid, name)
                    except LinkedImageNotFound as err:
                        self.log.warn("Linked image: %s not found, ignoring" % err.fname)
                        continue
                    img = IMG(src="images/%s" % src)
                    img.set("alt", alt or "Image")
                    if link is not None:
                        self.links.append((img, link, self.rid_map))
                    return img

    def drawing_to_html(self: _typing.Self, drawing: _typing.Any, page: _typing.Any) -> _typing.Iterator[_typing.Any]:
        xpath, get = self.namespace.XPath, self.namespace.get
        # First process the inline pictures
        for inline in xpath("./wp:inline")(drawing):
            style, alt = get_image_properties(inline, xpath, get)
            for pic in xpath("descendant::pic:pic")(inline):
                ans = self.pic_to_img(pic, alt, inline)
                if ans is not None:
                    if style:
                        ans.set(
                            "style",
                            "; ".join("%s: %s" % (k, v) for k, v in iteritems(style)),
                        )
                    yield ans

        # Now process the floats
        for anchor in xpath("./wp:anchor")(drawing):
            style, alt = get_image_properties(anchor, xpath, get)
            self.get_float_properties(anchor, style, page)
            for pic in xpath("descendant::pic:pic")(anchor):
                ans = self.pic_to_img(pic, alt, anchor)
                if ans is not None:
                    if style:
                        ans.set(
                            "style",
                            "; ".join("%s: %s" % (k, v) for k, v in iteritems(style)),
                        )
                    yield ans

    def pict_to_html(self: _typing.Self, pict: _typing.Any, page: _typing.Any) -> _typing.Iterator[_typing.Any]:
        xpath, get = self.namespace.XPath, self.namespace.get
        # First see if we have an <hr>
        is_hr = len(pict) == 1 and get(pict[0], "o:hr") in {"t", "true"}
        if is_hr:
            style = {}
            hr = HR()
            try:
                pct = float(get(pict[0], "o:hrpct"))
            except (ValueError, TypeError, AttributeError):
                pass
            else:
                if pct > 0:
                    style["width"] = "%.3g%%" % pct
            align = get(pict[0], "o:hralign", "center")
            if align in {"left", "right"}:
                style["margin-left"] = "0" if align == "left" else "auto"
                style["margin-right"] = "auto" if align == "left" else "0"
            if style:
                hr.set("style", "; ".join(("%s:%s" % (k, v) for k, v in iteritems(style))))
            yield hr

        for imagedata in xpath("descendant::v:imagedata[@r:id]")(pict):
            rid = get(imagedata, "r:id")
            if rid in self.rid_map:
                try:
                    src = self.generate_filename(rid)
                except LinkedImageNotFound as err:
                    self.log.warn("Linked image: %s not found, ignoring" % err.fname)
                    continue
                img = IMG(src="images/%s" % src, style="display:block")
                alt = get(imagedata, "o:title")
                img.set("alt", alt or "Image")
                yield img

    def get_float_properties(self: _typing.Self, anchor: _typing.Any, style: _typing.Any, page: _typing.Any) -> None:
        xpath, get = self.namespace.XPath, self.namespace.get
        if "display" not in style:
            style["display"] = "block"
        padding = get_image_margins(anchor)
        width = float(style.get("width", "100pt")[:-2])

        page_width = page.width - page.margin_left - page.margin_right
        if page_width <= 0:
            # Ignore margins
            page_width = page.width

        hpos = get_hpos(anchor, page_width, xpath, get) + width / (2 * page_width)

        wrap_elem = None
        dofloat = False

        for child in reversed(anchor):
            bt = barename(child.tag)
            if bt in {
                "wrapNone",
                "wrapSquare",
                "wrapThrough",
                "wrapTight",
                "wrapTopAndBottom",
            }:
                wrap_elem = child
                dofloat = bt not in {"wrapNone", "wrapTopAndBottom"}
                break

        if wrap_elem is not None:
            padding.update(get_image_margins(wrap_elem))
            wt = wrap_elem.get("wrapText", None)
            hpos = 0 if wt == "right" else 1 if wt == "left" else hpos
            if dofloat:
                style["float"] = "left" if hpos < 0.65 else "right"
            else:
                ml, mr = (None, None) if hpos < 0.34 else ("auto", None) if hpos > 0.65 else ("auto", "auto")
                if ml is not None:
                    style["margin-left"] = ml
                if mr is not None:
                    style["margin-right"] = mr

        style.update(padding)

    def to_html(self: _typing.Self, elem: _typing.Any, page: _typing.Any, docx: _typing.Any, dest_dir: _typing.Any) -> _typing.Iterator[_typing.Any]:
        dest = os.path.join(dest_dir, "images")
        if not os.path.exists(dest):
            os.mkdir(dest)
        self.dest_dir, self.docx = dest, docx
        if elem.tag.endswith("}drawing"):
            for tag in self.drawing_to_html(elem, page):
                yield tag
        else:
            for tag in self.pict_to_html(elem, page):
                yield tag
