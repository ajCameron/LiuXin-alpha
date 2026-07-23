from __future__ import with_statement, print_function
from __future__ import annotations

import typing as _typing

"""
Transform XHTML/OPS-ish content into Mobipocket HTML 3.2.
"""

import re
import copy
import logging
import numbers

from lxml import etree

from LiuXin_alpha.file_formats.oeb.base import namespace, barename
from LiuXin_alpha.file_formats.oeb.base import XHTML, XHTML_NS, urlnormalize
from LiuXin_alpha.file_formats.oeb.stylizer import Stylizer
from LiuXin_alpha.file_formats.oeb.transforms.flatcss import KeyMapper
from LiuXin_alpha.file_formats.mobi.utils import convert_color_for_font_tag

try:
    from LiuXin_alpha.utils.wrappers.magick.draw import identify_data
except Exception:
    try:
        from LiuXin_alpha.utils.plugins.fallbacks.magick import Image as _FallbackImage
    except Exception:
        _FallbackImage = None

    def identify_data(data: _typing.Any) -> tuple[_typing.Any, ...]:
        if _FallbackImage is None:
            raise RuntimeError("No image identify backend is available")
        meta = _FallbackImage(data).identify()
        return meta.get("width", 0), meta.get("height", 0), meta.get("format", "unknown")

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

__license__ = "GPL v3"
__copyright__ = "2008, Marshall T. Vandegrift <llasram@gmail.cam>"

logger = logging.getLogger(__name__)

MBP_NS = "http://mobipocket.com/ns/mbp"


def MBP(name: _typing.Any) -> _typing.Any:
    return "{%s}%s" % (MBP_NS, name)


MOBI_NSMAP = {None: XHTML_NS, "mbp": MBP_NS}
INLINE_TAGS = {
    "span",
    "a",
    "code",
    "u",
    "s",
    "big",
    "strike",
    "tt",
    "font",
    "q",
    "i",
    "b",
    "em",
    "strong",
    "sup",
    "sub",
}
HEADER_TAGS = {"h1", "h2", "h3", "h4", "h5", "h6"}
# GR: Added 'caption' to both sets
NESTABLE_TAGS = {"ol", "ul", "li", "table", "tr", "td", "th", "caption"}
TABLE_TAGS = {"table", "tr", "td", "th", "caption"}

SPECIAL_TAGS = {"hr", "br"}
CONTENT_TAGS = {"img", "hr", "br"}

NOT_VTAGS = HEADER_TAGS | NESTABLE_TAGS | TABLE_TAGS | SPECIAL_TAGS | CONTENT_TAGS
LEAF_TAGS = {
    "base",
    "basefont",
    "frame",
    "link",
    "meta",
    "area",
    "br",
    "col",
    "hr",
    "img",
    "input",
    "param",
}
PAGE_BREAKS = {"always", "left", "right"}

COLLAPSE = re.compile(r"[ \t\r\n\v]+")


def asfloat(value: _typing.Any) -> _typing.Any:
    if not isinstance(value, numbers.Real):
        return 0.0
    return float(value)


def _parse_numeric(value: _typing.Any, default: float = 0.0) -> _typing.Any:
    if isinstance(value, numbers.Real):
        return float(value)
    if isinstance(value, str):
        m = re.match(r"\s*(-?\d+(?:\.\d+)?)", value)
        if m is not None:
            try:
                return float(m.group(1))
            except Exception:
                return default
    return default


class _FallbackStyle(dict):
    _DEFAULTS = {
        "display": "inline",
        "visibility": "visible",
        "float": "none",
        "text-align": "auto",
        "text-indent": 0.0,
        "margin-left": 0.0,
        "margin-right": 0.0,
        "margin-top": 0.0,
        "margin-bottom": 0.0,
        "padding-left": 0.0,
        "padding-right": 0.0,
        "padding-top": 0.0,
        "padding-bottom": 0.0,
        "page-break-before": "auto",
        "page-break-after": "auto",
        "font-size": 12.0,
        "font-style": "normal",
        "font-weight": "normal",
        "white-space": "normal",
        "background-color": "transparent",
        "color": "black",
        "font-family": "serif",
        "vertical-align": "baseline",
        "width": 0.0,
        "height": 0.0,
    }

    def __init__(self: _typing.Self, elem: _typing.Any) -> None:
        super().__init__(self._DEFAULTS)
        self._raw = {}
        tag = barename(getattr(elem, "tag", "") or "").lower()
        if tag in {"div", "p", "h1", "h2", "h3", "h4", "h5", "h6", "blockquote", "table", "tr", "td", "th"}:
            self["display"] = "block"
        if tag in {"li"}:
            self["display"] = "list-item"
        if tag in {"table"}:
            self["display"] = "table"
        if tag in {"tr"}:
            self["display"] = "table-row"
        if tag in {"td", "th"}:
            self["display"] = "table-cell"
        if tag in {"ul", "ol"}:
            self["display"] = "block"

        align = elem.attrib.get("align")
        if isinstance(align, str) and align.strip():
            self["text-align"] = align.strip().lower()
        width_attr = elem.attrib.get("width")
        if width_attr is not None:
            self._raw["width"] = width_attr
            self["width"] = _parse_numeric(width_attr, default=0.0)
        height_attr = elem.attrib.get("height")
        if height_attr is not None:
            self._raw["height"] = height_attr
            self["height"] = _parse_numeric(height_attr, default=0.0)

        bgcolor = elem.attrib.get("bgcolor")
        if bgcolor:
            self["background-color"] = bgcolor
        fgcolor = elem.attrib.get("color")
        if fgcolor:
            self["color"] = fgcolor

        self.effective_text_decoration = "none"
        self.backgroundColor = self["background-color"]
        self.height = self["height"]

    def _get(self: _typing.Self, name: _typing.Any) -> _typing.Any:
        return self._raw.get(name, self.get(name))

    def _unit_convert(self: _typing.Self, value: _typing.Any, base: int = 500) -> _typing.Any:
        return _parse_numeric(value, default=0.0)

    def cssdict(self: _typing.Self) -> dict[_typing.Any, _typing.Any]:
        return {
            "width": self._raw.get("width", "auto"),
            "height": self._raw.get("height", "auto"),
            "vertical-align": self.get("vertical-align", "baseline"),
            "border": "",
            "border-width": "",
        }


class _FallbackStylizer:
    def __init__(self: _typing.Self) -> None:
        self._cache = {}

    def style(self: _typing.Self, elem: _typing.Any) -> _typing.Any:
        ans = self._cache.get(elem)
        if ans is None:
            ans = _FallbackStyle(elem)
            self._cache[elem] = ans
        return ans


def isspace(text: _typing.Any) -> _typing.Any:
    if not text:
        return True
    if "\xa0" in text:
        return False
    return text.isspace()


class BlockState(object):
    def __init__(self: _typing.Self, body: _typing.Any) -> None:
        self.body = body
        self.nested = []
        self.para = None
        self.inline = None
        self.anchor = None
        self.vpadding = 0.0
        self.vmargin = 0.0
        self.pbreak = False
        self.istate = None
        self.content = False


class FormatState(object):
    def __init__(self: _typing.Self) -> None:
        self.rendered = False
        self.left = 0.0
        self.halign = "auto"
        self.indent = 0.0
        self.fsize = 3
        self.ids = set()
        self.italic = False
        self.bold = False
        self.strikethrough = False
        self.underline = False
        self.preserve = False
        self.pre_wrap = False
        self.family = "serif"
        self.bgcolor = "transparent"
        self.fgcolor = "black"
        self.href = None
        self.list_num = 0
        self.attrib = {}

    def __eq__(self: _typing.Self, other: _typing.Any) -> bool:
        """
        Returns equal if every aspect of the format state (size, italic, bold e.t.c is the same).
        :param other:
        :return:
        """
        return (
            self.fsize == other.fsize
            and self.italic == other.italic
            and self.bold == other.bold
            and self.href == other.href
            and self.preserve == other.preserve
            and self.pre_wrap == other.pre_wrap
            and self.family == other.family
            and self.bgcolor == other.bgcolor
            and self.fgcolor == other.fgcolor
            and self.strikethrough == other.strikethrough
            and self.underline == other.underline
        )

    def __ne__(self: _typing.Self, other: _typing.Any) -> _typing.Any:
        return not self.__eq__(other)


class MobiMLizer(object):
    def __init__(self: _typing.Self, ignore_tables: bool = False) -> None:
        self.ignore_tables = ignore_tables

    def __call__(self: _typing.Self, oeb: _typing.Any, context: _typing.Any) -> None:
        """
        Convert the book to Mobiml markup
        :param oeb:
        :param context: Has to include a dest attribute, with a fnums dict
        :return:
        """
        oeb.logger.info("Converting XHTML to Mobipocket markup...")
        self.oeb = oeb
        self.log = self.oeb.logger
        self.opts = context
        self.profile = profile = context.dest
        self.fnums = fnums = dict((v, k) for k, v in profile.fnums.items())
        self.fmap = KeyMapper(profile.fbase, profile.fbase, fnums.keys())
        self.mobimlize_spine()

    def mobimlize_spine(self: _typing.Self) -> None:
        """
        Iterate over the spine and convert every element to MOBIML
        :return:
        """
        warned_fallback = False
        for item in self.oeb.spine:
            try:
                stylizer = Stylizer(item.data, item.href, self.oeb, self.opts, self.profile)
            except ModuleNotFoundError:
                if not warned_fallback:
                    self.oeb.logger.warning(
                        "cssutils is unavailable; using simplified style fallback for MOBI generation"
                    )
                    warned_fallback = True
                stylizer = _FallbackStylizer()
            body = item.data.find(XHTML("body"))
            nroot = etree.Element(XHTML("html"), nsmap=MOBI_NSMAP)
            nbody = etree.SubElement(nroot, XHTML("body"))
            self.current_spine_item = item
            self.mobimlize_elem(body, stylizer, BlockState(nbody), [FormatState()])
            item.data = nroot
            # print etree.tostring(nroot)

    def mobimlize_font(self: _typing.Self, ptsize: _typing.Any) -> _typing.Any:
        return self.fnums[self.fmap[ptsize]]

    def mobimlize_measure(self: _typing.Self, ptsize: _typing.Any) -> _typing.Any:
        if isinstance(ptsize, six_string_types):
            return ptsize
        embase = self.profile.fbase
        if round(ptsize) < embase:
            return "%dpt" % int(round(ptsize))
        return "%dem" % int(round(ptsize / embase))

    def preize_text(self: _typing.Self, text: _typing.Any, pre_wrap: bool = False) -> _typing.Any:
        text = six_unicode(text)
        if pre_wrap:
            # Replace n consecutive spaces with n-1 NBSP + space
            text = re.sub(r" {2,}", lambda m: ("\xa0" * (len(m.group()) - 1) + " "), text)
        else:
            text = text.replace(" ", "\xa0")

        text = text.replace("\r\n", "\n")
        text = text.replace("\r", "\n")
        lines = text.split("\n")
        result = lines[:1]
        for line in lines[1:]:
            result.append(etree.Element(XHTML("br")))
            if line:
                result.append(line)
        return result

    def mobimlize_content(self: _typing.Self, tag: _typing.Any, text: _typing.Any, bstate: _typing.Any, istates: _typing.Any) -> None:
        """
        Convert text content to mobiml markup.
        :param tag:
        :param text:
        :param bstate:
        :param istates:
        :return:
        """
        if text or tag != "br":
            bstate.content = True
        istate = istates[-1]
        para = bstate.para
        if tag in SPECIAL_TAGS and not text:
            para = para if para is not None else bstate.body
        elif para is None or tag in ("td", "th"):
            body = bstate.body
            if bstate.pbreak:
                etree.SubElement(body, MBP("pagebreak"))
                bstate.pbreak = False
            bstate.istate = None
            bstate.anchor = None
            parent = bstate.nested[-1] if bstate.nested else bstate.body
            indent = istate.indent
            left = istate.left

            if isinstance(indent, six_string_types):
                indent = 0

            if indent < 0 and abs(indent) < left:
                left += indent
                indent = 0
            elif indent != 0 and abs(indent) < self.profile.fbase:
                indent = (indent / abs(indent)) * self.profile.fbase

            if tag in NESTABLE_TAGS and not istate.rendered:
                para = wrapper = etree.SubElement(parent, XHTML(tag), attrib=istate.attrib)
                bstate.nested.append(para)
                if tag == "li" and len(istates) > 1:
                    istates[-2].list_num += 1
                    para.attrib["value"] = str(istates[-2].list_num)
            elif tag in NESTABLE_TAGS and istate.rendered:
                para = wrapper = bstate.nested[-1]
            elif not self.opts.mobi_ignore_margins and left > 0 and indent >= 0:
                ems = self.profile.mobi_ems_per_blockquote
                para = wrapper = etree.SubElement(parent, XHTML("blockquote"))
                para = wrapper
                emleft = int(round(left / self.profile.fbase)) - ems
                emleft = min((emleft, 10))
                while emleft > ems / 2.0:
                    para = etree.SubElement(para, XHTML("blockquote"))
                    emleft -= ems
            else:
                para = wrapper = etree.SubElement(parent, XHTML("p"))

            bstate.inline = bstate.para = para
            vspace = bstate.vpadding + bstate.vmargin
            bstate.vpadding = bstate.vmargin = 0
            if tag not in TABLE_TAGS:
                if tag in ("ul", "ol") and vspace > 0:
                    wrapper.addprevious(etree.Element(XHTML("div"), height=self.mobimlize_measure(vspace)))
                else:
                    wrapper.attrib["height"] = self.mobimlize_measure(vspace)
                para.attrib["width"] = self.mobimlize_measure(indent)
            elif tag == "table" and vspace > 0:
                vspace = int(round(vspace / self.profile.fbase))
                while vspace > 0:
                    wrapper.addprevious(etree.Element(XHTML("br")))
                    vspace -= 1

            if istate.halign != "auto" and isinstance(istate.halign, str):
                para.attrib["align"] = istate.halign

        istate.rendered = True
        pstate = bstate.istate
        if tag in CONTENT_TAGS:
            bstate.inline = para
            pstate = bstate.istate = None
            try:
                etree.SubElement(para, XHTML(tag), attrib=istate.attrib)
            except Exception:
                logger.exception("Invalid subelement: para=%r tag=%r attrib=%r", para, tag, istate.attrib)
                raise
        elif tag in TABLE_TAGS:
            para.attrib["valign"] = "top"

        if istate.ids:
            for id_ in istate.ids:
                anchor = etree.Element(XHTML("a"), attrib={"id": id_})
                if tag == "li":
                    try:
                        last = bstate.body[-1][-1]
                    except:
                        break
                    last.insert(0, anchor)
                    anchor.tail = last.text
                    last.text = None
                else:
                    last = bstate.body[-1]
                    # We use append instead of addprevious so that inline
                    # anchors in large blocks point to the correct place. See
                    # https://bugs.launchpad.net/calibre/+bug/899831
                    # This could potentially break if inserting an anchor at
                    # this point in the markup is illegal, but I cannot think
                    # of such a case offhand.
                    if barename(last.tag) in LEAF_TAGS:
                        last.addprevious(anchor)
                    else:
                        last.append(anchor)

            istate.ids.clear()

        if not text:
            return

        if not pstate or istate != pstate:
            inline = para
            fsize = istate.fsize
            href = istate.href

            if not href:
                bstate.anchor = None
            elif pstate and pstate.href == href:
                inline = bstate.anchor
            else:
                inline = etree.SubElement(inline, XHTML("a"), href=href)
                bstate.anchor = inline

            if fsize != 3:
                inline = etree.SubElement(inline, XHTML("font"), size=str(fsize))

            if istate.family == "monospace":
                inline = etree.SubElement(inline, XHTML("tt"))

            if istate.italic:
                inline = etree.SubElement(inline, XHTML("i"))

            if istate.bold:
                inline = etree.SubElement(inline, XHTML("b"))

            if istate.bgcolor is not None and istate.bgcolor != "transparent":
                inline = etree.SubElement(
                    inline,
                    XHTML("span"),
                    bgcolor=convert_color_for_font_tag(istate.bgcolor),
                )

            if istate.fgcolor != "black":
                inline = etree.SubElement(
                    inline,
                    XHTML("font"),
                    color=convert_color_for_font_tag(istate.fgcolor),
                )

            if istate.strikethrough:
                inline = etree.SubElement(inline, XHTML("s"))

            if istate.underline:
                inline = etree.SubElement(inline, XHTML("u"))
            bstate.inline = inline
        bstate.istate = istate
        inline = bstate.inline
        content = self.preize_text(text, pre_wrap=istate.pre_wrap) if istate.preserve or istate.pre_wrap else [text]
        for item in content:
            if isinstance(item, six_string_types):
                if len(inline) == 0:
                    inline.text = (inline.text or "") + item
                else:
                    last = inline[-1]
                    last.tail = (last.tail or "") + item
            else:
                inline.append(item)

    def mobimlize_elem(self: _typing.Self, elem: _typing.Any, stylizer: _typing.Any, bstate: _typing.Any, istates: _typing.Any, ignore_valign: bool = False) -> None:
        if not isinstance(elem.tag, six_string_types) or namespace(elem.tag) != XHTML_NS:
            return
        style = stylizer.style(elem)
        # <mbp:frame-set/> does not exist lalalala
        if (
            style["display"] in ("none", "oeb-page-head", "oeb-page-foot") or style["visibility"] == "hidden"
        ) and elem.get("data-calibre-jacket-searchable-tags", None) != "1":
            id_ = elem.get("id", None)
            if id_:
                # Keep anchors so people can use display:none
                # to generate hidden TOCs
                tail = elem.tail
                elem.clear()
                elem.text = None
                elem.set("id", id_)
                elem.tail = tail
                elem.tag = XHTML("a")
            else:
                return
        tag = barename(elem.tag)
        istate = copy.copy(istates[-1])
        istate.rendered = False
        istate.list_num = 0

        if tag == "ol" and "start" in elem.attrib:
            try:
                istate.list_num = int(elem.attrib["start"]) - 1
            except:
                pass

        istates.append(istate)
        left = 0
        display = style["display"]

        if display == "table-cell":
            display = "inline"
        elif display.startswith("table"):
            display = "block"

        isblock = not display.startswith("inline") and style["display"] != "none"
        isblock = isblock and style["float"] == "none"
        isblock = isblock and tag != "br"

        if isblock:
            bstate.para = None
            istate.halign = style["text-align"]
            rawti = style._get("text-indent")
            istate.indent = style["text-indent"]
            if hasattr(rawti, "strip") and "%" in rawti:
                # We have a percentage text indent, these can come out looking
                # too large if the user chooses a wide output profile like
                # tablet
                istate.indent = min(style._unit_convert(rawti, base=500), istate.indent)
            if style["margin-left"] == "auto" and style["margin-right"] == "auto":
                istate.halign = "center"
            margin = asfloat(style["margin-left"])
            padding = asfloat(style["padding-left"])
            if tag != "body":
                left = margin + padding
            istate.left += left
            vmargin = asfloat(style["margin-top"])
            bstate.vmargin = max((bstate.vmargin, vmargin))
            vpadding = asfloat(style["padding-top"])
            if vpadding > 0:
                bstate.vpadding += bstate.vmargin
                bstate.vmargin = 0
                bstate.vpadding += vpadding
        elif not istate.href:
            margin = asfloat(style["margin-left"])
            padding = asfloat(style["padding-left"])
            lspace = margin + padding
            if lspace > 0:
                spaces = int(round((lspace * 3) / style["font-size"]))
                elem.text = ("\xa0" * spaces) + (elem.text or "")
            margin = asfloat(style["margin-right"])
            padding = asfloat(style["padding-right"])
            rspace = margin + padding
            if rspace > 0:
                spaces = int(round((rspace * 3) / style["font-size"]))
                if len(elem) == 0:
                    elem.text = (elem.text or "") + ("\xa0" * spaces)
                else:
                    last = elem[-1]
                    last.text = (last.text or "") + ("\xa0" * spaces)

        if bstate.content and style["page-break-before"] in PAGE_BREAKS:
            bstate.pbreak = True

        istate.fsize = self.mobimlize_font(style["font-size"])
        istate.italic = True if style["font-style"] == "italic" else False
        weight = style["font-weight"]
        istate.bold = weight in ("bold", "bolder") or asfloat(weight) > 400
        istate.preserve = style["white-space"] == "pre"
        istate.pre_wrap = style["white-space"] == "pre-wrap"
        istate.bgcolor = style["background-color"]
        istate.fgcolor = style["color"]
        istate.strikethrough = style.effective_text_decoration == "line-through"
        istate.underline = style.effective_text_decoration == "underline"

        ff = style["font-family"].lower() if hasattr(style["font-family"], "lower") else ""

        if "monospace" in ff or "courier" in ff or ff.endswith(" mono"):
            istate.family = "monospace"
        elif "sans-serif" in ff or "sansserif" in ff or "verdana" in ff or "arial" in ff or "helvetica" in ff:
            istate.family = "sans-serif"
        else:
            istate.family = "serif"

        if "id" in elem.attrib:
            istate.ids.add(elem.attrib["id"])
        if "name" in elem.attrib:
            istate.ids.add(elem.attrib["name"])
        if tag == "a" and "href" in elem.attrib:
            istate.href = elem.attrib["href"]
        istate.attrib.clear()
        if tag == "img" and "src" in elem.attrib:
            istate.attrib["src"] = elem.attrib["src"]
            istate.attrib["align"] = "baseline"
            cssdict = style.cssdict()
            valign = cssdict.get("vertical-align", None)
            if valign in ("top", "bottom", "middle"):
                istate.attrib["align"] = valign

            for prop in ("width", "height"):
                if cssdict[prop] != "auto":
                    value = style[prop]
                    if value == getattr(self.profile, prop):
                        result = "100%"
                    else:
                        # Amazon's renderer does not support
                        # img sizes in units other than px
                        # See #7520 for test case
                        try:
                            pixs = int(round(float(value) / (72.0 / self.profile.dpi)))
                        except:
                            continue
                        result = str(pixs)
                    istate.attrib[prop] = result

            if "width" not in istate.attrib or "height" not in istate.attrib:
                href = self.current_spine_item.abshref(elem.attrib["src"])
                try:
                    item = self.oeb.manifest.hrefs[urlnormalize(href)]
                except:
                    self.oeb.logger.warn("Failed to find image:", href)
                else:
                    try:
                        width, height = identify_data(item.data)[:2]
                    except:
                        self.oeb.logger.warn("Invalid image:", href)
                    else:
                        if "width" not in istate.attrib and "height" not in istate.attrib:
                            istate.attrib["width"] = str(width)
                            istate.attrib["height"] = str(height)
                        else:
                            ar = float(width) / float(height)
                            if "width" not in istate.attrib:
                                try:
                                    width = int(istate.attrib["height"]) * ar
                                except:
                                    pass
                                istate.attrib["width"] = str(int(width))
                            else:
                                try:
                                    height = int(istate.attrib["width"]) / ar
                                except:
                                    pass
                                istate.attrib["height"] = str(int(height))
                        item.unload_data_from_memory()
        elif tag == "hr" and asfloat(style["width"]) > 0:
            prop = style["width"] / self.profile.width
            istate.attrib["width"] = "%d%%" % int(round(prop * 100))
        elif display == "table":
            tag = "table"
        elif display == "table-row":
            tag = "tr"
        elif display == "table-cell":
            tag = "td"
        if tag in TABLE_TAGS and self.ignore_tables:
            tag = "span" if tag == "td" else "div"

        if tag in ("table", "td", "tr"):
            col = style.backgroundColor
            if col:
                elem.set("bgcolor", col)
            css = style.cssdict()
            if "border" in css or "border-width" in css:
                elem.set("border", "1")
        if tag in TABLE_TAGS:
            for attr in ("rowspan", "colspan", "width", "border", "scope", "bgcolor"):
                if attr in elem.attrib:
                    istate.attrib[attr] = elem.attrib[attr]

        if tag == "q":
            t = elem.text
            if not t:
                t = ""
            elem.text = "\u201c" + t
            t = elem.tail
            if not t:
                t = ""
            elem.tail = "\u201d" + t

        text = None

        if elem.text:
            if istate.preserve or istate.pre_wrap:
                text = elem.text
            elif (
                len(elem) > 0
                and isspace(elem.text)
                and hasattr(elem[0].tag, "rpartition")
                and elem[0].tag.rpartition("}")[-1] not in INLINE_TAGS
            ):
                text = None
            else:
                text = COLLAPSE.sub(" ", elem.text)

        valign = style["vertical-align"]
        not_baseline = valign in (
            "super",
            "sub",
            "text-top",
            "text-bottom",
            "top",
            "bottom",
        ) or (isinstance(valign, (float, int)) and abs(valign) != 0)
        issup = valign in ("super", "text-top", "top") or (isinstance(valign, (float, int)) and valign > 0)
        vtag = "sup" if issup else "sub"
        if not_baseline and not ignore_valign and tag not in NOT_VTAGS and not isblock:
            nroot = etree.Element(XHTML("html"), nsmap=MOBI_NSMAP)
            vbstate = BlockState(etree.SubElement(nroot, XHTML("body")))
            vbstate.para = etree.SubElement(vbstate.body, XHTML("p"))
            self.mobimlize_elem(elem, stylizer, vbstate, istates, ignore_valign=True)
            if len(istates) > 0:
                istates.pop()
            if len(istates) == 0:
                istates.append(FormatState())
            at_start = bstate.para is None
            if at_start:
                self.mobimlize_content("span", "", bstate, istates)
            parent = bstate.para if bstate.inline is None else bstate.inline
            if parent is not None:
                vtag = etree.SubElement(parent, XHTML(vtag))
                vtag = etree.SubElement(vtag, XHTML("small"))
                # Add anchors
                for child in vbstate.body:
                    if child is not vbstate.para:
                        vtag.append(child)
                    else:
                        break
                if vbstate.para is not None:
                    if vbstate.para.text:
                        vtag.text = vbstate.para.text
                    for child in vbstate.para:
                        vtag.append(child)
                return

        if tag == "blockquote":
            old_mim = self.opts.mobi_ignore_margins
            self.opts.mobi_ignore_margins = False

        if (
            text
            or tag in CONTENT_TAGS
            or tag in NESTABLE_TAGS
            or (
                # We have an id but no text and no children, the id should still
                # be added.
                istate.ids
                and tag in ("a", "span", "i", "b", "u")
                and len(elem) == 0
            )
        ):
            if tag == "li" and len(istates) > 1 and "value" in elem.attrib:
                try:
                    value = int(elem.attrib["value"])
                    istates[-2].list_num = value - 1
                except:
                    pass
            self.mobimlize_content(tag, text, bstate, istates)

        for child in elem:
            self.mobimlize_elem(child, stylizer, bstate, istates)
            tail = None
            if child.tail:
                if istate.preserve or istate.pre_wrap:
                    tail = child.tail
                elif bstate.para is None and isspace(child.tail):
                    tail = None
                else:
                    tail = COLLAPSE.sub(" ", child.tail)
            if tail:
                self.mobimlize_content(tag, tail, bstate, istates)

        if tag == "blockquote":
            self.opts.mobi_ignore_margins = old_mim

        if bstate.content and style["page-break-after"] in PAGE_BREAKS:
            bstate.pbreak = True
        if isblock:
            para = bstate.para
            if para is not None and para.text == "\xa0" and len(para) < 1:
                if style.height > 2:
                    para.getparent().replace(para, etree.Element(XHTML("br")))
                else:
                    # This is too small to be rendered effectively, drop it
                    para.getparent().remove(para)
            bstate.para = None
            bstate.istate = None
            vmargin = asfloat(style["margin-bottom"])
            bstate.vmargin = max((bstate.vmargin, vmargin))
            vpadding = asfloat(style["padding-bottom"])
            if vpadding > 0:
                bstate.vpadding += bstate.vmargin
                bstate.vmargin = 0
                bstate.vpadding += vpadding
        if bstate.nested and bstate.nested[-1].tag == elem.tag:
            bstate.nested.pop()
        istates.pop()
