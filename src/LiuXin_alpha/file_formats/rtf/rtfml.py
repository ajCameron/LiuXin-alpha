# -*- coding: utf-8 -*-

"""
Transform OEB content into RTF markup.
"""

from __future__ import annotations

import os
import re
from io import BytesIO

from lxml import etree

from LiuXin_alpha.metadata.utils import authors_to_string
from LiuXin_alpha.utils.libraries.liuxin_six import six_cStringIO, six_string_types

try:
    from LiuXin_alpha.utils.wrappers.magick.draw import identify_data as _identify_data_backend
except Exception:
    _identify_data_backend = None

try:
    from LiuXin_alpha.utils.plugins.fallbacks.magick import Image as _FallbackImage
except Exception:
    _FallbackImage = None

try:
    from PIL import Image as _PILImage
except Exception:
    _PILImage = None


__license__ = "GPL 3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"

TAGS = {
    "b": "\\b",
    "del": "\\deleted",
    "h1": "\\s1 \\afs32",
    "h2": "\\s2 \\afs28",
    "h3": "\\s3 \\afs28",
    "h4": "\\s4 \\afs23",
    "h5": "\\s5 \\afs23",
    "h6": "\\s6 \\afs21",
    "i": "\\i",
    "li": "\t",
    "p": "\t",
    "sub": "\\sub",
    "sup": "\\super",
    "u": "\\ul",
}

SINGLE_TAGS = {
    "br": "\n{\\line }\n",
}

STYLES = [
    ("font-weight", {"bold": "\\b", "bolder": "\\b"}),
    ("font-style", {"italic": "\\i"}),
    ("text-align", {"center": "\\qc", "left": "\\ql", "right": "\\qr"}),
    ("text-decoration", {"line-through": "\\strike", "underline": "\\ul"}),
]

BLOCK_TAGS = [
    "div",
    "p",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "li",
]

BLOCK_STYLES = ["block"]


def _meta_value(raw):
    return getattr(raw, "value", raw)


def _ensure_bytes(data) -> bytes:
    if isinstance(data, bytes):
        return data
    if isinstance(data, bytearray):
        return bytes(data)
    if isinstance(data, str):
        return data.encode("utf-8", "replace")
    if hasattr(data, "read"):
        return _ensure_bytes(data.read())
    return bytes(data)


def _convert_image_to_jpeg_bytes(data: bytes) -> bytes:
    """
    Best-effort conversion to JPEG bytes for RTF's `\\jpegblip`.
    """
    raw = _ensure_bytes(data)
    if _PILImage is not None:
        try:
            with _PILImage.open(BytesIO(raw)) as image:
                if image.mode not in ("RGB", "L"):
                    image = image.convert("RGB")
                out = BytesIO()
                image.save(out, "JPEG")
                return out.getvalue()
        except Exception:
            pass
    if _FallbackImage is not None:
        try:
            with _FallbackImage(raw) as image:
                return image.to_bytes(format="jpeg")
        except Exception:
            pass
    return raw


def _identify_data(data: bytes):
    raw = _ensure_bytes(data)
    if _identify_data_backend is not None:
        try:
            return _identify_data_backend(raw)
        except Exception:
            pass
    if _FallbackImage is not None:
        try:
            with _FallbackImage(raw) as image:
                meta = image.identify()
            return meta.get("width", 0), meta.get("height", 0), meta.get("format", "unknown")
        except Exception:
            pass
    if _PILImage is not None:
        try:
            with _PILImage.open(BytesIO(raw)) as image:
                return image.width, image.height, (image.format or "unknown")
        except Exception:
            pass
    return 0, 0, "unknown"


def txt2rtf(text):
    if text is None:
        return ""
    if isinstance(text, bytes):
        text = text.decode("utf-8", "replace")
    if not isinstance(text, str):
        text = str(text)

    # Escape control characters in plain text first. Backslash must go first.
    text = text.replace("\\", r"\'5c")
    text = text.replace("{", r"\'7b")
    text = text.replace("}", r"\'7d")

    buf = six_cStringIO()
    for char in text:
        val = ord(char)
        if val == 160:
            buf.write("\\~")
        elif val <= 127:
            buf.write(char)
        else:
            buf.write(r"\u{0:d}?".format(val))
    return buf.getvalue()


class RTFMLizer(object):
    def __init__(self, log):
        self.log = log

    def extract_content(self, oeb_book, opts):
        self.log.info("Converting XHTML to RTF markup...")
        self.oeb_book = oeb_book
        self.opts = opts
        return self.mlize_spine()

    def mlize_spine(self):
        from LiuXin_alpha.file_formats.oeb.base import XHTML
        from LiuXin_alpha.file_formats.oeb.stylizer import Stylizer

        output = self.header()
        if "titlepage" in self.oeb_book.guide:
            href = self.oeb_book.guide["titlepage"].href
            item = self.oeb_book.manifest.hrefs[href]
            if item.spine_position is None:
                stylizer = Stylizer(item.data, item.href, self.oeb_book, self.opts, self.opts.output_profile)
                self.currently_dumping_item = item
                output += self.dump_text(item.data.find(XHTML("body")), stylizer)
                output += "{\\page }"

        for item in self.oeb_book.spine:
            self.log.debug("Converting %s to RTF markup..." % item.href)
            # Comments containing `--` can make fromstring() fail.
            content = re.sub(
                r"<!--.*?-->",
                "",
                etree.tostring(item.data, encoding="unicode"),
                flags=re.DOTALL,
            )
            content = self.remove_newlines(content)
            content = self.remove_tabs(content)
            content = etree.fromstring(content)
            stylizer = Stylizer(content, item.href, self.oeb_book, self.opts, self.opts.output_profile)
            self.currently_dumping_item = item
            output += self.dump_text(content.find(XHTML("body")), stylizer)
            output += "{\\page }"

        output += self.footer()
        output = self.insert_images(output)
        output = self.clean_text(output)
        return output

    def remove_newlines(self, text):
        self.log.debug("\tRemove newlines for processing...")
        text = text.replace("\r\n", " ")
        text = text.replace("\n", " ")
        text = text.replace("\r", " ")
        return text

    def remove_tabs(self, text):
        self.log.debug("\tReplace tabs with space for processing...")
        return text.replace("\t", " ")

    def header(self):
        title_items = getattr(self.oeb_book.metadata, "title", ()) or ()
        creator_items = getattr(self.oeb_book.metadata, "creator", ()) or ()
        title = _meta_value(title_items[0]) if title_items else "Unknown"
        creators = [_meta_value(x) for x in creator_items]
        author = authors_to_string(creators) if creators else "Unknown"
        header = "{\\rtf1{\\info{\\title %s}{\\author %s}}\\ansi\\ansicpg1252\\deff0\\deflang1033\n" % (
            title,
            author,
        )
        return (
            header
            + "{\\fonttbl{\\f0\\froman\\fprq2\\fcharset128 Times New Roman;}{\\f1\\froman\\fprq2\\fcharset128 Times New Roman;}{\\f2\\fswiss\\fprq2\\fcharset128 Arial;}{\\f3\\fnil\\fprq2\\fcharset128 Arial;}{\\f4\\fnil\\fprq2\\fcharset128 MS Mincho;}{\\f5\\fnil\\fprq2\\fcharset128 Tahoma;}{\\f6\\fnil\\fprq0\\fcharset128 Tahoma;}}\n"
            "{\\stylesheet{\\ql \\li0\\ri0\\nowidctlpar\\wrapdefault\\faauto\\rin0\\lin0\\itap0 \\rtlch\\fcs1 \\af25\\afs24\\alang1033 \\ltrch\\fcs0 \\fs24\\lang1033\\langfe255\\cgrid\\langnp1033\\langfenp255 \\snext0 Normal;}\n"
            "{\\s1\\ql \\li0\\ri0\\sb240\\sa120\\keepn\\nowidctlpar\\wrapdefault\\faauto\\outlinelevel0\\rin0\\lin0\\itap0 \\rtlch\\fcs1 \\ab\\af0\\afs32\\alang1033 \\ltrch\\fcs0 \\b\\fs32\\lang1033\\langfe255\\loch\\f1\\hich\\af1\\dbch\\af26\\cgrid\\langnp1033\\langfenp255 \\sbasedon15 \\snext16 \\slink21 heading 1;}\n"
            "{\\s2\\ql \\li0\\ri0\\sb240\\sa120\\keepn\\nowidctlpar\\wrapdefault\\faauto\\outlinelevel1\\rin0\\lin0\\itap0 \\rtlch\\fcs1 \\ab\\ai\\af0\\afs28\\alang1033 \\ltrch\\fcs0 \\b\\i\\fs28\\lang1033\\langfe255\\loch\\f1\\hich\\af1\\dbch\\af26\\cgrid\\langnp1033\\langfenp255 \\sbasedon15 \\snext16 \\slink22 heading 2;}\n"
            "{\\s3\\ql \\li0\\ri0\\sb240\\sa120\\keepn\\nowidctlpar\\wrapdefault\\faauto\\outlinelevel2\\rin0\\lin0\\itap0 \\rtlch\\fcs1 \\ab\\af0\\afs28\\alang1033 \\ltrch\\fcs0 \\b\\fs28\\lang1033\\langfe255\\loch\\f1\\hich\\af1\\dbch\\af26\\cgrid\\langnp1033\\langfenp255 \\sbasedon15 \\snext16 \\slink23 heading 3;}\n"
            "{\\s4\\ql \\li0\\ri0\\sb240\\sa120\\keepn\\nowidctlpar\\wrapdefault\\faauto\\outlinelevel3\\rin0\\lin0\\itap0 \\rtlch\\fcs1 \\ab\\ai\\af0\\afs23\\alang1033 \\ltrch\\fcs0\\b\\i\\fs23\\lang1033\\langfe255\\loch\\f1\\hich\\af1\\dbch\\af26\\cgrid\\langnp1033\\langfenp255 \\sbasedon15 \\snext16 \\slink24 heading 4;}\n"
            "{\\s5\\ql \\li0\\ri0\\sb240\\sa120\\keepn\\nowidctlpar\\wrapdefault\\faauto\\outlinelevel4\\rin0\\lin0\\itap0 \\rtlch\\fcs1 \\ab\\af0\\afs23\\alang1033 \\ltrch\\fcs0 \\b\\fs23\\lang1033\\langfe255\\loch\\f1\\hich\\af1\\dbch\\af26\\cgrid\\langnp1033\\langfenp255 \\sbasedon15 \\snext16 \\slink25 heading 5;}\n"
            "{\\s6\\ql \\li0\\ri0\\sb240\\sa120\\keepn\\nowidctlpar\\wrapdefault\\faauto\\outlinelevel5\\rin0\\lin0\\itap0 \\rtlch\\fcs1 \\ab\\af0\\afs21\\alang1033 \\ltrch\\fcs0 \\b\\fs21\\lang1033\\langfe255\\loch\\f1\\hich\\af1\\dbch\\af26\\cgrid\\langnp1033\\langfenp255 \\sbasedon15 \\snext16 \\slink26 heading 6;}}\n"
        )

    def footer(self):
        return " }"

    def insert_images(self, text):
        from LiuXin_alpha.file_formats.oeb.base import OEB_RASTER_IMAGES

        for item in self.oeb_book.manifest:
            if item.media_type in OEB_RASTER_IMAGES:
                src = item.href
                try:
                    data, width, height = self.image_to_hexstring(item.data)
                except Exception:
                    log_warn = getattr(self.log, "warning", None) or getattr(self.log, "warn", None)
                    if log_warn is not None:
                        log_warn("Image %s is corrupted, ignoring" % item.href)
                    repl = "\n\n"
                else:
                    repl = "\n\n{\\*\\shppict{\\pict\\jpegblip\\picw%i\\pich%i \n%s\n}}\n\n" % (
                        width,
                        height,
                        data,
                    )
                text = text.replace("SPECIAL_IMAGE-%s-REPLACE_ME" % src, repl)
        return text

    def image_to_hexstring(self, data):
        data = _convert_image_to_jpeg_bytes(_ensure_bytes(data))
        width, height = _identify_data(data)[:2]
        raw_hex = data.hex()

        # Images must be wrapped so each line is no longer than 128 chars.
        hex_lines = [raw_hex[i : i + 128] for i in range(0, len(raw_hex), 128)]
        hex_string = "\n".join(hex_lines)
        return hex_string, width, height

    def clean_text(self, text):
        text = re.sub("%s{3,}" % os.linesep, "%s%s" % (os.linesep, os.linesep), text)
        text = re.sub("[ ]{2,}", " ", text)
        text = re.sub("\t{2,}", "\t", text)
        text = re.sub("\t ", "\t", text)
        text = re.sub(r"(\{\\line \}\s*){3,}", r"{\\line }{\\line }", text)
        text = text.replace("\xa0", " ")
        text = text.replace("\n\r", "\n")
        return text

    def dump_text(self, elem, stylizer, tag_stack=None):
        from LiuXin_alpha.file_formats.oeb.base import XHTML_NS, barename, namespace, urlnormalize

        if tag_stack is None:
            tag_stack = []

        if not isinstance(elem.tag, six_string_types) or namespace(elem.tag) != XHTML_NS:
            p = elem.getparent()
            if p is not None and isinstance(p.tag, six_string_types) and namespace(p.tag) == XHTML_NS and elem.tail:
                return elem.tail
            return ""

        text = ""
        style = stylizer.style(elem)

        if style["display"] in ("none", "oeb-page-head", "oeb-page-foot") or style["visibility"] == "hidden":
            if hasattr(elem, "tail") and elem.tail:
                return elem.tail
            return ""

        tag = barename(elem.tag)
        tag_count = 0

        if tag in BLOCK_TAGS or style["display"] in BLOCK_STYLES:
            if "block" not in tag_stack:
                tag_count += 1
                tag_stack.append("block")

        if tag == "img":
            src = elem.get("src")
            if src:
                src = urlnormalize(self.currently_dumping_item.abshref(src))
                block_start = ""
                block_end = ""
                if "block" not in tag_stack:
                    block_start = "{\\par\\pard\\hyphpar "
                    block_end = "}"
                text += "%s SPECIAL_IMAGE-%s-REPLACE_ME %s" % (block_start, src, block_end)

        single_tag = SINGLE_TAGS.get(tag)
        if single_tag:
            text += single_tag

        rtf_tag = TAGS.get(tag)
        if rtf_tag and rtf_tag not in tag_stack:
            tag_count += 1
            text += "{%s\n" % rtf_tag
            tag_stack.append(rtf_tag)

        for style_key, style_map in STYLES:
            style_tag = style_map.get(style[style_key])
            if style_tag and style_tag not in tag_stack:
                tag_count += 1
                text += "{%s\n" % style_tag
                tag_stack.append(style_tag)

        if hasattr(elem, "text") and elem.text:
            text += txt2rtf(elem.text)

        for item in elem:
            text += self.dump_text(item, stylizer, tag_stack)

        for _ in range(0, tag_count):
            end_tag = tag_stack.pop()
            if end_tag != "block":
                if tag in BLOCK_TAGS:
                    text += "\\par\\pard\\plain\\hyphpar}"
                else:
                    text += "}"

        if hasattr(elem, "tail") and elem.tail:
            if "block" in tag_stack:
                text += "%s" % txt2rtf(elem.tail)
            else:
                text += "{\\par\\pard\\hyphpar %s}" % txt2rtf(elem.tail)

        return text
