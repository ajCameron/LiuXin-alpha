from __future__ import with_statement

"""
CSS case-mangling transform.
"""

from lxml import etree

from LiuXin_alpha.file_formats.oeb.base import XHTML, XHTML_NS
from LiuXin_alpha.file_formats.oeb.base import CSS_MIME
from LiuXin_alpha.file_formats.oeb.base import namespace
from LiuXin_alpha.file_formats.oeb.stylizer import Stylizer

from LiuXin_alpha.utils.icu import lower as icu_lower
from LiuXin_alpha.utils.icu import title_case as icu_title
from LiuXin_alpha.utils.icu import upper as icu_upper
from LiuXin_alpha.utils.lx_libraries.liuxin_six import six_string_types

__license__ = "GPL v3"
__copyright__ = "2008, Marshall T. Vandegrift <llasram@gmail.com>"


CASE_MANGLER_CSS = """
.calibre_lowercase {
    font-variant: normal;
    font-size: 0.65em;
}
"""

TEXT_TRANSFORMS = {"capitalize", "uppercase", "lowercase"}


class CaseMangler(object):
    """
    Apply CSS case transforms - compress CSS case transforms down to a single application.
    """

    @classmethod
    def config(cls, cfg):
        return cfg

    @classmethod
    def generate(cls, opts):
        return cls()

    def __call__(self, oeb, context):
        oeb.logger.info("Applying case-transforming CSS...")
        self.oeb = oeb
        self.opts = context
        self.profile = context.source
        self.mangle_spine()

    def mangle_spine(self):
        """
        Apply the case mangling to ever element in the spine.
        :return:
        """
        local_id, href = self.oeb.manifest.generate("manglecase", "manglecase.css")
        self.oeb.manifest.add(local_id, href, CSS_MIME, data=CASE_MANGLER_CSS)
        for item in self.oeb.spine:
            html = item.data
            relhref = item.relhref(href)
            etree.SubElement(
                html.find(XHTML("head")),
                XHTML("link"),
                rel="stylesheet",
                href=relhref,
                type=CSS_MIME,
            )
            stylizer = Stylizer(html, item.href, self.oeb, self.opts, self.profile)
            self.mangle_elem(html.find(XHTML("body")), stylizer)

    def text_transform(self, transform, text):
        """
        Apply an individual transform to some text.
        :param transform:
        :param text:
        :return:
        """
        if transform == "capitalize":
            return icu_title(text)
        elif transform == "uppercase":
            return icu_upper(text)
        elif transform == "lowercase":
            return icu_lower(text)
        return text

    def split_text(self, text):
        results = [""]
        isupper = text[0].isupper()
        for char in text:
            if char.isupper() == isupper:
                results[-1] += char
            else:
                isupper = not isupper
                results.append(char)
        return results

    def smallcaps_elem(self, elem, attr):
        texts = self.split_text(getattr(elem, attr))
        setattr(elem, attr, None)
        last = elem if attr == "tail" else None
        attrib = {"class": "calibre_lowercase"}
        for text in texts:
            if text.isupper():
                if last is None:
                    elem.text = text
                else:
                    last.tail = text
            else:
                child = etree.Element(XHTML("span"), attrib=attrib)
                child.text = text.upper()
                if last is None:
                    elem.insert(0, child)
                else:
                    # addnext() moves the tail for some reason
                    tail = last.tail
                    last.addnext(child)
                    last.tail = tail
                    child.tail = None
                last = child

    def mangle_elem(self, elem, stylizer):
        """
        Apply mangling to an element.
        :param elem:
        :param stylizer:
        :return:
        """
        if not isinstance(elem.tag, six_string_types) or namespace(elem.tag) != XHTML_NS:
            return
        children = list(elem)
        style = stylizer.style(elem)
        transform = style["text-transform"]
        variant = style["font-variant"]
        if elem.text:
            if transform in TEXT_TRANSFORMS:
                elem.text = self.text_transform(transform, elem.text)
            if variant == "small-caps":
                self.smallcaps_elem(elem, "text")
        for child in children:
            self.mangle_elem(child, stylizer)
            if child.tail:
                if transform in TEXT_TRANSFORMS:
                    child.tail = self.text_transform(transform, child.tail)
                if variant == "small-caps":
                    self.smallcaps_elem(child, "tail")
