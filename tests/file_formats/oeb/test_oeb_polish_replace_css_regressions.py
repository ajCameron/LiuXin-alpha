from __future__ import annotations

from LiuXin_alpha.file_formats.oeb.polish.css import (
    filter_css,
    get_imported_sheets,
    remove_unused_css,
)
from LiuXin_alpha.file_formats.oeb.polish.replace import LinkRebaser, LinkReplacer
from LiuXin_alpha.utils.libraries.liuxin_etree import etree


class _EmptyRuleList:
    def rulesOfType(self, _kind):
        return []

    def remove(self, _rule):
        return None


class _EmptySheet:
    def __init__(self):
        self.cssRules = _EmptyRuleList()
        self.namespaces = {}


def _xhtml_with_style_and_link() -> etree._Element:
    ns = "http://www.w3.org/1999/xhtml"
    root = etree.Element("{%s}html" % ns, nsmap={None: ns})
    head = etree.SubElement(root, "{%s}head" % ns)
    style = etree.SubElement(head, "{%s}style" % ns, type="text/css")
    style.text = "p { color: red; }"
    etree.SubElement(head, "{%s}link" % ns, href="C:/outside.css")
    body = etree.SubElement(root, "{%s}body" % ns)
    etree.SubElement(body, "{%s}p" % ns, style="color: blue").text = "txt"
    return root


def test_link_replacer_ignores_invalid_absolute_paths() -> None:
    class _Container:
        def href_to_name(self, href, base):
            raise ValueError("invalid absolute path")

        def name_to_href(self, name, base):
            return name

    lr = LinkReplacer("index.xhtml", _Container(), {"old": "new"}, lambda name, frag: frag)
    href = "C:/outside.xhtml#frag"
    assert lr(href) == href
    assert lr.replaced is False


def test_link_rebaser_ignores_invalid_absolute_paths() -> None:
    class _Container:
        def href_to_name(self, href, base):
            raise ValueError("invalid absolute path")

        def name_to_href(self, name, base):
            return name

    lr = LinkRebaser(_Container(), "a.xhtml", "b.xhtml")
    href = "C:/outside.xhtml#frag"
    assert lr(href) == href
    assert lr.replaced is False


def test_get_imported_sheets_skips_bad_import_hrefs() -> None:
    class _Rule:
        href = "C:/outside.css"

    class _Rules:
        def rulesOfType(self, _kind):
            return [_Rule()]

    class _Sheet:
        cssRules = _Rules()

    class _Container:
        def href_to_name(self, href, base):
            raise ValueError("invalid absolute path")

    sheets = {"styles/main.css": _Sheet()}
    assert get_imported_sheets("styles/main.css", _Container(), sheets) == set()


def test_remove_unused_css_handles_parse_failures_and_bad_links() -> None:
    class _Container:
        mime_map = {
            "index.xhtml": "application/xhtml+xml",
            "styles/main.css": "text/css",
        }
        log = object()

        def __init__(self):
            self._root = _xhtml_with_style_and_link()
            self._sheet = _EmptySheet()

        def parsed(self, name):
            if name == "index.xhtml":
                return self._root
            if name == "styles/main.css":
                return self._sheet
            raise KeyError(name)

        def parse_css(self, text, is_declaration=False):
            raise ValueError("synthetic parse failure")

        def href_to_name(self, href, base=None):
            raise ValueError("invalid absolute path")

        def dirty(self, name):
            return None

    report_lines = []
    changed = remove_unused_css(_Container(), report=report_lines.append, remove_unused_classes=False)
    assert changed is False
    assert any("No unused CSS style rules found" in x for x in report_lines)


def test_filter_css_handles_parse_failures_gracefully() -> None:
    class _Container:
        mime_map = {"index.xhtml": "application/xhtml+xml"}

        def __init__(self):
            self._root = _xhtml_with_style_and_link()
            self.dirty_calls = []

        def parsed(self, name):
            assert name == "index.xhtml"
            return self._root

        def parse_css(self, text, is_declaration=False):
            raise ValueError("synthetic parse failure")

        def dirty(self, name):
            self.dirty_calls.append(name)

    c = _Container()
    changed = filter_css(c, {"color"})
    assert changed is False
    assert c.dirty_calls == []
