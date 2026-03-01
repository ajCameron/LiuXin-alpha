from __future__ import annotations

from pathlib import Path

import pytest

from LiuXin_alpha.file_formats.oeb.polish.check.main import _safe_line_offset
from LiuXin_alpha.file_formats.oeb.polish.check.parsing import check_ids
from LiuXin_alpha.file_formats.oeb.polish.errors import MalformedMarkup
from LiuXin_alpha.file_formats.oeb.polish.split import AbortError, split
from LiuXin_alpha.file_formats.oeb.polish.toc import TOC, create_ncx, from_links, node_from_loc
from LiuXin_alpha.utils.libraries.liuxin_etree import etree


def _xhtml_root() -> etree._Element:
    ns = "http://www.w3.org/1999/xhtml"
    return etree.Element("{%s}html" % ns, nsmap={None: ns})


def test_safe_line_offset_defaults_to_zero_for_missing_source_lines() -> None:
    elem = etree.Element("div")
    assert _safe_line_offset(elem) == 0


def test_check_ids_handles_missing_sourcelines_without_crashing() -> None:
    root = _xhtml_root()
    body = etree.SubElement(root, "{http://www.w3.org/1999/xhtml}body")
    etree.SubElement(body, "{http://www.w3.org/1999/xhtml}div", id="dup")
    etree.SubElement(body, "{http://www.w3.org/1999/xhtml}span", id="dup")

    class _Container:
        mime_map = {"index.xhtml": "application/xhtml+xml"}

        def parsed(self, name):
            assert name == "index.xhtml"
            return root

    errors = check_ids(_Container())
    assert len(errors) == 1
    assert errors[0].name == "index.xhtml"
    assert errors[0].all_locations == [("index.xhtml", 1, None)]


def test_from_links_keeps_no_fragment_links_and_skips_bad_absolute_paths() -> None:
    root1 = _xhtml_root()
    body1 = etree.SubElement(root1, "{http://www.w3.org/1999/xhtml}body")
    a1 = etree.SubElement(body1, "{http://www.w3.org/1999/xhtml}a", href="chapter2.xhtml")
    a1.text = "No frag"
    a2 = etree.SubElement(body1, "{http://www.w3.org/1999/xhtml}a", href="chapter2.xhtml#frag")
    a2.text = "With frag"
    a3 = etree.SubElement(body1, "{http://www.w3.org/1999/xhtml}a", href="C:/outside.xhtml")
    a3.text = "Bad link"

    root2 = _xhtml_root()
    body2 = etree.SubElement(root2, "{http://www.w3.org/1999/xhtml}body")
    etree.SubElement(body2, "{http://www.w3.org/1999/xhtml}div", id="frag")

    class _Container:
        spine_items = ["/tmp/ch1.xhtml"]

        def abspath_to_name(self, path):
            return Path(path).name

        def parsed(self, name):
            if name == "ch1.xhtml":
                return root1
            if name == "chapter2.xhtml":
                return root2
            raise KeyError(name)

        def href_to_name(self, href, base=None):
            if href.startswith("C:/"):
                raise ValueError("absolute windows path")
            if href.startswith("chapter2.xhtml"):
                return "chapter2.xhtml"
            return None

    toc = from_links(_Container())
    children = list(toc)
    assert len(children) == 2
    assert {c.frag for c in children} == {None, "frag"}
    assert all(c.dest_exists for c in children)


def test_node_from_loc_raises_malformed_markup_for_missing_or_bad_locs() -> None:
    no_body_root = etree.fromstring(b"<html><head/></html>")
    with pytest.raises(MalformedMarkup):
        node_from_loc(no_body_root, [0])

    body_root = etree.fromstring(b"<html><body><div/></body></html>")
    with pytest.raises(MalformedMarkup):
        node_from_loc(body_root, [2])


def test_split_reports_clean_abort_for_missing_or_invalid_xpath() -> None:
    root = etree.fromstring(b"<html><body><p id='p1'>x</p></body></html>")

    class _Container:
        def parsed(self, name):
            return root

    with pytest.raises(AbortError):
        split(_Container(), "index.xhtml", '//*[@id="missing"]')
    with pytest.raises(AbortError):
        split(_Container(), "index.xhtml", "//*[")


def test_create_ncx_defaults_to_en_when_lang_is_missing() -> None:
    toc = TOC()
    toc.add("Chapter 1", "chapter1.xhtml")
    ncx = create_ncx(toc, lambda name: name, "Book", None, "uid-1")
    assert ncx.get("{http://www.w3.org/XML/1998/namespace}lang") == "en"
