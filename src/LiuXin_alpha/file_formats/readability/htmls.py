from __future__ import annotations

import typing as _typing

import re

import lxml.html
from lxml.html import tostring

from LiuXin_alpha.file_formats.chardet import xml_to_unicode
from LiuXin_alpha.file_formats.readability.cleaners import clean_attributes, normalize_spaces
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems


def build_doc(page: _typing.Any) -> _typing.Any:
    page_unicode = xml_to_unicode(page, strip_encoding_pats=True)[0]
    return lxml.html.document_fromstring(page_unicode)


def js_re(src: _typing.Any, pattern: _typing.Any, flags: _typing.Any, repl: _typing.Any) -> _typing.Any:
    return re.compile(pattern, flags).sub(repl.replace("$", "\\"), src)


def normalize_entities(cur_title: _typing.Any) -> _typing.Any:
    entities = {
        "\u2014": "-",
        "\u2013": "-",
        "&mdash;": "-",
        "&ndash;": "-",
        "\u00A0": " ",
        "\u00AB": '"',
        "\u00BB": '"',
        "&quot;": '"',
    }
    for c, r in iteritems(entities):
        if c in cur_title:
            cur_title = cur_title.replace(c, r)
    return cur_title


def norm_title(title: _typing.Any) -> _typing.Any:
    return normalize_entities(normalize_spaces(title))


def get_title(doc: _typing.Any) -> _typing.Any:
    title_elem = doc.find(".//title")
    title = title_elem.text if title_elem is not None else None
    if not title:
        return "[no-title]"
    return norm_title(title)


def add_match(collection: _typing.Any, text: _typing.Any, orig: _typing.Any) -> None:
    text = norm_title(text)
    if len(text.split()) >= 2 and len(text) >= 15:
        if text.replace('"', "") in orig.replace('"', ""):
            collection.add(text)


def _iter_css_candidates(doc: _typing.Any, selector: _typing.Any) -> _typing.Any:
    try:
        from cssselect import HTMLTranslator  # type: ignore

        xpath_expr = HTMLTranslator().css_to_xpath(selector)
        return list(doc.xpath(xpath_expr))
    except Exception:
        # Fallback for simple selectors used by readability.
        if selector.startswith("#"):
            sid = selector[1:]
            return list(doc.xpath('.//*[@id="%s"]' % sid))
        if selector.startswith("."):
            cls = selector[1:]
            return list(
                doc.xpath(
                    './/*[contains(concat(" ", normalize-space(@class), " "), " %s ")]'
                    % cls
                )
            )
        return []


def shorten_title(doc: _typing.Any) -> _typing.Any:
    title = get_title(doc)
    if not title or title == "[no-title]":
        return ""

    orig = title
    candidates = set()

    for item in [".//h1", ".//h2", ".//h3"]:
        for elem in list(doc.iterfind(item)):
            if elem.text:
                add_match(candidates, elem.text, orig)
            if elem.text_content():
                add_match(candidates, elem.text_content(), orig)

    for item in (
        "#title",
        "#head",
        "#heading",
        ".pageTitle",
        ".news_title",
        ".title",
        ".head",
        ".heading",
        ".contentheading",
        ".small_header_red",
    ):
        for elem in _iter_css_candidates(doc, item):
            if elem.text:
                add_match(candidates, elem.text, orig)
            if elem.text_content():
                add_match(candidates, elem.text_content(), orig)

    if candidates:
        title = sorted(candidates, key=len)[-1]
    else:
        for delimiter in [" | ", " - ", " :: ", " / "]:
            if delimiter in title:
                parts = orig.split(delimiter)
                if len(parts[0].split()) >= 4:
                    title = parts[0]
                    break
                if len(parts[-1].split()) >= 4:
                    title = parts[-1]
                    break
        else:
            if ": " in title:
                parts = orig.split(": ")
                if len(parts[-1].split()) >= 4:
                    title = parts[-1]
                else:
                    title = orig.split(": ", 1)[1]

    if not 15 < len(title) < 150:
        return orig
    return title


def get_body(doc: _typing.Any) -> _typing.Any:
    for elem in list(doc.xpath(".//script | .//link | .//style")):
        parent = elem.getparent()
        if parent is not None:
            parent.remove(elem)
    root = doc.body if getattr(doc, "body", None) is not None else doc
    raw_html = tostring(root, encoding="unicode")
    return clean_attributes(raw_html)


__all__ = [
    "add_match",
    "build_doc",
    "get_body",
    "get_title",
    "js_re",
    "norm_title",
    "normalize_entities",
    "shorten_title",
]
