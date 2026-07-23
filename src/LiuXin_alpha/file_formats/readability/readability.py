#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import annotations

import typing as _typing

import re
import sys
from collections import defaultdict

from lxml.etree import tostring
from lxml.html import document_fromstring, fragment_fromstring, tostring as htostring

from LiuXin_alpha.file_formats.readability.cleaners import clean_attributes, html_cleaner
from LiuXin_alpha.file_formats.readability.htmls import (
    build_doc,
    get_body,
    get_title,
    shorten_title,
)


def tounicode(tree_or_node: _typing.Any, **kwargs: _typing.Any) -> _typing.Any:
    kwargs["encoding"] = "unicode"
    return htostring(tree_or_node, **kwargs)


REGEXES = {
    "unlikelyCandidatesRe": re.compile(
        "combx|comment|community|disqus|extra|foot|header|menu|remark|rss|shoutbox|sidebar|sponsor|ad-break|agegate|pagination|pager|popup|tweet|twitter",
        re.I,
    ),
    "okMaybeItsACandidateRe": re.compile("and|article|body|column|main|shadow", re.I),
    "positiveRe": re.compile(
        "article|body|content|entry|hentry|main|page|pagination|post|text|blog|story",
        re.I,
    ),
    "negativeRe": re.compile(
        "combx|comment|com-|contact|foot|footer|footnote|masthead|media|meta|outbrain|promo|related|scroll|shoutbox|sidebar|sponsor|shopping|tags|tool|widget",
        re.I,
    ),
    "divToPElementsRe": re.compile("<(a|blockquote|dl|div|img|ol|p|pre|table|ul)", re.I),
}


def describe(node: _typing.Any, depth: int = 1) -> _typing.Any:
    if not hasattr(node, "tag"):
        return "[%s]" % type(node)
    name = node.tag
    if node.get("id", ""):
        name += "#" + node.get("id")
    if node.get("class", ""):
        name += "." + node.get("class").replace(" ", ".")
    if name[:4] in ["div#", "div."]:
        name = name[3:]
    if depth and node.getparent() is not None:
        return name + " - " + describe(node.getparent(), depth - 1)
    return name


def to_int(x: _typing.Any) -> _typing.Any:
    if not x:
        return None
    x = x.strip()
    if x.endswith("px"):
        return int(x[:-2])
    if x.endswith("em"):
        return int(x[:-2]) * 12
    return int(x)


def clean(text: _typing.Any) -> _typing.Any:
    text = re.sub(r"\s*\n\s*", "\n", text)
    text = re.sub(r"[ \t]{2,}", " ", text)
    return text.strip()


def text_length(i: _typing.Any) -> _typing.Any:
    return len(clean(i.text_content() or ""))


class Unparseable(ValueError):
    pass


class Document:
    TEXT_LENGTH_THRESHOLD = 25
    RETRY_LENGTH = 250

    def __init__(self: _typing.Self, input: _typing.Any, log: _typing.Any, **options: _typing.Any) -> None:
        self.input = input
        self.options = defaultdict(lambda: None)
        for key, value in options.items():
            self.options[key] = value
        self.html = None
        self.log = log
        self.keep_elements = set()

    def _html(self: _typing.Self, force: bool = False) -> _typing.Any:
        if force or self.html is None:
            self.html = self._parse(self.input)
            path = self.options["keep_elements"]
            if path is not None:
                self.keep_elements = set(self.html.xpath(path))

        return self.html

    def _parse(self: _typing.Self, input: _typing.Any) -> _typing.Any:
        doc = build_doc(input)
        doc = html_cleaner.clean_html(doc)
        base_href = self.options["url"]
        if base_href:
            doc.make_links_absolute(base_href, resolve_base_href=True)
        else:
            doc.resolve_base_href()
        return doc

    def content(self: _typing.Self) -> _typing.Any:
        return get_body(self._html(True))

    def title(self: _typing.Self) -> _typing.Any:
        return get_title(self._html(True))

    def short_title(self: _typing.Self) -> _typing.Any:
        return shorten_title(self._html(True))

    def summary(self: _typing.Self) -> _typing.Any:
        try:
            ruthless = True
            while True:
                self._html(True)

                for item in self.tags(self.html, "script", "style"):
                    item.drop_tree()
                for item in self.tags(self.html, "body"):
                    item.set("id", "readabilityBody")
                if ruthless:
                    self.remove_unlikely_candidates()
                self.transform_misused_divs_into_paragraphs()
                candidates = self.score_paragraphs()

                best_candidate = self.select_best_candidate(candidates)
                if best_candidate:
                    article = self.get_article(candidates, best_candidate)
                else:
                    if ruthless:
                        self.log.debug("ruthless removal did not work. ")
                        ruthless = False
                        self.debug("ended up stripping too much - going for a safer _parse")
                        continue
                    self.log.debug("Ruthless and lenient parsing did not work. Returning raw html")
                    article = self.html.find("body")
                    if article is None:
                        article = self.html

                cleaned_article = self.sanitize(article, candidates)
                min_len = self.options["retry_length"] or self.RETRY_LENGTH
                if ruthless and len(cleaned_article or "") < min_len:
                    ruthless = False
                    continue
                return cleaned_article
        except Exception as err:
            self.log.exception("error getting summary: ")
            raise Unparseable(str(err))

    def get_article(self: _typing.Self, candidates: _typing.Any, best_candidate: _typing.Any) -> _typing.Any:
        sibling_score_threshold = max([10, best_candidate["content_score"] * 0.2])
        output = document_fromstring("<div/>")
        parent = output.xpath("//div")[0]
        best_elem = best_candidate["elem"]
        for sibling in best_elem.getparent().getchildren():
            append = False
            if sibling is best_elem:
                append = True
            if sibling in candidates and candidates[sibling]["content_score"] >= sibling_score_threshold:
                append = True
            if sibling in self.keep_elements:
                append = True

            if sibling.tag == "p":
                link_density = self.get_link_density(sibling)
                node_content = sibling.text or ""
                node_length = len(node_content)

                if node_length > 80 and link_density < 0.25:
                    append = True
                elif node_length < 80 and link_density == 0 and re.search(r"\.( |$)", node_content):
                    append = True

            if append:
                parent.append(sibling)
        return output.find("body")

    def select_best_candidate(self: _typing.Self, candidates: _typing.Any) -> _typing.Any:
        sorted_candidates = sorted(candidates.values(), key=lambda x: x["content_score"], reverse=True)
        for candidate in sorted_candidates[:5]:
            elem = candidate["elem"]
            self.debug("Top 5 : %6.3f %s" % (candidate["content_score"], describe(elem)))

        if len(sorted_candidates) == 0:
            return None

        return sorted_candidates[0]

    def get_link_density(self: _typing.Self, elem: _typing.Any) -> _typing.Any:
        link_length = 0
        for item in elem.findall(".//a"):
            link_length += text_length(item)
        total_length = text_length(elem)
        return float(link_length) / max(total_length, 1)

    def score_paragraphs(self: _typing.Self) -> _typing.Any:
        min_len = self.options.get("min_text_length", self.TEXT_LENGTH_THRESHOLD)
        candidates = {}
        ordered = []
        for elem in self.tags(self.html, "p", "pre", "td"):
            parent_node = elem.getparent()
            if parent_node is None:
                continue
            grand_parent_node = parent_node.getparent()

            inner_text = clean(elem.text_content() or "")
            inner_text_len = len(inner_text)
            if inner_text_len < min_len:
                continue

            if parent_node not in candidates:
                candidates[parent_node] = self.score_node(parent_node)
                ordered.append(parent_node)

            if grand_parent_node is not None and grand_parent_node not in candidates:
                candidates[grand_parent_node] = self.score_node(grand_parent_node)
                ordered.append(grand_parent_node)

            content_score = 1
            content_score += len(inner_text.split(","))
            content_score += min((inner_text_len / 100), 3)
            candidates[parent_node]["content_score"] += content_score
            if grand_parent_node is not None:
                candidates[grand_parent_node]["content_score"] += content_score / 2.0

        for elem in ordered:
            candidate = candidates[elem]
            link_density = self.get_link_density(elem)
            score = candidate["content_score"]
            self.debug(
                "Candid: %6.3f %s link density %.3f -> %6.3f"
                % (score, describe(elem), link_density, score * (1 - link_density))
            )
            candidate["content_score"] *= 1 - link_density

        return candidates

    def class_weight(self: _typing.Self, elem: _typing.Any) -> _typing.Any:
        weight = 0
        if elem.get("class", None):
            if REGEXES["negativeRe"].search(elem.get("class")):
                weight -= 25
            if REGEXES["positiveRe"].search(elem.get("class")):
                weight += 25

        if elem.get("id", None):
            if REGEXES["negativeRe"].search(elem.get("id")):
                weight -= 25
            if REGEXES["positiveRe"].search(elem.get("id")):
                weight += 25

        return weight

    def score_node(self: _typing.Self, elem: _typing.Any) -> dict[_typing.Any, _typing.Any]:
        content_score = self.class_weight(elem)
        name = elem.tag.lower()
        if name == "div":
            content_score += 5
        elif name in ["pre", "td", "blockquote"]:
            content_score += 3
        elif name in ["address", "ol", "ul", "dl", "dd", "dt", "li", "form"]:
            content_score -= 3
        elif name in ["h1", "h2", "h3", "h4", "h5", "h6", "th"]:
            content_score -= 5
        return {"content_score": content_score, "elem": elem}

    def debug(self: _typing.Self, *parts: _typing.Any) -> None:
        self.log.debug(*parts)

    def remove_unlikely_candidates(self: _typing.Self) -> None:
        for elem in self.html.iter():
            if elem in self.keep_elements:
                continue
            marker = "%s %s" % (elem.get("class", ""), elem.get("id", ""))
            if (
                REGEXES["unlikelyCandidatesRe"].search(marker)
                and (not REGEXES["okMaybeItsACandidateRe"].search(marker))
                and elem.tag != "body"
            ):
                self.debug("Removing unlikely candidate - %s" % describe(elem))
                elem.drop_tree()

    def transform_misused_divs_into_paragraphs(self: _typing.Self) -> None:
        for elem in self.tags(self.html, "div"):
            child_markup = "".join(tostring(child, encoding="unicode") for child in list(elem))
            if not REGEXES["divToPElementsRe"].search(child_markup):
                elem.tag = "p"

        for elem in self.tags(self.html, "div"):
            if elem.text and elem.text.strip():
                p = fragment_fromstring("<p/>")
                p.text = elem.text
                elem.text = None
                elem.insert(0, p)

            for pos, child in reversed(list(enumerate(elem))):
                if child.tail and child.tail.strip():
                    p = fragment_fromstring("<p/>")
                    p.text = child.tail
                    child.tail = None
                    elem.insert(pos + 1, p)
                if child.tag == "br":
                    child.drop_tree()

    def tags(self: _typing.Self, node: _typing.Any, *tag_names: _typing.Any) -> _typing.Iterator[_typing.Any]:
        for tag_name in tag_names:
            for elem in node.findall(".//%s" % tag_name):
                yield elem

    def reverse_tags(self: _typing.Self, node: _typing.Any, *tag_names: _typing.Any) -> _typing.Iterator[_typing.Any]:
        for tag_name in tag_names:
            for elem in reversed(node.findall(".//%s" % tag_name)):
                yield elem

    def sanitize(self: _typing.Self, node: _typing.Any, candidates: _typing.Any) -> _typing.Any:
        min_len = self.options.get("min_text_length", self.TEXT_LENGTH_THRESHOLD)
        for header in self.tags(node, "h1", "h2", "h3", "h4", "h5", "h6"):
            if self.class_weight(header) < 0 or self.get_link_density(header) > 0.33:
                header.drop_tree()

        for elem in self.tags(node, "form", "iframe", "textarea"):
            elem.drop_tree()
        allowed = {}

        for el in self.reverse_tags(node, "table", "ul", "div"):
            if el in allowed or el in self.keep_elements:
                continue
            weight = self.class_weight(el)
            if el in candidates:
                content_score = candidates[el]["content_score"]
            else:
                content_score = 0
            tag = el.tag

            if weight + content_score < 0:
                self.debug("Cleaned %s with score %6.3f and weight %-3s" % (describe(el), content_score, weight))
                el.drop_tree()
            elif el.text_content().count(",") < 10:
                counts = {}
                for kind in ["p", "img", "li", "a", "embed", "input"]:
                    counts[kind] = len(el.findall(".//%s" % kind))
                counts["li"] -= 100

                content_length = text_length(el)
                link_density = self.get_link_density(el)
                parent_node = el.getparent()
                if parent_node is not None and parent_node in candidates:
                    content_score = candidates[parent_node]["content_score"]
                else:
                    content_score = 0
                to_remove = False
                reason = ""

                if counts["p"] and counts["img"] > counts["p"]:
                    reason = "too many images (%s)" % counts["img"]
                    to_remove = True
                elif counts["li"] > counts["p"] and tag != "ul" and tag != "ol":
                    reason = "more <li>s than <p>s"
                    to_remove = True
                elif counts["input"] > (counts["p"] / 3):
                    reason = "less than 3x <p>s than <input>s"
                    to_remove = True
                elif content_length < min_len and (counts["img"] == 0 or counts["img"] > 2):
                    reason = "too short content length %s without a single image" % content_length
                    to_remove = True
                elif weight < 25 and link_density > 0.2:
                    reason = "too many links %.3f for its weight %s" % (link_density, weight)
                    to_remove = True
                elif weight >= 25 and link_density > 0.5:
                    reason = "too many links %.3f for its weight %s" % (link_density, weight)
                    to_remove = True
                elif (counts["embed"] == 1 and content_length < 75) or counts["embed"] > 1:
                    reason = "<embed>s with too short content length, or too many <embed>s"
                    to_remove = True

                    i = 0
                    j = 0
                    x = 1
                    siblings = []
                    for sib in el.itersiblings():
                        sib_content_length = text_length(sib)
                        if sib_content_length:
                            i += 1
                            siblings.append(sib_content_length)
                            if i == x:
                                break
                    for sib in el.itersiblings(preceding=True):
                        sib_content_length = text_length(sib)
                        if sib_content_length:
                            j += 1
                            siblings.append(sib_content_length)
                            if j == x:
                                break
                    if siblings and sum(siblings) > 1000:
                        to_remove = False
                        self.debug("Allowing %s" % describe(el))
                        for desnode in self.tags(el, "table", "ul", "div"):
                            allowed[desnode] = True

                if to_remove:
                    self.debug(
                        "Cleaned %6.3f %s with weight %s cause it has %s."
                        % (content_score, describe(el), weight, reason)
                    )
                    el.drop_tree()

        return clean_attributes(tounicode(node))


def option_parser() -> _typing.Any:
    from LiuXin_alpha.utils.config.config_tools import OptionParser

    parser = OptionParser(usage="%prog: [options] file")
    parser.add_option(
        "-v",
        "--verbose",
        default=False,
        action="store_true",
        dest="verbose",
        help="Show detailed output information. Useful for debugging",
    )
    parser.add_option(
        "-k",
        "--keep-elements",
        default=None,
        action="store",
        dest="keep_elements",
        help="XPath specifying elements that should not be removed",
    )

    return parser


def main() -> None:
    from LiuXin_alpha.utils.logging import default_log

    parser = option_parser()
    options, args = parser.parse_args()

    if len(args) != 1:
        parser.print_help()
        raise SystemExit(1)

    with open(args[0], "rb") as f:
        raw = f.read()

    enc = sys.__stdout__.encoding or "utf-8"
    if options.verbose and hasattr(default_log, "setLevel"):
        try:
            import logging as _logging

            default_log.setLevel(_logging.DEBUG)
        except Exception:
            pass

    print(
        Document(raw, default_log, debug=options.verbose, keep_elements=options.keep_elements)
        .summary()
        .encode(enc, "replace")
    )


if __name__ == "__main__":
    main()
