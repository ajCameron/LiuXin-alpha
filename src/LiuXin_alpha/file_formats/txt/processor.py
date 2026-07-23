# -*- coding: utf-8 -*-

"""
Read content from txt file.
"""
from __future__ import annotations

import typing as _typing

import os
import re

from LiuXin_alpha.file_formats.conversion.preprocess import DocAnalysis

from LiuXin_alpha.file_formats.opf.opf2 import OPFCreator

from LiuXin_alpha.utils.calibre import prepare_string_for_xml, isbytestring
from LiuXin_alpha.utils.libraries.cleantext import clean_ascii_chars

# Py2/Py3 compatibility
from LiuXin_alpha.utils.libraries.liuxin_six import memory_range
from LiuXin_alpha.utils.libraries.liuxin_six import six_long


__license__ = "GPL v3"
__copyright__ = "2009, John Schember <john@nachtimwald.com>"
__docformat__ = "restructuredtext en"


HTML_TEMPLATE = (
    '<html><head><meta http-equiv="Content-Type" content="text/html; charset=utf-8"/><title>%s '
    "</title></head><body>\n%s\n</body></html>"
)


def clean_txt(txt: _typing.Any) -> _typing.Any:
    """
    Run transformations on the text to put it into
    consistent state.
    :param txt:
    """
    if isinstance(txt, bytes):
        txt = txt.decode("utf-8", "replace")
    # Strip whitespace from the end of the line. Also replace
    # all line breaks with \n.
    txt = "\n".join([line.rstrip() for line in txt.splitlines()])

    # Replace whitespace at the beginning of the line with &nbsp;
    txt = re.sub("(?m)(?<=^)([ ]{2,}|\t+)(?=.)", "&nbsp;" * 4, txt)

    # Condense redundant spaces
    txt = re.sub("[ ]{2,}", " ", txt)

    # Remove blank space from the beginning and end of the document.
    txt = re.sub(r"^\s+(?=.)", "", txt)
    txt = re.sub(r"(?<=.)\s+$", "", txt)
    # Remove excessive line breaks.
    txt = re.sub("\n{5,}", "\n\n\n\n", txt)
    # remove ASCII invalid chars : 0 to 8 and 11-14 to 24
    txt = clean_ascii_chars(txt)

    return txt


def split_txt(txt: _typing.Any, epub_split_size_kb: int = 0) -> _typing.Any:
    """
    Ensure there are split points for converting
    to EPUB. A misdetected paragraph type can
    result in the entire document being one giant
    paragraph. In this case the EPUB parser will not
    be able to determine where to split the file
    to accomidate the EPUB file size limitation
    and will fail.
    :param txt:
    :param epub_split_size_kb:
    :return:
    """
    # Takes care if there is no point to split
    if epub_split_size_kb > 0:
        if isinstance(txt, str):
            txt_bytes = txt.encode("utf-8")
        else:
            txt_bytes = bytes(txt)
        length_byte = len(txt_bytes)
        # Calculating the average chunk value for easy splitting as EPUB (+2 as a safe margin)
        chunk_size = six_long(length_byte / (int(length_byte / (epub_split_size_kb * 1024)) + 2))
        # if there are chunks with a superior size then go and break
        if any(len(x) > chunk_size for x in txt_bytes.split(b"\n\n")):
            txt = "\n\n".join(
                split_string_separator(line.decode("utf-8", "replace"), chunk_size)
                for line in txt_bytes.split(b"\n\n")
            )
            return txt
        txt = txt_bytes
    if isinstance(txt, bytes):
        txt = txt.decode("utf-8")

    return txt


def convert_basic(txt: _typing.Any, title: str = "", epub_split_size_kb: int = 0) -> _typing.Any:
    """
    Converts plain text to html by putting all paragraphs in <p> tags. It condense and retains blank lines when
    necessary.

    Requires paragraphs to be in single line format.
    :param txt:
    :param title:
    :param epub_split_size_kb:
    :return:
    """
    txt = clean_txt(txt)
    txt = split_txt(txt, epub_split_size_kb)

    lines = []
    blank_count = 0
    # Split into paragraphs based on having a blank line between text.
    for line in txt.split("\n"):
        if line.strip():
            blank_count = 0
            lines.append("<p>%s</p>" % prepare_string_for_xml(line.replace("\n", " ")))
        else:
            blank_count += 1
            if blank_count == 2:
                lines.append("<p>&nbsp;</p>")

    return HTML_TEMPLATE % (title, "\n".join(lines))


def convert_markdown(txt: _typing.Any, title: str = "", extensions: tuple[_typing.Any, ...] = ("footnotes", "tables", "toc")) -> _typing.Any:
    from LiuXin_alpha.file_formats.conversion.plugins.txt_input import MD_EXTENSIONS
    from LiuXin_alpha.file_formats.markdown import Markdown

    extensions = [x.lower() for x in extensions if x.lower() in MD_EXTENSIONS]
    md = Markdown(extensions, safe_mode=False)
    return HTML_TEMPLATE % (title, md.convert(txt))


def convert_textile(txt: _typing.Any, title: str = "") -> _typing.Any:
    try:
        from LiuXin_alpha.file_formats.textile.functions import textile
    except Exception:
        # Textile stack is optional while porting; fall back to plain conversion.
        return convert_basic(txt, title=title)

    try:
        html = textile(txt, encoding="utf-8")
    except Exception:
        return convert_basic(txt, title=title)
    return HTML_TEMPLATE % (title, html)


def normalize_line_endings(txt: _typing.Any) -> _typing.Any:
    txt = txt.replace("\r\n", "\n")
    txt = txt.replace("\r", "\n")
    return txt


def separate_paragraphs_single_line(txt: _typing.Any) -> _typing.Any:
    txt = txt.replace("\n", "\n\n")
    return txt


def separate_paragraphs_print_formatted(txt: _typing.Any) -> _typing.Any:
    txt = re.sub(
        r"(?miu)^(?P<indent>\t+|[ ]{2,})(?=.)",
        lambda mo: "\n%s" % mo.group("indent"),
        txt,
    )
    return txt


def separate_hard_scene_breaks(txt: _typing.Any) -> _typing.Any:
    def sep_break(line: _typing.Any) -> _typing.Any:
        if len(line.strip()) > 0:
            return "\n%s\n" % line
        else:
            return line

    txt = re.sub(r"(?miu)^[ \t-=~/_]+$", lambda mo: sep_break(mo.group()), txt)
    return txt


def block_to_single_line(txt: _typing.Any) -> _typing.Any:
    txt = re.sub(r"(?<=.)\n(?=.)", " ", txt)
    return txt


def preserve_spaces(txt: _typing.Any) -> _typing.Any:
    """
    Replaces spaces multiple spaces with &nbsp; entities.
    :param txt:
    :return:
    """
    txt = re.sub(
        r"(?P<space>[ ]{2,})",
        lambda mo: " " + ("&nbsp;" * (len(mo.group("space")) - 1)),
        txt,
    )
    txt = txt.replace("\t", "&nbsp;&nbsp;&nbsp;&nbsp;")
    return txt


def remove_indents(txt: _typing.Any) -> _typing.Any:
    """
    Remove whitespace at the beginning of each line.
    :param txt:
    :return:
    """
    txt = re.sub(r"(?miu)^\s+", "", txt)
    return txt


def opf_writer(path: _typing.Any, opf_name: _typing.Any, manifest: _typing.Any, spine: _typing.Any, mi: _typing.Any) -> None:
    opf = OPFCreator(path, mi)
    opf.create_manifest(manifest)
    opf.create_spine(spine)
    with open(os.path.join(path, opf_name), "wb") as opffile:
        opf.render(opffile)


def split_string_separator(txt: _typing.Any, size: _typing.Any) -> _typing.Any:
    """
    Splits the text by putting \n\n at the point size.
    :param txt:
    :param size:
    :return:
    """
    if len(txt) > size:
        txt = "".join(
            [
                re.sub(r"\.(?P<ends>[^.]*)$", r".\n\n\g<ends>", txt[i : i + size], 1)
                for i in memory_range(0, len(txt), size)
            ]
        )
    return txt


def detect_paragraph_type(txt: _typing.Any) -> str:
    """
    Tries to determine the paragraph type of the document.

    block: Paragraphs are separated by a blank line.
    single: Each line is a paragraph.
    print: Each paragraph starts with a 2+ spaces or a tab
           and ends when a new paragraph is reached.
    unformatted: most lines have hard line breaks, few/no blank lines or indents

    returns block, single, print, unformatted
    :param txt:
    :return:
    """
    txt = txt.replace("\r\n", "\n")
    txt = txt.replace("\r", "\n")
    txt_line_count = len(re.findall(r"(?mu)^\s*.+$", txt))
    if txt_line_count == 0:
        return "single"

    # Check for hard line breaks - true if 55% of the doc breaks in the same region
    docanalysis = DocAnalysis("txt", txt)
    hardbreaks = docanalysis.line_histogram(0.55)

    if hardbreaks:
        # Determine print percentage
        tab_line_count = len(re.findall(r"(?mu)^(\t|\s{2,}).+$", txt))
        print_percent = tab_line_count / float(txt_line_count)

        # Determine block percentage
        empty_line_count = len(re.findall(r"(?mu)^\s*$", txt))
        block_percent = empty_line_count / float(txt_line_count)

        # Compare the two types - the type with the larger number of instances wins
        # in cases where only one or the other represents the vast majority of the document neither wins
        if print_percent >= block_percent:
            if 0.15 <= print_percent <= 0.75:
                return "print"
        elif 0.15 <= block_percent <= 0.75:
            return "block"

        # Assume unformatted text with hardbreaks if nothing else matches
        return "unformatted"

    # return single if hardbreaks is false
    return "single"


def detect_formatting_type(txt: _typing.Any) -> str:
    """
    Tries to determine the formatting of the document.

    markdown: Markdown formatting is used.
    textile: Textile formatting is used.
    heuristic: When none of the above formatting types are
               detected heuristic is returned.
    :param txt:
    :return:
    """
    # Keep a count of the number of format specific object
    # that are found in the text.
    markdown_count = 0
    textile_count = 0

    # Check for markdown
    # Headings
    markdown_count += len(re.findall(r"(?mu)^#+", txt))
    markdown_count += len(re.findall(r"(?mu)^=+$", txt))
    markdown_count += len(re.findall(r"(?mu)^-+$", txt))
    # Images
    markdown_count += len(re.findall(r"(?u)!\[.*?\](\[|\()", txt))
    # Links
    markdown_count += len(re.findall(r"(?u)(?<!\!)\[[^\]]+\](\[|\()", txt))

    # Check for textile
    # Headings
    textile_count += len(re.findall(r"(?mu)^h[1-6]\.", txt))
    # Block quote.
    textile_count += len(re.findall(r"(?mu)^bq\.", txt))
    # Images
    textile_count += len(re.findall(r"(?mu)(?<=\!)\S+(?=\!)", txt))
    # Links
    textile_count += len(re.findall(r'"[^"]*":\S+', txt))
    # paragraph blocks
    textile_count += len(re.findall(r"(?mu)^p(<|<>|=|>)?\. ", txt))

    # Decide if either markdown or textile is used in the text
    # based on the number of unique formatting elements found.
    if markdown_count > 5 or textile_count > 5:
        if markdown_count > textile_count:
            return "markdown"
        else:
            return "textile"

    return "heuristic"
