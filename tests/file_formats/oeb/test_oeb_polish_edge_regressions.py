"""
Provide test oeb polish edge regressions utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test oeb polish edge regressions through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_edge_regressions.py
"""
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
    """
    Perform the xhtml root operation under explicit file-format and conversion rules.

    Example:
        Exercise  xhtml root through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_edge_regressions.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ns = "http://www.w3.org/1999/xhtml"
    return etree.Element("{%s}html" % ns, nsmap={None: ns})


def test_safe_line_offset_defaults_to_zero_for_missing_source_lines() -> None:
    """
    Perform the test safe line offset defaults to zero for missing source lines operation under explicit file-format and conversion rules.

    Example:
        Exercise test safe line offset defaults to zero for missing source lines through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_edge_regressions.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    elem = etree.Element("div")
    assert _safe_line_offset(elem) == 0


def test_check_ids_handles_missing_sourcelines_without_crashing() -> None:
    """
    Perform the test check ids handles missing sourcelines without crashing operation under explicit file-format and conversion rules.

    Example:
        Exercise test check ids handles missing sourcelines without crashing through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_edge_regressions.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    root = _xhtml_root()
    body = etree.SubElement(root, "{http://www.w3.org/1999/xhtml}body")
    etree.SubElement(body, "{http://www.w3.org/1999/xhtml}div", id="dup")
    etree.SubElement(body, "{http://www.w3.org/1999/xhtml}span", id="dup")

    class _Container:
        """
        Provide the container contract for validated ebook processing.

        Example:
            Exercise test check ids handles missing sourcelines without crashing. Container through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_edge_regressions.py
        """
        mime_map = {"index.xhtml": "application/xhtml+xml"}

        def parsed(self, name):
            """
            Perform the parsed operation under explicit file-format and conversion rules.

            Example:
                Exercise test check ids handles missing sourcelines without crashing. Container.parsed through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_edge_regressions.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            assert name == "index.xhtml"
            return root

    errors = check_ids(_Container())
    assert len(errors) == 1
    assert errors[0].name == "index.xhtml"
    assert errors[0].all_locations == [("index.xhtml", 1, None)]


def test_from_links_keeps_no_fragment_links_and_skips_bad_absolute_paths() -> None:
    """
    Perform the test from links keeps no fragment links and skips bad absolute paths operation under explicit file-format and conversion rules.

    Example:
        Exercise test from links keeps no fragment links and skips bad absolute paths through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_edge_regressions.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
        """
        Provide the container contract for validated ebook processing.

        Example:
            Exercise test from links keeps no fragment links and skips bad absolute paths. Container through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_edge_regressions.py
        """
        spine_items = ["/tmp/ch1.xhtml"]

        def abspath_to_name(self, path):
            """
            Perform the abspath to name operation under explicit file-format and conversion rules.

            Example:
                Exercise test from links keeps no fragment links and skips bad absolute paths. Container.abspath to name through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_edge_regressions.py


            :param path: Filesystem path read, written, normalized or validated by the
                operation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return Path(path).name

        def parsed(self, name):
            """
            Perform the parsed operation under explicit file-format and conversion rules.

            Example:
                Exercise test from links keeps no fragment links and skips bad absolute paths. Container.parsed through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_edge_regressions.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if name == "ch1.xhtml":
                return root1
            if name == "chapter2.xhtml":
                return root2
            raise KeyError(name)

        def href_to_name(self, href, base=None):
            """
            Perform the href to name operation under explicit file-format and conversion rules.

            Example:
                Exercise test from links keeps no fragment links and skips bad absolute paths. Container.href to name through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_edge_regressions.py


            :param href: Value supplied for href under the utility contract.
            :param base: Value supplied for base under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
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
    """
    Perform the test node from loc raises malformed markup for missing or bad locs operation under explicit file-format and conversion rules.

    Example:
        Exercise test node from loc raises malformed markup for missing or bad locs through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_edge_regressions.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    no_body_root = etree.fromstring(b"<html><head/></html>")
    with pytest.raises(MalformedMarkup):
        node_from_loc(no_body_root, [0])

    body_root = etree.fromstring(b"<html><body><div/></body></html>")
    with pytest.raises(MalformedMarkup):
        node_from_loc(body_root, [2])


def test_split_reports_clean_abort_for_missing_or_invalid_xpath() -> None:
    """
    Perform the test split reports clean abort for missing or invalid xpath operation under explicit file-format and conversion rules.

    Example:
        Exercise test split reports clean abort for missing or invalid xpath through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_edge_regressions.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    root = etree.fromstring(b"<html><body><p id='p1'>x</p></body></html>")

    class _Container:
        """
        Provide the container contract for validated ebook processing.

        Example:
            Exercise test split reports clean abort for missing or invalid xpath. Container through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_edge_regressions.py
        """
        def parsed(self, name):
            """
            Perform the parsed operation under explicit file-format and conversion rules.

            Example:
                Exercise test split reports clean abort for missing or invalid xpath. Container.parsed through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_edge_regressions.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return root

    with pytest.raises(AbortError):
        split(_Container(), "index.xhtml", '//*[@id="missing"]')
    with pytest.raises(AbortError):
        split(_Container(), "index.xhtml", "//*[")


def test_create_ncx_defaults_to_en_when_lang_is_missing() -> None:
    """
    Perform the test create ncx defaults to en when lang is missing operation under explicit file-format and conversion rules.

    Example:
        Exercise test create ncx defaults to en when lang is missing through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_edge_regressions.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    toc = TOC()
    toc.add("Chapter 1", "chapter1.xhtml")
    ncx = create_ncx(toc, lambda name: name, "Book", None, "uid-1")
    assert ncx.get("{http://www.w3.org/XML/1998/namespace}lang") == "en"
