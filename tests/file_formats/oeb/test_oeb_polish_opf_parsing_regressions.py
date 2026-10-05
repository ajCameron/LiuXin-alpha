"""
Provide test oeb polish opf parsing regressions utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test oeb polish opf parsing regressions through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_opf_parsing_regressions.py
"""
from __future__ import annotations

import pytest

from LiuXin_alpha.file_formats.oeb.polish.container import OPF_NAMESPACES
from LiuXin_alpha.file_formats.oeb.polish.opf import get_book_language, set_guide_item
from LiuXin_alpha.file_formats.oeb.polish.parsing import parse, parse_html5
from LiuXin_alpha.utils.libraries.liuxin_etree import etree


class _OpfContainer:
    """
    Provide the opfcontainer contract for validated ebook processing.

    Example:
        Exercise  OpfContainer through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_opf_parsing_regressions.py
    """
    opf_name = "content.opf"

    def __init__(self, opf_xml: str):
        """
        Initialize and validate the opfcontainer state.

        Example:
            Exercise  OpfContainer.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_opf_parsing_regressions.py


        :param opf_xml: Value supplied for opf xml under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.opf = etree.fromstring(opf_xml.encode("utf-8"))
        self.dirty_calls = []

    def opf_xpath(self, expr):
        """
        Perform the opf xpath operation under explicit file-format and conversion rules.

        Example:
            Exercise  OpfContainer.opf xpath through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_opf_parsing_regressions.py


        :param expr: Value supplied for expr under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.opf.xpath(expr, namespaces=OPF_NAMESPACES)

    def insert_into_xml(self, parent, elem, index=None):
        """
        Perform the insert into xml operation under explicit file-format and conversion rules.

        Example:
            Exercise  OpfContainer.insert into xml through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_opf_parsing_regressions.py


        :param parent: Value supplied for parent under the utility contract.
        :param elem: Value supplied for elem under the utility contract.
        :param index: Value supplied for index under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if index is None:
            parent.append(elem)
        else:
            parent.insert(index, elem)

    def remove_from_xml(self, elem):
        """
        Perform the remove from xml operation under explicit file-format and conversion rules.

        Example:
            Exercise  OpfContainer.remove from xml through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_opf_parsing_regressions.py


        :param elem: Value supplied for elem under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        parent = elem.getparent()
        if parent is not None:
            parent.remove(elem)

    def dirty(self, name):
        """
        Perform the dirty operation under explicit file-format and conversion rules.

        Example:
            Exercise  OpfContainer.dirty through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_opf_parsing_regressions.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.dirty_calls.append(name)


def test_get_book_language_skips_bad_entries(monkeypatch) -> None:
    """
    Perform the test get book language skips bad entries operation under explicit file-format and conversion rules.

    Example:
        Exercise test get book language skips bad entries through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_opf_parsing_regressions.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    xml = """
    <package xmlns="http://www.idpf.org/2007/opf" xmlns:dc="http://purl.org/dc/elements/1.1/">
      <metadata>
        <dc:language>bad-lang</dc:language>
        <dc:language>en-US</dc:language>
      </metadata>
    </package>
    """
    c = _OpfContainer(xml)

    def _canon(code: str):
        """
        Perform the canon operation under explicit file-format and conversion rules.

        Example:
            Exercise test get book language skips bad entries. canon through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_opf_parsing_regressions.py


        :param code: Value supplied for code under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if code == "bad-lang":
            raise ValueError("bad")
        return "en"

    import LiuXin_alpha.file_formats.oeb.polish.opf as opf_mod

    monkeypatch.setattr(opf_mod, "canonicalize_lang", _canon)
    assert get_book_language(c) == "en"


def test_set_guide_item_removes_existing_match_when_href_is_invalid() -> None:
    """
    Perform the test set guide item removes existing match when href is invalid operation under explicit file-format and conversion rules.

    Example:
        Exercise test set guide item removes existing match when href is invalid through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_opf_parsing_regressions.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    xml = """
    <package xmlns="http://www.idpf.org/2007/opf">
      <guide>
        <reference type="cover" title="Old" href="old.xhtml" />
      </guide>
    </package>
    """
    c = _OpfContainer(xml)

    def _bad_name_to_href(name, base):
        """
        Perform the bad name to href operation under explicit file-format and conversion rules.

        Example:
            Exercise test set guide item removes existing match when href is invalid. bad name to href through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_opf_parsing_regressions.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param base: Value supplied for base under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise ValueError("invalid path")

    c.name_to_href = _bad_name_to_href  # type: ignore[attr-defined]
    set_guide_item(c, "cover", None, "C:/outside.xhtml")
    assert c.opf_xpath('//opf:guide/opf:reference[@type="cover"]') == []


def test_set_guide_item_creates_guide_and_reference_without_title_when_none() -> None:
    """
    Perform the test set guide item creates guide and reference without title when none operation under explicit file-format and conversion rules.

    Example:
        Exercise test set guide item creates guide and reference without title when none through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_opf_parsing_regressions.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    xml = """
    <package xmlns="http://www.idpf.org/2007/opf">
      <metadata />
      <manifest />
      <spine />
    </package>
    """
    c = _OpfContainer(xml)
    c.name_to_href = lambda name, base: name  # type: ignore[attr-defined]

    set_guide_item(c, "cover", None, "images/cover.jpg", frag="top")

    refs = c.opf_xpath('//opf:guide/opf:reference[@type="cover"]')
    assert len(refs) == 1
    ref = refs[0]
    assert ref.get("href") == "images/cover.jpg#top"
    assert ref.get("title") is None
    assert c.dirty_calls == ["content.opf"]


def test_parse_html5_rejects_none_input_cleanly() -> None:
    """
    Perform the test parse html5 rejects none input cleanly operation under explicit file-format and conversion rules.

    Example:
        Exercise test parse html5 rejects none input cleanly through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_opf_parsing_regressions.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(ValueError, match="raw input is None"):
        parse_html5(None)  # type: ignore[arg-type]


def test_parse_rejects_none_input_cleanly() -> None:
    """
    Perform the test parse rejects none input cleanly operation under explicit file-format and conversion rules.

    Example:
        Exercise test parse rejects none input cleanly through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_opf_parsing_regressions.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with pytest.raises(ValueError, match="raw input is None"):
        parse(None)  # type: ignore[arg-type]
