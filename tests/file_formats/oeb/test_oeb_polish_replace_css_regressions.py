"""
Provide test oeb polish replace css regressions utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test oeb polish replace css regressions through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py
"""
from __future__ import annotations

from LiuXin_alpha.file_formats.oeb.polish.css import (
    filter_css,
    get_imported_sheets,
    remove_unused_css,
)
from LiuXin_alpha.file_formats.oeb.polish.replace import LinkRebaser, LinkReplacer
from LiuXin_alpha.utils.libraries.liuxin_etree import etree


class _EmptyRuleList:
    """
    Provide the emptyrulelist contract for validated ebook processing.

    Example:
        Exercise  EmptyRuleList through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py
    """
    def rulesOfType(self, _kind):
        """
        Perform the rulesOfType operation under explicit file-format and conversion rules.

        Example:
            Exercise  EmptyRuleList.rulesOfType through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


        :param _kind: Value supplied for kind under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return []

    def remove(self, _rule):
        """
        Perform the remove operation under explicit file-format and conversion rules.

        Example:
            Exercise  EmptyRuleList.remove through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


        :param _rule: Value supplied for rule under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None


class _EmptySheet:
    """
    Provide the emptysheet contract for validated ebook processing.

    Example:
        Exercise  EmptySheet through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py
    """
    def __init__(self):
        """
        Initialize and validate the emptysheet state.

        Example:
            Exercise  EmptySheet.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


        :return: None; validated state is stored on the receiving object.
        """
        self.cssRules = _EmptyRuleList()
        self.namespaces = {}


def _xhtml_with_style_and_link() -> etree._Element:
    """
    Perform the xhtml with style and link operation under explicit file-format and conversion rules.

    Example:
        Exercise  xhtml with style and link through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    """
    Perform the test link replacer ignores invalid absolute paths operation under explicit file-format and conversion rules.

    Example:
        Exercise test link replacer ignores invalid absolute paths through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    class _Container:
        """
        Provide the container contract for validated ebook processing.

        Example:
            Exercise test link replacer ignores invalid absolute paths. Container through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py
        """
        def href_to_name(self, href, base):
            """
            Perform the href to name operation under explicit file-format and conversion rules.

            Example:
                Exercise test link replacer ignores invalid absolute paths. Container.href to name through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


            :param href: Value supplied for href under the utility contract.
            :param base: Value supplied for base under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            raise ValueError("invalid absolute path")

        def name_to_href(self, name, base):
            """
            Perform the name to href operation under explicit file-format and conversion rules.

            Example:
                Exercise test link replacer ignores invalid absolute paths. Container.name to href through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


            :param name: Field, file, function or resource name addressed by the operation.
            :param base: Value supplied for base under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return name

    lr = LinkReplacer("index.xhtml", _Container(), {"old": "new"}, lambda name, frag: frag)
    href = "C:/outside.xhtml#frag"
    assert lr(href) == href
    assert lr.replaced is False


def test_link_rebaser_ignores_invalid_absolute_paths() -> None:
    """
    Perform the test link rebaser ignores invalid absolute paths operation under explicit file-format and conversion rules.

    Example:
        Exercise test link rebaser ignores invalid absolute paths through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    class _Container:
        """
        Provide the container contract for validated ebook processing.

        Example:
            Exercise test link rebaser ignores invalid absolute paths. Container through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py
        """
        def href_to_name(self, href, base):
            """
            Perform the href to name operation under explicit file-format and conversion rules.

            Example:
                Exercise test link rebaser ignores invalid absolute paths. Container.href to name through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


            :param href: Value supplied for href under the utility contract.
            :param base: Value supplied for base under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            raise ValueError("invalid absolute path")

        def name_to_href(self, name, base):
            """
            Perform the name to href operation under explicit file-format and conversion rules.

            Example:
                Exercise test link rebaser ignores invalid absolute paths. Container.name to href through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


            :param name: Field, file, function or resource name addressed by the operation.
            :param base: Value supplied for base under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return name

    lr = LinkRebaser(_Container(), "a.xhtml", "b.xhtml")
    href = "C:/outside.xhtml#frag"
    assert lr(href) == href
    assert lr.replaced is False


def test_get_imported_sheets_skips_bad_import_hrefs() -> None:
    """
    Perform the test get imported sheets skips bad import hrefs operation under explicit file-format and conversion rules.

    Example:
        Exercise test get imported sheets skips bad import hrefs through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    class _Rule:
        """
        Provide the rule contract for validated ebook processing.

        Example:
            Exercise test get imported sheets skips bad import hrefs. Rule through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py
        """
        href = "C:/outside.css"

    class _Rules:
        """
        Provide the rules contract for validated ebook processing.

        Example:
            Exercise test get imported sheets skips bad import hrefs. Rules through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py
        """
        def rulesOfType(self, _kind):
            """
            Perform the rulesOfType operation under explicit file-format and conversion rules.

            Example:
                Exercise test get imported sheets skips bad import hrefs. Rules.rulesOfType through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


            :param _kind: Value supplied for kind under the utility contract.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return [_Rule()]

    class _Sheet:
        """
        Provide the sheet contract for validated ebook processing.

        Example:
            Exercise test get imported sheets skips bad import hrefs. Sheet through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py
        """
        cssRules = _Rules()

    class _Container:
        """
        Provide the container contract for validated ebook processing.

        Example:
            Exercise test get imported sheets skips bad import hrefs. Container through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py
        """
        def href_to_name(self, href, base):
            """
            Perform the href to name operation under explicit file-format and conversion rules.

            Example:
                Exercise test get imported sheets skips bad import hrefs. Container.href to name through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


            :param href: Value supplied for href under the utility contract.
            :param base: Value supplied for base under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            raise ValueError("invalid absolute path")

    sheets = {"styles/main.css": _Sheet()}
    assert get_imported_sheets("styles/main.css", _Container(), sheets) == set()


def test_remove_unused_css_handles_parse_failures_and_bad_links() -> None:
    """
    Perform the test remove unused css handles parse failures and bad links operation under explicit file-format and conversion rules.

    Example:
        Exercise test remove unused css handles parse failures and bad links through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    class _Container:
        """
        Provide the container contract for validated ebook processing.

        Example:
            Exercise test remove unused css handles parse failures and bad links. Container through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py
        """
        mime_map = {
            "index.xhtml": "application/xhtml+xml",
            "styles/main.css": "text/css",
        }
        log = object()

        def __init__(self):
            """
            Initialize and validate the container state.

            Example:
                Exercise test remove unused css handles parse failures and bad links. Container.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


            :return: None; validated state is stored on the receiving object.
            """
            self._root = _xhtml_with_style_and_link()
            self._sheet = _EmptySheet()

        def parsed(self, name):
            """
            Perform the parsed operation under explicit file-format and conversion rules.

            Example:
                Exercise test remove unused css handles parse failures and bad links. Container.parsed through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            if name == "index.xhtml":
                return self._root
            if name == "styles/main.css":
                return self._sheet
            raise KeyError(name)

        def parse_css(self, text, is_declaration=False):
            """
            Parse css under the format's safety and compatibility rules.

            Example:
                Exercise test remove unused css handles parse failures and bad links. Container.parse css through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


            :param text: Text parsed, normalized or rendered.
            :param is_declaration: Value supplied for is declaration under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            raise ValueError("synthetic parse failure")

        def href_to_name(self, href, base=None):
            """
            Perform the href to name operation under explicit file-format and conversion rules.

            Example:
                Exercise test remove unused css handles parse failures and bad links. Container.href to name through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


            :param href: Value supplied for href under the utility contract.
            :param base: Value supplied for base under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            raise ValueError("invalid absolute path")

        def dirty(self, name):
            """
            Perform the dirty operation under explicit file-format and conversion rules.

            Example:
                Exercise test remove unused css handles parse failures and bad links. Container.dirty through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return None

    report_lines = []
    changed = remove_unused_css(_Container(), report=report_lines.append, remove_unused_classes=False)
    assert changed is False
    assert any("No unused CSS style rules found" in x for x in report_lines)


def test_filter_css_handles_parse_failures_gracefully() -> None:
    """
    Perform the test filter css handles parse failures gracefully operation under explicit file-format and conversion rules.

    Example:
        Exercise test filter css handles parse failures gracefully through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    class _Container:
        """
        Provide the container contract for validated ebook processing.

        Example:
            Exercise test filter css handles parse failures gracefully. Container through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py
        """
        mime_map = {"index.xhtml": "application/xhtml+xml"}

        def __init__(self):
            """
            Initialize and validate the container state.

            Example:
                Exercise test filter css handles parse failures gracefully. Container.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


            :return: None; validated state is stored on the receiving object.
            """
            self._root = _xhtml_with_style_and_link()
            self.dirty_calls = []

        def parsed(self, name):
            """
            Perform the parsed operation under explicit file-format and conversion rules.

            Example:
                Exercise test filter css handles parse failures gracefully. Container.parsed through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            assert name == "index.xhtml"
            return self._root

        def parse_css(self, text, is_declaration=False):
            """
            Parse css under the format's safety and compatibility rules.

            Example:
                Exercise test filter css handles parse failures gracefully. Container.parse css through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


            :param text: Text parsed, normalized or rendered.
            :param is_declaration: Value supplied for is declaration under the utility contract.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            raise ValueError("synthetic parse failure")

        def dirty(self, name):
            """
            Perform the dirty operation under explicit file-format and conversion rules.

            Example:
                Exercise test filter css handles parse failures gracefully. Container.dirty through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_replace_css_regressions.py


            :param name: Field, file, function or resource name addressed by the operation.
            :return: None; the operation mutates state, writes output or performs cleanup in
                place.
            """
            self.dirty_calls.append(name)

    c = _Container()
    changed = filter_css(c, {"color"})
    assert changed is False
    assert c.dirty_calls == []
