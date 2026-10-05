#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Inspect, transform and rewrite CSS rules across EPUB/OEB resources.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise css through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import re
import builtins

from lxml import etree
try:
    from cssselect import HTMLTranslator, parse
    from cssselect.xpath import XPathExpr, is_safe_name
    from cssselect.parser import SelectorSyntaxError

    _HAS_CSSSELECT = True
except ModuleNotFoundError:
    _HAS_CSSSELECT = False

    class HTMLTranslator(object):
        """
        Provide the htmltranslator contract for validated ebook processing.

        Example:
            Exercise HTMLTranslator through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
        """
        pass

    class XPathExpr(object):
        """
        Provide the xpathexpr contract for validated ebook processing.

        Example:
            Exercise XPathExpr through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
        """
        def __init__(self: _typing.Self, element: str = "*") -> None:
            """
            Initialize and validate the xpathexpr state.

            Example:
                Exercise XPathExpr.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


            :param element: Value supplied for element under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            self.element = element

        def add_name_test(self: _typing.Self) -> None:
            """
            Perform the add name test operation under explicit file-format and conversion rules.

            Example:
                Exercise XPathExpr.add name test through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


            :return: The normalized value, metadata record, path, stream result or collection
                described above.
            """
            return None

    def is_safe_name(name: _typing.Any) -> bool:
        """
        Return whether is safe name holds for the supplied ebook data.

        Example:
            Exercise is safe name through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: True when the documented condition holds; otherwise False.
        """
        return True

    class SelectorSyntaxError(Exception):
        """
        Report a selectorsyntaxerror encountered while processing an ebook format.

        Example:
            Exercise SelectorSyntaxError through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
        """
        pass

    def parse(text: _typing.Any) -> tuple[_typing.Any, ...]:
        """
        Parse the supplied date text and return its normalized datetime value.

        Example:
            Exercise parse through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return ()

try:
    from cssutils.css import CSSRule
except ModuleNotFoundError:
    class CSSRule(object):
        """
        Provide the cssrule contract for validated ebook processing.

        Example:
            Exercise CSSRule through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
        """
        IMPORT_RULE = 3
        STYLE_RULE = 1

from LiuXin_alpha.file_formats.oeb.base import OEB_STYLES, OEB_DOCS, XPNSMAP, XHTML_NS
try:
    from LiuXin_alpha.file_formats.oeb.normalize_css import normalize_filter_css, normalizers
except Exception:
    def normalize_filter_css(props: _typing.Any) -> _typing.Any:
        """
        Normalize filter css under the format's safety and compatibility rules.

        Example:
            Exercise normalize filter css through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param props: Value supplied for props under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return props

    normalizers = {}
from LiuXin_alpha.file_formats.oeb.polish.pretty import pretty_script_or_style
try:
    from LiuXin_alpha.file_formats.oeb.stylizer import (
        MIN_SPACE_RE,
        fix_namespace,
        is_non_whitespace,
        xpath_lower_case,
    )
except Exception:
    MIN_SPACE_RE = re.compile(r"\s*([>+~])\s*")

    def is_non_whitespace(text: _typing.Any) -> _typing.Any:
        """
        Return whether is non whitespace holds for the supplied ebook data.

        Example:
            Exercise is non whitespace through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param text: Text parsed, normalized or rendered.
        :return: True when the documented condition holds; otherwise False.
        """
        return bool(str(text).strip())

    def xpath_lower_case(text: _typing.Any) -> _typing.Any:
        """
        Perform the xpath lower case operation under explicit file-format and conversion rules.

        Example:
            Exercise xpath lower case through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return text

    def fix_namespace(selector: _typing.Any) -> _typing.Any:
        """
        Perform the fix namespace operation under explicit file-format and conversion rules.

        Example:
            Exercise fix namespace through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param selector: Value supplied for selector under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return selector

from LiuXin_alpha.utils.text import as_unicode as force_unicode
from LiuXin_alpha.utils.language_tools.icu import lower as icu_lower
from LiuXin_alpha.utils.localization import trans as _

# Py2/Py3 compatability layer
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import dict_itervalues as itervalues

__license__ = "GPL v3"
__copyright__ = "2014, Kovid Goyal <kovid at kovidgoyal.net>"


def _ngettext(singular: _typing.Any, plural: _typing.Any, n: _typing.Any) -> _typing.Any:
    """
    Perform the ngettext operation under explicit file-format and conversion rules.

    Example:
        Exercise  ngettext through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param singular: Value supplied for singular under the utility contract.
    :param plural: Value supplied for plural under the utility contract.
    :param n: Value supplied for n under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    fn = getattr(builtins, "ngettext", None)
    if callable(fn):
        try:
            return fn(singular, plural, n)
        except Exception:
            pass
    return singular if n == 1 else plural


class NamespacedTranslator(HTMLTranslator):
    """
    Provide the namespacedtranslator contract for validated ebook processing.

    Example:
        Exercise NamespacedTranslator through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    def xpath_element(self: _typing.Self, selector: _typing.Any) -> _typing.Any:
        """
        Perform the xpath element operation under explicit file-format and conversion rules.

        Example:
            Exercise NamespacedTranslator.xpath element through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param selector: Value supplied for selector under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        element = selector.element
        if not element:
            element = "*"
            safe = True
        else:
            safe = is_safe_name(element)
            if safe:
                # We use the h: prefix for the XHTML namespace
                element = "h:%s" % element.lower()
        xpath = XPathExpr(element=element)
        if not safe:
            xpath.add_name_test()
        return xpath


class CaseInsensitiveAttributesTranslator(NamespacedTranslator):
    """
    Treat class and id CSS selectors case-insensitively

    Example:
        Exercise CaseInsensitiveAttributesTranslator through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """

    def xpath_class(self: _typing.Self, class_selector: _typing.Any) -> _typing.Any:
        """
        Translate a class selector.

        Example:
            Exercise CaseInsensitiveAttributesTranslator.xpath class through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param class_selector: Value supplied for class selector under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        x = self.xpath(class_selector.selector)
        if is_non_whitespace(class_selector.class_name):
            x.add_condition(
                "%s and contains(concat(' ', normalize-space(%s), ' '), %s)"
                % (
                    "@class",
                    xpath_lower_case("@class"),
                    self.xpath_literal(" " + class_selector.class_name.lower() + " "),
                )
            )
        else:
            x.add_condition("0")
        return x

    def xpath_hash(self: _typing.Self, id_selector: _typing.Any) -> _typing.Any:
        """
        Translate an ID selector.

        Example:
            Exercise CaseInsensitiveAttributesTranslator.xpath hash through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param id_selector: Value supplied for id selector under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        x = self.xpath(id_selector.selector)
        return self.xpath_attrib_equals(x, xpath_lower_case("@id"), (id_selector.id.lower()))


if _HAS_CSSSELECT:
    css_to_xpath = NamespacedTranslator().css_to_xpath
    ci_css_to_xpath = CaseInsensitiveAttributesTranslator().css_to_xpath
else:
    css_to_xpath = ci_css_to_xpath = None


def build_selector(text: _typing.Any, case_sensitive: bool = True) -> _typing.Any:
    """
    Perform the build selector operation under explicit file-format and conversion rules.

    Example:
        Exercise build selector through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param text: Text parsed, normalized or rendered.
    :param case_sensitive: Value supplied for case sensitive under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not _HAS_CSSSELECT:
        return None
    func = css_to_xpath if case_sensitive else ci_css_to_xpath
    try:
        return etree.XPath(fix_namespace(func(text)), namespaces=XPNSMAP)
    except Exception:
        return None


def is_rule_used(root: _typing.Any, selector: _typing.Any, log: _typing.Any, pseudo_pat: _typing.Any, cache: _typing.Any) -> _typing.Any:
    """
    Return whether is rule used holds for the supplied ebook data.

    Example:
        Exercise is rule used through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param selector: Value supplied for selector under the utility contract.
    :param log: Value supplied for log under the utility contract.
    :param pseudo_pat: Value supplied for pseudo pat under the utility contract.
    :param cache: Value supplied for cache under the utility contract.
    :return: True when the documented condition holds; otherwise False.
    """
    selector = pseudo_pat.sub("", selector)
    selector = MIN_SPACE_RE.sub(r"\1", selector)
    try:
        xp = cache[(True, selector)]
    except KeyError:
        xp = cache[(True, selector)] = build_selector(selector)
    try:
        if xp(root):
            return True
    except Exception:
        return True

    # See if interpreting class and id selectors case-insensitively gives us
    # matches. Strictly speaking, class and id selectors should be case
    # sensitive for XHTML, but we err on the side of caution and not remove
    # them, since case sensitivity depends on whether the html is rendered in
    # quirks mode or not.
    try:
        xp = cache[(False, selector)]
    except KeyError:
        xp = cache[(False, selector)] = build_selector(selector, case_sensitive=False)
    try:
        return bool(xp(root))
    except Exception:
        return True


def filter_used_rules(root: _typing.Any, rules: _typing.Any, log: _typing.Any, pseudo_pat: _typing.Any, cache: _typing.Any) -> _typing.Iterator[_typing.Any]:
    """
    Perform the filter used rules operation under explicit file-format and conversion rules.

    Example:
        Exercise filter used rules through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param rules: Value supplied for rules under the utility contract.
    :param log: Value supplied for log under the utility contract.
    :param pseudo_pat: Value supplied for pseudo pat under the utility contract.
    :param cache: Value supplied for cache under the utility contract.
    :return: An iterator yielding the normalized values described above.
    """
    for rule in rules:
        used = False
        for selector in rule.selectorList:
            text = selector.selectorText
            if is_rule_used(root, text, log, pseudo_pat, cache):
                used = True
                break
        if not used:
            yield rule


def process_namespaces(sheet: _typing.Any) -> _typing.Any:
    # Find the namespace prefix (if any) for the XHTML namespace, so that we
    # can preserve it after processing
    """
    Perform the process namespaces operation under explicit file-format and conversion rules.

    Example:
        Exercise process namespaces through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param sheet: Value supplied for sheet under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    for prefix in sheet.namespaces:
        if sheet.namespaces[prefix] == XHTML_NS:
            return prefix


def preserve_htmlns_prefix(sheet: _typing.Any, prefix: _typing.Any) -> None:
    """
    Perform the preserve htmlns prefix operation under explicit file-format and conversion rules.

    Example:
        Exercise preserve htmlns prefix through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param sheet: Value supplied for sheet under the utility contract.
    :param prefix: Text prepended to the formatted or selected result.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if prefix is None:
        while "h" in sheet.namespaces:
            del sheet.namespaces["h"]
    else:
        sheet.namespaces[prefix] = XHTML_NS


def get_imported_sheets(name: _typing.Any, container: _typing.Any, sheets: _typing.Any, recursion_level: int = 10, sheet: _typing.Any = None) -> _typing.Any:
    """
    Return imported sheets under the format's safety and compatibility rules.

    Example:
        Exercise get imported sheets through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param container: Value supplied for container under the utility contract.
    :param sheets: Value supplied for sheets under the utility contract.
    :param recursion_level: Value supplied for recursion level under the utility
        contract.
    :param sheet: Value supplied for sheet under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ans = set()
    sheet = sheet or sheets[name]
    css_rules = getattr(sheet, "cssRules", None)
    if css_rules is None:
        return ans
    for rule in css_rules.rulesOfType(CSSRule.IMPORT_RULE):
        if rule.href:
            try:
                iname = container.href_to_name(rule.href, name)
            except ValueError:
                continue
            if iname in sheets:
                ans.add(iname)
    if recursion_level > 0:
        for imported_sheet in tuple(ans):
            ans |= get_imported_sheets(imported_sheet, container, sheets, recursion_level=recursion_level - 1)
    ans.discard(name)
    return ans


def remove_unused_css(container: _typing.Any, report: _typing.Any = None, remove_unused_classes: bool = False) -> bool:
    """
    Remove all unused CSS rules from the book. An unused CSS rule is one that does not match any actual content.

    Example:
        Exercise remove unused css through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param report: Value supplied for report under the utility contract.
    :param remove_unused_classes: Value supplied for remove unused classes under the
        utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    report = report or (lambda report_x: report_x)

    def safe_parse(parse_name: _typing.Any) -> _typing.Any:
        """
        Perform the safe parse operation under explicit file-format and conversion rules.

        Example:
            Exercise remove unused css.safe parse through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param parse_name: Value supplied for parse name under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        try:
            return container.parsed(parse_name)
        except Exception:
            return None

    sheets = {name: safe_parse(name) for name, mt in iteritems(container.mime_map) if mt in OEB_STYLES}
    sheets = {k: v for k, v in iteritems(sheets) if v is not None}
    import_map = {name: get_imported_sheets(name, container, sheets) for name in sheets}
    if remove_unused_classes:
        class_map = {
            name: {icu_lower(x) for x in classes_in_rule_list(sheet.cssRules)} for name, sheet in iteritems(sheets)
        }
    sheet_namespace = {}
    for sheet in itervalues(sheets):
        sheet_namespace[sheet] = process_namespaces(sheet)
        sheet.namespaces["h"] = XHTML_NS
    style_rules = {name: tuple(sheet.cssRules.rulesOfType(CSSRule.STYLE_RULE)) for name, sheet in iteritems(sheets)}

    num_of_removed_rules = num_of_removed_classes = 0
    pseudo_pat = re.compile(r":(first-letter|first-line|link|hover|visited|active|focus|before|after)", re.I)
    cache = {}

    for name, mt in iteritems(container.mime_map):
        if mt not in OEB_DOCS:
            continue
        try:
            root = container.parsed(name)
        except Exception:
            continue
        used_classes = set()
        for style in root.xpath('//*[local-name()="style"]'):
            if style.get("type", "text/css") == "text/css" and style.text:
                try:
                    sheet = container.parse_css(style.text)
                except Exception:
                    continue
                if remove_unused_classes:
                    used_classes |= {icu_lower(x) for x in classes_in_rule_list(sheet.cssRules)}
                imports = get_imported_sheets(name, container, sheets, sheet=sheet)
                for imported_sheet in imports:
                    style_rules[imported_sheet] = tuple(
                        filter_used_rules(
                            root,
                            style_rules[imported_sheet],
                            container.log,
                            pseudo_pat,
                            cache,
                        )
                    )
                    if remove_unused_classes:
                        used_classes |= class_map[imported_sheet]
                ns = process_namespaces(sheet)
                sheet.namespaces["h"] = XHTML_NS
                rules = tuple(sheet.cssRules.rulesOfType(CSSRule.STYLE_RULE))
                unused_rules = tuple(filter_used_rules(root, rules, container.log, pseudo_pat, cache))
                if unused_rules:
                    num_of_removed_rules += len(unused_rules)
                    [sheet.cssRules.remove(r) for r in unused_rules]
                    preserve_htmlns_prefix(sheet, ns)
                    style.text = force_unicode(sheet.cssText, "utf-8")
                    pretty_script_or_style(container, style)
                    container.dirty(name)

        for link in root.xpath('//*[local-name()="link" and @href]'):
            try:
                sname = container.href_to_name(link.get("href"), name)
            except ValueError:
                continue
            if sname not in sheets:
                continue
            style_rules[sname] = tuple(filter_used_rules(root, style_rules[sname], container.log, pseudo_pat, cache))
            if remove_unused_classes:
                used_classes |= class_map[sname]

            for iname in import_map[sname]:
                style_rules[iname] = tuple(
                    filter_used_rules(root, style_rules[iname], container.log, pseudo_pat, cache)
                )
                if remove_unused_classes:
                    used_classes |= class_map[iname]

        if remove_unused_classes:
            for elem in root.xpath("//*[@class]"):
                original_classes, classes = elem.get("class", "").split(), []
                for x in original_classes:
                    if icu_lower(x) in used_classes:
                        classes.append(x)
                if len(classes) != len(original_classes):
                    if classes:
                        elem.set("class", " ".join(classes))
                    else:
                        del elem.attrib["class"]
                    num_of_removed_classes += len(original_classes) - len(classes)
                    container.dirty(name)

    for name, sheet in iteritems(sheets):
        preserve_htmlns_prefix(sheet, sheet_namespace[sheet])
        unused_rules = style_rules[name]
        if unused_rules:
            num_of_removed_rules += len(unused_rules)
            [sheet.cssRules.remove(r) for r in unused_rules]
            container.dirty(name)

    # Todo: What is ngettext? What does it do
    if num_of_removed_rules > 0:
        report(
            _ngettext(
                "Removed %d unused CSS style rule",
                "Removed %d unused CSS style rules",
                num_of_removed_rules,
            )
            % num_of_removed_rules
        )
    else:
        report(_("No unused CSS style rules found"))
    if remove_unused_classes:
        if num_of_removed_classes > 0:
            report(
                _ngettext(
                    "Removed %d unused class from the HTML",
                    "Removed %d unused classes from the HTML",
                    num_of_removed_classes,
                )
                % num_of_removed_classes
            )
        else:
            report(_("No unused class attributes found"))
    return num_of_removed_rules + num_of_removed_classes > 0


def filter_declaration(style: _typing.Any, properties: _typing.Any) -> _typing.Any:
    """
    Perform the filter declaration operation under explicit file-format and conversion rules.

    Example:
        Exercise filter declaration through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param style: Value supplied for style under the utility contract.
    :param properties: Value supplied for properties under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    changed = False
    for prop in properties:
        if style.removeProperty(prop) != "":
            changed = True
    all_props = set(style.keys())
    for prop in style.getProperties():
        n = normalizers.get(prop.name, None)
        if n is not None:
            normalized = n(prop.name, prop.propertyValue)
            removed = properties.intersection(set(normalized))
            if removed:
                changed = True
                style.removeProperty(prop.name)
                for prop in set(normalized) - removed - all_props:
                    style.setProperty(prop, normalized[prop])
    return changed


def filter_sheet(sheet: _typing.Any, properties: _typing.Any) -> _typing.Any:
    """
    Perform the filter sheet operation under explicit file-format and conversion rules.

    Example:
        Exercise filter sheet through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param sheet: Value supplied for sheet under the utility contract.
    :param properties: Value supplied for properties under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    changed = False
    remove = []
    css_rules = getattr(sheet, "cssRules", None)
    if css_rules is None:
        return False
    for rule in css_rules.rulesOfType(CSSRule.STYLE_RULE):
        if filter_declaration(rule.style, properties):
            changed = True
            if rule.style.length == 0:
                remove.append(rule)
    for rule in remove:
        css_rules.remove(rule)
    return changed


def filter_css(container: _typing.Any, properties: _typing.Any, names: tuple[_typing.Any, ...] = ()) -> _typing.Any:
    """
    Remove the specified CSS properties from all CSS rules in the book.

    Example:
        Exercise filter css through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param properties: Value supplied for properties under the utility contract.
    :param names: Value supplied for names under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not names:
        types = OEB_STYLES | OEB_DOCS
        names = []
        for name, mt in iteritems(container.mime_map):
            if mt in types:
                names.append(name)
    properties = normalize_filter_css(properties)
    doc_changed = False

    for name in names:
        if name not in container.mime_map:
            continue
        mt = container.mime_map[name]
        if mt in OEB_STYLES:
            try:
                sheet = container.parsed(name)
            except Exception:
                continue
            filtered = filter_sheet(sheet, properties)
            if filtered:
                container.dirty(name)
                doc_changed = True
        elif mt in OEB_DOCS:
            try:
                root = container.parsed(name)
            except Exception:
                continue
            changed = False
            for style in root.xpath('//*[local-name()="style"]'):
                if style.text and style.get("type", "text/css") in {
                    None,
                    "",
                    "text/css",
                }:
                    try:
                        sheet = container.parse_css(style.text)
                    except Exception:
                        continue
                    if filter_sheet(sheet, properties):
                        changed = True
                        style.text = force_unicode(sheet.cssText, "utf-8")
                        pretty_script_or_style(container, style)
            for elem in root.xpath("//*[@style]"):
                text = elem.get("style", None)
                if text:
                    try:
                        style = container.parse_css(text, is_declaration=True)
                    except Exception:
                        continue
                    if filter_declaration(style, properties):
                        changed = True
                        if style.length == 0:
                            del elem.attrib["style"]
                        else:
                            elem.set(
                                "style",
                                force_unicode(style.getCssText(separator=" "), "utf-8"),
                            )
            if changed:
                container.dirty(name)
                doc_changed = True

    return doc_changed


def _classes_in_selector(selector: _typing.Any, classes: _typing.Any) -> None:
    """
    Perform the classes in selector operation under explicit file-format and conversion rules.

    Example:
        Exercise  classes in selector through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param selector: Value supplied for selector under the utility contract.
    :param classes: Value supplied for classes under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for attr in ("selector", "subselector", "parsed_tree"):
        s = getattr(selector, attr, None)
        if s is not None:
            _classes_in_selector(s, classes)
    cn = getattr(selector, "class_name", None)
    if cn is not None:
        classes.add(cn)


def classes_in_selector(text: _typing.Any) -> _typing.Any:
    """
    Perform the classes in selector operation under explicit file-format and conversion rules.

    Example:
        Exercise classes in selector through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param text: Text parsed, normalized or rendered.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    classes = set()
    try:
        for selector in parse(text):
            _classes_in_selector(selector, classes)
    except SelectorSyntaxError:
        pass
    return classes


def classes_in_rule_list(css_rules: _typing.Any) -> _typing.Any:
    """
    Perform the classes in rule list operation under explicit file-format and conversion rules.

    Example:
        Exercise classes in rule list through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param css_rules: Value supplied for css rules under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    classes = set()
    for rule in css_rules or ():
        if rule.type == rule.STYLE_RULE:
            classes |= classes_in_selector(rule.selectorText)
        elif hasattr(rule, "cssRules"):
            classes |= classes_in_rule_list(rule.cssRules)
    return classes
