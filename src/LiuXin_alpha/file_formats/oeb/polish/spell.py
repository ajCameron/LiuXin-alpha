#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Find and update misspelled words across book content.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise spell through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
"""
from __future__ import (
    absolute_import,
    annotations,
    division,
    print_function,
    unicode_literals,
)

import sys
import typing as _typing
from collections import defaultdict

from LiuXin_alpha.file_formats.oeb.base import barename
from LiuXin_alpha.file_formats.oeb.polish.container import OPF_NAMESPACES, get_container
from LiuXin_alpha.file_formats.oeb.polish.toc import find_existing_toc

try:
    from LiuXin_alpha.utils.spell.break_iterator import index_of, split_into_words
    from LiuXin_alpha.utils.spell.dictionary import parse_lang_code
except ModuleNotFoundError:
    class _FallbackLocale(object):
        """
        Provide the fallbacklocale contract for validated ebook processing.

        Example:
            Exercise  FallbackLocale through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
        """
        def __init__(self: _typing.Self, langcode: _typing.Any) -> None:
            """
            Initialize and validate the fallbacklocale state.

            Example:
                Exercise  FallbackLocale.  init   through a consuming regression::

                    python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


            :param langcode: Value supplied for langcode under the utility contract.
            :return: None; validated state is stored on the receiving object.
            """
            self.langcode = langcode

    def split_into_words(text: _typing.Any, lang: _typing.Any) -> _typing.Any:
        """
        Perform the split into words operation under explicit file-format and conversion rules.

        Example:
            Exercise split into words through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param text: Text parsed, normalized or rendered.
        :param lang: Value supplied for lang under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return str(text).split()

    def index_of(word: _typing.Any, text: _typing.Any, lang: _typing.Any = None) -> _typing.Any:
        """
        Perform the index of operation under explicit file-format and conversion rules.

        Example:
            Exercise index of through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param word: Value supplied for word under the utility contract.
        :param text: Text parsed, normalized or rendered.
        :param lang: Value supplied for lang under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return str(text).find(str(word))

    def parse_lang_code(code: _typing.Any) -> _typing.Any:
        """
        Parse lang code under the format's safety and compatibility rules.

        Example:
            Exercise parse lang code through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param code: Value supplied for code under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not code:
            code = "eng"
        return _FallbackLocale(str(code).split("-")[0].lower())

# Py2/Py3 compatibility layer
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode

__license__ = "GPL v3"
__copyright__ = "2014, Kovid Goyal <kovid at kovidgoyal.net>"

_patterns = None


class Patterns(object):

    """
    Provide the patterns contract for validated ebook processing.

    Example:
        Exercise Patterns through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    __slots__ = ("sanitize_invisible_pat", "split_pat", "digit_pat", "fr_elision_pat")

    def __init__(self: _typing.Self) -> None:

        """
        Initialize and validate the patterns state.

        Example:
            Exercise Patterns.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: None; validated state is stored on the receiving object.
        """
        import regex

        # Remove soft hyphens/zero width spaces/control codes
        self.sanitize_invisible_pat = regex.compile(
            r"[\u00ad\u200b\u200c\u200d\ufeff\0-\x08\x0b\x0c\x0e-\x1f\x7f]",
            regex.VERSION1 | regex.UNICODE,
        )
        self.split_pat = regex.compile(r"\W+", flags=regex.VERSION1 | regex.WORD | regex.FULLCASE | regex.UNICODE)
        self.digit_pat = regex.compile(r"^\d+$", flags=regex.VERSION1 | regex.WORD | regex.UNICODE)
        # French words with prefixes are reduced to the stem word, so that the
        # words appear only once in the word list
        self.fr_elision_pat = regex.compile(
            "^(?:l|d|m|t|s|j|c|ç|lorsqu|puisqu|quoiqu|qu)['’]",
            flags=regex.UNICODE | regex.VERSION1 | regex.IGNORECASE,
        )


def patterns() -> _typing.Any:
    """
    Perform the patterns operation under explicit file-format and conversion rules.

    Example:
        Exercise patterns through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    global _patterns
    if _patterns is None:
        _patterns = Patterns()
    return _patterns


class Location(object):

    """
    Provide the location contract for validated ebook processing.

    Example:
        Exercise Location through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    __slots__ = (
        "file_name",
        "sourceline",
        "original_word",
        "location_node",
        "node_item",
        "elided_prefix",
    )

    def __init__(
        self: _typing.Self,
        file_name: _typing.Any = None,
        elided_prefix: str = "",
        original_word: _typing.Any = None,
        location_node: _typing.Any = None,
        node_item: tuple[_typing.Any, ...] = (None, None),
    ) -> None:
        """
        Initialize and validate the location state.

        Example:
            Exercise Location.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param file_name: Value supplied for file name under the utility contract.
        :param elided_prefix: Value supplied for elided prefix under the utility contract.
        :param original_word: Value supplied for original word under the utility contract.
        :param location_node: Value supplied for location node under the utility contract.
        :param node_item: Value supplied for node item under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.file_name, self.elided_prefix, self.original_word = (
            file_name,
            elided_prefix,
            original_word,
        )
        self.location_node, self.node_item, self.sourceline = (
            location_node,
            node_item,
            location_node.sourceline,
        )

    def __repr__(self: _typing.Self) -> _typing.Any:
        """
        Perform the repr operation under explicit file-format and conversion rules.

        Example:
            Exercise Location.  repr   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "%s @ %s:%s" % (self.original_word, self.file_name, self.sourceline)

    __str__ = __repr__

    def replace(self: _typing.Self, new_word: _typing.Any) -> None:
        """
        Perform the replace operation under explicit file-format and conversion rules.

        Example:
            Exercise Location.replace through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param new_word: Value supplied for new word under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.original_word = self.elided_prefix + new_word


def filter_words(word: _typing.Any) -> bool:
    """
    Perform the filter words operation under explicit file-format and conversion rules.

    Example:
        Exercise filter words through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param word: Value supplied for word under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if not word:
        return False
    p = patterns()
    if p.digit_pat.match(word) is not None:
        return False
    return True


def get_words(text: _typing.Any, lang: _typing.Any) -> _typing.Any:
    """
    Return words under the format's safety and compatibility rules.

    Example:
        Exercise get words through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param text: Text parsed, normalized or rendered.
    :param lang: Value supplied for lang under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        ans = split_into_words(six_unicode(text), lang)
    except (TypeError, ValueError):
        return ()
    return filter(filter_words, ans)


def add_words(text: _typing.Any, node: _typing.Any, words: _typing.Any, file_name: _typing.Any, locale: _typing.Any, node_item: _typing.Any) -> None:
    """
    Perform the add words operation under explicit file-format and conversion rules.

    Example:
        Exercise add words through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param text: Text parsed, normalized or rendered.
    :param node: Value supplied for node under the utility contract.
    :param words: Value supplied for words under the utility contract.
    :param file_name: Value supplied for file name under the utility contract.
    :param locale: Value supplied for locale under the utility contract.
    :param node_item: Value supplied for node item under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    candidates = get_words(text, locale.langcode)
    if candidates:
        p = patterns()
        is_fr = locale.langcode == "fra"
        for word in candidates:
            sword = p.sanitize_invisible_pat.sub("", word)
            elided_prefix = ""
            if is_fr:
                m = p.fr_elision_pat.match(sword)
                if m is not None and len(sword) > len(elided_prefix):
                    elided_prefix = m.group()
                    sword = sword[len(elided_prefix) :]
            loc = Location(file_name, elided_prefix, word, node, node_item)
            words[(sword, locale)].append(loc)


def add_words_from_attr(node: _typing.Any, attr: _typing.Any, words: _typing.Any, file_name: _typing.Any, locale: _typing.Any) -> None:
    """
    Perform the add words from attr operation under explicit file-format and conversion rules.

    Example:
        Exercise add words from attr through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param node: Value supplied for node under the utility contract.
    :param attr: Value supplied for attr under the utility contract.
    :param words: Value supplied for words under the utility contract.
    :param file_name: Value supplied for file name under the utility contract.
    :param locale: Value supplied for locale under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    text = node.get(attr, None)
    if text:
        add_words(text, node, words, file_name, locale, (True, attr))


def add_words_from_text(node: _typing.Any, attr: _typing.Any, words: _typing.Any, file_name: _typing.Any, locale: _typing.Any) -> None:
    """
    Perform the add words from text operation under explicit file-format and conversion rules.

    Example:
        Exercise add words from text through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param node: Value supplied for node under the utility contract.
    :param attr: Value supplied for attr under the utility contract.
    :param words: Value supplied for words under the utility contract.
    :param file_name: Value supplied for file name under the utility contract.
    :param locale: Value supplied for locale under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    add_words(getattr(node, attr), node, words, file_name, locale, (False, attr))


_opf_file_as = "{%s}file-as" % OPF_NAMESPACES["opf"]

opf_spell_tags = {"title", "creator", "subject", "description", "publisher"}

# We can only use barename() for tag names and simple attribute checks so that
# this code matches up with the syntax highlighter base spell checking


def read_words_from_opf(root: _typing.Any, words: _typing.Any, file_name: _typing.Any, book_locale: _typing.Any) -> None:
    """
    Read words from opf under the format's safety and compatibility rules.

    Example:
        Exercise read words from opf through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param words: Value supplied for words under the utility contract.
    :param file_name: Value supplied for file name under the utility contract.
    :param book_locale: Value supplied for book locale under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for tag in root.iterdescendants("*"):
        if tag.text is not None and barename(tag.tag) in opf_spell_tags:
            add_words_from_text(tag, "text", words, file_name, book_locale)
        add_words_from_attr(tag, _opf_file_as, words, file_name, book_locale)


ncx_spell_tags = {"text"}
xml_spell_tags = opf_spell_tags | ncx_spell_tags


def read_words_from_ncx(root: _typing.Any, words: _typing.Any, file_name: _typing.Any, book_locale: _typing.Any) -> None:
    """
    Read words from ncx under the format's safety and compatibility rules.

    Example:
        Exercise read words from ncx through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param words: Value supplied for words under the utility contract.
    :param file_name: Value supplied for file name under the utility contract.
    :param book_locale: Value supplied for book locale under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    for tag in root.xpath('//*[local-name()="text"]'):
        if tag.text is not None:
            add_words_from_text(tag, "text", words, file_name, book_locale)


html_spell_tags = {"script", "style", "link"}


def read_words_from_html_tag(tag: _typing.Any, words: _typing.Any, file_name: _typing.Any, parent_locale: _typing.Any, locale: _typing.Any) -> None:
    """
    Read words from html tag under the format's safety and compatibility rules.

    Example:
        Exercise read words from html tag through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param tag: Value supplied for tag under the utility contract.
    :param words: Value supplied for words under the utility contract.
    :param file_name: Value supplied for file name under the utility contract.
    :param parent_locale: Value supplied for parent locale under the utility contract.
    :param locale: Value supplied for locale under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if tag.text is not None and barename(tag.tag) not in html_spell_tags:
        add_words_from_text(tag, "text", words, file_name, locale)
    for attr in {"alt", "title"}:
        add_words_from_attr(tag, attr, words, file_name, locale)
    if tag.tail is not None and tag.getparent() is not None and barename(tag.getparent().tag) not in html_spell_tags:
        add_words_from_text(tag, "tail", words, file_name, parent_locale)


def locale_from_tag(tag: _typing.Any) -> _typing.Any:
    """
    Perform the locale from tag operation under explicit file-format and conversion rules.

    Example:
        Exercise locale from tag through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param tag: Value supplied for tag under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if "lang" in tag.attrib:
        try:
            loc = parse_lang_code(tag.get("lang"))
        except ValueError:
            loc = None
        if loc is not None:
            return loc
    if "{http://www.w3.org/XML/1998/namespace}lang" in tag.attrib:
        try:
            loc = parse_lang_code(tag.get("{http://www.w3.org/XML/1998/namespace}lang"))
        except ValueError:
            loc = None
        if loc is not None:
            return loc


def read_words_from_html(root: _typing.Any, words: _typing.Any, file_name: _typing.Any, book_locale: _typing.Any) -> None:
    """
    Read words from html under the format's safety and compatibility rules.

    Example:
        Exercise read words from html through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param root: Root directory that bounds path resolution or traversal.
    :param words: Value supplied for words under the utility contract.
    :param file_name: Value supplied for file name under the utility contract.
    :param book_locale: Value supplied for book locale under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    stack = [(root, book_locale)]
    while stack:
        parent, parent_locale = stack.pop()
        locale = locale_from_tag(parent) or parent_locale
        read_words_from_html_tag(parent, words, file_name, parent_locale, locale)
        stack.extend((tag, locale) for tag in parent.iterchildren("*"))


def group_sort(locations: _typing.Any) -> _typing.Any:
    """
    Perform the group sort operation under explicit file-format and conversion rules.

    Example:
        Exercise group sort through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param locations: Value supplied for locations under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    order = {}
    for loc in locations:
        if loc.file_name not in order:
            order[loc.file_name] = len(order)
    return sorted(locations, key=lambda l: (order[l.file_name], l.sourceline))


def get_checkable_file_names(container: _typing.Any) -> tuple[_typing.Any, ...]:
    """
    Return checkable file names under the format's safety and compatibility rules.

    Example:
        Exercise get checkable file names through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    file_names = [name for name, linear in container.spine_names] + [container.opf_name]
    toc = find_existing_toc(container)
    if toc is not None and container.exists(toc):
        file_names.append(toc)
    return file_names, toc


def get_all_words(container: _typing.Any, book_locale: _typing.Any) -> _typing.Any:
    """
    Return all words under the format's safety and compatibility rules.

    Example:
        Exercise get all words through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param book_locale: Value supplied for book locale under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    words = defaultdict(list)
    file_names, toc = get_checkable_file_names(container)
    for file_name in file_names:
        if not container.exists(file_name):
            continue
        root = container.parsed(file_name)
        if file_name == container.opf_name:
            read_words_from_opf(root, words, file_name, book_locale)
        elif file_name == toc:
            read_words_from_ncx(root, words, file_name, book_locale)
        else:
            read_words_from_html(root, words, file_name, book_locale)

    return {k: group_sort(v) for k, v in iteritems(words)}


def merge_locations(locs1: _typing.Any, locs2: _typing.Any) -> _typing.Any:
    """
    Perform the merge locations operation under explicit file-format and conversion rules.

    Example:
        Exercise merge locations through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param locs1: Value supplied for locs1 under the utility contract.
    :param locs2: Value supplied for locs2 under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return group_sort(locs1 + locs2)


def replace(text: _typing.Any, original_word: _typing.Any, new_word: _typing.Any, lang: _typing.Any) -> tuple[_typing.Any, ...]:
    """
    Perform the replace operation under explicit file-format and conversion rules.

    Example:
        Exercise replace through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param text: Text parsed, normalized or rendered.
    :param original_word: Value supplied for original word under the utility contract.
    :param new_word: Value supplied for new word under the utility contract.
    :param lang: Value supplied for lang under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    indices = []
    original_word, new_word, text = (
        six_unicode(original_word),
        six_unicode(new_word),
        six_unicode(text),
    )
    q = text
    offset = 0
    while True:
        idx = index_of(original_word, q, lang=lang)
        if idx == -1:
            break
        indices.append(offset + idx)
        offset += idx + len(original_word)
        q = text[offset:]
    for idx in reversed(indices):
        text = text[:idx] + new_word + text[idx + len(original_word) :]
    return text, bool(indices)


def replace_word(container: _typing.Any, new_word: _typing.Any, locations: _typing.Any, locale: _typing.Any) -> _typing.Any:
    """
    Perform the replace word operation under explicit file-format and conversion rules.

    Example:
        Exercise replace word through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param container: Value supplied for container under the utility contract.
    :param new_word: Value supplied for new word under the utility contract.
    :param locations: Value supplied for locations under the utility contract.
    :param locale: Value supplied for locale under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    changed = set()
    for loc in locations:
        node = loc.location_node
        is_attr, attr = loc.node_item
        if is_attr:
            text = node.get(attr)
        else:
            text = getattr(node, attr)
        replacement = loc.elided_prefix + new_word
        text, replaced = replace(text, loc.original_word, replacement, locale.langcode)
        if replaced:
            if is_attr:
                node.set(attr, text)
            else:
                setattr(node, attr, text)
            container.replace(loc.file_name, node.getroottree().getroot())
            changed.add(loc.file_name)
    return changed


if __name__ == "__main__":
    import pprint

    main_container = get_container(sys.argv[-1], tweak_mode=True)
    pprint.pprint(get_all_words(main_container, "en"))
