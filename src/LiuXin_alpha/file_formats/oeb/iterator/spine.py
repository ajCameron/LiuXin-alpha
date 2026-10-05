#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Resolve OEB spine items and page boundaries for iteration.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise spine through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
"""
from __future__ import (
    absolute_import,
    annotations,
    division,
    print_function,
    unicode_literals,
)

import os
import re
import typing as _typing
from collections import namedtuple
from functools import partial
from operator import attrgetter

try:
    from past.builtins import unicode
except ModuleNotFoundError:
    from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode as unicode

from LiuXin_alpha.utils.calibre import guess_type, replace_entities
from LiuXin_alpha.utils.libraries.calibre_chardet import xml_to_unicode

# Py2/Py3 compatability layer
from LiuXin_alpha.utils.libraries.liuxin_six import six_map
from LiuXin_alpha.utils.localization import trans as _

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


def character_count(html: _typing.Any) -> _typing.Any:
    """
    Return the number of "significant" text characters in a HTML string.

    Example:
        Exercise character count through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param html: Value supplied for html under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    count = 0
    strip_space = re.compile(r"\s+")
    for match in re.finditer(r">[^<]+<", html):
        count += len(strip_space.sub(" ", match.group())) - 2
    return count


def anchor_map(html: _typing.Any) -> _typing.Any:
    """
    Return map of all anchor names to their offsets in the html

    Example:
        Exercise anchor map through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param html: Value supplied for html under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ans = {}
    for match in re.finditer(r"""(?:id|name)\s*=\s*['"]([^'"]+)['"]""", html):
        anchor = match.group(1)
        ans[anchor] = ans.get(anchor, match.start())
    return ans


def all_links(html: _typing.Any) -> _typing.Any:
    """
    Return set of all links in the file

    Example:
        Exercise all links through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param html: Value supplied for html under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    ans = set()
    for match in re.finditer(
        r"""<\s*[Aa]\s+.*?[hH][Rr][Ee][Ff]\s*=\s*(['"])(.+?)\1""",
        html,
        re.MULTILINE | re.DOTALL,
    ):
        ans.add(replace_entities(match.group(2)))
    return ans


class SpineItem(unicode):
    """
    Provide the spineitem contract for validated ebook processing.

    Example:
        Exercise SpineItem through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """
    def __new__(
        cls: type[_typing.Self],
        path: _typing.Any,
        mime_type: _typing.Any = None,
        read_anchor_map: bool = True,
        run_char_count: bool = True,
        from_epub: bool = False,
        read_links: bool = True,
    ) -> _typing.Any:
        """
        Perform the new operation under explicit file-format and conversion rules.

        Example:
            Exercise SpineItem.  new   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param mime_type: Value supplied for mime type under the utility contract.
        :param read_anchor_map: Value supplied for read anchor map under the utility
            contract.
        :param run_char_count: Value supplied for run char count under the utility contract.
        :param from_epub: Value supplied for from epub under the utility contract.
        :param read_links: Value supplied for read links under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ppath = path.partition("#")[0]
        if not os.path.exists(path) and os.path.exists(ppath):
            path = ppath
        obj = super(SpineItem, cls).__new__(cls, path)
        with open(path, "rb") as f:
            raw = f.read()
        if from_epub:
            # According to the spec, HTML in EPUB must be encoded in utf-8 or
            # utf-16. Furthermore, there exist epub files produced by the usual
            # incompetents that have utf-8 encoded HTML files that contain
            # incorrect encoding declarations. See
            # http://www.idpf.org/epub/20/spec/OPS_2.0.1_draft.htm#Section1.4.1.2
            # http://www.idpf.org/epub/30/spec/epub30-publications.html#confreq-xml-enc
            # https://bugs.launchpad.net/bugs/1188843
            # So we first decode with utf-8 and only if that fails we try xml_to_unicode. This
            # is the same algorithm as that used by the conversion pipeline (modulo
            # some BOM based detection). Sigh.
            try:
                raw, obj.encoding = raw.decode("utf-8"), "utf-8"
            except UnicodeDecodeError:
                raw, obj.encoding = xml_to_unicode(raw)
        else:
            raw, obj.encoding = xml_to_unicode(raw)
        obj.character_count = character_count(raw) if run_char_count else 10000
        obj.anchor_map = anchor_map(raw) if read_anchor_map else {}
        obj.all_links = all_links(raw) if read_links else set()
        obj.verified_links = set()
        obj.start_page = -1
        obj.pages = -1
        obj.max_page = -1
        obj.index_entries = []
        if mime_type is None:
            mime_type = guess_type(obj)[0]
        obj.mime_type = mime_type
        obj.is_single_page = None
        return obj


class IndexEntry(object):
    """
    Provide the indexentry contract for validated ebook processing.

    Example:
        Exercise IndexEntry through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """
    def __init__(self: _typing.Self, spine: _typing.Any, toc_entry: _typing.Any, num: _typing.Any) -> None:
        """
        Initialize and validate the indexentry state.

        Example:
            Exercise IndexEntry.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param spine: Value supplied for spine under the utility contract.
        :param toc_entry: Value supplied for toc entry under the utility contract.
        :param num: Value supplied for num under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.num = num
        self.text = toc_entry.text or _("Unknown")
        self.key = toc_entry.abspath
        self.anchor = self.start_anchor = toc_entry.fragment or None
        try:
            self.spine_pos = spine.index(self.key)
        except ValueError:
            self.spine_pos = -1
        self.anchor_pos = 0
        if self.spine_pos > -1:
            self.anchor_pos = spine[self.spine_pos].anchor_map.get(self.anchor, 0)

        self.depth = 0
        p = toc_entry.parent
        while p is not None:
            self.depth += 1
            p = p.parent

        self.sort_key = (self.spine_pos, self.anchor_pos)
        self.spine_count = len(spine)

    def find_end(self: _typing.Self, all_entries: _typing.Any) -> None:
        """
        Find end under the format's safety and compatibility rules.

        Example:
            Exercise IndexEntry.find end through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param all_entries: Value supplied for all entries under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        potential_enders = [
            i
            for i in all_entries
            if i.depth <= self.depth
            and ((i.spine_pos == self.spine_pos and i.anchor_pos > self.anchor_pos) or i.spine_pos > self.spine_pos)
        ]
        if potential_enders:
            # potential_enders is sorted by (spine_pos, anchor_pos)
            end = potential_enders[0]
            self.end_spine_pos = end.spine_pos
            self.end_anchor = end.anchor
        else:
            self.end_spine_pos = self.spine_count - 1
            self.end_anchor = None


def create_indexing_data(spine: _typing.Any, toc: _typing.Any) -> None:
    """
    Create indexing data under the format's safety and compatibility rules.

    Example:
        Exercise create indexing data through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


    :param spine: Value supplied for spine under the utility contract.
    :param toc: Value supplied for toc under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    if not toc:
        return
    f = partial(IndexEntry, spine)
    index_entries = list(
        six_map(
            f,
            (t for t in toc.flat() if t is not toc),
            (i - 1 for i, t in enumerate(toc.flat()) if t is not toc),
        )
    )
    index_entries.sort(key=attrgetter("sort_key"))
    [i.find_end(index_entries) for i in index_entries]

    ie = namedtuple("IndexEntry", "entry start_anchor end_anchor")

    for spine_pos, spine_item in enumerate(spine):
        for i in index_entries:
            if i.end_spine_pos < spine_pos or i.spine_pos > spine_pos:
                continue  # Does not touch this file
            start = i.anchor if i.spine_pos == spine_pos else None
            end = i.end_anchor if i.spine_pos == spine_pos else None
            spine_item.index_entries.append(ie(i, start, end))
