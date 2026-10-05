#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Read and write persisted OEB reader bookmarks.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise bookmarks through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import os
from io import BytesIO

from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.libraries.calibre_zipfile import safe_replace

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"

BM_FIELD_SEP = "*|!|?|*"
BM_LEGACY_ESC = "esc-text-%&*#%(){}ads19-end-esc"


class BookmarksMixin(object):
    """
    Provide the bookmarksmixin contract for validated ebook processing.

    Example:
        Exercise BookmarksMixin through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py
    """
    def parse_bookmarks(self: _typing.Self, raw: _typing.Any) -> None:
        """
        Parse bookmarks under the format's safety and compatibility rules.

        Example:
            Exercise BookmarksMixin.parse bookmarks through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param raw: Value supplied for raw under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for line in raw.splitlines():
            bm = None
            if line.count("^") > 0:
                tokens = line.rpartition("^")
                title, ref = tokens[0], tokens[2]
                try:
                    spine, _, pos = ref.partition("#")
                    spine = int(spine.strip())
                except:
                    continue
                bm = {"type": "legacy", "title": title, "spine": spine, "pos": pos}
            elif BM_FIELD_SEP in line:
                try:
                    title, spine, pos = line.strip().split(BM_FIELD_SEP)
                    spine = int(spine)
                except:
                    continue
                # Unescape from serialization
                pos = pos.replace(BM_LEGACY_ESC, "^")
                # Check for pos being a scroll fraction
                try:
                    pos = float(pos)
                except:
                    pass
                bm = {"type": "cfi", "title": title, "pos": pos, "spine": spine}

            if bm:
                self.bookmarks.append(bm)

    def serialize_bookmarks(self: _typing.Self, bookmarks: _typing.Any) -> _typing.Any:
        """
        Serialize bookmarks under the format's safety and compatibility rules.

        Example:
            Exercise BookmarksMixin.serialize bookmarks through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param bookmarks: Value supplied for bookmarks under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        dat = []
        for bm in bookmarks:
            if bm["type"] == "legacy":
                rec = "%s^%d#%s" % (bm["title"], bm["spine"], bm["pos"])
            else:
                pos = bm["pos"]
                if isinstance(pos, (int, float)):
                    pos = six_unicode(pos)
                else:
                    pos = pos.replace("^", BM_LEGACY_ESC)
                rec = BM_FIELD_SEP.join([bm["title"], six_unicode(bm["spine"]), pos])
            dat.append(rec)
        return "\n".join(dat) + "\n"

    def read_bookmarks(self: _typing.Self) -> None:
        """
        Read bookmarks under the format's safety and compatibility rules.

        Example:
            Exercise BookmarksMixin.read bookmarks through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.bookmarks = []
        bmfile = os.path.join(self.base, "META-INF", "calibre_bookmarks.txt")
        raw = ""
        if os.path.exists(bmfile):
            with open(bmfile, "rb") as f:
                raw = f.read()
        else:
            saved = self.config["bookmarks_" + self.pathtoebook]
            if saved:
                raw = saved
        if not isinstance(raw, six_string_types):
            raw = raw.decode("utf-8")
        self.parse_bookmarks(raw)

    def save_bookmarks(self: _typing.Self, bookmarks: _typing.Any = None) -> None:
        """
        Perform the save bookmarks operation under explicit file-format and conversion rules.

        Example:
            Exercise BookmarksMixin.save bookmarks through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param bookmarks: Value supplied for bookmarks under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        if bookmarks is None:
            bookmarks = self.bookmarks
        dat = self.serialize_bookmarks(bookmarks)
        if os.path.splitext(self.pathtoebook)[1].lower() == ".epub" and os.access(self.pathtoebook, os.R_OK):
            try:
                zf = open(self.pathtoebook, "r+b")
            except IOError:
                return
            safe_replace(
                zf,
                "META-INF/calibre_bookmarks.txt",
                BytesIO(dat.encode("utf-8")),
                add_missing=True,
            )
        else:
            self.config["bookmarks_" + self.pathtoebook] = dat

    def add_bookmark(self: _typing.Self, bm: _typing.Any) -> None:
        """
        Perform the add bookmark operation under explicit file-format and conversion rules.

        Example:
            Exercise BookmarksMixin.add bookmark through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param bm: Value supplied for bm under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.bookmarks = [x for x in self.bookmarks if x["title"] != bm["title"]]
        self.bookmarks.append(bm)
        self.save_bookmarks()

    def set_bookmarks(self: _typing.Self, bookmarks: _typing.Any) -> None:
        """
        Set bookmarks under the format's safety and compatibility rules.

        Example:
            Exercise BookmarksMixin.set bookmarks through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_backend_smoke.py


        :param bookmarks: Value supplied for bookmarks under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.bookmarks = bookmarks
