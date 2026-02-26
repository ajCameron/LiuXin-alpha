#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import unicode_literals, division, absolute_import, print_function

import os
from io import BytesIO

from LiuXin_alpha.utils.libraries.liuxin_six import six_string_types
from LiuXin_alpha.utils.libraries.liuxin_six import six_unicode
from LiuXin_alpha.utils.calibre_utils.calibre_zipfile import safe_replace

__license__ = "GPL v3"
__copyright__ = "2012, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"

BM_FIELD_SEP = "*|!|?|*"
BM_LEGACY_ESC = "esc-text-%&*#%(){}ads19-end-esc"


class BookmarksMixin(object):
    def parse_bookmarks(self, raw):
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

    def serialize_bookmarks(self, bookmarks):
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

    def read_bookmarks(self):
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

    def save_bookmarks(self, bookmarks=None):
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

    def add_bookmark(self, bm):
        self.bookmarks = [x for x in self.bookmarks if x["title"] != bm["title"]]
        self.bookmarks.append(bm)
        self.save_bookmarks()

    def set_bookmarks(self, bookmarks):
        self.bookmarks = bookmarks
