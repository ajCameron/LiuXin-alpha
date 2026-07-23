#!/usr/bin/env python2
# vim:fileencoding=utf-8
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

from collections import OrderedDict

# Py2/Py3
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


class Note(object):
    def __init__(self: _typing.Self, namespace: _typing.Any, parent: _typing.Any, rels: _typing.Any) -> None:
        self.type = namespace.get(parent, "w:type", "normal")
        self.parent = parent
        self.rels = rels
        self.namespace = namespace

    def __iter__(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        for p in self.namespace.descendants(self.parent, "w:p", "w:tbl"):
            yield p


class Footnotes(object):
    def __init__(self: _typing.Self, namespace: _typing.Any) -> None:
        self.namespace = namespace
        self.footnotes = {}
        self.endnotes = {}
        self.counter = 0
        self.notes = OrderedDict()

    def __call__(self: _typing.Self, footnotes: _typing.Any, footnotes_rels: _typing.Any, endnotes: _typing.Any, endnotes_rels: _typing.Any) -> None:
        xpath, get = self.namespace.XPath, self.namespace.get
        if footnotes is not None:
            for footnote in xpath("./w:footnote[@w:id]")(footnotes):
                fid = get(footnote, "w:id")
                if fid:
                    self.footnotes[fid] = Note(self.namespace, footnote, footnotes_rels)

        if endnotes is not None:
            for endnote in xpath("./w:endnote[@w:id]")(endnotes):
                fid = get(endnote, "w:id")
                if fid:
                    self.endnotes[fid] = Note(self.namespace, endnote, endnotes_rels)

    def get_ref(self: _typing.Self, ref: _typing.Any) -> tuple[_typing.Any, ...]:
        fid = self.namespace.get(ref, "w:id")
        notes = self.footnotes if ref.tag.endswith("}footnoteReference") else self.endnotes
        note = notes.get(fid, None)
        if note is not None and note.type == "normal":
            self.counter += 1
            anchor = "note_%d" % self.counter
            self.notes[anchor] = (type("")(self.counter), note)
            return anchor, type("")(self.counter)
        return None, None

    def __iter__(self: _typing.Self) -> _typing.Iterator[_typing.Any]:
        for anchor, (counter, note) in iteritems(self.notes):
            yield anchor, counter, note

    @property
    def has_notes(self: _typing.Self) -> _typing.Any:
        return bool(self.notes)
