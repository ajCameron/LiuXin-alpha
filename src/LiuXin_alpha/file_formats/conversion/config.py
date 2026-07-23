#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

from __future__ import with_statement
from __future__ import annotations

import typing as _typing

import os

from LiuXin_alpha.customize.conversion import OptionRecommendation

from LiuXin_alpha.utils.calibre import sanitize_file_name
from LiuXin_alpha.utils.config.config_tools import config_dir
from LiuXin_alpha.utils.lock import ExclusiveFile

__license__ = "GPL v3"
__copyright__ = "2009, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


config_dir = os.path.join(config_dir, "conversion")
if not os.path.exists(config_dir):
    os.makedirs(config_dir)


def name_to_path(name: _typing.Any) -> _typing.Any:
    return os.path.join(config_dir, sanitize_file_name(name) + ".py")


def save_defaults(name: _typing.Any, recs: _typing.Any) -> None:
    path = name_to_path(name)
    raw = str(recs)
    with open(path, "wb"):
        pass
    with ExclusiveFile(path) as f:
        f.write(raw)


def load_defaults(name: _typing.Any) -> _typing.Any:
    path = name_to_path(name)
    if not os.path.exists(path):
        open(path, "wb").close()
    with ExclusiveFile(path) as f:
        raw = f.read()
    r = GuiRecommendations()
    if raw:
        r.from_string(raw)
    return r


def save_specifics(db: _typing.Any, book_id: _typing.Any, recs: _typing.Any) -> None:
    raw = str(recs)
    db.set_conversion_options(book_id, "PIPE", raw)


def load_specifics(db: _typing.Any, book_id: _typing.Any) -> _typing.Any:
    raw = db.conversion_options(book_id, "PIPE")
    r = GuiRecommendations()
    if raw:
        r.from_string(raw)
    return r


def delete_specifics(db: _typing.Any, book_id: _typing.Any) -> None:
    db.delete_conversion_options(book_id, "PIPE")


class GuiRecommendations(dict):
    def __new__(cls: type[_typing.Self], *args: _typing.Any) -> _typing.Any:
        dict.__new__(cls)
        obj = super(GuiRecommendations, cls).__new__(cls, *args)
        obj.disabled_options = set([])
        return obj

    def to_recommendations(self: _typing.Self, level: _typing.Any = OptionRecommendation.LOW) -> _typing.Any:
        ans = []
        for key, val in self.items():
            ans.append((key, val, level))
        return ans

    def __str__(self: _typing.Self) -> _typing.Any:
        ans = ["{"]
        for key, val in self.items():
            ans.append("\t" + repr(key) + " : " + repr(val) + ",")
        ans.append("}")
        return "\n".join(ans)

    def from_string(self: _typing.Self, raw: _typing.Any) -> None:
        try:
            d = eval(raw)
        except (SyntaxError, TypeError):
            d = None
        if d:
            self.update(d)

    def merge_recommendations(self: _typing.Self, get_option: _typing.Any, level: _typing.Any, options: _typing.Any, only_existing: bool = False) -> None:
        for name in options:
            if only_existing and name not in self:
                continue
            opt = get_option(name)
            if opt is None:
                continue
            if opt.level == OptionRecommendation.HIGH:
                self[name] = opt.recommended_value
                self.disabled_options.add(name)
            elif opt.level > level or name not in self:
                self[name] = opt.recommended_value
