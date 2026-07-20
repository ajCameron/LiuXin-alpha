# -*- coding: utf-8 -*-
#  jisyo.py
#
# Copyright 2011 Hiroshi Miura <miurahr@linux.com>
from __future__ import annotations

import typing as _typing
import marshal
from zlib import decompress

from LiuXin_alpha.utils.lx_libraries.liuxin_six import six_pickle as cPickle
from LiuXin_alpha.utils.resources import P


class jisyo(object):
    kanwadict = None
    itaijidict = None
    kanadict = None
    jisyo_table = {}

    # this class is Borg
    _shared_state = {}

    def __new__(cls: type[_typing.Self], *p: _typing.Any, **k: _typing.Any) -> _typing.Any:
        self = object.__new__(cls, *p, **k)
        self.__dict__ = cls._shared_state
        return self

    def __init__(self: _typing.Self) -> None:
        if self.kanwadict is None:
            self.kanwadict = cPickle.loads(P("localization/pykakasi/kanwadict2.pickle", data=True))
        if self.itaijidict is None:
            self.itaijidict = cPickle.loads(P("localization/pykakasi/itaijidict2.pickle", data=True))
        if self.kanadict is None:
            self.kanadict = cPickle.loads(P("localization/pykakasi/kanadict2.pickle", data=True))

    def load_jisyo(self: _typing.Self, char: _typing.Any) -> _typing.Any:
        try:  # python2
            key = "%04x" % ord(unicode(char))
        except:  # python3
            key = "%04x" % ord(char)

        try:  # already exist?
            table = self.jisyo_table[key]
        except:
            try:
                table = self.jisyo_table[key] = marshal.loads(decompress(self.kanwadict[key]))
            except:
                return None
        return table
