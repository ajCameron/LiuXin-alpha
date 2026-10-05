# -*- coding: utf-8 -*-
#  jisyo.py
#
# Copyright 2011 Hiroshi Miura <miurahr@linux.com>
"""
Load and query retained Japanese transliteration dictionaries.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise jisyo through a consuming regression::

        python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
"""
from __future__ import annotations

import typing as _typing
import marshal
from zlib import decompress

from LiuXin_alpha.utils.lx_libraries.liuxin_six import six_pickle as cPickle
from LiuXin_alpha.utils.resources import P


class jisyo(object):
    """
    Provide the jisyo contract for validated ebook processing.

    Example:
        Exercise jisyo through a consuming regression::

            python -m pytest -q tests/file_formats/txt/test_txt_modernized.py
    """
    kanwadict = None
    itaijidict = None
    kanadict = None
    jisyo_table = {}

    # this class is Borg
    _shared_state = {}

    def __new__(cls: type[_typing.Self], *p: _typing.Any, **k: _typing.Any) -> _typing.Any:
        """
        Perform the new operation under explicit file-format and conversion rules.

        Example:
            Exercise jisyo.  new   through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param p: Path-like value normalized or validated by the operation.
        :param k: Value supplied for k under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self = object.__new__(cls, *p, **k)
        self.__dict__ = cls._shared_state
        return self

    def __init__(self: _typing.Self) -> None:
        """
        Initialize and validate the jisyo state.

        Example:
            Exercise jisyo.  init   through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :return: None; validated state is stored on the receiving object.
        """
        if self.kanwadict is None:
            self.kanwadict = cPickle.loads(P("localization/pykakasi/kanwadict2.pickle", data=True))
        if self.itaijidict is None:
            self.itaijidict = cPickle.loads(P("localization/pykakasi/itaijidict2.pickle", data=True))
        if self.kanadict is None:
            self.kanadict = cPickle.loads(P("localization/pykakasi/kanadict2.pickle", data=True))

    def load_jisyo(self: _typing.Self, char: _typing.Any) -> _typing.Any:
        """
        Perform the load jisyo operation under explicit file-format and conversion rules.

        Example:
            Exercise jisyo.load jisyo through a consuming regression::

                python -m pytest -q tests/file_formats/txt/test_txt_modernized.py


        :param char: Value supplied for char under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
