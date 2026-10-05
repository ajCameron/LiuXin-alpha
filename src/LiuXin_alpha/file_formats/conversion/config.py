#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Load, merge and persist conversion option profiles.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise config through a consuming regression::

        python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py
"""
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
    """
    Perform the name to path operation under explicit file-format and conversion rules.

    Example:
        Exercise name to path through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return os.path.join(config_dir, sanitize_file_name(name) + ".py")


def save_defaults(name: _typing.Any, recs: _typing.Any) -> None:
    """
    Perform the save defaults operation under explicit file-format and conversion rules.

    Example:
        Exercise save defaults through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param recs: Value supplied for recs under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    path = name_to_path(name)
    raw = str(recs)
    with open(path, "wb"):
        pass
    with ExclusiveFile(path) as f:
        f.write(raw)


def load_defaults(name: _typing.Any) -> _typing.Any:
    """
    Perform the load defaults operation under explicit file-format and conversion rules.

    Example:
        Exercise load defaults through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
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
    """
    Perform the save specifics operation under explicit file-format and conversion rules.

    Example:
        Exercise save specifics through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


    :param db: Value supplied for db under the utility contract.
    :param book_id: Value supplied for book id under the utility contract.
    :param recs: Value supplied for recs under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    raw = str(recs)
    db.set_conversion_options(book_id, "PIPE", raw)


def load_specifics(db: _typing.Any, book_id: _typing.Any) -> _typing.Any:
    """
    Perform the load specifics operation under explicit file-format and conversion rules.

    Example:
        Exercise load specifics through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


    :param db: Value supplied for db under the utility contract.
    :param book_id: Value supplied for book id under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    raw = db.conversion_options(book_id, "PIPE")
    r = GuiRecommendations()
    if raw:
        r.from_string(raw)
    return r


def delete_specifics(db: _typing.Any, book_id: _typing.Any) -> None:
    """
    Perform the delete specifics operation under explicit file-format and conversion rules.

    Example:
        Exercise delete specifics through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


    :param db: Value supplied for db under the utility contract.
    :param book_id: Value supplied for book id under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    db.delete_conversion_options(book_id, "PIPE")


class GuiRecommendations(dict):
    """
    Provide the guirecommendations contract for validated ebook processing.

    Example:
        Exercise GuiRecommendations through a consuming regression::

            python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py
    """
    def __new__(cls: type[_typing.Self], *args: _typing.Any) -> _typing.Any:
        """
        Perform the new operation under explicit file-format and conversion rules.

        Example:
            Exercise GuiRecommendations.  new   through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


        :param args: Positional values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        dict.__new__(cls)
        obj = super(GuiRecommendations, cls).__new__(cls, *args)
        obj.disabled_options = set([])
        return obj

    def to_recommendations(self: _typing.Self, level: _typing.Any = OptionRecommendation.LOW) -> _typing.Any:
        """
        Perform the to recommendations operation under explicit file-format and conversion rules.

        Example:
            Exercise GuiRecommendations.to recommendations through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


        :param level: Value supplied for level under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = []
        for key, val in self.items():
            ans.append((key, val, level))
        return ans

    def __str__(self: _typing.Self) -> _typing.Any:
        """
        Perform the str operation under explicit file-format and conversion rules.

        Example:
            Exercise GuiRecommendations.  str   through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        ans = ["{"]
        for key, val in self.items():
            ans.append("\t" + repr(key) + " : " + repr(val) + ",")
        ans.append("}")
        return "\n".join(ans)

    def from_string(self: _typing.Self, raw: _typing.Any) -> None:
        """
        Perform the from string operation under explicit file-format and conversion rules.

        Example:
            Exercise GuiRecommendations.from string through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


        :param raw: Value supplied for raw under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        try:
            d = eval(raw)
        except (SyntaxError, TypeError):
            d = None
        if d:
            self.update(d)

    def merge_recommendations(self: _typing.Self, get_option: _typing.Any, level: _typing.Any, options: _typing.Any, only_existing: bool = False) -> None:
        """
        Perform the merge recommendations operation under explicit file-format and conversion rules.

        Example:
            Exercise GuiRecommendations.merge recommendations through a consuming regression::

                python -m pytest -q tests/file_formats/conversion/test_conversion_top_level_smoke.py


        :param get_option: Value supplied for get option under the utility contract.
        :param level: Value supplied for level under the utility contract.
        :param options: Value supplied for options under the utility contract.
        :param only_existing: Value supplied for only existing under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
