#!/usr/bin/env python
# vim:fileencoding=utf-8

"""
Provide shared fixtures and assertions for OEB polishing regressions.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise base through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
"""
from __future__ import unicode_literals, division, absolute_import, print_function
from __future__ import annotations

import typing as _typing

import os
import shutil
import unittest
import glob

import LiuXin_alpha.file_formats.oeb.polish.container as pc

from LiuXin_alpha.utils.storage.local import CurrentDir
from LiuXin_alpha.utils.ptempfiles import TemporaryDirectory
from LiuXin_alpha.utils.ptempfiles import PersistentTemporaryDirectory
from LiuXin_alpha.utils.resources import I
from LiuXin_alpha.utils.resources import P

# Py2/Py3 compatability layer
from LiuXin_alpha.utils.libraries.liuxin_six import dict_iteritems as iteritems

__license__ = "GPL v3"
__copyright__ = "2013, Kovid Goyal <kovid at kovidgoyal.net>"


def get_cache() -> _typing.Any:
    """
    Return cache under the format's safety and compatibility rules.

    Example:
        Exercise get cache through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from LiuXin_alpha.constants import cache_dir

    cache = os.path.join(cache_dir(), "polish-test")
    os.makedirs(cache, exist_ok=True)
    return cache


def needs_recompile(obj: _typing.Any, srcs: _typing.Any) -> bool:
    """
    Perform the needs recompile operation under explicit file-format and conversion rules.

    Example:
        Exercise needs recompile through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param obj: Value supplied for obj under the utility contract.
    :param srcs: Value supplied for srcs under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    if isinstance(srcs, type("")):
        srcs = [srcs]
    try:
        obj_mtime = os.stat(obj).st_mtime
    except OSError:
        return True
    for src in srcs:
        if os.stat(src).st_mtime > obj_mtime:
            return True
    return False


def build_book(src: _typing.Any, dest: _typing.Any, args: tuple[_typing.Any, ...] = ()) -> None:
    """
    Perform the build book operation under explicit file-format and conversion rules.

    Example:
        Exercise build book through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param src: Value supplied for src under the utility contract.
    :param dest: Value supplied for dest under the utility contract.
    :param args: Positional values forwarded to the compatibility implementation.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.file_formats.conversion.cli import main

    main(["ebook-convert", src, dest] + list(args))


def add_resources(raw: _typing.Any, rmap: _typing.Any) -> _typing.Any:
    """
    Perform the add resources operation under explicit file-format and conversion rules.

    Example:
        Exercise add resources through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param raw: Value supplied for raw under the utility contract.
    :param rmap: Value supplied for rmap under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    for placeholder, path in iteritems(rmap):
        if not path:
            raise RuntimeError("Missing required polish test resource for placeholder: %s" % placeholder)
        fname = os.path.basename(path)
        shutil.copy2(path, ".")
        raw = raw.replace(placeholder, fname)
    return raw


def _existing_path(*paths: _typing.Any) -> _typing.Any:
    """
    Perform the existing path operation under explicit file-format and conversion rules.

    Example:
        Exercise  existing path through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param paths: Value supplied for paths under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    for path in paths:
        if path and os.path.exists(path):
            return path
    return None


def _find_any_ttf_font() -> _typing.Any:
    """
    Find any ttf font under the format's safety and compatibility rules.

    Example:
        Exercise  find any ttf font through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    for pattern in (
        "/usr/share/fonts/**/*.ttf",
        "/usr/local/share/fonts/**/*.ttf",
    ):
        matches = glob.glob(pattern, recursive=True)
        if matches:
            return matches[0]
    return None


def get_simple_book(fmt: str = "epub") -> _typing.Any:
    """
    Return simple book under the format's safety and compatibility rules.

    Example:
        Exercise get simple book through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param fmt: Date, number or template format specification.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    cache = get_cache()
    ans = os.path.join(cache, "simple." + fmt)
    src = os.path.join(os.path.dirname(__file__), "simple.html")
    if needs_recompile(ans, src):
        with TemporaryDirectory("bpt") as tdir:
            with CurrentDir(tdir):
                raw = open(src, "rb").read().decode("utf-8")
                raw = add_resources(
                    raw,
                    {
                        "LMONOI": _existing_path(
                            P("fonts/liberation/LiberationMono-Italic.ttf"),
                            P("fonts/liberation2/LiberationMono-Italic.ttf"),
                            _find_any_ttf_font(),
                        ),
                        "LMONOR": _existing_path(
                            P("fonts/liberation/LiberationMono-Regular.ttf"),
                            P("fonts/liberation2/LiberationMono-Regular.ttf"),
                            _find_any_ttf_font(),
                        ),
                        "IMAGE1": I("marked.png"),
                        "IMAGE2": I("textures/light_wood.png"),
                    },
                )
                shutil.copy2(I("lt.png"), ".")
                x = "index.html"
                with open(x, "wb") as f:
                    f.write(raw.encode("utf-8"))
                build_book(
                    x,
                    ans,
                    args=[
                        "--level1-toc=//h:h2",
                        "--language=en",
                        "--authors=Kovid Goyal",
                        "--cover=lt.png",
                    ],
                )
    return ans


def get_split_book(fmt: str = "epub") -> _typing.Any:
    """
    Return split book under the format's safety and compatibility rules.

    Example:
        Exercise get split book through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param fmt: Date, number or template format specification.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    cache = get_cache()
    ans = os.path.join(cache, "split." + fmt)
    src = os.path.join(os.path.dirname(__file__), "split.html")
    if needs_recompile(ans, src):
        raw = open(src, "rb").read().decode("utf-8")
        with TemporaryDirectory("bpt") as tdir:
            with CurrentDir(tdir):
                x = "index.html"
                with open(x, "wb") as f:
                    f.write(raw.encode("utf-8"))
                build_book(
                    x,
                    ans,
                    args=[
                        "--level1-toc=//h:h2",
                        "--language=en",
                        "--authors=Kovid Goyal",
                        "--cover=" + I("lt.png"),
                    ],
                )
    return ans


class DevNull(object):
    """
    Provide the devnull contract for validated ebook processing.

    Example:
        Exercise DevNull through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    def __call__(self: _typing.Self, *args: _typing.Any, **kwargs: _typing.Any) -> None:
        """
        Perform the call operation under explicit file-format and conversion rules.

        Example:
            Exercise DevNull.  call   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return None

    def __getattr__(self: _typing.Self, name: _typing.Any) -> _typing.Any:
        """
        Perform the getattr operation under explicit file-format and conversion rules.

        Example:
            Exercise DevNull.  getattr   through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self


devnull = DevNull()


class BaseTest(unittest.TestCase):

    """
    Provide the basetest contract for validated ebook processing.

    Example:
        Exercise BaseTest through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
    """
    longMessage = True
    maxDiff = None

    def setUp(self: _typing.Self) -> None:
        """
        Perform the setUp operation under explicit file-format and conversion rules.

        Example:
            Exercise BaseTest.setUp through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        pc.default_log = devnull
        self.tdir = PersistentTemporaryDirectory(suffix="-polish-test")

    def tearDown(self: _typing.Self) -> None:
        """
        Perform the tearDown operation under explicit file-format and conversion rules.

        Example:
            Exercise BaseTest.tearDown through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        shutil.rmtree(self.tdir, ignore_errors=True)
        del self.tdir

    def check_links(self: _typing.Self, container: _typing.Any) -> None:
        """
        Perform the check links operation under explicit file-format and conversion rules.

        Example:
            Exercise BaseTest.check links through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param container: Value supplied for container under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for name in container.name_path_map:
            for link in container.iterlinks(name, get_line_numbers=False):
                dest = container.href_to_name(link, name)
                if dest:
                    self.assertTrue(
                        container.exists(dest),
                        "The link %s in %s does not exist" % (link, name),
                    )
