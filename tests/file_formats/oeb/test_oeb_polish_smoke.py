"""
Provide test oeb polish smoke utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test oeb polish smoke through a consuming regression::

        python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
"""
from __future__ import annotations

import builtins
import importlib
import io
import pkgutil
import unittest
from collections import namedtuple

import pytest


def test_oeb_polish_package_import_sweep() -> None:
    """
    Perform the test oeb polish package import sweep operation under explicit file-format and conversion rules.

    Example:
        Exercise test oeb polish package import sweep through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    import LiuXin_alpha.file_formats.oeb.polish as polish_pkg

    failures = []
    for modinfo in pkgutil.walk_packages(polish_pkg.__path__, polish_pkg.__name__ + "."):
        try:
            importlib.import_module(modinfo.name)
        except Exception as e:  # pragma: no cover - failure reporting path
            failures.append((modinfo.name, type(e).__name__, str(e)))
    assert not failures, failures


def test_oeb_polish_internal_unittest_suite_smoke() -> None:
    """
    Perform the test oeb polish internal unittest suite smoke operation under explicit file-format and conversion rules.

    Example:
        Exercise test oeb polish internal unittest suite smoke through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    pytest.importorskip("cssutils", reason="cssutils not installed; embedded oeb polish unittest suite requires it")

    from LiuXin_alpha.file_formats.oeb.polish.tests.main import find_tests

    suite = find_tests()
    stream = io.StringIO()
    result = unittest.TextTestRunner(stream=stream, verbosity=1).run(suite)
    assert result.wasSuccessful(), stream.getvalue()


def test_polish_one_skips_font_ops_when_stats_dependencies_missing(monkeypatch) -> None:
    """
    Perform the test polish one skips font ops when stats dependencies missing operation under explicit file-format and conversion rules.

    Example:
        Exercise test polish one skips font ops when stats dependencies missing through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    main_mod = importlib.import_module("LiuXin_alpha.file_formats.oeb.polish.main")

    def _missing_stats(*args, **kwargs):
        """
        Perform the missing stats operation under explicit file-format and conversion rules.

        Example:
            Exercise test polish one skips font ops when stats dependencies missing. missing stats through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise ModuleNotFoundError("PyQt5 is required for oeb polish font statistics.")

    called = {"embed": False, "subset": False}

    def _embed(*args, **kwargs):
        """
        Perform the embed operation under explicit file-format and conversion rules.

        Example:
            Exercise test polish one skips font ops when stats dependencies missing. embed through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        called["embed"] = True
        return False

    def _subset(*args, **kwargs):
        """
        Perform the subset operation under explicit file-format and conversion rules.

        Example:
            Exercise test polish one skips font ops when stats dependencies missing. subset through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param args: Positional values forwarded to the compatibility implementation.
        :param kwargs: Keyword values forwarded to the compatibility implementation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        called["subset"] = True
        return False

    monkeypatch.setattr(main_mod, "StatsCollector", _missing_stats)
    monkeypatch.setattr(main_mod, "embed_all_fonts", _embed)
    monkeypatch.setattr(main_mod, "subset_all_fonts", _subset)

    opts_map = main_mod.ALL_OPTS.copy()
    opts_map["embed"] = True
    opts_map["subset"] = True
    Opts = namedtuple("Options", " ".join(opts_map))
    opts = Opts(**opts_map)
    report_lines = []

    changed = main_mod.polish_one(object(), opts, report_lines.append)
    assert changed is False
    assert called["embed"] is False
    assert called["subset"] is False
    assert any("Skipping requested font operations" in line for line in report_lines)


def test_polish_embed_font_reports_cleanly_without_font_scanner(monkeypatch) -> None:
    """
    Perform the test polish embed font reports cleanly without font scanner operation under explicit file-format and conversion rules.

    Example:
        Exercise test polish embed font reports cleanly without font scanner through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    embed_mod = importlib.import_module("LiuXin_alpha.file_formats.oeb.polish.embed")
    original_import = builtins.__import__

    def _import_blocker(name, globals=None, locals=None, fromlist=(), level=0):
        """
        Perform the import blocker operation under explicit file-format and conversion rules.

        Example:
            Exercise test polish embed font reports cleanly without font scanner. import blocker through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param globals: Value supplied for globals under the utility contract.
        :param locals: Local template variables available during evaluation.
        :param fromlist: Value supplied for fromlist under the utility contract.
        :param level: Value supplied for level under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if name == "LiuXin_alpha.utils.fonts.scanner":
            raise ModuleNotFoundError("forced for test")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _import_blocker)

    report_lines = []
    warned = set()
    out = embed_mod.embed_font(
        container=object(),
        font={
            "font-family": "Missing Family",
            "font-weight": "400",
            "font-style": "normal",
            "font-stretch": "normal",
        },
        all_font_rules=(),
        report=report_lines.append,
        warned=warned,
    )
    assert out is None
    assert any("Font scanner support is unavailable" in line for line in report_lines)


def test_polish_cmyk_fix_noops_cleanly_without_pyqt(monkeypatch) -> None:
    """
    Perform the test polish cmyk fix noops cleanly without pyqt operation under explicit file-format and conversion rules.

    Example:
        Exercise test polish cmyk fix noops cleanly without pyqt through a consuming regression::

            python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    images_mod = importlib.import_module("LiuXin_alpha.file_formats.oeb.polish.check.images")
    original_import = builtins.__import__

    def _import_blocker(name, globals=None, locals=None, fromlist=(), level=0):
        """
        Perform the import blocker operation under explicit file-format and conversion rules.

        Example:
            Exercise test polish cmyk fix noops cleanly without pyqt. import blocker through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param globals: Value supplied for globals under the utility contract.
        :param locals: Local template variables available during evaluation.
        :param fromlist: Value supplied for fromlist under the utility contract.
        :param level: Value supplied for level under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if name.startswith("PyQt5"):
            raise ModuleNotFoundError("forced for test")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _import_blocker)

    class _Container:
        """
        Provide the container contract for validated ebook processing.

        Example:
            Exercise test polish cmyk fix noops cleanly without pyqt. Container through a consuming regression::

                python -m pytest -q tests/file_formats/oeb/test_oeb_polish_smoke.py
        """
        mime_map = {"cover.jpg": "image/jpeg"}

    err = images_mod.CMYKImage("Image is in CMYK colorspace", "cover.jpg")
    assert err(_Container()) is False
