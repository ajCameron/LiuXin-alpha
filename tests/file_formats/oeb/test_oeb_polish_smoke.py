from __future__ import annotations

import builtins
import importlib
import io
import pkgutil
import unittest
from collections import namedtuple


def test_oeb_polish_package_import_sweep() -> None:
    import LiuXin_alpha.file_formats.oeb.polish as polish_pkg

    failures = []
    for modinfo in pkgutil.walk_packages(polish_pkg.__path__, polish_pkg.__name__ + "."):
        try:
            importlib.import_module(modinfo.name)
        except Exception as e:  # pragma: no cover - failure reporting path
            failures.append((modinfo.name, type(e).__name__, str(e)))
    assert not failures, failures


def test_oeb_polish_internal_unittest_suite_smoke() -> None:
    from LiuXin_alpha.file_formats.oeb.polish.tests.main import find_tests

    suite = find_tests()
    stream = io.StringIO()
    result = unittest.TextTestRunner(stream=stream, verbosity=1).run(suite)
    assert result.wasSuccessful(), stream.getvalue()


def test_polish_one_skips_font_ops_when_stats_dependencies_missing(monkeypatch) -> None:
    main_mod = importlib.import_module("LiuXin_alpha.file_formats.oeb.polish.main")

    def _missing_stats(*args, **kwargs):
        raise ModuleNotFoundError("PyQt5 is required for oeb polish font statistics.")

    called = {"embed": False, "subset": False}

    def _embed(*args, **kwargs):
        called["embed"] = True
        return False

    def _subset(*args, **kwargs):
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
    embed_mod = importlib.import_module("LiuXin_alpha.file_formats.oeb.polish.embed")
    original_import = builtins.__import__

    def _import_blocker(name, globals=None, locals=None, fromlist=(), level=0):
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
    images_mod = importlib.import_module("LiuXin_alpha.file_formats.oeb.polish.check.images")
    original_import = builtins.__import__

    def _import_blocker(name, globals=None, locals=None, fromlist=(), level=0):
        if name.startswith("PyQt5"):
            raise ModuleNotFoundError("forced for test")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _import_blocker)

    class _Container:
        mime_map = {"cover.jpg": "image/jpeg"}

    err = images_mod.CMYKImage("Image is in CMYK colorspace", "cover.jpg")
    assert err(_Container()) is False
