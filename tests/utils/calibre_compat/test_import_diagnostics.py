from __future__ import annotations

import builtins
import importlib.util
import logging
import sys
import types

import pytest


LOGGER_NAME = "LiuXin_alpha.calibre_compat.imports"


def _make_fake_pkg(name: str) -> types.ModuleType:
    m = types.ModuleType(name)
    # Mark as a package for import machinery
    m.__path__ = []  # type: ignore[attr-defined]
    return m


def test_missing_calibre_utils_import_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    from LiuXin_alpha.utils.calibre_compat import import_diagnostics as d

    d.reset_calibre_import_failure_dedupe()

    d.install_calibre_import_failure_logging(LOGGER_NAME)
    try:
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            with pytest.raises(ModuleNotFoundError):
                builtins.__import__("calibre.utils.definitely_missing", fromlist=("x",), level=0)

        assert any(
            "Missing calibre import" in r.getMessage()
            and "requested=calibre.utils.definitely_missing" in r.getMessage()
            for r in caplog.records
        )
    finally:
        d.uninstall_calibre_import_failure_logging()


def test_from_calibre_utils_import_x_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    from LiuXin_alpha.utils.calibre_compat import import_diagnostics as d

    d.reset_calibre_import_failure_dedupe()

    d.install_calibre_import_failure_logging(LOGGER_NAME)
    try:
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            with pytest.raises(ModuleNotFoundError):
                builtins.__import__("calibre.utils", fromlist=("some_missing",), level=0)

        assert any(
            "Missing calibre import" in r.getMessage()
            and "requested=calibre.utils" in r.getMessage()
            for r in caplog.records
        )
    finally:
        d.uninstall_calibre_import_failure_logging()


def test_dedupe_only_logs_once(caplog: pytest.LogCaptureFixture) -> None:
    from LiuXin_alpha.utils.calibre_compat import import_diagnostics as d

    d.reset_calibre_import_failure_dedupe()

    d.install_calibre_import_failure_logging(LOGGER_NAME)
    try:
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            for _ in range(2):
                with pytest.raises(ModuleNotFoundError):
                    builtins.__import__("calibre.utils.definitely_missing", fromlist=("x",), level=0)

        msgs = [r.getMessage() for r in caplog.records if "Missing calibre import" in r.getMessage()]
        assert len(msgs) == 1
    finally:
        d.uninstall_calibre_import_failure_logging()


def test_non_calibre_missing_import_is_not_logged(caplog: pytest.LogCaptureFixture) -> None:
    from LiuXin_alpha.utils.calibre_compat import import_diagnostics as d

    d.reset_calibre_import_failure_dedupe()

    d.install_calibre_import_failure_logging(LOGGER_NAME)
    try:
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            with pytest.raises(ModuleNotFoundError):
                builtins.__import__("this_module_does_not_exist_abcdefg", level=0)

        assert not caplog.records
    finally:
        d.uninstall_calibre_import_failure_logging()


def test_uninstall_restores_import_and_stops_logging(caplog: pytest.LogCaptureFixture) -> None:
    from LiuXin_alpha.utils.calibre_compat import import_diagnostics as d

    d.reset_calibre_import_failure_dedupe()

    d.install_calibre_import_failure_logging(LOGGER_NAME)
    d.uninstall_calibre_import_failure_logging()

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
        with pytest.raises(ModuleNotFoundError):
            builtins.__import__("calibre.utils.definitely_missing", fromlist=("x",), level=0)

    assert not caplog.records


def test_meta_path_observer_logs_missing_specs(caplog: pytest.LogCaptureFixture) -> None:
    from LiuXin_alpha.utils.calibre_compat import import_diagnostics as d

    calibre_pkg = _make_fake_pkg("calibre")
    utils_pkg = _make_fake_pkg("calibre.utils")
    sys.modules["calibre"] = calibre_pkg
    sys.modules["calibre.utils"] = utils_pkg
    setattr(calibre_pkg, "utils", utils_pkg)

    try:
        obs = d.install_calibre_meta_path_observer(LOGGER_NAME)
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            spec = importlib.util.find_spec("calibre.utils.nope")
            assert spec is None

        assert any("No import spec found for calibre.utils.nope" in r.getMessage() for r in caplog.records)
    finally:
        d.uninstall_calibre_meta_path_observer()
        sys.modules.pop("calibre.utils", None)
        sys.modules.pop("calibre", None)
