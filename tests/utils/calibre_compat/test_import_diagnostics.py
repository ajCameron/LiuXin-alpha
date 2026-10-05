"""
Provide test import diagnostics utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test import diagnostics through a consuming regression::

        python -m pytest -q tests/utils/calibre_compat/test_import_diagnostics.py
"""
from __future__ import annotations

import builtins
import importlib.util
import logging
import sys
import types

import pytest


LOGGER_NAME = "LiuXin_alpha.calibre_compat.imports"


def _make_fake_pkg(name: str) -> types.ModuleType:
    """
    Perform the make fake pkg utility operation under explicit compatibility rules.

    Example:
        Exercise  make fake pkg through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_import_diagnostics.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    m = types.ModuleType(name)
    # Mark as a package for import machinery
    m.__path__ = []  # type: ignore[attr-defined]
    return m


def test_missing_calibre_utils_import_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    """
    Perform the test missing calibre utils import is logged utility operation under explicit compatibility rules.

    Example:
        Exercise test missing calibre utils import is logged through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_import_diagnostics.py


    :param caplog: Value supplied for caplog under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test from calibre utils import x is logged utility operation under explicit compatibility rules.

    Example:
        Exercise test from calibre utils import x is logged through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_import_diagnostics.py


    :param caplog: Value supplied for caplog under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test dedupe only logs once utility operation under explicit compatibility rules.

    Example:
        Exercise test dedupe only logs once through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_import_diagnostics.py


    :param caplog: Value supplied for caplog under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test non calibre missing import is not logged utility operation under explicit compatibility rules.

    Example:
        Exercise test non calibre missing import is not logged through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_import_diagnostics.py


    :param caplog: Value supplied for caplog under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test uninstall restores import and stops logging utility operation under explicit compatibility rules.

    Example:
        Exercise test uninstall restores import and stops logging through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_import_diagnostics.py


    :param caplog: Value supplied for caplog under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
    """
    Perform the test meta path observer logs missing specs utility operation under explicit compatibility rules.

    Example:
        Exercise test meta path observer logs missing specs through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_import_diagnostics.py


    :param caplog: Value supplied for caplog under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
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
