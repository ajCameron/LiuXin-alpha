"""
Observe Calibre-compat import resolution and expose actionable diagnostics.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise import diagnostics through a consuming regression::

        python -m pytest -q tests/utils/calibre_compat/test_import_diagnostics.py
"""

from __future__ import annotations

import builtins
import importlib.abc
import importlib.machinery
import logging
import sys
import threading
from contextlib import contextmanager
from typing import Any, Callable, Iterator, Optional, Sequence


_DEFAULT_LOGGER_NAME = "LiuXin_alpha.calibre_compat.imports"
_CALIBRE_UTILS_PREFIXES = ("calibre.utils", "calibre.utils.")


_lock = threading.RLock()

# Saved import function (to restore on uninstall).
_prev_import: Optional[Callable[..., Any]] = None
_installed: bool = False
_seen: set[tuple[str, str]] = set()


def _is_calibre_utils_name(name: str) -> bool:
    """
    Perform the is calibre utils name utility operation under explicit compatibility rules.

    Example:
        Exercise  is calibre utils name through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_import_diagnostics.py


    :param name: Field, file, function or resource name addressed by the operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return name == "calibre.utils" or name.startswith("calibre.utils.")


def _logged_import(
    name: str,
    globals: dict[str, Any] | None = None,
    locals: dict[str, Any] | None = None,
    fromlist: Sequence[str] | tuple[str, ...] = (),
    level: int = 0,
) -> Any:
    """
    Replacement for :func:`builtins.__import__` that logs missing calibre imports.

    Example:
        Exercise  logged import through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_import_diagnostics.py


    :param name: Field, file, function or resource name addressed by the operation.
    :param globals: Value supplied for globals under the utility contract.
    :param locals: Local template variables available during evaluation.
    :param fromlist: Value supplied for fromlist under the utility contract.
    :param level: Value supplied for level under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    try:
        return _prev_import(  # type: ignore[misc]
            name, globals, locals, fromlist, level
        )
    except ModuleNotFoundError as e:
        # NOTE: When importing `calibre.utils.foo`, Python may raise with `e.name == "calibre"`
        # if the root package is missing. We still want to log the *requested* calibre.utils.*.
        requested = name
        missing = getattr(e, "name", None) or ""

        should_log = _is_calibre_utils_name(requested) or _is_calibre_utils_name(missing)

        if should_log:
            key = (requested, missing)
            with _lock:
                first_time = key not in _seen
                if first_time:
                    _seen.add(key)

            if first_time:
                logger = logging.getLogger(_DEFAULT_LOGGER_NAME)
                logger.warning(
                    "Missing calibre import (requested=%s, missing=%s, fromlist=%r, level=%s)",
                    requested,
                    missing,
                    tuple(fromlist) if fromlist else (),
                    level,
                    exc_info=True,
                )
        raise


def install_calibre_import_failure_logging(logger_name: str = _DEFAULT_LOGGER_NAME) -> None:
    """
    Install the import-failure logger.

    Example:
        Exercise install calibre import failure logging through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_import_diagnostics.py


    :param logger_name: Value supplied for logger name under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    global _prev_import, _installed

    with _lock:
        if _installed:
            return

        # Capture current import (in case someone else already wrapped it).
        _prev_import = builtins.__import__
        _installed = True

        # Ensure logger name is initialized (but do not configure handlers here).
        logging.getLogger(logger_name)

        builtins.__import__ = _logged_import  # type: ignore[assignment]


def uninstall_calibre_import_failure_logging() -> None:
    """
    Uninstall the import-failure logger (restore previous import).

    Example:
        Exercise uninstall calibre import failure logging through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_import_diagnostics.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    global _prev_import, _installed

    with _lock:
        if not _installed:
            return

        if builtins.__import__ is _logged_import and _prev_import is not None:
            builtins.__import__ = _prev_import  # type: ignore[assignment]

        _prev_import = None
        _installed = False


def reset_calibre_import_failure_dedupe() -> None:
    """
    Clear internal de-duplication state (useful in tests).

    Example:
        Exercise reset calibre import failure dedupe through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_import_diagnostics.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    with _lock:
        _seen.clear()


@contextmanager
def calibre_import_failure_logging(
    logger_name: str = _DEFAULT_LOGGER_NAME,
) -> Iterator[None]:
    """
    Context manager to temporarily enable import-failure logging.

    Example:
        Exercise calibre import failure logging through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_import_diagnostics.py


    :param logger_name: Value supplied for logger name under the utility contract.
    :return: An iterator yielding the normalized values described above.
    """
    install_calibre_import_failure_logging(logger_name=logger_name)
    try:
        yield
    finally:
        uninstall_calibre_import_failure_logging()


class CalibreUtilsSpecObserver(importlib.abc.MetaPathFinder):
    """
    A meta_path finder that *observes* missing ``calibre.utils.*`` specs.

    Example:
        Exercise CalibreUtilsSpecObserver through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_import_diagnostics.py
    """

    def __init__(self, logger_name: str = _DEFAULT_LOGGER_NAME) -> None:
        """
        Initialize and validate the CalibreUtilsSpecObserver state.

        Example:
            Exercise CalibreUtilsSpecObserver.  init   through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_import_diagnostics.py


        :param logger_name: Value supplied for logger name under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self._logger = logging.getLogger(logger_name)
        self._seen: set[str] = set()

    def find_spec(self, fullname: str, path=None, target=None):  # type: ignore[override]
        """
        Find spec under the documented compatibility and safety rules.

        Example:
            Exercise CalibreUtilsSpecObserver.find spec through a consuming regression::

                python -m pytest -q tests/utils/calibre_compat/test_import_diagnostics.py


        :param fullname: Value supplied for fullname under the utility contract.
        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :param target: Value supplied for target under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not _is_calibre_utils_name(fullname):
            return None

        if fullname in sys.modules:
            return None

        spec = importlib.machinery.PathFinder.find_spec(fullname, path)

        if spec is None and fullname not in self._seen:
            self._seen.add(fullname)
            self._logger.warning("No import spec found for %s (likely needs a shim)", fullname)

        # Never provide/intercept.
        return None


_spec_observer: Optional[CalibreUtilsSpecObserver] = None


def install_calibre_meta_path_observer(logger_name: str = _DEFAULT_LOGGER_NAME) -> CalibreUtilsSpecObserver:
    """
    Install a :class:`CalibreUtilsSpecObserver` into :data:`sys.meta_path`.

    Example:
        Exercise install calibre meta path observer through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_import_diagnostics.py


    :param logger_name: Value supplied for logger name under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    global _spec_observer

    with _lock:
        if _spec_observer is not None:
            return _spec_observer

        obs = CalibreUtilsSpecObserver(logger_name=logger_name)
        sys.meta_path.insert(0, obs)
        _spec_observer = obs
        return obs


def uninstall_calibre_meta_path_observer() -> None:
    """
    Remove the installed :class:`CalibreUtilsSpecObserver` from :data:`sys.meta_path`.

    Example:
        Exercise uninstall calibre meta path observer through a consuming regression::

            python -m pytest -q tests/utils/calibre_compat/test_import_diagnostics.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    global _spec_observer
    with _lock:
        if _spec_observer is None:
            return
        try:
            sys.meta_path.remove(_spec_observer)
        except ValueError:
            pass
        _spec_observer = None
