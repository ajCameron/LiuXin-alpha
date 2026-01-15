"""Import diagnostics helpers for the calibre compatibility layer.

The intent is to help identify missing calibre modules (especially ``calibre.utils.*``)
encountered when loading third-party calibre plugins inside LiuXin.

These helpers are **observational**:
- They never swallow import errors.
- They aim to be idempotent and safe to enable temporarily in tests or during plugin load.

Typical usage::

    from LiuXin_alpha.utils.calibre_compat.import_diagnostics import (
        calibre_import_failure_logging,
    )

    with calibre_import_failure_logging():
        ...  # load plugins / run plugin code

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
    return name == "calibre.utils" or name.startswith("calibre.utils.")


def _logged_import(
    name: str,
    globals: dict[str, Any] | None = None,
    locals: dict[str, Any] | None = None,
    fromlist: Sequence[str] | tuple[str, ...] = (),
    level: int = 0,
) -> Any:
    """Replacement for :func:`builtins.__import__` that logs missing calibre imports."""
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
    """Install the import-failure logger.

    This wraps :data:`builtins.__import__` and logs *first-time* missing imports for
    ``calibre.utils`` and its submodules.

    The function is idempotent.
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
    """Uninstall the import-failure logger (restore previous import)."""
    global _prev_import, _installed

    with _lock:
        if not _installed:
            return

        if builtins.__import__ is _logged_import and _prev_import is not None:
            builtins.__import__ = _prev_import  # type: ignore[assignment]

        _prev_import = None
        _installed = False


def reset_calibre_import_failure_dedupe() -> None:
    """Clear internal de-duplication state (useful in tests)."""
    with _lock:
        _seen.clear()


@contextmanager
def calibre_import_failure_logging(
    logger_name: str = _DEFAULT_LOGGER_NAME,
) -> Iterator[None]:
    """Context manager to temporarily enable import-failure logging."""
    install_calibre_import_failure_logging(logger_name=logger_name)
    try:
        yield
    finally:
        uninstall_calibre_import_failure_logging()


class CalibreUtilsSpecObserver(importlib.abc.MetaPathFinder):
    """A meta_path finder that *observes* missing ``calibre.utils.*`` specs.

    This does not intercept or provide a spec; it only logs when the standard
    :class:`~importlib.machinery.PathFinder` cannot find a spec for a matching name.
    """

    def __init__(self, logger_name: str = _DEFAULT_LOGGER_NAME) -> None:
        self._logger = logging.getLogger(logger_name)
        self._seen: set[str] = set()

    def find_spec(self, fullname: str, path=None, target=None):  # type: ignore[override]
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
    """Install a :class:`CalibreUtilsSpecObserver` into :data:`sys.meta_path`.

    The observer is inserted at the front. Idempotent.
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
    """Remove the installed :class:`CalibreUtilsSpecObserver` from :data:`sys.meta_path`."""
    global _spec_observer
    with _lock:
        if _spec_observer is None:
            return
        try:
            sys.meta_path.remove(_spec_observer)
        except ValueError:
            pass
        _spec_observer = None
