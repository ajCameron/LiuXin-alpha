"""Register LiuXin's calibre compatibility modules into :mod:`sys.modules`.

This lets code written for calibre (e.g. plugins) import canonical names like:

    from calibre.ebooks.metadata.book.base import Metadata

…and receive LiuXin's drop-in replacement.
"""

from __future__ import annotations

import sys
import types
from typing import Iterable


def _ensure_pkg(fullname: str) -> types.ModuleType:
    """Ensure *fullname* exists in sys.modules as a package-like module."""
    mod = sys.modules.get(fullname)
    if mod is None:
        mod = types.ModuleType(fullname)
        # Mark as a package for import machinery.
        mod.__path__ = []  # type: ignore[attr-defined]
        sys.modules[fullname] = mod
    return mod


def _wire(parent: str, child: str) -> None:
    """Set parent.child attribute to the child module object."""
    p = sys.modules[parent]
    c = sys.modules[child]
    attr = child.rsplit(".", 1)[-1]
    setattr(p, attr, c)


def install_calibre_shims(extra_modules: Iterable[str] = ()) -> None:
    """Install the minimal set of calibre modules LiuXin can emulate.

    Default registrations:

    - ``calibre.ebooks.metadata`` (helpers like ``string_to_authors``)
    - ``calibre.ebooks.metadata.book`` (constants)
    - ``calibre.ebooks.metadata.book.base`` (``Metadata`` class)
    """
    pkgs = [
        "calibre",
        "calibre.ebooks",
        "calibre.ebooks.metadata",
        "calibre.ebooks.metadata.book",
    ]
    for p in pkgs:
        _ensure_pkg(p)

    # Import our implementations and register them under calibre.* names
    import LiuXin_alpha.utils.calibre_compat.ebooks.metadata as md_helpers
    import LiuXin_alpha.utils.calibre_compat.ebooks.metadata.book as md_book
    from LiuXin_alpha.utils.calibre_compat.ebooks.metadata.book import base as base_mod

    sys.modules["calibre.ebooks.metadata"] = md_helpers
    _wire("calibre.ebooks", "calibre.ebooks.metadata")

    sys.modules["calibre.ebooks.metadata.book"] = md_book
    _wire("calibre.ebooks.metadata", "calibre.ebooks.metadata.book")

    sys.modules["calibre.ebooks.metadata.book.base"] = base_mod
    _wire("calibre.ebooks.metadata.book", "calibre.ebooks.metadata.book.base")

    for name in extra_modules:
        __import__(name)
