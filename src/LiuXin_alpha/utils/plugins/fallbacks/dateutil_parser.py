"""
Serve the ``dateutil_parser`` plugin from LiuXin's vendored python-dateutil.

This is the backup for installs without python-dateutil: the same parser,
vendored by ``scripts/vendor_python_dateutil.py`` into
``utils.libraries.python_dateutil``.

The resolver remembers the layer it chose for each plugin. So that choosing
this copy does not outlast installing python-dateutil, its probe declines
whenever the installed package can be imported; the resolver then returns to
the ``fallback_alpha`` layer, which serves it.

Example:
    >>> parse("3 February 2001").date().isoformat()
    '2001-02-03'
"""

from __future__ import annotations

from LiuXin_alpha.utils.libraries.python_dateutil import (
    __version__ as _VENDORED_VERSION,
)
from LiuXin_alpha.utils.libraries.python_dateutil.parser import (  # noqa: F401
    ParserError,
    isoparse,
    parse,
    parser,
    parserinfo,
)

SOURCE = f"vendored python-dateutil {_VENDORED_VERSION}"
"""Where this layer's parser comes from."""


def __liuxin_plugin_probe__() -> tuple[bool, str]:  # noqa: N807 - the resolver's hook name
    """
    Serve the vendored copy only while python-dateutil cannot be imported.

    Example:
        >>> __liuxin_plugin_probe__()[1] in ("python-dateutil is installed", SOURCE)
        True


    :return: Whether this layer should serve the plugin, and why.
    """
    try:
        import dateutil.parser  # noqa: F401
    except Exception:
        return True, SOURCE
    return False, "python-dateutil is installed"
