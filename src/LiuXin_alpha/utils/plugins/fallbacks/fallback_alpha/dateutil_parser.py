"""
Serve the ``dateutil_parser`` plugin from the installed python-dateutil package.

python-dateutil is a LiuXin dependency, so this layer normally answers. Where
it is not installed, importing this module fails and the resolver moves on to
the vendored copy in ``fallbacks.dateutil_parser``.

Example:
    >>> from LiuXin_alpha.utils.plugins import plugins
    >>> plugins["dateutil_parser"][0].parse("2001-02-03").year
    2001
"""

from __future__ import annotations

from dateutil.parser import (  # noqa: F401
    ParserError,
    isoparse,
    parse,
    parser,
    parserinfo,
)

SOURCE = "python-dateutil"
"""Where this layer's parser comes from."""


def __liuxin_plugin_probe__() -> tuple[bool, str]:  # noqa: N807 - the resolver's hook name
    """
    Report the installed python-dateutil, which this layer serves.

    Example:
        >>> __liuxin_plugin_probe__()[0]
        True


    :return: Whether the layer can serve the plugin, and its version.
    """
    import dateutil

    return True, f"python-dateutil {getattr(dateutil, '__version__', 'unknown version')}"
