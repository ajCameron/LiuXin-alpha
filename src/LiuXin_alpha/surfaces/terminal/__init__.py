"""
Expose terminal command/plugin packages and lazily resolve historical application exports.

Leaf imports do not load the browser application or curses adapter. Browser,
wizard, parser, and runner names remain discoverable through the public export
list and are resolved through the compatibility facade when requested.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from . import commands as commands
from . import plugins as plugins

if TYPE_CHECKING:
    from .text_browser import (
        DatabaseCreationWizardConfig as DatabaseCreationWizardConfig,
    )
    from .text_browser import (
        TextDatabaseBrowser as TextDatabaseBrowser,
    )
    from .text_browser import (
        build_parser as build_parser,
    )
    from .text_browser import (
        create_database_from_wizard as create_database_from_wizard,
    )
    from .text_browser import (
        main as main,
    )
    from .text_browser import (
        run_database_creation_wizard as run_database_creation_wizard,
    )
    from .text_browser import (
        run_windowed_text_browser as run_windowed_text_browser,
    )


def __getattr__(name: str) -> object:
    """
    Resolve an otherwise missing public attribute through the historical text-browser facade.

    Normal attribute lookup calls this only after checking existing globals, so
    eagerly imported command/plugin packages need no facade resolution. Values are
    returned without caching another binding in this module.

    Example:
        >>> __getattr__("build_parser").__module__
        'LiuXin_alpha.surfaces.terminal.app'


    :param name: Missing module attribute requested by the caller.
    :return: Facade attribute for an advertised public name.
    :raises AttributeError: If the name is unadvertised or absent from the delegated facade.
    """
    if name in __all__:
        from . import text_browser

        return getattr(text_browser, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """
    List existing globals and advertised lazy exports without resolving those exports.

    Example:
        >>> "TextDatabaseBrowser" in __dir__()
        True


    :return: Sorted unique attribute-name list for module introspection.
    """
    return sorted(set(globals()) | set(__all__))


__all__ = [
    "commands",
    "plugins",
    "DatabaseCreationWizardConfig",
    "TextDatabaseBrowser",
    "run_database_creation_wizard",
    "create_database_from_wizard",
    "build_parser",
    "run_windowed_text_browser",
    "main",
]
