"""Terminal-oriented user surfaces for LiuXin."""

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
    """Resolve historical exports without loading the application for leaf imports."""
    if name in __all__:
        from . import text_browser

        return getattr(text_browser, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    """Keep lazy public exports discoverable to shells and introspection tools."""
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
