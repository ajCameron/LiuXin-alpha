"""Define the lightweight namespace for bundled plugin providers.

Importing this package does not discover or instantiate every built-in plugin.
Consumers that need the complete compatibility fleet must import
``LiuXin_alpha.plugin_system.builtins.registry`` explicitly. Individual provider
modules remain independently importable.

See ``dev-docs/file_formats/plugin-composition.md``.
"""

from __future__ import annotations

__all__: list[str] = []
