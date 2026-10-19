"""Define the lightweight namespace for bundled plugin providers.

Importing this package does not discover or instantiate every built-in plugin.
Consumers that need the complete compatibility fleet must import
``LiuXin_alpha.plugins.builtins.registry`` explicitly. Individual provider
modules remain independently importable.

See ``dev-docs/plugins/plugin-composition.md``.
"""

from __future__ import annotations

__all__: list[str] = []
