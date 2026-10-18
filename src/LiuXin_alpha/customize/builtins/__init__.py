"""Define the lightweight namespace for bundled plugin providers.

Importing this package does not discover or instantiate every built-in plugin.
Consumers that need the complete compatibility fleet must import
``LiuXin_alpha.customize.builtins.registry`` explicitly. Individual provider
modules remain independently importable.

See ``dev-docs/file-formats/plugin-composition.md``.
"""

from __future__ import annotations

__all__: list[str] = []
