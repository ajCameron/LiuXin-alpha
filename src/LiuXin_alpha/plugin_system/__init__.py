"""Passive foundations shared by plugin implementations and composition layers.

Importing this package does not discover providers or assemble registries. Concrete
plugin families and application composition belong to their owning domains.

Separation of concerns: plugin_system owns format-neutral lifecycle and resource
contracts; it does not own provider discovery, conversion policy, or application
composition. See ``dev-docs/04 - Seperation of Concerns.md``.
"""

__all__: tuple[str, ...] = ()
