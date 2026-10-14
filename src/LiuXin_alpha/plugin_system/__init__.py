"""Passive foundations shared by plugin implementations and composition layers.

Importing this package does not discover providers or assemble registries. Concrete
plugin families and application composition belong to their owning domains.

Separation of concerns: plugin_system owns format-neutral lifecycle, file-hook and
stream contracts, shared settings, and ZIP loading mechanics. It does not select
provider fleets, own conversion policy, or compose application services. See
``dev-docs/04 - Seperation of Concerns.md``.
"""

__all__: tuple[str, ...] = ()
