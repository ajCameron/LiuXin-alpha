"""Frozen compatibility facade for legacy row-oriented metadata tools.

Existing library and driver initialization paths still compose ``Add`` and
``Ensure`` from here. New code should enter through :class:`Catalog`; this
package remains reference material for future direct-SQL implementations and
must not gain new callers or behavior.
"""

# Standard functions for making objects, checking that those objects don't already exist using the standardization
# rules and chaining those objects together to make data structures.

# Only deals with the metadata size of the database - adding physical assets - like covers - involves the folder stores
# and so is handled over in the library module

from LiuXin_alpha.catalog.legacy_versions import LEGACY_METADATA_TOOLS_VERSION

__md_tools_version__ = LEGACY_METADATA_TOOLS_VERSION

from LiuXin_alpha.catalog.metadata_tools.add import Add
from LiuXin_alpha.catalog.metadata_tools.ensure import Ensure

__all__ = ["Add", "Ensure"]
