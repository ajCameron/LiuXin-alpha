"""Catalog facade helpers for row-oriented metadata tools.

The top-level :class:`~LiuXin_alpha.catalog.Catalog` facade composes these
helpers and exposes them as ``catalog.add``, ``catalog.ensure``,
``catalog.apply``, and ``catalog.intralink``. Direct imports remain supported
for compatibility and characterization tests.
"""

# Standard functions for making objects, checking that those objects don't already exist using the standardization
# rules and chaining those objects together to make data structures.

# Only deals with the metadata size of the database - adding physical assets - like covers - involves the folder stores
# and so is handled over in the library module

from LiuXin_alpha.catalog.legacy_versions import LEGACY_METADATA_TOOLS_VERSION

__md_tools_version__ = LEGACY_METADATA_TOOLS_VERSION

from LiuXin_alpha.catalog.metadata_tools.add import Add
from LiuXin_alpha.catalog.metadata_tools.apply import Apply
from LiuXin_alpha.catalog.metadata_tools.ensure import Ensure
from LiuXin_alpha.catalog.metadata_tools.intralinker import Intralinker

__all__ = ["Add", "Apply", "Ensure", "Intralinker"]
