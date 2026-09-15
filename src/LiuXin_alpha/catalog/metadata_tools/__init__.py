"""
Compose and export legacy row-oriented Catalog metadata helpers.

Catalog exposes Add, Apply, Ensure and Intralinker through its composition
root. Keep this compatibility path and its stable version export available;
new work should use semantic repositories, coordinated mutations or writers.
These helpers deal in metadata Rows; physical asset management belongs to
storage/library services. Importing this module loads helper classes but does
not construct a Catalog or open a database.
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
