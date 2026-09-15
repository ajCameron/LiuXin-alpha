"""
Publish the stable version tuple for preserved legacy Catalog metadata tools.

The tuple is shared by compatibility exports so version inspection need not
import row helpers or the database stack. It does not advertise new features.
"""

from __future__ import annotations


LEGACY_METADATA_TOOLS_VERSION = (1, 0, 1)


__all__ = ["LEGACY_METADATA_TOOLS_VERSION"]
