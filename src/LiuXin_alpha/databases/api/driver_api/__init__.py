"""
Export the composite low-level database driver contract.

DatabaseDriverAPI composes the driver mixin contracts. Importing this namespace does
not load a concrete backend or open a database.
"""

from __future__ import annotations

from LiuXin_alpha.databases.api.driver_api.driver_api import DatabaseDriverAPI

__all__ = [
    "DatabaseDriverAPI",
]
