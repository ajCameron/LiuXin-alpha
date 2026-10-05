
"""
Export custom-column contracts, the legacy facade and the per-table manager.

The package exposes CustomColumnDataAdapter, CustomColumnMetadata, CustomColumnsAPI, CustomColumns and CustomColumnsManager. Importing these definitions does not create a facade or load database rows; constructing the facade can perform schema cleanup and trigger installation.
"""

from LiuXin_alpha.databases.api.custom_columns_api import (
    CustomColumnDataAdapter,
    CustomColumnMetadata,
    CustomColumnsAPI,
)
from LiuXin_alpha.databases.custom_columns.custom_columns import CustomColumns
from LiuXin_alpha.databases.custom_columns.custom_columns_manager import CustomColumnsManager

__all__ = [
    "CustomColumnDataAdapter",
    "CustomColumnMetadata",
    "CustomColumns",
    "CustomColumnsAPI",
    "CustomColumnsManager",
]
