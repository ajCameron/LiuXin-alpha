"""
Export the database driver-wrapper interface.

DatabaseDriverWrapperAPI defines the bridge between database objects and low-level
driver operations. Importing this package exposes the contract without constructing
a wrapper or opening a connection.

Example:
    >>> import inspect
    >>> inspect.isabstract(DatabaseDriverWrapperAPI)
    True
"""

from __future__ import annotations

from LiuXin_alpha.databases.api.driver_wrapper_api.driver_wrapper_api import DatabaseDriverWrapperAPI

__all__ = [
    "DatabaseDriverWrapperAPI",
]
