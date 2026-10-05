"""
Support retained legacy database-fixture init behavior.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/databases/test_test_resources_manager.py
"""

from .objects import TestObjectsHandler
from .setup_constants import test_asset_version
from .tools import BasicMetadataFramework, DatabaseValidator

__all__ = [
    "BasicMetadataFramework",
    "DatabaseValidator",
    "TestObjectsHandler",
    "test_asset_version",
]
