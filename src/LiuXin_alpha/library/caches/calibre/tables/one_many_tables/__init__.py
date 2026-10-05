"""
Expose the supported one many tables compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""

from LiuXin_alpha.library.caches.calibre.tables.one_many_tables.one_to_many_table import CalibreOneToManyTable
from LiuXin_alpha.library.caches.calibre.tables.one_many_tables.priority_one_to_many_table import (
    CalibrePriorityOneToManyTable,
)
from LiuXin_alpha.library.caches.calibre.tables.one_many_tables.priority_typed_one_to_many_table import (
    CalibrePriorityTypedOneToManyTable,
)
from LiuXin_alpha.library.caches.calibre.tables.one_many_tables.typed_one_to_many_table import CalibreTypedOneToManyTable
from LiuXin_alpha.library.caches.calibre.tables.one_many_tables.specific_one_to_many_tables import CalibreIdentifiersTable

__all__ = [
    "CalibreOneToManyTable",
    "CalibrePriorityOneToManyTable",
    "CalibrePriorityTypedOneToManyTable",
    "CalibreTypedOneToManyTable",
    "CalibreIdentifiersTable",
]
