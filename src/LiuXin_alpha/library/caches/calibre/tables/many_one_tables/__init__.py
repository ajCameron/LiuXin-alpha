"""
Expose the supported many one tables compatibility surface.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise   init   through a consuming regression::

        python -m pytest -q tests/library/test_unified_library.py
"""

from LiuXin_alpha.library.caches.calibre.tables.many_one_tables.many_to_one_table import CalibreManyToOneTable
from LiuXin_alpha.library.caches.calibre.tables.many_one_tables.priority_many_to_one_table import (
    CalibrePriorityManyToOneTable,
)
from LiuXin_alpha.library.caches.calibre.tables.many_one_tables.priority_typed_many_to_one_table import (
    CalibrePriorityTypedManyToOneTable,
)
from LiuXin_alpha.library.caches.calibre.tables.many_one_tables.typed_many_to_one_table import (
    CalibreTypedManyToOneTable,
)

from LiuXin_alpha.library.caches.calibre.tables.many_one_tables.specific_many_to_one_tables import (
    CalibreRatingTable,
    CalibreCustomColumnsManyOneTable,
)



__all__ = [
    "CalibreManyToOneTable",
    "CalibrePriorityManyToOneTable",
    "CalibrePriorityTypedManyToOneTable",
    "CalibreRatingTable",
    "CalibreCustomColumnsManyOneTable",
]
