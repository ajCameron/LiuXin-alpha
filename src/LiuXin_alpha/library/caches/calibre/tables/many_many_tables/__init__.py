"""
Interface for the ManyToMany tables - please import everything from here.
"""

from LiuXin_alpha.library.caches.calibre.tables.many_many_tables.many_to_many_table import CalibreManyToManyTable
from LiuXin_alpha.library.caches.calibre.tables.many_many_tables.priority_many_to_many_table import (
    CalibrePriorityManyToManyTable,
)
from LiuXin_alpha.library.caches.calibre.tables.many_many_tables.priority_typed_many_to_many_table import (
    CalibrePriorityTypedManyToManyTable,
)
from LiuXin_alpha.library.caches.calibre.tables.many_many_tables.specific_many_to_many_tables import (
    CalibreAuthorsTable,
    CalibreFormatsTable,
)
from LiuXin_alpha.library.caches.calibre.tables.many_many_tables.typed_many_to_many_table import (
    CalibreTypedManyToManyTable,
)

__all__ = [
    "CalibreManyToManyTable",
    "CalibrePriorityManyToManyTable",
    "CalibrePriorityTypedManyToManyTable",
    "CalibreAuthorsTable",
    "CalibreFormatsTable",
    "CalibreTypedManyToManyTable",
]
