"""
Interface for the ManyToMany tables - please import everything from here.
"""

from LiuXin.databases.caches.calibre.tables.many_many_tables.many_to_many_table import CalibreManyToManyTable
from LiuXin.databases.caches.calibre.tables.many_many_tables.priority_many_to_many_table import (
    CalibrePriorityManyToManyTable,
)
from LiuXin.databases.caches.calibre.tables.many_many_tables.priority_typed_many_to_many_table import (
    CalibrePriorityTypedManyToManyTable,
)
from LiuXin.databases.caches.calibre.tables.many_many_tables.specific_many_to_many_tables import (
    CalibreAuthorsTable,
    CalibreFormatsTable,
)
from LiuXin.databases.caches.calibre.tables.many_many_tables.typed_many_to_many_table import (
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
