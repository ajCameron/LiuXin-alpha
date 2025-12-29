"""
API for the ManyToOneTables - if possible, import all the CalibreManyToOne table classes and subclasses from here.
"""

from LiuXin.databases.caches.calibre.tables.many_one_tables.many_to_one_table import CalibreManyToOneTable
from LiuXin.databases.caches.calibre.tables.many_one_tables.priority_many_to_one_table import (
    CalibrePriorityManyToOneTable,
)
from LiuXin.databases.caches.calibre.tables.many_one_tables.priority_typed_many_to_one_table import (
    CalibrePriorityTypedManyToOneTable,
)
from LiuXin.databases.caches.calibre.tables.many_one_tables.specific_many_to_one_tables import (
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
