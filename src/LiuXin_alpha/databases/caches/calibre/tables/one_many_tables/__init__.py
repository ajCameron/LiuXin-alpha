"""
OneToManyTable is keyed with one book and valued with many items.

The items are just linked to the book - a single item cannot be linked to multiple books
(that would be a ManyToMany table).
E.g. (though it isn't currently in the db schema) - "text_hash"
A single book can have many text hashes - (formatting would do it) - but no two books should share text hashes.
"""

from LiuXin.databases.caches.calibre.tables.one_many_tables.one_to_many_table import CalibreOneToManyTable
from LiuXin.databases.caches.calibre.tables.one_many_tables.priority_one_to_many_table import (
    CalibrePriorityOneToManyTable,
)
from LiuXin.databases.caches.calibre.tables.one_many_tables.priority_typed_one_to_many_table import (
    CalibrePriorityTypedOneToManyTable,
)
from LiuXin.databases.caches.calibre.tables.one_many_tables.typed_one_to_many_table import CalibreTypedOneToManyTable
from LiuXin.databases.caches.calibre.tables.one_many_tables.specific_one_to_many_tables import CalibreIdentifiersTable

__all__ = [
    "CalibreOneToManyTable",
    "CalibrePriorityOneToManyTable",
    "CalibrePriorityTypedOneToManyTable",
    "CalibreTypedOneToManyTable",
    "CalibreIdentifiersTable",
]
