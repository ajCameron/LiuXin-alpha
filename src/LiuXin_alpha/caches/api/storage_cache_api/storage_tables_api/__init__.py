
"""
Legacy Calibre-shaped storage-table cache contracts.

The cache is mostly for display and fast UI work.

 - Cache

Everything required to form a cache of the books table in memory.
There is a single table of note, and that is the books table.

 - Tables

Is concerned with the books table.
Pretty much everything is orientated around this, and this alone.
As such, some of the conceptual architecture is a little weird.
A calibre table is a table as linked to books. Not a first class object in its own right.
The cache object contains data on the link and the thing linked to.

 - Fields

An abstraction above tables - holds data for a field on the display.
Which could be a composite, data held in another table, and so forth.

 - Views

Views into the cache - the top level which you might actually want to point a browser at.
These could be
 - restricted column counts
 - restricted ids (via a saved search or otherwise)
 - restricted via tags or categories

LIUXIN

Likewise, for responsive display and UI work.
Intended to be more general, but the main WEMI spine is going to need such special treatment this may not be very
practical.

 - Cache

Everything required to form a cache of the currently loaded bits of the database in memory.
There are four tables of note (WEMI) which form a conceptual stack which is analogous to the books table.
The cache

 - Tables

First class objects in their own rights.
Each corresponds to an individual table from the database.
(Probably not all will be loaded at once).

 - Link tables

First class objects representing link tables - connecting two other tables.

 - Linked tables.

Tables linked to other tables.
Probably hold a link tables and table object, which allows for queries.

 - Fields

Explicitly for display - responsible for presenting info from an underlying Linked Table.
There's going to have to be some fairly heavy customization - but we'll deal with that later.

 - Views

A cache can have multiple views into the data it contains.
These could be
 - restricted column counts
 - restricted ids (via a saved search or otherwise)
 - restricted via tags or categories


"""

from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.base_table import (
    MANY_MANY,
    MANY_ONE,
    ONE_MANY,
    ONE_ONE,
    StorageCacheBaseTableAPI,
    TableMetadata,
    TableTypes,
    null,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api import (
    ManyManyLink,
    ManyOneLink,
    OneManyLink,
    OneOneLink,
    StorageCacheItemCalibreUUIDTableAPI,
    StorageCacheLinkTableBaseAPI,
    StorageCacheManyManyGetterAPI,
    StorageCacheManyOneGetterAPI,
    StorageCacheManyToManyLinkTable,
    StorageCacheManyToOneLinkTable,
    StorageCacheOneManyGetterAPI,
    StorageCacheOneOneGetterAPI,
    StorageCacheOneToManyLinkTable,
    StorageCacheOneToOneLinkTable,
    StorageCacheOneToOneLinkTableAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.single_table import (
    StorageCacheSingleTableAPI,
    StorageStorageCacheSingleTableAPI,
)

__all__ = [
    "MANY_MANY",
    "MANY_ONE",
    "ONE_MANY",
    "ONE_ONE",
    "ManyManyLink",
    "ManyOneLink",
    "OneManyLink",
    "OneOneLink",
    "StorageCacheBaseTableAPI",
    "StorageCacheItemCalibreUUIDTableAPI",
    "StorageCacheLinkTableBaseAPI",
    "StorageCacheManyManyGetterAPI",
    "StorageCacheManyOneGetterAPI",
    "StorageCacheManyToManyLinkTable",
    "StorageCacheManyToOneLinkTable",
    "StorageCacheOneManyGetterAPI",
    "StorageCacheOneOneGetterAPI",
    "StorageCacheOneToManyLinkTable",
    "StorageCacheOneToOneLinkTable",
    "StorageCacheOneToOneLinkTableAPI",
    "StorageCacheSingleTableAPI",
    "StorageStorageCacheSingleTableAPI",
    "TableMetadata",
    "TableTypes",
    "null",
]
