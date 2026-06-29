
"""
These represent a link between two tables and include methods to get info on both sides of it.

Other names that where considered for these where "join tables".
But that seemed a bit wordy and not as clear.
"""

from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.link_table_base import (
    StorageCacheLinkTableBaseAPI,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.many_many_tables_api import (
    ManyManyLink,
    StorageCacheManyManyGetterAPI,
    StorageCacheManyToManyLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.many_one_tables_api import (
    ManyOneLink,
    StorageCacheManyOneGetterAPI,
    StorageCacheManyToOneLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.one_many_tables_api import (
    OneManyLink,
    StorageCacheOneManyGetterAPI,
    StorageCacheOneToManyLinkTable,
)
from LiuXin_alpha.caches.api.storage_cache_api.storage_tables_api.link_tables_api.one_one_tables_api import (
    OneOneLink,
    StorageCacheItemCalibreUUIDTableAPI,
    StorageCacheOneOneGetterAPI,
    StorageCacheOneToOneLinkTable,
    StorageCacheOneToOneLinkTableAPI,
)

__all__ = [
    "ManyManyLink",
    "ManyOneLink",
    "OneManyLink",
    "OneOneLink",
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
]
