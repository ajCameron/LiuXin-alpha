"""Construct generic caches independently of Catalog and metadata services.

Use ``caches.metadata.CatalogCache`` to compose metadata operations over a loaded
cache. The core owns queries, lifecycle, indexes and physical reconciliation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from LiuXin_alpha.caches.read_cache import ReadCache

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI


class Cache(ReadCache):
    """Expose the generic cache core without metadata or persistence entry points.

    Example:
        >>> cache = create_cache(database)  # doctest: +SKIP
        >>> cache.get("works", 7)  # doctest: +SKIP
    """


def create_cache(
    database: DatabaseAPI,
    storage_type: str = "schema_backed",
    *,
    load: bool = True,
    **storage_kwargs: Any,
) -> Cache:
    """
    Construct the metadata-independent cache and load it by default.

    Construction and loading errors propagate. This factory does not close a
    partially initialized backend if loading fails.

    Example:
        >>> cache = create_cache(database, "schema_backed", load=False)  # doctest: +SKIP


    :param database: Attached database object required by Cache.
    :param storage_type: Registered storage plugin name or alias.
    :param load: Whether to call cache.load before returning; false leaves EMPTY state.
    :param storage_kwargs: Options forwarded to storage plugin construction.
    :return: New Cache facade, loaded when load is true.
    """
    cache = Cache(
        database,
        storage_type=storage_type,
        storage_kwargs=storage_kwargs,
    )
    if load:
        cache.load()
    return cache


__all__ = ["Cache", "create_cache"]
