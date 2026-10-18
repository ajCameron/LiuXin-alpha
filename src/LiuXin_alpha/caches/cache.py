"""Compose the writable cache facade over the Catalog-independent read cache.

``ReadCache`` owns lifecycle and structured reads without importing Catalog. This
module adds the explicit Catalog mutation bridge used by the public ``Cache`` API.
Keeping that bridge here lets Catalog retrieval create cache-backed readers without
forming a Catalog/cache import cycle.
"""

from __future__ import annotations

from typing import Any, cast

from LiuXin_alpha.caches.read_cache import ReadCache, _CacheCatalogFacade


class Cache(ReadCache):
    """Add Catalog-mediated writes to the reusable read-cache facade."""

    @staticmethod
    def _update_dependencies(
        update: Any,
    ) -> tuple[set[str], set[tuple[str, str]], set[str]]:
        """Derive cache refresh dependencies from one normalized Catalog update."""
        from LiuXin_alpha.catalog.write import (
            CatalogColumnUpdate,
            CatalogOwnedRowUpdate,
            LinkUpdate,
        )

        tables: set[str] = set()
        links: set[tuple[str, str]] = set()
        fields: set[str] = set()
        if isinstance(update, CatalogColumnUpdate):
            table = str(update.table_spec.name)
            tables.add(table)
            fields.add(f"{table}.{update.column_spec.name}")
            return tables, links, fields
        if isinstance(update, (CatalogOwnedRowUpdate, LinkUpdate)):
            link_spec = update.link_spec
            primary = str(link_spec.primary_table)
            secondary = str(link_spec.secondary_table)
            tables.add(secondary)
            links.add((primary, secondary))
            if primary != secondary:
                links.add((secondary, primary))
            return tables, links, fields
        raise TypeError(f"unsupported catalog update type: {type(update).__name__}")

    def create_writer(
        self,
        src_table: str,
        dst_column: str,
        *,
        force_refresh: bool = False,
        destination_owned: bool | None = None,
    ) -> Any:
        """Bind a schema-resolved Catalog writer to this cache for reconciliation.

        The cache must be ready when the writer is created. Applying the writer checks
        attachment again, delegates semantic validation and persistence to Catalog, and
        reconciles the affected cache dependencies after a successful commit.

        :param src_table: Table whose row IDs key updates.
        :param dst_column: Destination value column resolved through schema discovery.
        :param force_refresh: Refresh schema discovery before resolving the writer target.
        :param destination_owned: Optional ownership override for a one-to-one destination.
        :return: Catalog writer whose normalized updates pass through the cache boundary.
        """
        from LiuXin_alpha.catalog.catalog import Catalog
        from LiuXin_alpha.catalog.write import create_catalog_writer
        from LiuXin_alpha.catalog.write.host_api import CatalogWriterHostAPI

        with self._lock:
            self._assert_ready()
            facade = _CacheCatalogFacade(self, Catalog(self.database))
            return create_catalog_writer(
                cast(CatalogWriterHostAPI, cast(object, facade)),
                src_table,
                dst_column,
                force_refresh=force_refresh,
                destination_owned=destination_owned,
            )


def create_cache(
    database: Any,
    storage_type: str = "schema_backed",
    *,
    load: bool = True,
    **storage_kwargs: Any,
) -> Cache:
    """Construct the writable application cache and load it by default.

    Construction and loading errors propagate. This factory does not close a
    partially initialized backend if loading fails.

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
