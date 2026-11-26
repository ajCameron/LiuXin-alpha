"""Compose Catalog semantics with one borrowed generic cache.

CatalogCache is the metadata-aware application layer. It implements the same
metadata contract as MetadataAPI, requires a loaded cache and shares that cache's
lock, indexes and lifecycle. It neither subclasses nor copies the generic cache.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.caches.metadata.service import MetadataAPI
from LiuXin_alpha.caches.metadata.writers import _CacheCatalogFacade

if TYPE_CHECKING:
    from LiuXin_alpha.caches.read_cache import ReadCache
    from LiuXin_alpha.catalog.catalog import Catalog
    from LiuXin_alpha.catalog.write import SchemaCatalogWriter


class CatalogCache(MetadataAPI):
    """Expose metadata services and coordinated writers over a borrowed cache.

    The caller owns Catalog, the database and the core cache. Direct Catalog
    writes still require explicit invalidation. Closing or reattaching the core
    invalidates this facade and all retained child services and writers.

    Example:
        >>> cached = CatalogCache(catalog, cache=cache)  # doctest: +SKIP
        >>> cached.works.create({"title": "Dune"})  # doctest: +SKIP
    """

    def __init__(
        self,
        catalog: Catalog,
        *,
        cache: ReadCache,
        allow_database_fallback: bool = False,
    ) -> None:
        """Bind metadata services to a required, loaded core for the same database.

        Example:
            ``CatalogCache(catalog, cache=core)`` borrows an existing snapshot.

        :param catalog: Authoritative metadata persistence service.
        :param cache: Loaded generic cache attached to the same database object.
        :param allow_database_fallback: Permit unavailable cached reads to use Catalog.
        :return: None; retains the shared resources without taking ownership.
        """
        if cache is None:
            raise TypeError("CatalogCache requires a loaded cache")
        super().__init__(
            catalog, cache=cache, allow_database_fallback=allow_database_fallback
        )
        self._core = cache

    @property
    def cache(self) -> ReadCache:
        """Return the borrowed core for physical queries and explicit lifecycle work.

        Example:
            ``cached.cache.query(query)`` uses the existing physical indexes.

        :return: The exact cache supplied at construction; no new state is allocated.
        """
        return self._core

    def create_writer(
        self,
        src_table: str,
        dst_column: str,
        *,
        force_refresh: bool = False,
        destination_owned: bool | None = None,
    ) -> SchemaCatalogWriter:
        """Bind a schema-directed Catalog writer to the borrowed cache.

        Example:
            ``cached.create_writer("works", "work_title")`` retains this binding.

        :param src_table: Table whose row IDs key updates.
        :param dst_column: Destination value column resolved through schema discovery.
        :param force_refresh: Refresh schema discovery before resolving the target.
        :param destination_owned: Optional ownership override for an owned destination.
        :return: Catalog writer coordinating persistence and cache reconciliation.
        """
        from LiuXin_alpha.catalog.write import create_catalog_writer

        with self._core._operation_lock():
            self._assert_attached()
            host = _CacheCatalogFacade(self._core, self._catalog)
            with host._writer_scope():
                return create_catalog_writer(
                    host,
                    src_table,
                    dst_column,
                    force_refresh=force_refresh,
                    destination_owned=destination_owned,
                )

    def write(
        self,
        src_table: str,
        dst_column: str,
        *args: Any,
        force_refresh: bool = False,
        destination_owned: bool | None = None,
        **kwargs: Any,
    ) -> Mapping[int, object]:
        """
        Resolve a Catalog writer and apply its bulk-write operation.

        Persistence and cache reconciliation are separate stages. A reconciliation
        error does not mean the durable update failed; inspect its receipt before
        deciding whether to retry.
        force_refresh and destination_owned are consumed by create_writer; remaining
        arguments are forwarded to writer.write without interpretation here.

        Example:
            ``cache.write("books", "title", {1: "Dune"})`` applies a scalar update
            when the schema exposes that column on books.


        :param src_table: Table whose row IDs key updates.
        :param dst_column: Destination value column.
        :param args: Positional arguments for the resolved writer's write method.
        :param force_refresh: Refresh schema discovery before resolving the writer target.
        :param destination_owned: Optional ownership override for a one-to-one destination.
        :param kwargs: Keyword arguments forwarded to the resolved writer's write method.
        :return: Authoritative Catalog write receipt.
        """

        writer = self.create_writer(
            src_table,
            dst_column,
            force_refresh=force_refresh,
            destination_owned=destination_owned,
        )
        return writer.write(*args, **kwargs)

    def write_one(
        self,
        src_table: str,
        dst_column: str,
        src_id: int,
        dst_value: Any,
        *,
        force_refresh: bool = False,
        destination_owned: bool | None = None,
        **kwargs: Any,
    ) -> Mapping[int, object]:
        """
        Resolve a Catalog writer and apply one source-row update.

        Persistence and cache reconciliation are separate stages. A reconciliation
        error does not mean the durable update failed; inspect its receipt before
        deciding whether to retry.
        Construction flags are consumed by create_writer; src_id, dst_value and the
        remaining options are passed to writer.write_one without interpretation here.

        Example:
            ``cache.write_one("books", "title", 1, "Dune")`` updates one scalar
            value when supported by the schema.


        :param src_table: Table containing the source row.
        :param dst_column: Destination value column.
        :param src_id: Source row identity passed to the writer.
        :param dst_value: New value in the resolved writer's expected shape.
        :param force_refresh: Refresh schema discovery before resolving the writer target.
        :param destination_owned: Optional ownership override for a one-to-one destination.
        :param kwargs: Keyword arguments forwarded to the resolved writer's write_one method.
        :return: Authoritative Catalog write receipt.
        """

        writer = self.create_writer(
            src_table,
            dst_column,
            force_refresh=force_refresh,
            destination_owned=destination_owned,
        )
        return writer.write_one(src_id, dst_value, **kwargs)


__all__ = ["CatalogCache"]
