"""
Compose the writable cache facade over the Catalog-independent read cache.

``ReadCache`` owns lifecycle and structured reads without importing Catalog. This
module adds the explicit Catalog mutation bridge used by the public ``Cache`` API.
Keeping that bridge here lets Catalog retrieval create cache-backed readers without
forming a Catalog/cache import cycle.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, Any, cast

from LiuXin_alpha.caches.api.cache_api import CacheState
from LiuXin_alpha.caches.read_cache import ReadCache

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.metadata_access import MetadataAPIInterface
    from LiuXin_alpha.catalog.api.common import DatabaseHandle
    from LiuXin_alpha.catalog.catalog import Catalog
    from LiuXin_alpha.catalog.write import (
        CatalogColumnUpdate,
        CatalogOwnedRowUpdate,
        LinkUpdate,
        SchemaCatalogWriter,
    )
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
    from LiuXin_alpha.databases.db_types.macro_types import LinkRow


class _CacheCatalogFacade:
    """
    Intercept normalized Catalog writes to reconcile their owning cache.

    Store one Catalog instance and its database identity. The three private
    prepared-update methods pass through the owner lock and reconciliation; other
    attributes are delegated directly to Catalog and are not automatically
    intercepted. Attachment is checked again each time a normalized write runs.

    Example:
        A writer created by Cache.create_writer uses this boundary to keep a
        Catalog column update and subsequent cache reconciliation together.
    """

    def __init__(self, owner: ReadCache, catalog: Catalog) -> None:
        """
        Create a Catalog delegate tied to the owner's current database identity.

        This does not load the cache or validate its readiness; the write boundary
        checks attachment and readiness when an update is applied.

        Example:
            If the owner later switches databases, the retained identity causes a
            previously created writer to be rejected on use.


        :param owner: Cache whose database, lock and reconciliation methods will be used.
        :param catalog: Catalog delegate providing normalized write operations for this
            database.
        :return: None; retains the owner, Catalog delegate and original database object.
        """

        self._owner = owner
        self._catalog = catalog
        self.db = owner.database

    def __getattr__(self, name: str) -> Any:
        """
        Forward attributes not implemented by this boundary to its Catalog.

        Delegated methods do not acquire the owner lock or add reconciliation here.

        Example:
            Schema helpers accessed by the writer factory resolve through the Catalog
            delegate when this boundary has no matching attribute.


        :param name: Attribute name requested from the delegate.
        :return: Catalog attribute, preserving the delegate's own binding behavior.
        :raises AttributeError: The Catalog delegate also lacks the requested attribute.
        """

        return getattr(self._catalog, name)

    def _assert_attached(self) -> None:
        """
        Require an open, ready owner still attached to the original database.

        Example:
            Changing either owner.database or owner.storage.db invalidates a writer
            created against the previous attachment.


        :return: None when owner and storage share the retained database identity.
        :raises CacheClosedError: The owner is closed.
        :raises RuntimeError: The owner or storage database is no longer the retained object.
        :raises CacheNotReadyError: The owner has not been loaded or storage is uninitialized.
        """

        self._owner._assert_open()
        if self._owner.database is not self.db or self._owner.storage.db is not self.db:
            raise RuntimeError(
                "cache writer cannot apply after its cache is closed, detached, "
                "or attached to a different database"
            )
        self._owner._assert_ready()

    def _apply[UpdateT, ResultT](
        self, operation: Callable[[UpdateT], Mapping[int, ResultT]], update: UpdateT
    ) -> Mapping[int, ResultT]:
        """
        Apply one normalized Catalog write under the owner's reconciliation lock.

        Check attachment, then refresh preexisting DIRTY state unless a macro
        transaction is open. Invoke Catalog, then reconcile its result. Catalog errors
        propagate without reconciliation; post-write refresh errors preserve the
        successful receipt in CacheReconciliationError.

        Example:
            An _apply_column_update call refreshes earlier external invalidations before
            applying the new column update when no macro transaction is open.


        :param operation: Bound Catalog method accepting this normalized update.
        :param update: Normalized update passed unchanged to that method.
        :return: Catalog receipt after successful reconciliation or permitted deferral.
        """

        with self._owner._lock:
            self._assert_attached()
            if (
                self._owner.state == CacheState.DIRTY
                and not self._owner._transaction_is_open()
            ):
                self._owner._refresh_dirty()
            result = operation(update)
            self._owner._reconcile_catalog_update(update, result)
            return result

    def _apply_column_update(
        self, update: CatalogColumnUpdate[object]
    ) -> Mapping[int, object]:
        """
        Apply a CatalogColumnUpdate for a same-table value through the cache boundary.

        Delegate to _apply for attachment checks, locking and reconciliation.
        Validation/persistence failures and receipt-bearing refresh failures propagate.

        Example:
            Writers call this private host seam after normalizing caller input.


        :param update: Normalized update consumed by Catalog.
        :return: Authoritative Catalog receipt after reconciliation or deferred refresh.
        """

        return self._apply(self._catalog._apply_column_update, update)

    def _apply_link_update(
        self, update: LinkUpdate
    ) -> Mapping[int, tuple[LinkRow, ...]]:
        """
        Apply a LinkUpdate for a shared relationship through the cache boundary.

        Delegate to _apply for attachment checks, locking and reconciliation.
        Validation/persistence failures and receipt-bearing refresh failures propagate.

        Example:
            Writers call this private host seam after normalizing caller input.


        :param update: Normalized update consumed by Catalog.
        :return: Authoritative Catalog receipt after reconciliation or deferred refresh.
        """

        return self._apply(self._catalog._apply_link_update, update)

    def _apply_owned_row_update(
        self, update: CatalogOwnedRowUpdate[object]
    ) -> Mapping[int, tuple[LinkRow, ...]]:
        """
        Apply a CatalogOwnedRowUpdate for an owned destination through the cache boundary.

        Delegate to _apply for attachment checks, locking and reconciliation.
        Validation/persistence failures and receipt-bearing refresh failures propagate.

        Example:
            Writers call this private host seam after normalizing caller input.


        :param update: Normalized update consumed by Catalog.
        :return: Authoritative Catalog receipt after reconciliation or deferred refresh.
        """

        return self._apply(self._catalog._apply_owned_row_update, update)


class Cache(ReadCache):
    """
    Add Catalog-mediated writes to the reusable read-cache facade.

    Example:
        >>> cache = create_cache(database)  # doctest: +SKIP
        >>> receipt = cache.metadata.works.create({"title": "Dune"})  # doctest: +SKIP
    """

    @property
    def metadata(self) -> MetadataAPIInterface:
        """
        Bind native metadata reads and coordinated writes to this loaded cache.

        The service borrows this cache and retains its original database identity.
        It uses complete cached reads with database fallback disabled. Use an
        explicit MetadataAPI constructor to select a different fallback policy.

        Example:
            ``cache.metadata.search.find("work.title:Dune")`` returns native references.


        :return: Lazily retained metadata service; use rechecks the original database
            binding.
        """
        from LiuXin_alpha.caches.metadata import MetadataAPI
        from LiuXin_alpha.catalog.catalog import Catalog

        with self._lock:
            self._assert_ready()
            if not hasattr(self, "_metadata_service"):
                self._metadata_service = MetadataAPI(
                    Catalog(cast("DatabaseHandle", self.database)), cache=self
                )
            return self._metadata_service

    @staticmethod
    def _update_dependencies(
        update: object,
    ) -> tuple[set[str], set[tuple[str, str]], set[str]]:
        """
        Derive cache refresh dependencies from one normalized Catalog update.

        Example:
            A scalar column update identifies both its owning table and canonical field.


        :param update: Normalized Catalog column, owned-row or link update.
        :return: Sets of affected main tables, directed table pairs and canonical
            fields.
        """
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
    ) -> SchemaCatalogWriter:
        """
        Bind a schema-resolved Catalog writer to this cache for reconciliation.

        The cache must be ready when the writer is created. Applying the writer checks
        attachment again, delegates semantic validation and persistence to Catalog, and
        reconciles the affected cache dependencies after a successful commit.

        Example:
            ``cache.create_writer("works", "work_title")`` resolves a scalar writer.


        :param src_table: Table whose row IDs key updates.
        :param dst_column: Destination value column resolved through schema discovery.
        :param force_refresh: Refresh schema discovery before resolving the writer target.
        :param destination_owned: Optional ownership override for a one-to-one destination.
        :return: Catalog writer whose normalized updates pass through the cache boundary.
        """
        from LiuXin_alpha.catalog.catalog import Catalog
        from LiuXin_alpha.catalog.write import create_catalog_writer

        with self._lock:
            self._assert_ready()
            # Catalog's read handle requires metadata_sql; native DatabaseAPI allows
            # it to be absent. This writer route uses driver/schema and prepared
            # update capabilities, not that optional metadata read surface.
            catalog = Catalog(cast("DatabaseHandle", self.database))
            facade = _CacheCatalogFacade(self, catalog)
            return create_catalog_writer(
                facade,
                src_table,
                dst_column,
                force_refresh=force_refresh,
                destination_owned=destination_owned,
            )


def create_cache(
    database: DatabaseAPI,
    storage_type: str = "schema_backed",
    *,
    load: bool = True,
    **storage_kwargs: Any,
) -> Cache:
    """
    Construct the writable application cache and load it by default.

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
