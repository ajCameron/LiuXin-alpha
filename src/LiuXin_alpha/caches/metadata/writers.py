"""Translate Catalog writer receipts into generic cache refresh dependencies."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.caches.api import CacheState

if TYPE_CHECKING:
    from LiuXin_alpha.caches.read_cache import ReadCache
    from LiuXin_alpha.catalog.catalog import Catalog
    from LiuXin_alpha.catalog.write import (
        CatalogColumnUpdate,
        CatalogOwnedRowUpdate,
        LinkUpdate,
    )
    from LiuXin_alpha.databases.db_types.macro_types import LinkRow


def update_dependencies(
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


class _CacheCatalogFacade:
    """
    Intercept normalized Catalog writes to reconcile their owning cache.

    Store one Catalog instance and its database identity. The three private
    prepared-update methods pass through the owner lock and reconciliation; other
    attributes are delegated directly to Catalog and are not automatically
    intercepted. Attachment is checked again each time a normalized write runs.

    Example:
        A writer created by CatalogCache.create_writer uses this boundary to keep a
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
        if (
            self._catalog.db is not self.db
            or self._owner.database is not self.db
            or self._owner.storage.db is not self.db
        ):
            raise RuntimeError(
                "cache writer cannot apply after its cache is closed, detached, "
                "or attached to a different database"
            )
        self._owner._assert_ready()

    @contextmanager
    def _writer_scope(self) -> Iterator[None]:
        """Guard the entire application before acquiring a SQL transaction.

        Check attachment and pending publication before resolving new values or
        reading link replacements. Further writes in the owning managed unit
        remain permitted. Borrowed units cannot promise observable completion.

        Example:
            Another thread's retained link writer fails promptly while a title
            update awaits its enclosing commit.

        :return: Context retaining the cache lock throughout writer application.
        """
        with self._owner._operation_lock():
            self._assert_attached()
            if self._owner._transaction_is_open():
                from LiuXin_alpha.databases.transaction_participants import (
                    owned_transaction_active,
                )

                if not owned_transaction_active(self.db.macros):
                    raise RuntimeError(
                        "Cache writers require an owned managed transaction; "
                        "a borrowed raw transaction has no observable completion"
                    )
            if (
                self._owner.state == CacheState.DIRTY
                and not self._owner._transaction_is_open()
            ):
                self._owner._refresh_dirty()
            yield

    def _apply[UpdateT, ResultT](
        self, operation: Callable[[UpdateT], Mapping[int, ResultT]], update: UpdateT
    ) -> Mapping[int, ResultT]:
        """Apply a normalized Catalog write and reconcile its physical dependencies.

        Example:
            A failed refresh preserves the successful write receipt and never
            retries persistence.

        :param operation: Catalog method accepting this normalized update.
        :param update: Normalized instruction passed unchanged to Catalog.
        :return: Authoritative receipt after reconciliation or permitted deferral.
        """
        with self._writer_scope():
            result = operation(update)
            if result:
                tables, links, fields = update_dependencies(update)
                self._owner.reconcile(result, tables=tables, links=links, fields=fields)
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
