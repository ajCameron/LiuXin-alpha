"""
Bind a live configured Store to optional database-row persistence and cached status.

The adapter borrows the Store and database. It neither owns byte routing or
cross-Store policy nor guarantees rollback across row writes and later reloads.
Its configuration and status snapshots can differ from current backend state.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.storage.api import StoreAPI, StoreConfiguration, StoreStatus
from LiuXin_alpha.storage.utils.store_configuration import (
    store_configuration_from_row,
    store_configuration_to_row_dict,
)

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI


@dataclasses.dataclass(slots=True)
class StoreContainer:
    """
    Pair a borrowed Store with mutable configuration, row identity, and status cache. Construction
    checks only that the configured UUID equals the live Store's reference. It does not inspect the
    database row, start/probe the Store, or validate a supplied cache. Byte operations remain on
    store; manager policy remains outside this adapter. Reloading configuration does not rebuild the
    Store or invalidate status, and no Store/database lifetime is transferred.

    Example:
        >>> container = StoreContainer.from_store(store, db=db)  # doctest: +SKIP
        >>> saved = container.save_configuration_to_db()  # doctest: +SKIP


    :ivar store: Borrowed live Store facade; retained rather than copied or reconstructed.
    :ivar configuration: Current configuration snapshot, mutable by replacement and initially checked against store.store_ref.
    :ivar db: Optional borrowed database used by explicit persistence operations.
    :ivar store_id: Optional bound stores-row identifier; assigned after insertion and cleared after successful deletion.
    :ivar _status_cache: Optional last returned status; trusted until an explicit refresh/startup/probe replaces it.
    """

    store: StoreAPI
    configuration: StoreConfiguration
    db: "DatabaseAPI | None" = None
    store_id: int | None = None
    _status_cache: StoreStatus | None = None

    def __post_init__(self) -> None:
        """
        Require initial UUID agreement between configuration and the live Store. No other
        configuration, row, or status-cache check occurs, and later attribute replacement does not
        rerun this hook.

        Example:
            >>> container.__post_init__()  # doctest: +SKIP


        :return: None for matching references; ValueError for a mismatch, with attribute-access failures propagated.
        """
        if self.store.store_ref != self.configuration.store_uuid:
            raise ValueError(
                "Store and StoreConfiguration UUIDs must match."
            )

    def startup(self) -> StoreStatus:
        """
        Start the borrowed Store and cache the exact returned status. A raised startup error
        propagates before cache replacement; backend side effects can already have occurred. No
        database configuration is written.

        Example:
            >>> status = container.startup()  # doctest: +SKIP


        :return: The status returned by store.startup, also retained in _status_cache.
        """
        self._status_cache = self.store.startup()
        return self._status_cache

    def probe(self) -> StoreStatus:
        """
        Probe the borrowed Store and replace the cache only after a successful return. This does not
        rebuild configuration or persist status; failures leave the previous cached object intact.

        Example:
            >>> status = container.probe()  # doctest: +SKIP


        :return: The status returned by store.probe, also retained in _status_cache.
        """
        self._status_cache = self.store.probe()
        return self._status_cache

    def status(self, *, refresh: bool = False) -> StoreStatus:
        """
        Return the cached status unless it is None or refresh is truthy. When delegation is needed,
        pass refresh through to store.status and cache its return. A delegated error leaves the
        prior cache; no freshness interval is enforced.

        Example:
            >>> status = container.status(refresh=True)  # doctest: +SKIP


        :param refresh: Whether to bypass this adapter's cache and request a backend refresh; false also delegates when no cache exists.
        :return: The cached or newly returned StoreStatus object, without copying.
        """
        if refresh or self._status_cache is None:
            self._status_cache = self.store.status(refresh=refresh)
        return self._status_cache

    def reload_configuration_from_db(self) -> StoreConfiguration:
        """
        Replace this adapter's configuration from its bound Store row after checking UUID identity.
        Require both database and row ID, fetch the row, and translate it using that ID as a
        fallback. Only a decoded UUID equal to the live Store reference is accepted. Missing rows,
        decoding errors, or identity mismatches leave the current configuration in place. Successful
        reload does not reconstruct or reconfigure the Store and does not clear cached status.

        Example:
            >>> configuration = container.reload_configuration_from_db()  # doctest: +SKIP


        :return: The newly assigned configuration; unbound adapters raise RuntimeError, absent rows raise KeyError, and identity mismatches raise ValueError.
        """
        if self.db is None or self.store_id is None:
            raise RuntimeError(
                "StoreContainer is not bound to a database Store row."
            )
        row = self.db.get_row_from_id("stores", self.store_id)
        if row is None:
            raise KeyError(f"Unknown Store row: {self.store_id}")
        loaded = store_configuration_from_row(
            row,
            fallback_store_id=self.store_id,
        )
        if loaded.store_uuid != self.store.store_ref:
            raise ValueError(
                "persisted Store UUID no longer matches the live Store."
            )
        self.configuration = loaded
        return loaded

    def save_configuration_to_db(self) -> StoreConfiguration:
        """
        Insert or update configuration columns, then reload the persisted representation. Require a
        database and project against its stores headings with default null omission. No projected
        values raises ValueError. An unbound row is inserted and its integer ID retained; an
        existing row receives changed allowed fields and sync is called even when none changed.
        Reload then checks the persisted UUID against the live Store and replaces this adapter's
        configuration.

        This opens no encompassing transaction and does not validate UUID agreement before writing.
        Insertion/update can succeed before a later reload error. Omitted None values leave old
        nullable columns intact; an empty headings set disables filtering in the projection helper
        rather than allowing no columns. The live Store and status cache remain unchanged.

        Example:
            >>> saved = container.save_configuration_to_db()  # doctest: +SKIP


        :return: The reloaded configuration after successful persistence; errors may follow a row write or newly assigned store_id.
        """
        if self.db is None:
            raise RuntimeError("StoreContainer is not bound to a database.")
        columns = set(self.db.get_column_headings("stores"))
        values = store_configuration_to_row_dict(
            self.configuration,
            allowed_columns=columns,
        )
        if not values:
            raise ValueError(
                "StoreConfiguration yielded no writable stores columns."
            )
        if self.store_id is None:
            row = Row.from_idless_row_dict(
                self.db,
                row_dict=values,
                table="stores",
            )
            self.store_id = int(row["store_id"])
        else:
            row = self.db.get_row_from_id("stores", self.store_id)
            if row is None:
                raise KeyError(f"Unknown Store row: {self.store_id}")
            for key, value in values.items():
                if key in row.allowed_columns and row[key] != value:
                    row[key] = value
            row.sync()
        return self.reload_configuration_from_db()

    def delete_from_db(self) -> bool:
        """
        Delete the bound metadata row and clear its ID only after successful deletion. Require
        database and row identity. If the row is already absent, return False and retain store_id.
        Database deletion owns constraint/cascade behavior. This does not delete physical Store
        contents, close the Store, or clear the database reference, configuration, or status cache.

        Example:
            >>> deleted = container.delete_from_db()  # doctest: +SKIP


        :return: True after deletion and ID clearing, False for an absent row; unbound state raises RuntimeError and database errors propagate.
        """
        if self.db is None or self.store_id is None:
            raise RuntimeError(
                "StoreContainer is not bound to a database Store row."
            )
        row = self.db.get_row_from_id("stores", self.store_id)
        if row is None:
            return False
        self.db.delete(row)
        self.store_id = None
        return True

    @classmethod
    def from_store(
        cls,
        store: StoreAPI,
        *,
        configuration: StoreConfiguration | None = None,
        db: "DatabaseAPI | None" = None,
        store_id: int | None = None,
    ) -> "StoreContainer":
        """
        Construct an adapter using the supplied truthy configuration or the Store's own
        configuration. Forward Store, database, and row ID without lookup, startup, or ownership
        transfer; class construction checks UUID agreement.

        Example:
            >>> container = StoreContainer.from_store(store, db=db, store_id=7)  # doctest: +SKIP


        :param store: Borrowed Store whose reference must match the selected configuration UUID.
        :param configuration: Optional configuration snapshot; falsey input falls back to store.configuration.
        :param db: Optional borrowed database to bind for later persistence calls.
        :param store_id: Optional existing stores-row ID; existence is not checked during construction.
        :return: A new instance of cls with an initially empty status cache; construction errors propagate.
        """
        configured = configuration or store.configuration
        return cls(
            store=store,
            configuration=configured,
            db=db,
            store_id=store_id,
        )


__all__ = ["StoreContainer"]
