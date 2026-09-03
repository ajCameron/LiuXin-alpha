"""Database binding for one second-generation configured Store."""

from __future__ import annotations

import dataclasses

from typing import TYPE_CHECKING

from LiuXin_alpha.databases import Row
from LiuXin_alpha.storage.api import StoreAPI, StoreConfiguration, StoreStatus
from LiuXin_alpha.storage.store_spec_utils import (
    store_configuration_from_row,
    store_configuration_to_row_dict,
)

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI


@dataclasses.dataclass(slots=True)
class StoreContainer:
    """Pair one live Store with its durable configuration and optional DB row.

    The container is deliberately not part of the Store or manager protocol.
    It is a persistence adapter: byte operations remain on ``store`` and
    cross-Store policy remains on ``StorageManagerAPI``.
    """

    store: StoreAPI
    configuration: StoreConfiguration
    db: "DatabaseAPI | None" = None
    store_id: int | None = None
    _status_cache: StoreStatus | None = None

    def __post_init__(self) -> None:
        if self.store.store_ref != self.configuration.store_uuid:
            raise ValueError(
                "Store and StoreConfiguration UUIDs must match."
            )

    def startup(self) -> StoreStatus:
        self._status_cache = self.store.startup()
        return self._status_cache

    def probe(self) -> StoreStatus:
        self._status_cache = self.store.probe()
        return self._status_cache

    def status(self, *, refresh: bool = False) -> StoreStatus:
        if refresh or self._status_cache is None:
            self._status_cache = self.store.status(refresh=refresh)
        return self._status_cache

    def reload_configuration_from_db(self) -> StoreConfiguration:
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
        configured = configuration or store.configuration
        return cls(
            store=store,
            configuration=configured,
            db=db,
            store_id=store_id,
        )


__all__ = ["StoreContainer"]
