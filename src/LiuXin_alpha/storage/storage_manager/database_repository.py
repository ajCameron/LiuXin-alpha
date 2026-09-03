"""
Durable database repository used by the application storage manager.

The public storage values deliberately remain independent of database rows.
This adapter stores their useful scalar fields in the existing catalogue
columns and a versioned, lossless envelope in each table's scratch column.
The database is the source of truth whenever this repository is bound. Reads
may be routed through LiuXin's shared cache facade, but this adapter does not
retain a second manager-owned copy of the catalogue.
"""

from __future__ import annotations

import dataclasses
import json

from collections.abc import Callable, Iterable, Iterator, Mapping, MutableMapping
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Generic, TypeVar
from uuid import UUID

import LiuXin_alpha.storage.api as api

from LiuXin_alpha.storage.store_spec_utils import store_configuration_to_row_dict


_K = TypeVar("_K")
_V = TypeVar("_V")
_FORMAT = "liuxin-storage-record"
_FORMAT_VERSION = 1


class RepositoryRecordMapping(MutableMapping[_K, _V], Generic[_K, _V]):
    """
    Mapping-shaped orchestration port over an authoritative repository.

    The mapping exists so the manager core can stay storage-neutral. It owns
    no record dictionary: reads go to the repository (and therefore the shared
    LiuXin cache when one is attached), while writes commit before returning.
    """

    def __init__(
        self,
        *,
        get_one: Callable[[_K], _V],
        load_all: Callable[[], Mapping[_K, _V]],
        upsert: Callable[[_V], None],
        remove: Callable[[_K], None],
        key_of: Callable[[_V], _K] | None = None,
    ) -> None:
        """
        Bind mapping operations to authoritative repository callables.


        :param get_one:
        :param load_all:
        :param upsert:
        :param remove:
        :param key_of:
        :return:
        """

        self._get_one = get_one
        self._load_all = load_all
        self._upsert = upsert
        self._remove = remove
        self._key_of = key_of

    def __getitem__(self, key: _K) -> _V:
        """
        Load one value directly from the repository.


        :param key:
        :return:
        """

        return self._get_one(key)

    def __setitem__(self, key: _K, value: _V) -> None:
        """
        Validate identity, when configured, and persist ``value``.


        :param key:
        :param value:
        :return:
        """

        if self._key_of is not None and self._key_of(value) != key:
            raise ValueError(
                "repository mapping key does not match record identity."
            )
        self._upsert(value)

    def __delitem__(self, key: _K) -> None:
        """
        Require and remove one repository value.


        :param key:
        :return:
        """

        self._get_one(key)
        self._remove(key)

    def __iter__(self) -> Iterator[_K]:
        """
        Iterate keys from a freshly loaded repository snapshot.


        :return:
        """

        return iter(self._load_all())

    def __len__(self) -> int:
        """
        Return the size of a freshly loaded repository snapshot.


        :return:
        """

        return len(self._load_all())

    def __contains__(self, key: object) -> bool:
        """
        Return whether the authoritative repository contains ``key``.


        :param key:
        :return:
        """

        try:
            self._get_one(key)  # type: ignore[arg-type]
        except KeyError:
            return False
        return True

    def get(self, key: _K, default: Any = None) -> _V | Any:
        """
        Load ``key`` or return ``default`` without retaining local state.


        :param key:
        :param default:
        :return:
        """

        try:
            return self._get_one(key)
        except KeyError:
            return default

    def values(self):
        """
        Return values from a freshly loaded repository snapshot.


        :return:
        """

        return self._load_all().values()

    def items(self):
        """
        Return items from a freshly loaded repository snapshot.


        :return:
        """

        return self._load_all().items()

    def pop(self, key: _K, default: Any = dataclasses.MISSING) -> _V | Any:
        """
        Load and remove ``key``, applying normal mapping default semantics.


        :param key:
        :param default:
        :return:
        """

        try:
            value = self._get_one(key)
        except KeyError:
            if default is dataclasses.MISSING:
                raise KeyError(key)
            return default
        self._remove(key)
        return value


class RepositoryItemTargetMapping(
    MutableMapping[
        tuple[api.ItemID, str],
        tuple[str, api.DigitalAssetID | api.CompositeDigitalAssetID],
    ]
):
    """
    Repository-backed mapping for role-keyed Item targets.
    """

    def __init__(
        self,
        *,
        repository: DatabaseStorageMetadataRepository,
    ) -> None:
        """
        Bind Item-target operations to one metadata repository.


        :param repository:
        :return:
        """

        self._repository = repository

    def __getitem__(
        self, key: tuple[api.ItemID, str]
    ) -> tuple[str, api.DigitalAssetID | api.CompositeDigitalAssetID]:
        """
        Load the target assigned to one Item role.


        :param key:
        :return:
        """

        return self._repository.get_item_target(key)

    def __setitem__(
        self,
        key: tuple[api.ItemID, str],
        value: tuple[str, api.DigitalAssetID | api.CompositeDigitalAssetID],
    ) -> None:
        """
        Persist the atomic or Composite target for one Item role.


        :param key:
        :param value:
        :return:
        """

        self._repository.upsert_item_target((key, value))

    def __delitem__(self, key: tuple[api.ItemID, str]) -> None:
        """
        Require and remove one Item-role target.


        :param key:
        :return:
        """

        self._repository.get_item_target(key)
        self._repository.remove_item_target(key)

    def __iter__(self):
        """
        Iterate Item-role keys from a fresh database snapshot.


        :return:
        """

        return iter(self._repository.load_item_targets())

    def __len__(self) -> int:
        """
        Return the number of currently persisted Item-role targets.


        :return:
        """

        return len(self._repository.load_item_targets())

    def get(self, key, default=None):
        """
        Load an Item-role target or return ``default``.


        :param key:
        :param default:
        :return:
        """

        try:
            return self._repository.get_item_target(key)
        except KeyError:
            return default

    def values(self):
        """
        Return targets from a fresh database snapshot.


        :return:
        """

        return self._repository.load_item_targets().values()

    def items(self):
        """
        Return Item-role pairs from a fresh database snapshot.


        :return:
        """

        return self._repository.load_item_targets().items()

    def pop(self, key, default=dataclasses.MISSING):
        """
        Load and remove a target with normal mapping default semantics.


        :param key:
        :param default:
        :return:
        """

        try:
            value = self._repository.get_item_target(key)
        except KeyError:
            if default is dataclasses.MISSING:
                raise KeyError(key)
            return default
        self._repository.remove_item_target(key)
        return value


class DatabaseStorageMetadataRepository:
    """
    Translate storage-manager values through portable database macros.

    Scalar columns remain useful to queries and older LiuXin code, while a
    versioned JSON envelope in each scratch column preserves the complete typed
    public value.  Database rows are authoritative.  An attached shared cache
    may serve reads, but every mutation commits through the macro interface and
    invalidates affected records or relationship indexes.
    """

    _RECORD_IDENTITIES = {
        "digital_asset": (
            "digital_assets",
            "digital_asset_id",
            "digital_asset_scratch",
        ),
        "replica": (
            "asset_replicas",
            "asset_replica_id",
            "asset_replica_scratch",
        ),
        "composite": (
            "composite_digital_assets",
            "composite_digital_asset_id",
            "composite_digital_asset_scratch",
        ),
        "derivation": (
            "digital_asset_derivations",
            "digital_asset_derivation_id",
            "digital_asset_derivation_scratch",
        ),
        "replication_policy": (
            "replication_policies",
            "replication_policy_id",
            "replication_policy_scratch",
        ),
        "backup_policy": (
            "backup_policies",
            "backup_policy_id",
            "backup_policy_scratch",
        ),
    }

    _REQUIRED_TABLES = frozenset(
        {
            "digital_assets",
            "asset_replicas",
            "composite_digital_assets",
            "digital_asset_derivations",
            "replication_policies",
            "backup_policies",
            "digital_asset_item_links",
            "composite_digital_asset_item_links",
            "composite_digital_asset_digital_asset_links",
            "storage_ingest_operations",
            "storage_schema_migrations",
            "stores",
        }
    )
    _CATALOGUE_SENTINELS = frozenset(
        {"digital_assets", "asset_replicas", "composite_digital_assets"}
    )
    _CACHED_RECORD_TABLES = frozenset(
        {
            "digital_assets",
            "asset_replicas",
            "composite_digital_assets",
            "digital_asset_derivations",
            "replication_policies",
            "backup_policies",
        }
    )
    _ENVELOPE_COLUMNS = {
        "digital_assets": ("digital_asset_id", "digital_asset_scratch"),
        "asset_replicas": ("asset_replica_id", "asset_replica_scratch"),
        "composite_digital_assets": (
            "composite_digital_asset_id",
            "composite_digital_asset_scratch",
        ),
        "digital_asset_derivations": (
            "digital_asset_derivation_id",
            "digital_asset_derivation_scratch",
        ),
        "replication_policies": (
            "replication_policy_id",
            "replication_policy_scratch",
        ),
        "backup_policies": ("backup_policy_id", "backup_policy_scratch"),
        "digital_asset_item_links": (
            "digital_asset_item_link_id",
            "digital_asset_item_link_scratch",
        ),
        "composite_digital_asset_item_links": (
            "composite_digital_asset_item_link_id",
            "composite_digital_asset_item_link_scratch",
        ),
        "composite_digital_asset_digital_asset_links": (
            "composite_digital_asset_digital_asset_link_id",
            "composite_digital_asset_digital_asset_link_scratch",
        ),
        "storage_ingest_operations": (
            "storage_ingest_operation_id",
            "storage_ingest_operation_scratch",
        ),
    }

    def __init__(
        self,
        db: Any,
        *,
        additional_types: Iterable[type[Any]] = (),
        cache: Any | None = None,
    ) -> None:
        """
        Validate ``db`` and configure value decoding and optional caching.


        :param db:
        :param additional_types:
        :param cache:
        :return:
        """

        if not self.supports(db):
            raise TypeError(
                "database does not expose the portable storage metadata schema."
            )
        self.db = db
        self.macros = db.macros
        self._types = _storage_value_types(additional_types)
        self.cache: Any | None = None
        self._cached_tables: frozenset[str] = frozenset()
        self._cache_tables: frozenset[str] = frozenset()
        self.has_ingest_journal = (
            "storage_ingest_operations" in set(db.get_tables())
        )
        if cache is not None:
            self.set_cache(cache)

    @classmethod
    def supports(cls, db: Any) -> bool:
        """
        Return whether a database can provide durable manager metadata.


        :param db:
        :return:
        """

        try:
            tables = set(db.get_tables())
            macros = db.macros
        except Exception:
            return False
        return cls._REQUIRED_TABLES <= tables and all(
            callable(getattr(macros, name, None))
            for name in (
                "transaction",
                "get_row",
                "get_rows",
                "insert_row",
                "update_row",
                "delete_row",
            )
        )

    @classmethod
    def resembles_storage_catalogue(cls, db: Any) -> bool:
        """
        Return whether ``db`` appears intended to own storage metadata.

        Tiny Store-row adapters are useful for focused bootstrap tests and do
        not claim to be a manager catalogue. A real catalogue that is merely
        incomplete must not silently turn an application manager volatile.


        :param db:
        :return:
        """

        try:
            tables = set(db.get_tables())
        except Exception:
            return False
        return bool(cls._CATALOGUE_SENTINELS & tables)

    @classmethod
    def missing_tables(cls, db: Any) -> tuple[str, ...]:
        """
        Return durable catalogue tables absent from ``db``.


        :param db:
        :return:
        """

        try:
            tables = set(db.get_tables())
        except Exception:
            return tuple(sorted(cls._REQUIRED_TABLES))
        return tuple(sorted(cls._REQUIRED_TABLES - tables))

    def transaction(self):
        """
        Return the portable database transaction used by manager mutations.


        :return:
        """

        return self.macros.transaction()

    def migrate_envelopes(self) -> int:
        """
        Upgrade known older storage envelopes transactionally in place.


        :return:
        """

        upgraded = 0
        with self.macros.transaction():
            for table, (id_column, scratch_column) in self._ENVELOPE_COLUMNS.items():
                for row in self.macros.get_rows(table):
                    migrated = self._migrate_envelope(row.get(scratch_column))
                    if migrated is None:
                        continue
                    self.macros.update_row(
                        table,
                        row[id_column],
                        {scratch_column: migrated},
                        id_column=id_column,
                    )
                    upgraded += 1
        from LiuXin_alpha.storage.migrations import record_envelope_migration

        record_envelope_migration(self.db, upgraded)
        if upgraded:
            self._invalidate_records(*self._ENVELOPE_COLUMNS)
        return upgraded

    def allocate_record_id(self, kind: str) -> int:
        """
        Reserve a database-generated identity inside the caller transaction.

        The manager immediately replaces the reservation scratch value with a
        complete record before its surrounding transaction commits. This uses
        SQLite rowid or PostgreSQL identity allocation instead of a racy
        process-local ``max(id) + 1`` counter.


        :param kind:
        :return:
        """

        try:
            table, id_column, scratch_column = self._RECORD_IDENTITIES[kind]
        except KeyError:
            raise ValueError(f"Unknown storage metadata record kind: {kind!r}") from None
        reservation = json.dumps(
            {
                "format": _FORMAT,
                "version": _FORMAT_VERSION,
                "reservation": True,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return int(
            self.macros.insert_row(
                table,
                {scratch_column: reservation},
                id_column=id_column,
            )
        )

    def set_cache(self, cache: Any | None) -> None:
        """
        Route catalogue reads through LiuXin's shared cache when supplied.


        :param cache:
        :return:
        """

        if cache is None:
            self.cache = None
            self._cached_tables = frozenset()
            self._cache_tables = frozenset()
            return
        if getattr(cache, "database", None) is not self.db:
            raise ValueError(
                "storage metadata cache must use the manager database."
            )
        from LiuXin_alpha.caches import CacheState

        state = cache.state
        if state == CacheState.EMPTY:
            cache.load()
        elif state == CacheState.DIRTY:
            cache.reload()
        elif state == CacheState.CLOSED:
            raise ValueError("storage metadata cannot use a closed cache.")
        available = set(cache.table_columns())
        cached_tables = self._CACHED_RECORD_TABLES & available
        required_hot_tables = {"digital_assets", "asset_replicas"}
        if not required_hot_tables <= cached_tables:
            raise ValueError(
                "storage metadata cache cannot serve the Asset/Replica hot path."
            )
        self.cache = cache
        self._cached_tables = frozenset(cached_tables)
        self._cache_tables = frozenset(available)

    def asset_records(self) -> RepositoryRecordMapping[
        api.DigitalAssetID, api.DigitalAssetRecord
    ]:
        """
        Return the compatibility mapping for authoritative Asset records.


        :return:
        """

        return RepositoryRecordMapping(
            get_one=self.get_asset,
            load_all=self._load_assets,
            upsert=self.upsert_asset,
            remove=self.remove_asset,
            key_of=lambda record: record.digital_asset_id,
        )

    def replica_records(self) -> RepositoryRecordMapping[
        api.ReplicaID, api.ReplicaRecord
    ]:
        """
        Return the compatibility mapping for authoritative Replica records.


        :return:
        """

        return RepositoryRecordMapping(
            get_one=self.get_replica,
            load_all=self._load_replicas,
            upsert=self.upsert_replica,
            remove=self.remove_replica,
            key_of=lambda record: record.replica_id,
        )

    def composite_records(self) -> RepositoryRecordMapping[
        api.CompositeDigitalAssetID, api.CompositeDigitalAssetRecord
    ]:
        """
        Return the compatibility mapping for Composite records.


        :return:
        """

        return RepositoryRecordMapping(
            get_one=self.get_composite,
            load_all=self._load_composites,
            upsert=self.upsert_composite,
            remove=self.remove_composite,
            key_of=lambda record: record.composite_digital_asset_id,
        )

    def derivation_records(self) -> RepositoryRecordMapping[
        api.DigitalAssetDerivationID, api.DigitalAssetDerivationRecord
    ]:
        """
        Return the compatibility mapping for derivation records.


        :return:
        """

        return RepositoryRecordMapping(
            get_one=self.get_derivation,
            load_all=self._load_derivations,
            upsert=self.upsert_derivation,
            remove=self.remove_derivation,
            key_of=lambda record: record.digital_asset_derivation_id,
        )

    def replication_policy_records(self) -> RepositoryRecordMapping[
        api.ReplicationPolicyID, api.ReplicationPolicyRecord
    ]:
        """
        Return the compatibility mapping for replication policies.


        :return:
        """

        return RepositoryRecordMapping(
            get_one=self.get_replication_policy,
            load_all=self._load_replication_policies,
            upsert=self.upsert_replication_policy,
            remove=self.remove_replication_policy,
            key_of=lambda record: record.replication_policy_id,
        )

    def backup_policy_records(self) -> RepositoryRecordMapping[
        api.BackupPolicyID, api.BackupPolicyRecord
    ]:
        """
        Return the compatibility mapping for backup policies.


        :return:
        """

        return RepositoryRecordMapping(
            get_one=self.get_backup_policy,
            load_all=self._load_backup_policies,
            upsert=self.upsert_backup_policy,
            remove=self.remove_backup_policy,
            key_of=lambda record: record.backup_policy_id,
        )

    def item_targets(self) -> RepositoryItemTargetMapping:
        """
        Return the compatibility mapping for role-keyed Item targets.


        :return:
        """

        return RepositoryItemTargetMapping(repository=self)

    def ingest_operations(self) -> RepositoryRecordMapping[UUID, Any]:
        """
        Return the read/upsert facade for committed ingest operations.


        :return:
        """

        return RepositoryRecordMapping(
            get_one=self.get_committed_ingest_operation,
            load_all=self._load_committed_ingest_operations,
            upsert=self.commit_ingest_operation,
            remove=lambda _operation_id: None,
        )

    def get_asset(
        self, digital_asset_id: api.DigitalAssetID
    ) -> api.DigitalAssetRecord:
        """
        Load one typed Digital Asset record by its database identity.


        :param digital_asset_id:
        :return:
        """

        return self._get_record(
            "digital_assets",
            "digital_asset_id",
            digital_asset_id,
            self._load_assets,
        )

    def get_replica(self, replica_id: api.ReplicaID) -> api.ReplicaRecord:
        """
        Load one typed Replica record by its database identity.


        :param replica_id:
        :return:
        """

        return self._get_record(
            "asset_replicas",
            "asset_replica_id",
            replica_id,
            self._load_replicas,
        )

    def get_composite(
        self, composite_id: api.CompositeDigitalAssetID
    ) -> api.CompositeDigitalAssetRecord:
        """
        Load one typed Composite record by its database identity.


        :param composite_id:
        :return:
        """

        return self._get_record(
            "composite_digital_assets",
            "composite_digital_asset_id",
            composite_id,
            self._load_composites,
        )

    def get_derivation(
        self, derivation_id: api.DigitalAssetDerivationID
    ) -> api.DigitalAssetDerivationRecord:
        """
        Load one typed derivation record by its database identity.


        :param derivation_id:
        :return:
        """

        return self._get_record(
            "digital_asset_derivations",
            "digital_asset_derivation_id",
            derivation_id,
            self._load_derivations,
        )

    def get_replication_policy(
        self, policy_id: api.ReplicationPolicyID
    ) -> api.ReplicationPolicyRecord:
        """
        Load one typed replication-policy record by identity.


        :param policy_id:
        :return:
        """

        return self._get_record(
            "replication_policies",
            "replication_policy_id",
            policy_id,
            self._load_replication_policies,
        )

    def get_backup_policy(
        self, policy_id: api.BackupPolicyID
    ) -> api.BackupPolicyRecord:
        """
        Load one typed backup-policy record by identity.


        :param policy_id:
        :return:
        """

        return self._get_record(
            "backup_policies",
            "backup_policy_id",
            policy_id,
            self._load_backup_policies,
        )

    def load_item_targets(
        self,
    ) -> dict[
        tuple[api.ItemID, str],
        tuple[str, api.DigitalAssetID | api.CompositeDigitalAssetID],
    ]:
        """
        Load every role-keyed Item target from both link tables.


        :return:
        """

        return self._load_item_targets()

    def get_item_target(
        self, key: tuple[api.ItemID, str]
    ) -> tuple[str, api.DigitalAssetID | api.CompositeDigitalAssetID]:
        """
        Load one role-keyed Item target or raise ``KeyError``.


        :param key:
        :return:
        """

        try:
            return self._load_item_targets()[key]
        except KeyError:
            raise KeyError(key) from None

    def get_committed_ingest_operation(self, operation_id: UUID) -> Any:
        """
        Load one committed ingest operation or raise ``KeyError``.


        :param operation_id:
        :return:
        """

        row = self._journal_row(operation_id)
        if row is None or row.get("storage_ingest_operation_state") != "committed":
            raise KeyError(operation_id)
        payload = self._load(row["storage_ingest_operation_scratch"])
        operation = payload.get("operation") if isinstance(payload, dict) else None
        if operation is None:
            raise KeyError(operation_id)
        return operation

    def _get_record(
        self,
        table: str,
        id_column: str,
        key: _K,
        loader: Callable[[Iterable[Mapping[str, Any]] | None], Mapping[_K, _V]],
    ) -> _V:
        """
        Load and decode one record while preserving mapping ``KeyError``.


        :param table:
        :param id_column:
        :param key:
        :param loader:
        :return:
        """

        row = self._record_row(table, id_column, int(key))
        if row is None:
            raise KeyError(key)
        records = loader((row,))
        try:
            return records[key]
        except KeyError:
            raise KeyError(key) from None

    def _record_row(
        self,
        table: str,
        id_column: str,
        row_id: int,
    ) -> Mapping[str, Any] | None:
        """
        Read one raw row through the shared cache when it covers ``table``.


        :param table:
        :param id_column:
        :param row_id:
        :return:
        """

        if self.cache is None or table not in self._cached_tables:
            return self.macros.get_row(table, row_id, id_column=id_column)
        lookup = self.cache.get(table, row_id)
        return None if lookup.value is None else lookup.value.values

    def _record_rows(
        self,
        table: str,
        *,
        order_by: tuple[str, ...],
    ) -> tuple[Mapping[str, Any], ...]:
        """
        Read an ordered raw-row snapshot through cache or database macros.


        :param table:
        :param order_by:
        :return:
        """

        if self.cache is None or table not in self._cached_tables:
            return tuple(self.macros.get_rows(table, order_by=order_by))
        from LiuXin_alpha.caches import CacheQuery, CacheSort

        result = self.cache.query(
            CacheQuery(
                table=table,
                sort=tuple(CacheSort(column) for column in order_by),
            )
        )
        return tuple(record.values for record in result.records)

    def _invalidate_records(self, *tables: str) -> None:
        """
        Invalidate complete cached tables affected by a bulk mutation.


        :param tables:
        :return:
        """

        if self.cache is not None:
            cached = tuple(table for table in tables if table in self._cache_tables)
            if cached:
                self.cache.invalidate(tables=cached)

    def _invalidate_record_ids(self, table: str, *row_ids: int) -> None:
        """
        Invalidate selected durable records without reloading their catalogue.


        :param table:
        :param row_ids:
        :return:
        """

        if (
            self.cache is not None
            and table in self._cache_tables
            and row_ids
        ):
            self.cache.invalidate(
                ids={table: tuple(int(row_id) for row_id in row_ids)}
            )

    # ------------------------------------------------------------------
    # Store identity and record persistence
    # ------------------------------------------------------------------

    def ensure_store(self, configuration: api.StoreConfiguration) -> int:
        """
        Return the Store row ID, inserting a supplied configuration if needed.


        :param configuration:
        :return:
        """

        rows = self.macros.get_rows(
            "stores", where={"store_uuid": str(configuration.store_uuid)}
        )
        if rows:
            return int(rows[0]["store_id"])
        values = store_configuration_to_row_dict(
            configuration,
            allowed_columns=self.db.get_column_headings("stores"),
        )
        store_id = int(self.macros.insert_row("stores", values))
        self._invalidate_record_ids("stores", store_id)
        return store_id

    def update_store(self, configuration: api.StoreConfiguration) -> None:
        """
        Persist a complete replacement configuration for one Store UUID.


        :param configuration:
        :return:
        """

        rows = self.macros.get_rows(
            "stores", where={"store_uuid": str(configuration.store_uuid)}
        )
        if not rows:
            raise api.StoreConfigurationNotFound(
                f"No durable Store row for UUID {configuration.store_uuid}."
            )
        if len(rows) != 1:
            raise api.StorageManagementError(
                f"duplicate durable Store UUID {configuration.store_uuid}."
            )
        values = store_configuration_to_row_dict(
            configuration,
            allowed_columns=self.db.get_column_headings("stores"),
            include_nulls=True,
        )
        store_id = int(rows[0]["store_id"])
        self.macros.update_row("stores", store_id, values)
        self._invalidate_record_ids("stores", store_id)

    def remove_store(self, store_ref: api.StoreUUID) -> None:
        """
        Delete one unclaimed durable Store configuration.


        :param store_ref:
        :return:
        """

        rows = self.macros.get_rows(
            "stores", where={"store_uuid": str(store_ref)}
        )
        if not rows:
            raise api.StoreConfigurationNotFound(
                f"No durable Store row for UUID {store_ref}."
            )
        if len(rows) != 1:
            raise api.StorageManagementError(
                f"duplicate durable Store UUID {store_ref}."
            )
        store_id = int(rows[0]["store_id"])
        self.macros.delete_row("stores", store_id)
        self._invalidate_record_ids("stores", store_id)

    def upsert_asset(self, record: api.DigitalAssetRecord) -> None:
        """
        Persist searchable Asset scalars and its lossless typed envelope.


        :param record:
        :return:
        """

        values = {
            "digital_asset_name": _database_scalar_text(record.metadata.name),
            "digital_asset_mime_type": _database_scalar_text(
                record.metadata.media_type
            ),
            "digital_asset_original_name": _database_scalar_text(
                record.metadata.original_name
            ),
            "digital_asset_size_bytes": record.size_bytes,
            "digital_asset_hash_sha256": _digest_value(record.digests, "sha256"),
            "digital_asset_hash_blake3": _digest_value(record.digests, "blake3"),
            "digital_asset_replication_policy_id": record.replication_policy_id,
            "digital_asset_backup_policy_id": record.backup_policy_id,
            "digital_asset_scratch": self._dump(record),
        }
        self._upsert(
            "digital_assets",
            "digital_asset_id",
            int(record.digital_asset_id),
            values,
        )
        self._invalidate_record_ids("digital_assets", int(record.digital_asset_id))

    def remove_asset(self, digital_asset_id: api.DigitalAssetID) -> None:
        """
        Delete one Asset row and invalidate its cached record.


        :param digital_asset_id:
        :return:
        """

        self.macros.delete_row("digital_assets", int(digital_asset_id))
        self._invalidate_record_ids("digital_assets", int(digital_asset_id))

    def upsert_replica(self, record: api.ReplicaRecord) -> None:
        """
        Persist searchable Replica scalars and its lossless typed envelope.


        :param record:
        :return:
        """

        store_id = self._store_id(record.location.store_ref)
        values = {
            "asset_replica_digital_asset_id": int(record.digital_asset_id),
            "asset_replica_store_id": store_id,
            "asset_replica_storage_key": _database_scalar_text(
                record.location.key
            ),
            "asset_replica_mode": record.mode.value,
            "asset_replica_presence_status": record.state.value,
            "asset_replica_integrity_status": record.state.value,
            "asset_replica_last_seen_timestamp_ep_k": _epoch_ms(
                record.observation.checked_at
            ),
            "asset_replica_last_integrity_check_timestamp_ep_k": _epoch_ms(
                record.observation.checked_at
            ),
            "asset_replica_observed_size_bytes": (
                record.observation.observed_size_bytes
            ),
            "asset_replica_observed_hash_sha256": _digest_value(
                record.observation.observed_digests, "sha256"
            ),
            "asset_replica_observed_hash_blake3": _digest_value(
                record.observation.observed_digests, "blake3"
            ),
            "asset_replica_failure_reason": _database_scalar_text(
                record.observation.failure_reason
            ),
            "asset_replica_scratch": self._dump(record),
        }
        self._upsert(
            "asset_replicas",
            "asset_replica_id",
            int(record.replica_id),
            values,
        )
        self._invalidate_record_ids("asset_replicas", int(record.replica_id))

    def remove_replica(self, replica_id: api.ReplicaID) -> None:
        """
        Delete one Replica row and invalidate its cached record.


        :param replica_id:
        :return:
        """

        self.macros.delete_row("asset_replicas", int(replica_id))
        self._invalidate_record_ids("asset_replicas", int(replica_id))

    def upsert_composite(self, record: api.CompositeDigitalAssetRecord) -> None:
        """
        Atomically replace a Composite envelope and ordered member links.


        :param record:
        :return:
        """

        composite_id = int(record.composite_digital_asset_id)
        with self.macros.transaction():
            self._upsert(
                "composite_digital_assets",
                "composite_digital_asset_id",
                composite_id,
                {
                    "composite_digital_asset_name": record.name,
                    "composite_digital_asset_scratch": self._dump(record),
                },
            )
            self._delete_matching(
                "composite_digital_asset_digital_asset_links",
                "composite_digital_asset_digital_asset_link_id",
                {
                    "composite_digital_asset_digital_asset_link_composite_digital_asset_id": composite_id
                },
            )
            for member in record.members:
                member_type = (
                    member.role
                    if member.role in {"member", "chapter", "track", "disc_member", "part"}
                    else "member"
                )
                self.macros.insert_row(
                    "composite_digital_asset_digital_asset_links",
                    {
                        "composite_digital_asset_digital_asset_link_composite_digital_asset_id": composite_id,
                        "composite_digital_asset_digital_asset_link_digital_asset_id": int(
                            member.digital_asset_id
                        ),
                        "composite_digital_asset_digital_asset_link_type": member_type,
                        "composite_digital_asset_digital_asset_link_origin": "storage_manager",
                        "composite_digital_asset_digital_asset_link_sequence_number": member.sequence_number,
                        "composite_digital_asset_digital_asset_link_is_required": int(
                            member.required
                        ),
                        "composite_digital_asset_digital_asset_link_scratch": self._dump(
                            member
                        ),
                    },
                )
        self._invalidate_record_ids("composite_digital_assets", composite_id)
        if self.cache is not None:
            self.cache.invalidate(
                links=(
                    ("composite_digital_assets", "digital_assets"),
                    ("digital_assets", "composite_digital_assets"),
                )
            )

    def remove_composite(
        self, composite_digital_asset_id: api.CompositeDigitalAssetID
    ) -> None:
        """
        Delete one Composite and invalidate record and membership caches.


        :param composite_digital_asset_id:
        :return:
        """

        self.macros.delete_row(
            "composite_digital_assets", int(composite_digital_asset_id)
        )
        self._invalidate_record_ids(
            "composite_digital_assets", int(composite_digital_asset_id)
        )
        if self.cache is not None:
            self.cache.invalidate(
                links=(
                    ("composite_digital_assets", "digital_assets"),
                    ("digital_assets", "composite_digital_assets"),
                )
            )

    def upsert_derivation(self, record: api.DigitalAssetDerivationRecord) -> None:
        """
        Persist queryable provenance scalars and its complete envelope.


        :param record:
        :return:
        """

        declaration = record.declaration
        first_atomic_source = next(
            (
                source.digital_asset_id
                for source in declaration.sources
                if source.digital_asset_id is not None
            ),
            None,
        )
        values = {
            "digital_asset_derivation_parent_digital_asset_id": first_atomic_source,
            "digital_asset_derivation_child_digital_asset_id": int(
                declaration.result_digital_asset_id
            ),
            "digital_asset_derivation_run_id": declaration.workflow_id,
            "digital_asset_derivation_kind": declaration.kind.value,
            "digital_asset_derivation_note": declaration.notes,
            "digital_asset_derivation_scratch": self._dump(record),
        }
        if declaration.created_at is not None:
            values["digital_asset_derivation_created_timestamp_ep_k"] = _epoch_ms(
                declaration.created_at
            )
        self._upsert(
            "digital_asset_derivations",
            "digital_asset_derivation_id",
            int(record.digital_asset_derivation_id),
            values,
        )
        self._invalidate_record_ids(
            "digital_asset_derivations",
            int(record.digital_asset_derivation_id),
        )

    def remove_derivation(
        self, digital_asset_derivation_id: api.DigitalAssetDerivationID
    ) -> None:
        """
        Delete one derivation row and invalidate its cached record.


        :param digital_asset_derivation_id:
        :return:
        """

        self.macros.delete_row(
            "digital_asset_derivations", int(digital_asset_derivation_id)
        )
        self._invalidate_record_ids(
            "digital_asset_derivations", int(digital_asset_derivation_id)
        )

    def upsert_replication_policy(self, record: api.ReplicationPolicyRecord) -> None:
        """
        Persist queryable replication settings and their complete envelope.


        :param record:
        :return:
        """

        policy = record.policy
        self._upsert(
            "replication_policies",
            "replication_policy_id",
            int(record.replication_policy_id),
            {
                "replication_policy_name": policy.name,
                "replication_policy_name_norm": policy.name.casefold(),
                "replication_policy_min_copies": policy.min_copies,
                "replication_policy_target_copies": policy.target_copies,
                "replication_policy_distinct_by_json": json.dumps(
                    [value.value for value in policy.distinct_by]
                ),
                "replication_policy_max_copies_per_bucket": policy.max_copies_per_bucket,
                "replication_policy_required_store_tags_json": json.dumps(
                    sorted(policy.required_store_tags)
                ),
                "replication_policy_preferred_store_tags_json": json.dumps(
                    sorted(policy.preferred_store_tags)
                ),
                "replication_policy_forbidden_store_tags_json": json.dumps(
                    sorted(policy.forbidden_store_tags)
                ),
                "replication_policy_synchronous_write_copies": policy.synchronous_write_copies,
                "replication_policy_auto_heal": int(policy.auto_heal),
                "replication_policy_mode": policy.mode.value,
                "replication_policy_scratch": self._dump(record),
            },
        )
        self._invalidate_record_ids(
            "replication_policies", int(record.replication_policy_id)
        )

    def remove_replication_policy(
        self, replication_policy_id: api.ReplicationPolicyID
    ) -> None:
        """
        Delete one replication policy and invalidate its cached record.


        :param replication_policy_id:
        :return:
        """

        self.macros.delete_row("replication_policies", int(replication_policy_id))
        self._invalidate_record_ids(
            "replication_policies", int(replication_policy_id)
        )

    def upsert_backup_policy(self, record: api.BackupPolicyRecord) -> None:
        """
        Persist queryable backup settings and their complete envelope.


        :param record:
        :return:
        """

        policy = record.policy
        self._upsert(
            "backup_policies",
            "backup_policy_id",
            int(record.backup_policy_id),
            {
                "backup_policy_name": policy.name,
                "backup_policy_name_norm": policy.name.casefold(),
                "backup_policy_min_backup_copies": policy.min_copies,
                "backup_policy_target_backup_copies": policy.target_copies,
                "backup_policy_distinct_by_json": json.dumps(
                    [value.value for value in policy.distinct_by]
                ),
                "backup_policy_max_copies_per_bucket": policy.max_copies_per_bucket,
                "backup_policy_required_store_tags_json": json.dumps(
                    sorted(policy.required_store_tags)
                ),
                "backup_policy_preferred_store_tags_json": json.dumps(
                    sorted(policy.preferred_store_tags)
                ),
                "backup_policy_forbidden_store_tags_json": json.dumps(
                    sorted(policy.forbidden_store_tags)
                ),
                "backup_policy_periodic_verification": int(
                    policy.periodic_verification
                ),
                "backup_policy_retention_locked": int(policy.retention_locked),
                "backup_policy_mode": policy.mode.value,
                "backup_policy_scratch": self._dump(record),
            },
        )
        self._invalidate_record_ids("backup_policies", int(record.backup_policy_id))

    def remove_backup_policy(self, backup_policy_id: api.BackupPolicyID) -> None:
        """
        Delete one backup policy and invalidate its cached record.


        :param backup_policy_id:
        :return:
        """

        self.macros.delete_row("backup_policies", int(backup_policy_id))
        self._invalidate_record_ids("backup_policies", int(backup_policy_id))

    def upsert_item_target(
        self,
        value: tuple[
            tuple[api.ItemID, str],
            tuple[str, api.DigitalAssetID | api.CompositeDigitalAssetID],
        ],
    ) -> None:
        """
        Atomically replace one Item role's atomic or Composite target.


        :param value:
        :return:
        """

        (item_id, role), (kind, target_id) = value
        with self.macros.transaction():
            self._delete_item_target(item_id, role)
            if kind == "digital_asset":
                table = "digital_asset_item_links"
                prefix = "digital_asset_item_link"
                target_column = f"{prefix}_digital_asset_id"
                allowed_roles = {
                    "primary_payload",
                    "cover",
                    "page_scan",
                    "ocr_text",
                    "metadata_sidecar",
                    "preview",
                    "thumbnail",
                    "supplement",
                    "source_archive",
                    "derived_output",
                    "transcript",
                    "caption_track",
                }
            else:
                table = "composite_digital_asset_item_links"
                prefix = "composite_digital_asset_item_link"
                target_column = f"{prefix}_composite_digital_asset_id"
                allowed_roles = {
                    "primary_payload",
                    "supplement",
                    "source_archive",
                    "derived_output",
                    "transcript",
                    "caption_track",
                }
            self.macros.insert_row(
                table,
                {
                    f"{prefix}_item_id": int(item_id),
                    target_column: int(target_id),
                    f"{prefix}_priority": 0,
                    f"{prefix}_primary": int(role == "primary_payload"),
                    f"{prefix}_type": (
                        role if role in allowed_roles else "primary_payload"
                    ),
                    f"{prefix}_origin": "storage_manager",
                    f"{prefix}_source": "storage_manager",
                    f"{prefix}_scratch": self._dump({"role": role}),
                },
            )
        if self.cache is not None:
            target_table = (
                "digital_assets"
                if kind == "digital_asset"
                else "composite_digital_assets"
            )
            self.cache.invalidate(
                links=(("items", target_table), (target_table, "items"))
            )

    def remove_item_target(self, key: tuple[api.ItemID, str]) -> None:
        """
        Remove one Item-role target and invalidate relationship indexes.


        :param key:
        :return:
        """

        self._delete_item_target(*key)
        if self.cache is not None:
            self.cache.invalidate(
                links=(
                    ("items", "digital_assets"),
                    ("digital_assets", "items"),
                    ("items", "composite_digital_assets"),
                    ("composite_digital_assets", "items"),
                )
            )

    # ------------------------------------------------------------------
    # Durable ingest journal
    # ------------------------------------------------------------------

    def journal_start(self, operation_id: UUID, request: Any) -> None:
        """
        Start or idempotently restart a durable ingest journal entry.


        :param operation_id:
        :param request:
        :return:
        """

        if not self.has_ingest_journal:
            return
        row = self._journal_row(operation_id)
        payload = {"request": request}
        if row is None:
            self.macros.insert_row(
                "storage_ingest_operations",
                {
                    "storage_ingest_operation_uuid": str(operation_id),
                    "storage_ingest_operation_state": "started",
                    "storage_ingest_operation_scratch": self._dump(payload),
                },
            )
            return
        existing = self._load(row["storage_ingest_operation_scratch"])
        if not isinstance(existing, dict) or existing.get("request") != request:
            raise api.StoragePreconditionFailed(
                "ingest operation ID was already used for a different request."
            )
        self.macros.update_row(
            "storage_ingest_operations",
            row["storage_ingest_operation_id"],
            {
                "storage_ingest_operation_state": "started",
                "storage_ingest_operation_last_error": None,
            },
        )

    def journal_publication_pending(
        self,
        operation_id: UUID,
        *,
        asset_record: api.DigitalAssetRecord,
        asset_created: bool,
        location: api.Location,
        replica_mode: api.ReplicaMode,
        placement_hints: api.StoragePlacementHints | None,
    ) -> None:
        """
        Record enough planned publication state for crash recovery.


        :param operation_id:
        :param asset_record:
        :param asset_created:
        :param location:
        :param replica_mode:
        :param placement_hints:
        :return:
        """

        if not self.has_ingest_journal:
            return
        self._update_journal_payload(
            operation_id,
            state="publishing",
            values={
                "asset_record": asset_record,
                "asset_created": asset_created,
                "location": location,
                "replica_mode": replica_mode,
                "placement_hints": placement_hints,
            },
        )

    def journal_published(self, operation_id: UUID) -> None:
        """
        Mark physical publication complete before metadata commit.


        :param operation_id:
        :return:
        """

        if self.has_ingest_journal:
            self._update_journal_payload(operation_id, state="published", values={})

    def journal_failed(self, operation_id: UUID, error: BaseException) -> None:
        """
        Mark an existing ingest failed with an operator-safe error string.


        :param operation_id:
        :param error:
        :return:
        """

        if not self.has_ingest_journal:
            return
        row = self._journal_row(operation_id)
        if row is None:
            return
        self.macros.update_row(
            "storage_ingest_operations",
            row["storage_ingest_operation_id"],
            {
                "storage_ingest_operation_state": "failed",
                "storage_ingest_operation_last_error": _database_scalar_text(
                    (str(error) or type(error).__name__)[:2000]
                ),
            },
        )

    def commit_ingest_operation(self, operation: Any) -> None:
        """
        Persist a completed operation as the idempotent retry result.


        :param operation:
        :return:
        """

        if not self.has_ingest_journal:
            return
        operation_id = operation.result.operation_id
        row = self._journal_row(operation_id)
        values = {
            "storage_ingest_operation_state": "committed",
            "storage_ingest_operation_store_uuid": str(
                operation.result.replica_record.location.store_ref
            ),
            "storage_ingest_operation_storage_key": _database_scalar_text(
                operation.result.replica_record.location.key
            ),
            "storage_ingest_operation_digital_asset_id": int(
                operation.result.asset_record.digital_asset_id
            ),
            "storage_ingest_operation_asset_replica_id": int(
                operation.result.replica_record.replica_id
            ),
            "storage_ingest_operation_last_error": None,
            "storage_ingest_operation_scratch": self._dump(
                {"request": operation.request, "operation": operation}
            ),
        }
        if row is None:
            values["storage_ingest_operation_uuid"] = str(operation_id)
            self.macros.insert_row("storage_ingest_operations", values)
        else:
            self.macros.update_row(
                "storage_ingest_operations",
                row["storage_ingest_operation_id"],
                values,
            )

    def pending_ingests(self) -> tuple[tuple[UUID, str, dict[str, Any]], ...]:
        """
        Return decoded journal entries still eligible for recovery.


        :return:
        """

        if not self.has_ingest_journal:
            return ()
        pending: list[tuple[UUID, str, dict[str, Any]]] = []
        for row in self.macros.get_rows(
            "storage_ingest_operations",
            order_by=("storage_ingest_operation_id",),
        ):
            state = str(row["storage_ingest_operation_state"])
            if state in {"committed", "failed"}:
                continue
            payload = self._load(row["storage_ingest_operation_scratch"])
            if not isinstance(payload, dict):
                raise api.StorageManagementError(
                    "invalid durable ingest journal payload."
                )
            pending.append(
                (UUID(str(row["storage_ingest_operation_uuid"])), state, payload)
            )
        return tuple(pending)

    def ingest_journal_entry(
        self,
        operation_id: UUID,
    ) -> tuple[str, dict[str, Any], str | None] | None:
        """
        Return one decoded durable ingest entry for explicit recovery.


        :param operation_id:
        :return:
        """

        if not self.has_ingest_journal:
            return None
        row = self._journal_row(operation_id)
        if row is None:
            return None
        payload = self._load(row["storage_ingest_operation_scratch"])
        if not isinstance(payload, dict):
            raise api.StorageManagementError(
                "invalid durable ingest journal payload."
            )
        error = row.get("storage_ingest_operation_last_error")
        return (
            str(row["storage_ingest_operation_state"]),
            payload,
            None if error in (None, "") else str(error),
        )

    def ingest_journal_statuses(self) -> tuple[dict[str, object], ...]:
        """
        Return operator-safe journal summaries without decoded requests.


        :return:
        """

        if not self.has_ingest_journal:
            return ()
        return tuple(
            {
                "operation_id": UUID(
                    str(row["storage_ingest_operation_uuid"])
                ),
                "state": str(row["storage_ingest_operation_state"]),
                "last_error": row.get(
                    "storage_ingest_operation_last_error"
                ),
                "store_ref": row.get("storage_ingest_operation_store_uuid"),
                "storage_key": row.get(
                    "storage_ingest_operation_storage_key"
                ),
            }
            for row in self.macros.get_rows(
                "storage_ingest_operations",
                order_by=("storage_ingest_operation_id",),
            )
        )

    # ------------------------------------------------------------------
    # Loading helpers
    # ------------------------------------------------------------------

    def _load_assets(
        self,
        rows: Iterable[Mapping[str, Any]] | None = None,
    ) -> dict[api.DigitalAssetID, api.DigitalAssetRecord]:
        """
        Decode Asset envelopes, falling back to usable legacy scalars.


        :param rows:
        :return:
        """

        records: dict[api.DigitalAssetID, api.DigitalAssetRecord] = {}
        source = (
            self._record_rows("digital_assets", order_by=("digital_asset_id",))
            if rows is None
            else rows
        )
        for row in source:
            decoded = self._load_optional_record(row, "digital_asset_scratch")
            if isinstance(decoded, api.DigitalAssetRecord):
                records[decoded.digital_asset_id] = decoded
                continue
            digests = _row_digests(row, "digital_asset_hash_")
            size = row.get("digital_asset_size_bytes")
            if size is None or not digests:
                continue
            identifier = api.DigitalAssetID(int(row["digital_asset_id"]))
            records[identifier] = api.DigitalAssetRecord(
                identifier,
                int(size),
                digests,
                api.DigitalAssetMetadata(
                    name=_optional_text(row.get("digital_asset_name")),
                    media_type=_optional_text(row.get("digital_asset_mime_type")),
                    original_name=_optional_text(
                        row.get("digital_asset_original_name")
                    ),
                ),
                _optional_id(
                    row.get("digital_asset_replication_policy_id"),
                    api.ReplicationPolicyID,
                ),
                _optional_id(
                    row.get("digital_asset_backup_policy_id"), api.BackupPolicyID
                ),
                f"db-{identifier}",
            )
        return records

    def _load_replicas(
        self,
        rows: Iterable[Mapping[str, Any]] | None = None,
    ) -> dict[api.ReplicaID, api.ReplicaRecord]:
        """
        Decode Replica envelopes, falling back to usable legacy scalars.


        :param rows:
        :return:
        """

        records: dict[api.ReplicaID, api.ReplicaRecord] = {}
        source = (
            self._record_rows("asset_replicas", order_by=("asset_replica_id",))
            if rows is None
            else rows
        )
        for row in source:
            decoded = self._load_optional_record(row, "asset_replica_scratch")
            if isinstance(decoded, api.ReplicaRecord):
                records[decoded.replica_id] = decoded
                continue
            asset_id = row.get("asset_replica_digital_asset_id")
            store_id = row.get("asset_replica_store_id")
            key = row.get("asset_replica_storage_key")
            if asset_id is None or store_id is None or key is None:
                continue
            identifier = api.ReplicaID(int(row["asset_replica_id"]))
            checked_at = _datetime_from_epoch(
                row.get("asset_replica_last_integrity_check_timestamp_ep_k")
                or row.get("asset_replica_last_seen_timestamp_ep_k")
            )
            state_text = _optional_text(row.get("asset_replica_presence_status"))
            try:
                state = api.ReplicaState(state_text or "unverified")
                mode = api.ReplicaMode(str(row.get("asset_replica_mode") or "active"))
            except ValueError:
                continue
            records[identifier] = api.ReplicaRecord(
                identifier,
                api.DigitalAssetID(int(asset_id)),
                api.Location(self._store_uuid(int(store_id)), str(key)),
                mode,
                api.ReplicaObservation(
                    state,
                    observed_size_bytes=_optional_int(
                        row.get("asset_replica_observed_size_bytes")
                    ),
                    observed_digests=_row_digests(row, "asset_replica_observed_hash_"),
                    checked_at=checked_at,
                    failure_reason=_optional_text(
                        row.get("asset_replica_failure_reason")
                    ),
                ),
                revision=f"db-{identifier}",
            )
        return records

    def _load_composites(
        self,
        rows: Iterable[Mapping[str, Any]] | None = None,
    ) -> dict[api.CompositeDigitalAssetID, api.CompositeDigitalAssetRecord]:
        """
        Decode Composites, rebuilding legacy records from member links.


        :param rows:
        :return:
        """

        records: dict[api.CompositeDigitalAssetID, api.CompositeDigitalAssetRecord] = {}
        source = (
            self._record_rows(
                "composite_digital_assets",
                order_by=("composite_digital_asset_id",),
            )
            if rows is None
            else rows
        )
        for row in source:
            decoded = self._load_optional_record(
                row, "composite_digital_asset_scratch"
            )
            if isinstance(decoded, api.CompositeDigitalAssetRecord):
                records[decoded.composite_digital_asset_id] = decoded
                continue
            identifier = api.CompositeDigitalAssetID(
                int(row["composite_digital_asset_id"])
            )
            members: list[api.CompositeDigitalAssetMembership] = []
            links = self.macros.get_rows(
                "composite_digital_asset_digital_asset_links",
                where={
                    "composite_digital_asset_digital_asset_link_composite_digital_asset_id": int(
                        identifier
                    )
                },
                order_by=(
                    "composite_digital_asset_digital_asset_link_sequence_number",
                ),
            )
            for link in links:
                member = self._load_optional_record(
                    link, "composite_digital_asset_digital_asset_link_scratch"
                )
                if isinstance(member, api.CompositeDigitalAssetMembership):
                    members.append(member)
                    continue
                asset_id = link.get(
                    "composite_digital_asset_digital_asset_link_digital_asset_id"
                )
                if asset_id is None:
                    continue
                members.append(
                    api.CompositeDigitalAssetMembership(
                        api.DigitalAssetID(int(asset_id)),
                        int(
                            link.get(
                                "composite_digital_asset_digital_asset_link_sequence_number"
                            )
                            or 0
                        ),
                        role=_optional_text(
                            link.get(
                                "composite_digital_asset_digital_asset_link_type"
                            )
                        ),
                        required=bool(
                            link.get(
                                "composite_digital_asset_digital_asset_link_is_required"
                            )
                        ),
                    )
                )
            if members:
                records[identifier] = api.CompositeDigitalAssetRecord(
                    identifier,
                    tuple(members),
                    name=_optional_text(row.get("composite_digital_asset_name")),
                    revision=f"db-{identifier}",
                )
        return records

    def _load_derivations(
        self,
        rows: Iterable[Mapping[str, Any]] | None = None,
    ) -> dict[api.DigitalAssetDerivationID, api.DigitalAssetDerivationRecord]:
        """
        Decode derivations, rebuilding simple legacy parent-child records.


        :param rows:
        :return:
        """

        records: dict[api.DigitalAssetDerivationID, api.DigitalAssetDerivationRecord] = {}
        source = (
            self._record_rows(
                "digital_asset_derivations",
                order_by=("digital_asset_derivation_id",),
            )
            if rows is None
            else rows
        )
        for row in source:
            decoded = self._load_optional_record(
                row, "digital_asset_derivation_scratch"
            )
            if isinstance(decoded, api.DigitalAssetDerivationRecord):
                records[decoded.digital_asset_derivation_id] = decoded
                continue
            parent = row.get("digital_asset_derivation_parent_digital_asset_id")
            child = row.get("digital_asset_derivation_child_digital_asset_id")
            if parent is None or child is None:
                continue
            identifier = api.DigitalAssetDerivationID(
                int(row["digital_asset_derivation_id"])
            )
            try:
                kind = api.DigitalAssetDerivationKind(
                    str(row.get("digital_asset_derivation_kind") or "other")
                )
            except ValueError:
                kind = api.DigitalAssetDerivationKind.OTHER
            declaration = api.DigitalAssetDerivationDeclaration(
                result_digital_asset_id=api.DigitalAssetID(int(child)),
                sources=(
                    api.DigitalAssetDerivationSourceReference(
                        0, digital_asset_id=api.DigitalAssetID(int(parent))
                    ),
                ),
                kind=kind,
                notes=_optional_text(row.get("digital_asset_derivation_note")),
                workflow_id=_optional_int(row.get("digital_asset_derivation_run_id")),
            )
            records[identifier] = api.DigitalAssetDerivationRecord(
                identifier, declaration, f"db-{identifier}"
            )
        return records

    def _load_replication_policies(
        self,
        rows: Iterable[Mapping[str, Any]] | None = None,
    ) -> dict[api.ReplicationPolicyID, api.ReplicationPolicyRecord]:
        """
        Decode replication policies with scalar-column compatibility.


        :param rows:
        :return:
        """

        records: dict[api.ReplicationPolicyID, api.ReplicationPolicyRecord] = {}
        source = (
            self._record_rows(
                "replication_policies", order_by=("replication_policy_id",)
            )
            if rows is None
            else rows
        )
        for row in source:
            decoded = self._load_optional_record(row, "replication_policy_scratch")
            if isinstance(decoded, api.ReplicationPolicyRecord):
                records[decoded.replication_policy_id] = decoded
                continue
            identifier = api.ReplicationPolicyID(int(row["replication_policy_id"]))
            try:
                policy = api.ReplicationPolicy(
                    name=str(row.get("replication_policy_name") or f"policy-{identifier}"),
                    min_copies=int(row.get("replication_policy_min_copies") or 0),
                    target_copies=_optional_int(
                        row.get("replication_policy_target_copies")
                    ),
                    distinct_by=tuple(
                        api.ReplicaSeparationDimension(value)
                        for value in _json_list(
                            row.get("replication_policy_distinct_by_json"),
                            ["store"],
                        )
                    ),
                    max_copies_per_bucket=int(
                        row.get("replication_policy_max_copies_per_bucket") or 1
                    ),
                    required_store_tags=frozenset(
                        _json_list(row.get("replication_policy_required_store_tags_json"), [])
                    ),
                    preferred_store_tags=frozenset(
                        _json_list(row.get("replication_policy_preferred_store_tags_json"), [])
                    ),
                    forbidden_store_tags=frozenset(
                        _json_list(row.get("replication_policy_forbidden_store_tags_json"), [])
                    ),
                    synchronous_write_copies=int(
                        row.get("replication_policy_synchronous_write_copies") or 0
                    ),
                    auto_heal=bool(row.get("replication_policy_auto_heal")),
                    mode=api.ReplicaMode(
                        str(row.get("replication_policy_mode") or "active")
                    ),
                )
            except (TypeError, ValueError):
                continue
            records[identifier] = api.ReplicationPolicyRecord(
                identifier, policy, f"db-{identifier}"
            )
        return records

    def _load_backup_policies(
        self,
        rows: Iterable[Mapping[str, Any]] | None = None,
    ) -> dict[api.BackupPolicyID, api.BackupPolicyRecord]:
        """
        Decode backup policies with scalar-column compatibility.


        :param rows:
        :return:
        """

        records: dict[api.BackupPolicyID, api.BackupPolicyRecord] = {}
        source = (
            self._record_rows("backup_policies", order_by=("backup_policy_id",))
            if rows is None
            else rows
        )
        for row in source:
            decoded = self._load_optional_record(row, "backup_policy_scratch")
            if isinstance(decoded, api.BackupPolicyRecord):
                records[decoded.backup_policy_id] = decoded
                continue
            identifier = api.BackupPolicyID(int(row["backup_policy_id"]))
            try:
                policy = api.BackupPolicy(
                    name=str(row.get("backup_policy_name") or f"backup-{identifier}"),
                    min_copies=int(row.get("backup_policy_min_backup_copies") or 0),
                    target_copies=_optional_int(
                        row.get("backup_policy_target_backup_copies")
                    ),
                    distinct_by=tuple(
                        api.ReplicaSeparationDimension(value)
                        for value in _json_list(
                            row.get("backup_policy_distinct_by_json"), ["store"]
                        )
                    ),
                    max_copies_per_bucket=int(
                        row.get("backup_policy_max_copies_per_bucket") or 1
                    ),
                    required_store_tags=frozenset(
                        _json_list(row.get("backup_policy_required_store_tags_json"), [])
                    ),
                    preferred_store_tags=frozenset(
                        _json_list(row.get("backup_policy_preferred_store_tags_json"), [])
                    ),
                    forbidden_store_tags=frozenset(
                        _json_list(row.get("backup_policy_forbidden_store_tags_json"), [])
                    ),
                    periodic_verification=bool(
                        row.get("backup_policy_periodic_verification")
                    ),
                    retention_locked=bool(row.get("backup_policy_retention_locked")),
                    mode=api.ReplicaMode(
                        str(row.get("backup_policy_mode") or "backup")
                    ),
                )
            except (TypeError, ValueError):
                continue
            records[identifier] = api.BackupPolicyRecord(
                identifier, policy, f"db-{identifier}"
            )
        return records

    def _load_item_targets(self) -> dict[tuple[api.ItemID, str], tuple[str, Any]]:
        """
        Merge atomic and Composite Item-link rows into one role-keyed map.


        :return:
        """

        targets: dict[tuple[api.ItemID, str], tuple[str, Any]] = {}
        specs = (
            (
                "digital_asset_item_links",
                "digital_asset_item_link",
                "digital_asset",
                api.DigitalAssetID,
            ),
            (
                "composite_digital_asset_item_links",
                "composite_digital_asset_item_link",
                "composite_digital_asset",
                api.CompositeDigitalAssetID,
            ),
        )
        for table, prefix, kind, constructor in specs:
            for row in self.macros.get_rows(table, order_by=(f"{prefix}_id",)):
                item_id = row.get(f"{prefix}_item_id")
                target_id = row.get(f"{prefix}_{kind}_id")
                if item_id is None or target_id is None:
                    continue
                scratch = self._load_optional_record(row, f"{prefix}_scratch")
                role = (
                    _optional_text(scratch.get("role"))
                    if isinstance(scratch, dict)
                    else None
                ) or _optional_text(row.get(f"{prefix}_type")) or "primary_payload"
                targets[(api.ItemID(int(item_id)), role)] = (
                    kind,
                    constructor(int(target_id)),
                )
        return targets

    def _load_committed_ingest_operations(self) -> dict[UUID, Any]:
        """
        Decode committed operations used to make ingest retries idempotent.


        :return:
        """

        if not self.has_ingest_journal:
            return {}
        operations: dict[UUID, Any] = {}
        rows = self.macros.get_rows(
            "storage_ingest_operations",
            where={"storage_ingest_operation_state": "committed"},
            order_by=("storage_ingest_operation_id",),
        )
        for row in rows:
            payload = self._load(row["storage_ingest_operation_scratch"])
            operation = payload.get("operation") if isinstance(payload, dict) else None
            if operation is not None:
                operations[UUID(str(row["storage_ingest_operation_uuid"]))] = operation
        return operations

    # ------------------------------------------------------------------
    # Portable row/envelope helpers
    # ------------------------------------------------------------------

    def _upsert(
        self,
        table: str,
        id_column: str,
        row_id: int,
        values: Mapping[str, Any],
    ) -> None:
        """
        Insert or update only values supported by the bound table schema.


        :param table:
        :param id_column:
        :param row_id:
        :param values:
        :return:
        """

        payload = {
            key: value
            for key, value in values.items()
            if key in set(self.db.get_column_headings(table))
        }
        if self.macros.get_row(table, row_id, id_column=id_column) is None:
            self.macros.insert_row(
                table, {id_column: row_id, **payload}, id_column=id_column
            )
        else:
            self.macros.update_row(
                table, row_id, payload, id_column=id_column
            )

    def _delete_matching(
        self,
        table: str,
        id_column: str,
        where: Mapping[str, Any],
    ) -> None:
        """
        Delete every row matching ``where`` through portable macros.


        :param table:
        :param id_column:
        :param where:
        :return:
        """

        for row in self.macros.get_rows(table, where=where):
            self.macros.delete_row(
                table, row[id_column], id_column=id_column
            )

    def _delete_item_target(self, item_id: api.ItemID, role: str) -> None:
        """
        Delete matching atomic and Composite links for one Item role.


        :param item_id:
        :param role:
        :return:
        """

        for table, prefix in (
            ("digital_asset_item_links", "digital_asset_item_link"),
            (
                "composite_digital_asset_item_links",
                "composite_digital_asset_item_link",
            ),
        ):
            rows = self.macros.get_rows(
                table, where={f"{prefix}_item_id": int(item_id)}
            )
            for row in rows:
                scratch = self._load_optional_record(row, f"{prefix}_scratch")
                row_role = (
                    _optional_text(scratch.get("role"))
                    if isinstance(scratch, dict)
                    else None
                ) or _optional_text(row.get(f"{prefix}_type")) or "primary_payload"
                if row_role == role:
                    self.macros.delete_row(table, row[f"{prefix}_id"])

    def _store_id(self, store_ref: api.StoreUUID) -> int:
        """
        Resolve a public Store UUID to its durable foreign-key identity.


        :param store_ref:
        :return:
        """

        rows = self.macros.get_rows(
            "stores", where={"store_uuid": str(store_ref)}
        )
        if not rows:
            raise api.StoreConfigurationNotFound(
                f"No durable Store row for UUID {store_ref}."
            )
        return int(rows[0]["store_id"])

    def _store_uuid(self, store_id: int) -> api.StoreUUID:
        """
        Resolve a durable Store identity to its public UUID.


        :param store_id:
        :return:
        """

        row = self.macros.get_row("stores", store_id, id_column="store_id")
        if row is None or row.get("store_uuid") in (None, ""):
            raise api.StoreConfigurationNotFound(
                f"Store row {store_id} has no durable UUID."
            )
        return UUID(str(row["store_uuid"]))

    def _journal_row(self, operation_id: UUID) -> Mapping[str, Any] | None:
        """
        Return the unique raw journal row for an operation UUID.


        :param operation_id:
        :return:
        """

        rows = self.macros.get_rows(
            "storage_ingest_operations",
            where={"storage_ingest_operation_uuid": str(operation_id)},
        )
        if len(rows) > 1:
            raise api.StorageManagementError(
                f"duplicate durable ingest operation UUID {operation_id}."
            )
        return rows[0] if rows else None

    def _update_journal_payload(
        self,
        operation_id: UUID,
        *,
        state: str,
        values: Mapping[str, Any],
    ) -> None:
        """
        Merge typed recovery values and advance one journal state.


        :param operation_id:
        :param state:
        :param values:
        :return:
        """

        row = self._journal_row(operation_id)
        if row is None:
            raise api.StorageManagementError(
                f"durable ingest operation {operation_id} was not started."
            )
        payload = self._load(row["storage_ingest_operation_scratch"])
        if not isinstance(payload, dict):
            raise api.StorageManagementError(
                f"invalid durable ingest operation {operation_id}."
            )
        payload.update(values)
        updates: dict[str, Any] = {
            "storage_ingest_operation_state": state,
            "storage_ingest_operation_scratch": self._dump(payload),
        }
        location = payload.get("location")
        asset = payload.get("asset_record")
        if isinstance(location, api.Location):
            updates.update(
                {
                    "storage_ingest_operation_store_uuid": str(location.store_ref),
                    "storage_ingest_operation_storage_key": (
                        _database_scalar_text(location.key)
                    ),
                }
            )
        if isinstance(asset, api.DigitalAssetRecord):
            updates["storage_ingest_operation_digital_asset_id"] = int(
                asset.digital_asset_id
            )
        self.macros.update_row(
            "storage_ingest_operations",
            row["storage_ingest_operation_id"],
            updates,
        )

    def _load_optional_record(
        self, row: Mapping[str, Any], scratch_column: str
    ) -> Any | None:
        """
        Decode our marked envelope while ignoring unrelated scratch text.


        :param row:
        :param scratch_column:
        :return:
        """

        raw = row.get(scratch_column)
        if raw in (None, ""):
            return None
        try:
            envelope = json.loads(str(raw))
        except (TypeError, ValueError, json.JSONDecodeError):
            # Scratch columns predate this repository and may contain plain
            # application notes. Only our marked envelope is authoritative.
            return None
        if not isinstance(envelope, dict) or envelope.get("format") != _FORMAT:
            return None
        try:
            return self._load(raw)
        except Exception as error:
            raise api.StorageManagementError(
                f"Cannot decode {scratch_column} for durable row: {error}"
            ) from error

    def _migrate_envelope(self, raw: Any) -> str | None:
        """
        Upgrade a recognised version-zero envelope or return no change.


        :param raw:
        :return:
        """

        if raw in (None, ""):
            return None
        try:
            envelope = json.loads(str(raw))
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
        if not isinstance(envelope, dict) or envelope.get("format") != _FORMAT:
            return None
        version = envelope.get("version", 0)
        if version == _FORMAT_VERSION:
            return None
        if version != 0:
            raise api.StorageManagementError(
                f"storage envelope version {version!r} is newer than the "
                f"supported version {_FORMAT_VERSION}."
            )
        payload = envelope.get("payload", envelope.get("record"))
        try:
            _decode(payload, self._types)
        except Exception as error:
            raise api.StorageManagementError(
                f"cannot migrate storage envelope version 0: {error}"
            ) from error
        return json.dumps(
            {
                "format": _FORMAT,
                "version": _FORMAT_VERSION,
                "payload": payload,
            },
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    def _dump(self, value: Any) -> str:
        """
        Encode one typed value in the current lossless ASCII JSON envelope.


        :param value:
        :return:
        """

        return json.dumps(
            {
                "format": _FORMAT,
                "version": _FORMAT_VERSION,
                "payload": _encode(value),
            },
            # ASCII JSON escapes lone surrogate code points losslessly.  This
            # matters for POSIX filenames decoded with ``surrogateescape``;
            # SQLite and PostgreSQL text bindings reject those code points if
            # they are placed in a Python string literally.
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )

    def _load(self, raw: Any) -> Any:
        """
        Validate and decode one current-version storage envelope.


        :param raw:
        :return:
        """

        envelope = json.loads(str(raw))
        if (
            not isinstance(envelope, dict)
            or envelope.get("format") != _FORMAT
            or envelope.get("version") != _FORMAT_VERSION
        ):
            raise ValueError("unsupported storage record envelope.")
        return _decode(envelope.get("payload"), self._types)


def _database_scalar_text(value: str | None) -> str | None:
    """
    Make fallback text columns safe without weakening scratch envelopes.

    Well-formed Unicode is kept exactly.  Lone surrogates originating from a
    POSIX ``surrogateescape`` filename are rendered visibly as ``\\udcXX`` in
    legacy scalar columns; the authoritative JSON envelope retains and reloads
    the exact original string.


    :param value:
    :return:
    """

    if value is None:
        return None
    try:
        value.encode("utf-8", "strict")
    except UnicodeEncodeError:
        return value.encode("utf-8", "backslashreplace").decode("utf-8")
    return value


def _storage_value_types(additional: Iterable[type[Any]]) -> dict[str, type[Any]]:
    """
    Build the allowlist of enum and dataclass types accepted by decoding.


    :param additional:
    :return:
    """

    values: set[type[Any]] = set(additional)
    for name in dir(api):
        value = getattr(api, name)
        if isinstance(value, type) and (
            dataclasses.is_dataclass(value) or issubclass(value, Enum)
        ):
            values.add(value)
    return {_type_name(value): value for value in values}


def _type_name(value: type[Any]) -> str:
    """
    Return the stable module-qualified name stored in typed envelopes.


    :param value:
    :return:
    """

    return f"{value.__module__}.{value.__qualname__}"


def _encode(value: Any) -> Any:
    """
    Convert a supported typed storage value into JSON-compatible data.


    :param value:
    :return:
    """

    if isinstance(value, Enum):
        return {"$enum": _type_name(type(value)), "value": value.value}
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return {
            "$dataclass": _type_name(type(value)),
            "fields": {
                field.name: _encode(getattr(value, field.name))
                for field in dataclasses.fields(value)
            },
        }
    if isinstance(value, UUID):
        return {"$uuid": str(value)}
    if isinstance(value, datetime):
        return {"$datetime": value.isoformat()}
    if isinstance(value, tuple):
        return {"$tuple": [_encode(item) for item in value]}
    if isinstance(value, frozenset):
        return {"$frozenset": [_encode(item) for item in sorted(value, key=str)]}
    if isinstance(value, list):
        return [_encode(item) for item in value]
    if isinstance(value, dict):
        if not all(isinstance(key, str) for key in value):
            return {
                "$mapping": [
                    [_encode(key), _encode(item)] for key, item in value.items()
                ]
            }
        return {key: _encode(item) for key, item in value.items()}
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"unsupported durable storage value: {type(value).__name__}")


def _decode(value: Any, types: Mapping[str, type[Any]]) -> Any:
    """
    Reconstruct typed storage values using the supplied type allowlist.


    :param value:
    :param types:
    :return:
    """

    if isinstance(value, list):
        return [_decode(item, types) for item in value]
    if not isinstance(value, dict):
        return value
    if "$uuid" in value:
        return UUID(str(value["$uuid"]))
    if "$datetime" in value:
        return datetime.fromisoformat(str(value["$datetime"]))
    if "$tuple" in value:
        return tuple(_decode(item, types) for item in value["$tuple"])
    if "$frozenset" in value:
        return frozenset(_decode(item, types) for item in value["$frozenset"])
    if "$mapping" in value:
        return {
            _decode(key, types): _decode(item, types)
            for key, item in value["$mapping"]
        }
    if "$enum" in value:
        type_name = str(value["$enum"])
        if type_name not in types:
            raise ValueError(f"unknown storage enum type {type_name!r}.")
        return types[type_name](value["value"])
    if "$dataclass" in value:
        type_name = str(value["$dataclass"])
        if type_name not in types:
            raise ValueError(f"unknown storage value type {type_name!r}.")
        fields = value.get("fields")
        if not isinstance(fields, dict):
            raise ValueError("dataclass storage envelope has no fields mapping.")
        return types[type_name](
            **{key: _decode(item, types) for key, item in fields.items()}
        )
    return {key: _decode(item, types) for key, item in value.items()}


def _digest_value(digests: Iterable[api.Digest], algorithm: str) -> str | None:
    """
    Return the first digest value for ``algorithm`` when present.


    :param digests:
    :param algorithm:
    :return:
    """

    return next(
        (digest.value for digest in digests if digest.algorithm == algorithm), None
    )


def _row_digests(row: Mapping[str, Any], prefix: str) -> tuple[api.Digest, ...]:
    """
    Collect supported digest columns from one legacy database row.


    :param row:
    :param prefix:
    :return:
    """

    values: list[api.Digest] = []
    for algorithm in ("sha256", "blake3"):
        value = _optional_text(row.get(f"{prefix}{algorithm}"))
        if value is not None:
            values.append(api.Digest(algorithm, value))
    return tuple(values)


def _epoch_ms(value: datetime | None) -> int | None:
    """
    Convert an optional timestamp to integer Unix milliseconds.


    :param value:
    :return:
    """

    return None if value is None else int(value.timestamp() * 1000)


def _datetime_from_epoch(value: Any) -> datetime | None:
    """
    Convert optional Unix milliseconds to an aware UTC timestamp.


    :param value:
    :return:
    """

    parsed = _optional_int(value)
    return None if parsed is None else datetime.fromtimestamp(parsed / 1000, UTC)


def _optional_text(value: Any) -> str | None:
    """
    Normalise a nullable scalar to non-empty text or ``None``.


    :param value:
    :return:
    """

    if value is None:
        return None
    text = str(value)
    return text if text else None


def _optional_int(value: Any) -> int | None:
    """
    Normalise a nullable scalar to an integer or ``None``.


    :param value:
    :return:
    """

    if value in (None, ""):
        return None
    return int(value)


def _optional_id(value: Any, constructor: Callable[[int], Any]) -> Any | None:
    """
    Construct a typed identifier from a nullable database scalar.


    :param value:
    :param constructor:
    :return:
    """

    parsed = _optional_int(value)
    return None if parsed is None else constructor(parsed)


def _json_list(value: Any, default: list[str]) -> list[str]:
    """
    Decode a JSON string list, copying ``default`` for absent/non-list data.


    :param value:
    :param default:
    :return:
    """

    if value in (None, ""):
        return list(default)
    decoded = json.loads(str(value))
    return [str(item) for item in decoded] if isinstance(decoded, list) else list(default)


__all__ = [
    "DatabaseStorageMetadataRepository",
    "RepositoryItemTargetMapping",
    "RepositoryRecordMapping",
]
