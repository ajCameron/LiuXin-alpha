"""
Persist manager metadata through portable database macros and typed envelopes.

Supported domain values are encoded in marked scratch columns, with selected
scalar projections for queries and legacy compatibility. Reads prefer valid typed
envelopes, can reconstruct selected legacy records, and optionally use LiuXin's
shared cache. Mapping facades retain callbacks rather than a second catalogue.

Transaction boundaries are explicit: most single-row methods rely on their
caller, while Composite/link replacement and envelope migration open macro
contexts. Invalidation and migration bookkeeping can fail after database writes.
The journal records workflow evidence without verifying Store bytes, and the
codec has documented marker/type limitations rather than arbitrary losslessness.
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
    Adapt repository callables to a mutable mapping without retaining records.

    Each lookup or collection operation calls its supplied provider afresh. A write invokes upsert,
    and deletion first requires the value to exist. This facade adds no transaction, lock, commit,
    revision check, or rollback: those guarantees belong to the bound callables and their enclosing
    context. Its collection views belong to the mapping returned by load_all, not this facade.

    Example:
        >>> records = {}
        >>> mapping = RepositoryRecordMapping(
        ...     get_one=records.__getitem__, load_all=lambda: dict(records),
        ...     upsert=lambda value: records.update({value[0]: value}),
        ...     remove=records.__delitem__, key_of=lambda value: value[0],
        ... )
        >>> mapping[7] = (7, "book")
        >>> mapping[7]
        (7, 'book')
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
        Retain repository callbacks without calling or validating them. key_of optionally protects
        assignment identity; it is not used for reads or deletions.

        Example:
            >>> mapping = RepositoryRecordMapping(get_one=get_one, load_all=load_all, upsert=save, remove=remove)  # doctest: +SKIP


        :param get_one: Callable loading one key and raising KeyError when absent.
        :param load_all: Callable returning the mapping used for each fresh collection view or size query.
        :param upsert: Callable persisting the supplied value; the assignment key is not passed separately.
        :param remove: Callable removing one key after a successful lookup.
        :param key_of: Optional value-to-key extractor whose result must equal the assignment key.
        :return: None after retaining the callbacks.
        """

        self._get_one = get_one
        self._load_all = load_all
        self._upsert = upsert
        self._remove = remove
        self._key_of = key_of

    def __getitem__(self, key: _K) -> _V:
        """
        Load one record through the bound provider. Propagate KeyError and all other provider
        failures without substituting defaults or caching the result.

        Example:
            >>> value = mapping[key]  # doctest: +SKIP


        :param key: Repository key forwarded without coercion.
        :return: The provider-supplied record value.
        """

        return self._get_one(key)

    def __setitem__(self, key: _K, value: _V) -> None:
        """
        Check key_of(value) against key when configured, then call upsert(value). A mismatch raises
        ValueError before writing. Without key_of, the key is ignored by the write callback;
        persistence and commit timing belong to the provider.

        Example:
            >>> mapping[key] = value  # doctest: +SKIP


        :param key: Repository key forwarded without coercion.
        :param value: Complete value passed to the upsert callback.
        :return: None after the provider write returns; this wrapper does not commit a surrounding transaction.
        """

        if self._key_of is not None and self._key_of(value) != key:
            raise ValueError(
                "repository mapping key does not match record identity."
            )
        self._upsert(value)

    def __delitem__(self, key: _K) -> None:
        """
        Load the record to require its existence, then invoke removal. The read and remove calls are
        separate and not protected by this wrapper; later failures propagate after any provider side
        effects.

        Example:
            >>> del mapping[key]  # doctest: +SKIP


        :param key: Repository key forwarded without coercion.
        :return: None after successful lookup and removal.
        """

        self._get_one(key)
        self._remove(key)

    def __iter__(self) -> Iterator[_K]:
        """
        Call the load_all callback immediately and return an iterator over that mapping's keys.
        Ordering and snapshot stability belong to the returned mapping; later facade calls load
        independently.

        Example:
            >>> keys = tuple(mapping)  # doctest: +SKIP


        :return: An iterator over keys in the freshly obtained provider mapping.
        """

        return iter(self._load_all())

    def __len__(self) -> int:
        """
        Call the load_all callback and return its size. This can load every value and does not use a
        cached count or a dedicated database COUNT query.

        Example:
            >>> count = len(mapping)  # doctest: +SKIP


        :return: The number of entries in the newly obtained provider mapping.
        """

        return len(self._load_all())

    def __contains__(self, key: object) -> bool:
        """
        Attempt get_one(key), returning False only for KeyError. No key coercion or type validation
        is added; provider errors of other types propagate, and an existing value of None still
        counts as present.

        Example:
            >>> present = key in mapping  # doctest: +SKIP


        :param key: Candidate lookup key, forwarded even when its runtime type differs from the annotation.
        :return: True when lookup returns successfully, otherwise False for KeyError.
        """

        try:
            self._get_one(key)  # type: ignore[arg-type]
        except KeyError:
            return False
        return True

    def get(self, key: _K, default: Any = None) -> _V | Any:
        """
        Load the requested key and substitute default only for KeyError. Other decoding, database,
        or provider failures remain visible, and no result is cached by the facade.

        Example:
            >>> value = mapping.get(key, None)  # doctest: +SKIP


        :param key: Repository key forwarded without coercion.
        :param default: Value returned unchanged when the lookup raises KeyError; defaults to None.
        :return: The loaded value, or the exact supplied default for a missing key.
        """

        try:
            return self._get_one(key)
        except KeyError:
            return default

    def values(self):
        """
        Call the load_all callback and return its values view. The view belongs to that returned
        mapping; this method does not retain it or promise live updates from subsequent repository
        writes.

        Example:
            >>> snapshot = tuple(mapping.values())  # doctest: +SKIP


        :return: The provider mapping view containing its values.
        """

        return self._load_all().values()

    def items(self):
        """
        Call the load_all callback and return its items view. The view belongs to that returned
        mapping; this method does not retain it or promise live updates from subsequent repository
        writes.

        Example:
            >>> snapshot = tuple(mapping.items())  # doctest: +SKIP


        :return: The provider mapping view containing its key/value pairs.
        """

        return self._load_all().items()

    def pop(self, key: _K, default: Any = dataclasses.MISSING) -> _V | Any:
        """
        Load and remove one value, substituting a default only when lookup raises KeyError.

        The dataclasses.MISSING sentinel means no default, even if passed explicitly; that path
        raises a new KeyError(key). A successful lookup is followed by a separate remove call.
        Removal errors propagate rather than returning default, and a provider whose remove callback
        is a no-op retains the entry.

        Example:
            >>> previous = mapping.pop(key, None)  # doctest: +SKIP


        :param key: Repository key forwarded without coercion.
        :param default: Missing-key result, or dataclasses.MISSING to require the key.
        :return: The value loaded before removal, or the supplied default when lookup reports absence.
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
    Present role-keyed Item links as a mapping over the database repository.

    Keys are (ItemID, role) pairs and values identify an Asset or Composite. The facade retains only
    its repository; collection access reloads both link tables. It adds no transaction or
    normalization, and failed writes/deletions retain the repository's partial-failure and
    backend-constraint behavior.

    Example:
        >>> target = repository.item_targets().get((api.ItemID(7), "cover"))  # doctest: +SKIP
    """

    def __init__(
        self,
        *,
        repository: DatabaseStorageMetadataRepository,
    ) -> None:
        """
        Retain the metadata repository without loading links or opening a transaction.

        Example:
            >>> mapping = RepositoryItemTargetMapping(repository=repository)  # doctest: +SKIP


        :param repository: Authoritative metadata adapter supplying Item-link reads and writes.
        :return: None after retaining the repository.
        """

        self._repository = repository

    def __getitem__(
        self, key: tuple[api.ItemID, str]
    ) -> tuple[str, api.DigitalAssetID | api.CompositeDigitalAssetID]:
        """
        Load one Item target through the bound provider. Propagate KeyError and all other provider
        failures without substituting defaults or caching the result.

        Example:
            >>> value = mapping[key]  # doctest: +SKIP


        :param key: Pair of Item identity and exact role text.
        :return: The provider-supplied Item target value.
        """

        return self._repository.get_item_target(key)

    def __setitem__(
        self,
        key: tuple[api.ItemID, str],
        value: tuple[str, api.DigitalAssetID | api.CompositeDigitalAssetID],
    ) -> None:
        """
        Pass the key/value pair to upsert_item_target. The repository replaces matching role links
        in its transaction; this wrapper does not validate the kind, normalize the role, or resolve
        referenced objects.

        Example:
            >>> mapping[key] = value  # doctest: +SKIP


        :param key: Pair of Item identity and exact role text.
        :param value: Pair of target kind and Asset or Composite identity.
        :return: None after the provider write returns; this wrapper does not commit a surrounding transaction.
        """

        self._repository.upsert_item_target((key, value))

    def __delitem__(self, key: tuple[api.ItemID, str]) -> None:
        """
        Load the Item target to require its existence, then invoke removal. The read and remove
        calls are separate and not protected by this wrapper; later failures propagate after any
        provider side effects.

        Example:
            >>> del mapping[key]  # doctest: +SKIP


        :param key: Pair of Item identity and exact role text.
        :return: None after successful lookup and removal.
        """

        self._repository.get_item_target(key)
        self._repository.remove_item_target(key)

    def __iter__(self):
        """
        Call the repository link loader immediately and return an iterator over that mapping's keys.
        Ordering and snapshot stability belong to the returned mapping; later facade calls load
        independently.

        Example:
            >>> keys = tuple(mapping)  # doctest: +SKIP


        :return: An iterator over keys in the freshly obtained provider mapping.
        """

        return iter(self._repository.load_item_targets())

    def __len__(self) -> int:
        """
        Call the repository link loader and return its size. This can load every value and does not
        use a cached count or a dedicated database COUNT query.

        Example:
            >>> count = len(mapping)  # doctest: +SKIP


        :return: The number of entries in the newly obtained provider mapping.
        """

        return len(self._repository.load_item_targets())

    def get(self, key, default=None):
        """
        Load the requested key and substitute default only for KeyError. Other decoding, database,
        or provider failures remain visible, and no result is cached by the facade.

        Example:
            >>> value = mapping.get(key, None)  # doctest: +SKIP


        :param key: Pair of Item identity and exact role text.
        :param default: Value returned unchanged when the lookup raises KeyError; defaults to None.
        :return: The loaded value, or the exact supplied default for a missing key.
        """

        try:
            return self._repository.get_item_target(key)
        except KeyError:
            return default

    def values(self):
        """
        Call the repository link loader and return its values view. The view belongs to that
        returned mapping; this method does not retain it or promise live updates from subsequent
        repository writes.

        Example:
            >>> snapshot = tuple(mapping.values())  # doctest: +SKIP


        :return: The provider mapping view containing its values.
        """

        return self._repository.load_item_targets().values()

    def items(self):
        """
        Call the repository link loader and return its items view. The view belongs to that returned
        mapping; this method does not retain it or promise live updates from subsequent repository
        writes.

        Example:
            >>> snapshot = tuple(mapping.items())  # doctest: +SKIP


        :return: The provider mapping view containing its key/value pairs.
        """

        return self._repository.load_item_targets().items()

    def pop(self, key, default=dataclasses.MISSING):
        """
        Load and remove one value, substituting a default only when lookup raises KeyError.

        The dataclasses.MISSING sentinel means no default, even if passed explicitly; that path
        raises a new KeyError(key). A successful lookup is followed by a separate remove call.
        Removal errors propagate rather than returning default, and a provider whose remove callback
        is a no-op retains the entry.

        Example:
            >>> previous = mapping.pop(key, None)  # doctest: +SKIP


        :param key: Pair of Item identity and exact role text.
        :param default: Missing-key result, or dataclasses.MISSING to require the key.
        :return: The value loaded before removal, or the supplied default when lookup reports absence.
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
    Translate manager records through portable catalogue macros and optional shared caches.

    Row scalars support legacy queries; marked scratch envelopes preserve supported typed values and
    take precedence during loading. The codec has explicit type and container limits, so arbitrary
    Python values are not guaranteed to round-trip. Recognized malformed envelopes fail visibly
    instead of silently using scalars.

    The adapter retains no second record catalogue. Cached reads use the attached shared facade,
    while writes call database macros and then invalidate selected records or links. Most methods
    rely on the caller's transaction; only specified multi-row operations open their own context.
    Invalidation can fail after database mutation, and this class supplies no lock, universal
    rollback, or physical Store transaction. Manager reference, loss-policy, and byte-integrity
    checks are separate.

    Example:
        >>> repository = DatabaseStorageMetadataRepository(database)  # doctest: +SKIP
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
        Require supports(db), retain the database/macros, build the decoder registry, and optionally
        attach a shared cache. Unsupported schema/callable shape raises TypeError. Table inspection
        and cache initialization can have side effects or fail after attributes are assigned; no
        cleanup is added.

        Example:
            >>> repository = DatabaseStorageMetadataRepository(database, additional_types=private_types)  # doctest: +SKIP


        :param db: Database exposing the required storage tables and portable macros.
        :param additional_types: Extra constructor types used when decoding private or extension values.
        :param cache: Optional shared cache bound to this exact database object; None leaves reads on macros.
        :return: None after repository setup and any requested cache preparation.
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
        Check required table names and callable transaction/get/insert/update/delete macro
        attributes. Exceptions while obtaining tables or macros return False; failures during later
        callable-attribute inspection can propagate. Column definitions, transaction semantics, and
        actual operations are not exercised.

        Example:
            >>> DatabaseStorageMetadataRepository.supports(object())
            False


        :param db: Candidate database-like object to inspect without issuing writes.
        :return: Whether the required table/callable shape is present.
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
        Look for any Asset, Replica, or Composite sentinel table. Inspection exceptions return
        False. This deliberately distinguishes a partly provisioned catalogue from a tiny Store-only
        bootstrap adapter; it does not establish complete persistence support.

        Example:
            >>> DatabaseStorageMetadataRepository.resembles_storage_catalogue(object())
            False


        :param db: Candidate whose get_tables result supplies the heuristic table names.
        :return: True if at least one storage-catalogue sentinel table is present.
        """

        try:
            tables = set(db.get_tables())
        except Exception:
            return False
        return bool(cls._CATALOGUE_SENTINELS & tables)

    @classmethod
    def missing_tables(cls, db: Any) -> tuple[str, ...]:
        """
        Return sorted required table names absent from get_tables. If table inspection raises an
        ordinary Exception, report every required table as missing; no column or macro capability
        checks are made.

        Example:
            >>> bool(DatabaseStorageMetadataRepository.missing_tables(object()))
            True


        :param db: Candidate database whose table names are inspected.
        :return: A sorted tuple of missing required names, possibly empty.
        """

        try:
            tables = set(db.get_tables())
        except Exception:
            return tuple(sorted(cls._REQUIRED_TABLES))
        return tuple(sorted(cls._REQUIRED_TABLES - tables))

    def transaction(self):
        """
        Return macros.transaction() without entering it. Transaction nesting, commit, rollback, and
        resource ownership belong to the database provider; this wrapper neither begins a with block
        nor changes cache state.

        Example:
            >>> with repository.transaction():  # doctest: +SKIP
            ...     repository.upsert_asset(record)


        :return: The provider transaction context for the caller to enter.
        """

        return self.macros.transaction()

    def migrate_envelopes(self) -> int:
        """
        Rewrite recognized version-zero scratch envelopes in one macro transaction.

        Scan every configured envelope table, validate older payloads with the decoder, and update
        only changed scratch columns. Unsupported versions or write failures leave transaction
        handling to the provider. After successful context exit, record the migration separately,
        then invalidate covered tables if any rows changed. Migration-recording or invalidation
        errors can occur after the rewrites, and a recording failure prevents the subsequent
        invalidation call.

        Example:
            >>> result = repository.migrate_envelopes()  # doctest: +SKIP


        :return: The number of scratch columns rewritten after migration recording and requested invalidation succeed.
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
        Insert a reservation row and return its database-generated integer identity.

        Accept only digital_asset, replica, composite, derivation, replication_policy, or
        backup_policy; unknown kinds raise ValueError before insertion. The marker contains no
        record payload. Callers must replace it with the completed record within their transaction.
        This method neither opens that transaction nor guarantees rollback/reuse of allocated IDs on
        every database backend.

        Example:
            >>> identifier = repository.allocate_record_id("digital_asset")  # doctest: +SKIP


        :param kind: Exact metadata family key selecting the table and identity/scratch columns.
        :return: The int-converted database-generated reservation ID.
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
        Attach a usable shared cache for this exact database, or detach it with None.

        EMPTY caches load and DIRTY caches reload; CLOSED caches reject. Require Asset and Replica
        tables among the cached record tables, then retain the cache and its available table names.
        Other states receive no readiness validation here. Candidate load/reload can occur before a
        later rejection; an existing binding is replaced only after validation. Detachment clears
        routing metadata without closing the old cache, and provider errors propagate without a
        macro fallback.

        Example:
            >>> repository.set_cache(None)  # doctest: +SKIP


        :param cache: Shared cache whose database is self.db by identity, or None to disable cached reads.
        :return: None after successful attachment or detachment.
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
        Return a new mapping facade over Asset reads/upserts/removals. Assignment checks
        value.digital_asset_id against the key, but adds no revision, reference, or transaction
        checks. The facade owns no record snapshot; each operation consults the repository.

        Example:
            >>> result = repository.asset_records()  # doctest: +SKIP


        :return: A new RepositoryRecordMapping bound to this adapter's Asset operations.
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
        Return a new mapping facade over Replica reads/upserts/removals. Assignment checks
        value.replica_id against the key, but adds no revision, reference, or transaction checks.
        The facade owns no record snapshot; each operation consults the repository.

        Example:
            >>> result = repository.replica_records()  # doctest: +SKIP


        :return: A new RepositoryRecordMapping bound to this adapter's Replica operations.
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
        Return a new mapping facade over Composite reads/upserts/removals. Assignment checks
        value.composite_digital_asset_id against the key, but adds no revision, reference, or
        transaction checks. The facade owns no record snapshot; each operation consults the
        repository.

        Example:
            >>> result = repository.composite_records()  # doctest: +SKIP


        :return: A new RepositoryRecordMapping bound to this adapter's Composite operations.
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
        Return a new mapping facade over derivation reads/upserts/removals. Assignment checks
        value.digital_asset_derivation_id against the key, but adds no revision, reference, or
        transaction checks. The facade owns no record snapshot; each operation consults the
        repository.

        Example:
            >>> result = repository.derivation_records()  # doctest: +SKIP


        :return: A new RepositoryRecordMapping bound to this adapter's derivation operations.
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
        Return a new mapping facade over replication-policy reads/upserts/removals. Assignment
        checks value.replication_policy_id against the key, but adds no revision, reference, or
        transaction checks. The facade owns no record snapshot; each operation consults the
        repository.

        Example:
            >>> result = repository.replication_policy_records()  # doctest: +SKIP


        :return: A new RepositoryRecordMapping bound to this adapter's replication-policy operations.
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
        Return a new mapping facade over backup-policy reads/upserts/removals. Assignment checks
        value.backup_policy_id against the key, but adds no revision, reference, or transaction
        checks. The facade owns no record snapshot; each operation consults the repository.

        Example:
            >>> result = repository.backup_policy_records()  # doctest: +SKIP


        :return: A new RepositoryRecordMapping bound to this adapter's backup-policy operations.
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
        Return a fresh role-keyed Item-target facade bound to this repository. Construction loads no
        links; later reads and writes use the current repository and cache configuration.

        Example:
            >>> result = repository.item_targets()  # doctest: +SKIP


        :return: A new RepositoryItemTargetMapping without a local target dictionary.
        """

        return RepositoryItemTargetMapping(repository=self)

    def ingest_operations(self) -> RepositoryRecordMapping[UUID, Any]:
        """
        Expose committed ingest results through a compatibility mapping with no-op removal.

        Assignment ignores the mapping key and persists using operation.result.operation_id; no
        key_of check is installed. Reads require committed journal entries. Successful del/pop calls
        do not delete the journal because the removal callback does nothing; inherited bulk-clear
        behavior therefore must not be used to purge this facade.

        Example:
            >>> result = repository.ingest_operations()  # doctest: +SKIP


        :return: A new mapping for committed-operation reads and upserts, without journal deletion support.
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
        Load one Asset through the current cache/macro route and family decoder. The key is
        int-converted for row lookup but retained for the decoded mapping lookup. Missing rows,
        skipped legacy rows, or envelopes with a different embedded identity raise KeyError;
        malformed recognized envelopes propagate decoding errors.

        Example:
            >>> record = repository.get_asset(identifier)  # doctest: +SKIP


        :param digital_asset_id: Identity of the requested Asset record.
        :return: The decoded Asset record, with envelope values preferred over scalar columns.
        """

        return self._get_record(
            "digital_assets",
            "digital_asset_id",
            digital_asset_id,
            self._load_assets,
        )

    def get_replica(self, replica_id: api.ReplicaID) -> api.ReplicaRecord:
        """
        Load one Replica through the current cache/macro route and family decoder. The key is
        int-converted for row lookup but retained for the decoded mapping lookup. Missing rows,
        skipped legacy rows, or envelopes with a different embedded identity raise KeyError;
        malformed recognized envelopes propagate decoding errors.

        Example:
            >>> record = repository.get_replica(identifier)  # doctest: +SKIP


        :param replica_id: Identity of the requested Replica record.
        :return: The decoded Replica record, with envelope values preferred over scalar columns.
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
        Load one Composite through the current cache/macro route and family decoder. The key is
        int-converted for row lookup but retained for the decoded mapping lookup. Missing rows,
        skipped legacy rows, or envelopes with a different embedded identity raise KeyError;
        malformed recognized envelopes propagate decoding errors.

        Example:
            >>> record = repository.get_composite(identifier)  # doctest: +SKIP


        :param composite_id: Identity of the requested Composite record.
        :return: The decoded Composite record, with envelope values preferred over scalar columns.
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
        Load one derivation through the current cache/macro route and family decoder. The key is
        int-converted for row lookup but retained for the decoded mapping lookup. Missing rows,
        skipped legacy rows, or envelopes with a different embedded identity raise KeyError;
        malformed recognized envelopes propagate decoding errors.

        Example:
            >>> record = repository.get_derivation(identifier)  # doctest: +SKIP


        :param derivation_id: Identity of the requested derivation record.
        :return: The decoded derivation record, with envelope values preferred over scalar columns.
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
        Load one replication-policy through the current cache/macro route and family decoder. The
        key is int-converted for row lookup but retained for the decoded mapping lookup. Missing
        rows, skipped legacy rows, or envelopes with a different embedded identity raise KeyError;
        malformed recognized envelopes propagate decoding errors.

        Example:
            >>> record = repository.get_replication_policy(identifier)  # doctest: +SKIP


        :param policy_id: Identity of the requested replication-policy record.
        :return: The decoded replication-policy record, with envelope values preferred over scalar columns.
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
        Load one backup-policy through the current cache/macro route and family decoder. The key is
        int-converted for row lookup but retained for the decoded mapping lookup. Missing rows,
        skipped legacy rows, or envelopes with a different embedded identity raise KeyError;
        malformed recognized envelopes propagate decoding errors.

        Example:
            >>> record = repository.get_backup_policy(identifier)  # doctest: +SKIP


        :param policy_id: Identity of the requested backup-policy record.
        :return: The decoded backup-policy record, with envelope values preferred over scalar columns.
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
        Read both Item-link tables into a fresh map. Scratch role text takes precedence over scalar
        role and the primary_payload default. Later row IDs win within each table, and Composite
        targets overwrite atomic targets sharing the same Item/role key.

        Example:
            >>> result = repository.load_item_targets()  # doctest: +SKIP


        :return: A fresh dictionary from (ItemID, role) to (target kind, target ID).
        """

        return self._load_item_targets()

    def get_item_target(
        self, key: tuple[api.ItemID, str]
    ) -> tuple[str, api.DigitalAssetID | api.CompositeDigitalAssetID]:
        """
        Reload every Item target, then look up the exact pair. Missing keys raise a new KeyError
        without a chained missing-key cause; malformed rows and database errors propagate rather
        than producing an absent target.

        Example:
            >>> target = repository.get_item_target((api.ItemID(7), "cover"))  # doctest: +SKIP


        :param key: Exact Item identity and role pair used in the merged target mapping.
        :return: The target-kind/identity pair stored for that role.
        """

        try:
            return self._load_item_targets()[key]
        except KeyError:
            raise KeyError(key) from None

    def get_committed_ingest_operation(self, operation_id: UUID) -> Any:
        """
        Require one unique journal row in committed state and decode its operation entry.
        Missing/noncommitted rows or a non-dictionary payload or None operation raise KeyError; the
        returned operation is not type-checked or compared with the row UUID. This method does not
        bypass lookup when has_ingest_journal is false.

        Example:
            >>> operation = repository.get_committed_ingest_operation(operation_id)  # doctest: +SKIP


        :param operation_id: UUID used for exact durable journal lookup.
        :return: The non-None decoded operation retained in the committed payload.
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
        Int-convert key for one raw row lookup, decode that singleton through loader, then index by
        the original key. A missing row or absent decoded key raises KeyError; coercion, row access,
        and loader failures otherwise propagate.

        Example:
            >>> record = repository._get_record("digital_assets", "digital_asset_id", 7, repository._load_assets)  # doctest: +SKIP


        :param table: Metadata table routed through cache or macros.
        :param id_column: Explicit row identity column for macro lookup.
        :param key: Original repository key, int-converted only for row selection.
        :param loader: Family decoder accepting the singleton row iterable and returning an identity-keyed mapping.
        :return: The decoded value stored under the original key.
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
        Read one row from a covered shared cache, otherwise call macros.get_row with the explicit
        identity column. Cache misses return None without consulting macros, and cache errors remain
        visible. Cached record values are returned without copying.

        Example:
            >>> row = repository._record_row("digital_assets", "digital_asset_id", 7)  # doctest: +SKIP


        :param table: Metadata table whose cached coverage selects the read path.
        :param id_column: Row identity column passed only on the macro path.
        :param row_id: Integer row identity requested from the provider.
        :return: The raw row mapping, or None for a missing provider value.
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
        Materialize ordered row references from macros or a covered cache query. The cache path
        creates one ascending CacheSort per requested column and returns record.values. Neither path
        independently deep-copies rows, retries failure, or ensures a cross-table snapshot.

        Example:
            >>> rows = repository._record_rows("digital_assets", order_by=("digital_asset_id",))  # doctest: +SKIP


        :param table: Metadata table to enumerate.
        :param order_by: Ordered column names passed to macros or converted to ascending cache sorts.
        :return: A tuple of provider row mappings in the requested provider order.
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
        Filter requested names to tables exposed by the bound cache and invalidate that tuple. No
        cache or no covered names is a no-op. Input ordering and duplicates are retained;
        invalidation errors propagate after any prior database mutation.

        Example:
            >>> repository._invalidate_records("digital_assets")  # doctest: +SKIP


        :param tables: Variadic table names whose cached data may have changed.
        :return: None after the requested invalidation or no-op.
        """

        if self.cache is not None:
            cached = tuple(table for table in tables if table in self._cache_tables)
            if cached:
                self.cache.invalidate(tables=cached)

    def _invalidate_record_ids(self, table: str, *row_ids: int) -> None:
        """
        Invalidate int-converted IDs only when the cache covers the table and IDs were supplied.
        Uncovered/no-cache calls do not even convert IDs. No reload or transaction is added, and
        invalidation failures propagate.

        Example:
            >>> repository._invalidate_record_ids("digital_assets", 7)  # doctest: +SKIP


        :param table: Table whose available cache coverage permits targeted invalidation.
        :param row_ids: Variadic row identities, int-converted without deduplication on the active path.
        :return: None after targeted invalidation or a no-op.
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
        Find Store rows by UUID and return the first row ID without updating or rejecting
        duplicates. If absent, project configuration onto supported columns, insert it, and
        invalidate its ID. Lookup/insertion is not made atomic here; provider constraints arbitrate
        competing inserts.

        Example:
            >>> store_id = repository.ensure_store(configuration)  # doctest: +SKIP


        :param configuration: Store configuration used for UUID lookup and, only if absent, row insertion.
        :return: The int-converted existing or inserted Store row identity.
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
        Require exactly one row for the supplied UUID, project supported configuration columns
        including null values, then update and invalidate it. Missing rows raise
        StoreConfigurationNotFound and duplicates raise StorageManagementError. No facade lifecycle
        or manager reference validation occurs here.

        Example:
            >>> repository.update_store(configuration)  # doctest: +SKIP


        :param configuration: Complete replacement configuration whose UUID selects the existing row.
        :return: None after the row update and cache invalidation.
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
        Require exactly one durable row for the Store UUID, delete it, then invalidate its ID.
        Missing/duplicate rows raise typed manager errors. This method does not establish that the
        Store is unclaimed, close its facade, or delete physical contents; manager checks and
        backend constraints govern removal.

        Example:
            >>> repository.remove_store(store_uuid)  # doctest: +SKIP


        :param store_ref: UUID of the durable configuration to remove.
        :return: None after successful row deletion and invalidation.
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
        Persist searchable Asset scalars and the supplied record envelope.

        Project name/media/original-name text, size, SHA-256/BLAKE3 evidence, and policy IDs
        alongside the complete typed envelope. Selected scalar text escapes surrogates; the envelope
        retains supported original values. No bytes are read, and additional digest algorithms live
        only in the envelope.

        Use schema-filtered insert/update, then invalidate the record ID. No transaction is opened
        here, and an invalidation error can follow a successful database write.

        Example:
            >>> repository.upsert_asset(record)  # doctest: +SKIP


        :param record: Complete Asset record whose retained identity selects insertion or update.
        :return: None after persistence and cache invalidation return successfully.
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
        Delete the Asset row by int-converted identity, then invalidate its cached record. No
        existence, revision, reference, or loss-policy check is added, and backend
        missing-row/constraint behavior propagates. This does not delete Store bytes or open a
        transaction.

        Example:
            >>> repository.remove_asset(identifier)  # doctest: +SKIP


        :param digital_asset_id: Identity of the Asset row passed to portable deletion.
        :return: None after database deletion and requested invalidation.
        """

        self.macros.delete_row("digital_assets", int(digital_asset_id))
        self._invalidate_record_ids("digital_assets", int(digital_asset_id))

    def upsert_replica(self, record: api.ReplicaRecord) -> None:
        """
        Persist searchable Replica scalars and the supplied record envelope.

        Resolve the Location's Store UUID to a row ID before writing claim/mode/state, timestamps,
        size, SHA-256/BLAKE3 evidence, and the complete envelope. Selected scalar text escapes
        surrogates. No Store availability, byte integrity, or unique live-claim check is performed
        here.

        Use schema-filtered insert/update, then invalidate the record ID. No transaction is opened
        here, and an invalidation error can follow a successful database write.

        Example:
            >>> repository.upsert_replica(record)  # doctest: +SKIP


        :param record: Complete Replica record whose retained identity selects insertion or update.
        :return: None after persistence and cache invalidation return successfully.
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
        Delete the Replica row by int-converted identity, then invalidate its cached record. No
        existence, revision, reference, or loss-policy check is added, and backend
        missing-row/constraint behavior propagates. This does not delete Store bytes or open a
        transaction.

        Example:
            >>> repository.remove_replica(identifier)  # doctest: +SKIP


        :param replica_id: Identity of the Replica row passed to portable deletion.
        :return: None after database deletion and requested invalidation.
        """

        self.macros.delete_row("asset_replicas", int(replica_id))
        self._invalidate_record_ids("asset_replicas", int(replica_id))

    def upsert_composite(self, record: api.CompositeDigitalAssetRecord) -> None:
        """
        Replace the Composite row and all member links inside one macro transaction.

        Preserve the full Composite/member values in envelopes. Scalar roles retain
        member/chapter/track/disc_member/part and use member for other roles; member sequence and
        required flags are projected separately. Existing links are deleted before supplied members
        are inserted. Provider transaction behavior governs rollback. After exit, invalidate the
        Composite ID and both relationship directions; those later failures can leave the database
        already changed.

        Example:
            >>> repository.upsert_composite(record)  # doctest: +SKIP


        :param record: Complete Composite identity, membership, name, attributes, and revision to persist.
        :return: None after row/link replacement and subsequent cache invalidation.
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
        Delete the Composite row by int-converted identity, then invalidate its cached record. No
        existence, revision, reference, or loss-policy check is added, and backend
        missing-row/constraint behavior propagates. This does not delete Store bytes or open a
        transaction. Composite membership relationships are invalidated in both directions after
        record invalidation.

        Example:
            >>> repository.remove_composite(identifier)  # doctest: +SKIP


        :param composite_digital_asset_id: Identity of the Composite row passed to portable deletion.
        :return: None after database deletion and requested invalidation.
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
        Persist searchable derivation scalars and the supplied record envelope.

        Project the first direct atomic source, result, workflow ID, kind, and notes alongside the
        complete declaration envelope. A supplied creation time is projected, but None omits that
        column and can leave an older scalar value on update. Other sources and recipe evidence
        depend on the envelope; no graph or replay checks run.

        Use schema-filtered insert/update, then invalidate the record ID. No transaction is opened
        here, and an invalidation error can follow a successful database write.

        Example:
            >>> repository.upsert_derivation(record)  # doctest: +SKIP


        :param record: Complete derivation record whose retained identity selects insertion or update.
        :return: None after persistence and cache invalidation return successfully.
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
        Delete the derivation row by int-converted identity, then invalidate its cached record. No
        existence, revision, reference, or loss-policy check is added, and backend
        missing-row/constraint behavior propagates. This does not delete Store bytes or open a
        transaction.

        Example:
            >>> repository.remove_derivation(identifier)  # doctest: +SKIP


        :param digital_asset_derivation_id: Identity of the derivation row passed to portable deletion.
        :return: None after database deletion and requested invalidation.
        """

        self.macros.delete_row(
            "digital_asset_derivations", int(digital_asset_derivation_id)
        )
        self._invalidate_record_ids(
            "digital_asset_derivations", int(digital_asset_derivation_id)
        )

    def upsert_replication_policy(self, record: api.ReplicationPolicyRecord) -> None:
        """
        Persist searchable replication-policy scalars and the supplied record envelope.

        Project casefolded name, copy targets, separation dimensions, sorted tag lists, mode, and
        healing/write-copy settings alongside the complete record envelope. These writes do not
        validate policy assignments, place bytes, or reserve capacity.

        Use schema-filtered insert/update, then invalidate the record ID. No transaction is opened
        here, and an invalidation error can follow a successful database write.

        Example:
            >>> repository.upsert_replication_policy(record)  # doctest: +SKIP


        :param record: Complete replication-policy record whose retained identity selects insertion or update.
        :return: None after persistence and cache invalidation return successfully.
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
        Delete the replication policy row by int-converted identity, then invalidate its cached
        record. No existence, revision, reference, or loss-policy check is added, and backend
        missing-row/constraint behavior propagates. This does not delete Store bytes or open a
        transaction.

        Example:
            >>> repository.remove_replication_policy(identifier)  # doctest: +SKIP


        :param replication_policy_id: Identity of the replication policy row passed to portable deletion.
        :return: None after database deletion and requested invalidation.
        """

        self.macros.delete_row("replication_policies", int(replication_policy_id))
        self._invalidate_record_ids(
            "replication_policies", int(replication_policy_id)
        )

    def upsert_backup_policy(self, record: api.BackupPolicyRecord) -> None:
        """
        Persist searchable backup-policy scalars and the supplied record envelope.

        Project casefolded name, backup-copy targets, dimensions, sorted tags, mode, verification,
        and retention settings alongside the complete envelope. No referenced-Asset checks, backup
        execution, or physical retention enforcement is added.

        Use schema-filtered insert/update, then invalidate the record ID. No transaction is opened
        here, and an invalidation error can follow a successful database write.

        Example:
            >>> repository.upsert_backup_policy(record)  # doctest: +SKIP


        :param record: Complete backup-policy record whose retained identity selects insertion or update.
        :return: None after persistence and cache invalidation return successfully.
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
        Delete the backup policy row by int-converted identity, then invalidate its cached record.
        No existence, revision, reference, or loss-policy check is added, and backend
        missing-row/constraint behavior propagates. This does not delete Store bytes or open a
        transaction.

        Example:
            >>> repository.remove_backup_policy(identifier)  # doctest: +SKIP


        :param backup_policy_id: Identity of the backup policy row passed to portable deletion.
        :return: None after database deletion and requested invalidation.
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
        Replace matching Item-role links across both target tables in one macro transaction.

        Exact kind digital_asset selects the atomic table; every other kind selects the Composite
        table without validation. Preserve role text in the envelope, project recognized roles
        directly, and use primary_payload for unsupported scalar roles. The primary flag reflects
        the original role, not that fallback. After transaction exit, invalidate only the newly
        selected relationship pair; no independent invalidation of a replaced other-kind pair is
        issued here.

        Example:
            >>> repository.upsert_item_target(((api.ItemID(7), "cover"), ("digital_asset", api.DigitalAssetID(2))))  # doctest: +SKIP


        :param value: Nested pair ((ItemID, exact role), (target kind, target ID)) to persist.
        :return: None after link replacement and requested relationship invalidation.
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
        Delete every atomic/Composite link matching the exact Item-role key, then invalidate all
        four Item/target relationship directions. No surrounding transaction or prior existence
        check is added; multi-row deletion and later invalidation can fail after earlier mutations.

        Example:
            >>> repository.remove_item_target((api.ItemID(7), "cover"))  # doctest: +SKIP


        :param key: Item identity and exact role text whose links should be removed.
        :return: None after matching deletions and relationship invalidation.
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
        Insert a started request or reset an existing matching request to started.

        No journal support is a no-op. Otherwise require a unique operation UUID and compare the
        decoded existing request by equality; a non-dictionary or unequal request raises
        StoragePreconditionFailed. A match resets state and clears the last error even if the row
        was previously committed or failed, while retaining its other columns and payload. This is
        not a state-preserving no-op or an atomic compare-and-swap; callers own retry ordering and
        transaction boundaries.

        Example:
            >>> repository.journal_start(operation_id, request)  # doctest: +SKIP


        :param operation_id: UUID identifying the request across retries.
        :param request: Typed request value to encode or compare with the existing decoded request.
        :return: None after insertion/reset, or immediately when journal support is disabled.
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
        Merge recovery values into a started journal payload and set state to publishing. The helper
        requires an existing dictionary payload, updates selected scalar identity/location columns,
        and imposes no state-transition or byte-publication check. Disabled journal support is a
        no-op.

        Example:
            >>> repository.journal_publication_pending(operation_id, asset_record=asset, asset_created=True, location=location, replica_mode=api.ReplicaMode.ACTIVE, placement_hints=None)  # doctest: +SKIP


        :param operation_id: UUID of the existing journal entry.
        :param asset_record: Asset evidence to retain for recovery and scalar Asset-ID projection.
        :param asset_created: Whether the workflow created this Asset, retained for later recovery/cleanup decisions.
        :param location: Planned destination Location retained in the payload and projected into journal columns.
        :param replica_mode: Replica role to retain for later metadata registration.
        :param placement_hints: Optional placement hints retained unchanged for recovery.
        :return: None after the journal update, or a no-op when journaling is disabled.
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
        Set an existing dictionary journal payload to published without altering its recovery
        values. The helper may re-project existing location/Asset scalars; it does not inspect
        bytes, require a particular prior state, or clear a previous error. Disabled journal support
        is a no-op.

        Example:
            >>> repository.journal_published(operation_id)  # doctest: +SKIP


        :param operation_id: UUID whose existing recovery entry should be marked published.
        :return: None after update or disabled-journal no-op.
        """

        if self.has_ingest_journal:
            self._update_journal_payload(operation_id, state="published", values={})

    def journal_failed(self, operation_id: UUID, error: BaseException) -> None:
        """
        Mark an existing journal entry failed and project a bounded error description.

        Disabled support or an absent entry is a no-op. Use str(error), falling back to its type
        name when empty, then truncate to 2,000 characters before escaping unencodable scalar text.
        Escapes can expand the stored length. This is neither secret redaction nor removal of
        ordinary control characters, and stringification or write failures can replace the
        workflow's original error. Payload values and other scalar columns remain retained
        regardless of prior state.

        Example:
            >>> repository.journal_failed(operation_id, error)  # doctest: +SKIP


        :param operation_id: UUID of the journal row to mark failed when present.
        :param error: Failure whose string or class name supplies the stored diagnostic.
        :return: None after the update or a no-op for disabled/missing journal state.
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
        Upsert a committed journal result using the operation's own result UUID.

        Project destination/Asset/Replica identities, clear last_error, and replace scratch with
        only request and completed operation. Prior recovery-only fields disappear from that
        payload. Existing request equality, prior state, and byte presence are not checked here.
        Disabled journaling returns before accessing the operation. Persistence follows the caller's
        transaction, not this method's name.

        Example:
            >>> repository.commit_ingest_operation(operation)  # doctest: +SKIP


        :param operation: Completed request/result object exposing the required result identities and Replica Location.
        :return: None after insertion/update, or a no-op when journal support is disabled.
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
        Load journal rows in ID order, excluding exactly committed and failed state strings. All
        other states are included without transition validation. Require dictionary payloads and
        parse row UUIDs; malformed entries fail the whole call rather than returning an earlier
        partial tuple. Disabled journaling returns an empty tuple.

        Example:
            >>> result = repository.pending_ingests()  # doctest: +SKIP


        :return: An ordered tuple of (operation UUID, state text, decoded recovery dictionary).
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
        Read one unique journal entry regardless of state and require a decoded dictionary payload.
        Disabled support or absence yields None; empty/None last_error becomes None and other errors
        are stringified. No byte inspection, request validation, or secret redaction occurs.

        Example:
            >>> entry = repository.ingest_journal_entry(operation_id)  # doctest: +SKIP


        :param operation_id: UUID selecting the unique journal row.
        :return: A (state, payload, optional error text) tuple, or None for disabled/absent state.
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
        Return ID-ordered journal summaries without decoding scratch requests. UUID is parsed and
        state stringified, while last_error, store_ref, and storage_key retain raw row values.
        Excluding the payload does not redact sensitive text already present in those scalar
        columns. Disabled support returns an empty tuple.

        Example:
            >>> result = repository.ingest_journal_statuses()  # doctest: +SKIP


        :return: A tuple of summary dictionaries, including entries from every journal state.
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
        Decode Asset rows, preferring correctly typed scratch envelopes.

        An envelope's embedded identity keys the result without comparison to the scalar row ID;
        repeated decoded identities overwrite earlier entries. Recognized malformed envelopes raise
        instead of falling back.

        Require scalar size and at least one SHA-256/BLAKE3 digest when no typed Asset envelope is
        present; otherwise skip that legacy row. Rebuild selected metadata and optional policy IDs
        with a synthetic db-ID revision. Numeric, digest, and record-construction errors propagate,
        and no physical content or referenced policy is checked.

        Example:
            >>> records = repository._load_assets(())  # doctest: +SKIP


        :param rows: Optional row iterable consumed once; None obtains the family's ordered rows through current cache/macro routing.
        :return: A fresh identity-keyed dictionary of decoded or reconstructible Asset records.
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
        Decode Replica rows, preferring correctly typed scratch envelopes.

        An envelope's embedded identity keys the result without comparison to the scalar row ID;
        repeated decoded identities overwrite earlier entries. Recognized malformed envelopes raise
        instead of falling back.

        Legacy rows require non-None Asset ID, Store ID, and key. Invalid presence-state or mode
        text skips the row, but timestamp/conversion and record errors can propagate. Missing
        presence/mode default to unverified/active. Integrity-check time wins by truthiness over
        last-seen time. Resolve Store UUID through macros, restore only supported observation
        scalars, omit placement hints, and assign a synthetic db-ID revision; no byte read occurs.

        Example:
            >>> records = repository._load_replicas(())  # doctest: +SKIP


        :param rows: Optional row iterable consumed once; None obtains the family's ordered rows through current cache/macro routing.
        :return: A fresh identity-keyed dictionary of decoded or reconstructible Replica records.
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
        Decode Composite rows, preferring correctly typed scratch envelopes.

        An envelope's embedded identity keys the result without comparison to the scalar row ID;
        repeated decoded identities overwrite earlier entries. Recognized malformed envelopes raise
        instead of falling back.

        For legacy rows, query member links through macros in sequence order. Typed membership
        envelopes win; other links need an Asset ID and use sequence zero for false values, optional
        scalar role, and required-flag truthiness. Skip links lacking an Asset ID and Composites
        with no reconstructed members. The fallback retains name and a synthetic db-ID revision but
        not arbitrary attributes. Link/record validation errors propagate.

        Example:
            >>> records = repository._load_composites(())  # doctest: +SKIP


        :param rows: Optional row iterable consumed once; None obtains the family's ordered rows through current cache/macro routing.
        :return: A fresh identity-keyed dictionary of decoded or reconstructible Composite records.
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
        Decode derivation rows, preferring correctly typed scratch envelopes.

        An envelope's embedded identity keys the result without comparison to the scalar row ID;
        repeated decoded identities overwrite earlier entries. Recognized malformed envelopes raise
        instead of falling back.

        Legacy rows need both direct parent and child Asset IDs. Rebuild one source at index zero,
        optional notes/workflow ID, and the kind, using OTHER for invalid kind text. Recipe, other
        sources, workflow reference, and creation timestamp are not reconstructed from scalars.
        Missing endpoints skip the row; other conversion/construction errors propagate. No
        graph/reference or replay validation occurs.

        Example:
            >>> records = repository._load_derivations(())  # doctest: +SKIP


        :param rows: Optional row iterable consumed once; None obtains the family's ordered rows through current cache/macro routing.
        :return: A fresh identity-keyed dictionary of decoded or reconstructible derivation records.
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
        Decode replication-policy rows, preferring correctly typed scratch envelopes.

        An envelope's embedded identity keys the result without comparison to the scalar row ID;
        repeated decoded identities overwrite earlier entries. Recognized malformed envelopes raise
        instead of falling back.

        Legacy rows receive a fallback name, minimum/synchronous copies zero, STORE separation,
        bucket limit one, and ACTIVE mode when selected scalars are absent or false. JSON-list
        helpers string-convert members and bool uses scalar truthiness. Invalid construction or
        JSON/numeric conversion inside the policy block skips the row for TypeError/ValueError;
        converting the row ID happens outside that catch. Rebuilt records receive a synthetic db-ID
        revision.

        Missing copy settings can make these defaults invalid: zero copies conflict with the
        policy's default loss action, while a positive target requires positive synchronous copies.
        Such rows are skipped rather than returned as default policies.

        Example:
            >>> records = repository._load_replication_policies(())  # doctest: +SKIP


        :param rows: Optional row iterable consumed once; None obtains the family's ordered rows through current cache/macro routing.
        :return: A fresh identity-keyed dictionary of decoded or reconstructible replication-policy records.
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
        Decode backup-policy rows, preferring correctly typed scratch envelopes.

        An envelope's embedded identity keys the result without comparison to the scalar row ID;
        repeated decoded identities overwrite earlier entries. Recognized malformed envelopes raise
        instead of falling back.

        Legacy rows receive a fallback name, minimum copies zero, STORE separation, bucket limit
        one, and BACKUP mode for absent/false scalars. Verification/retention flags use truthiness,
        so nonempty text can mean True. JSON-list members are stringified. TypeError/ValueError
        inside policy construction skips that row, but row-ID conversion occurs outside the catch.
        Rebuilt records receive a synthetic db-ID revision.

        Example:
            >>> records = repository._load_backup_policies(())  # doctest: +SKIP


        :param rows: Optional row iterable consumed once; None obtains the family's ordered rows through current cache/macro routing.
        :return: A fresh identity-keyed dictionary of decoded or reconstructible backup-policy records.
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
        Read atomic links first and Composite links second, each in row-ID order through macros.
        Skip rows missing Item/target IDs; choose nonempty scratch role, then scalar type, then
        primary_payload without stripping. Later rows overwrite duplicate keys, and Composite
        targets win across tables. No target lookup or relationship-cache read is performed.

        Example:
            >>> result = repository._load_item_targets()  # doctest: +SKIP


        :return: A new role-keyed map of target kind and converted target ID.
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
        Load committed rows in ID order and decode their payloads. Retain any non-None operation
        entry without type or row-UUID agreement checks; skip non-dictionary/missing-operation
        payloads. Malformed envelopes/UUIDs raise, later duplicate UUIDs overwrite earlier values,
        and disabled journaling returns an empty dictionary.

        Example:
            >>> result = repository._load_committed_ingest_operations()  # doctest: +SKIP


        :return: A fresh mapping from journal-row UUID to its decoded completed operation.
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
        Filter supplied values to available columns, then insert or update the selected row.

        Column headings are queried while filtering each item. Unknown columns are dropped silently.
        The existence lookup and write are separate macro calls; no transaction, concurrency guard,
        or invalidation is added. Insert merges the supplied payload after the explicit identity, so
        a retained identity key in values can override it. Provider and conversion errors propagate.

        Example:
            >>> repository._upsert("digital_assets", "digital_asset_id", 7, columns)  # doctest: +SKIP


        :param table: Destination table inspected for supported columns.
        :param id_column: Identity column used for lookup and writing.
        :param row_id: Requested row identity used for existence lookup.
        :param values: Scalar/envelope column values filtered against the current schema.
        :return: None after the macro insertion or update.
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
        Read rows matching the supplied predicate and delete each by its explicit identity column.
        No surrounding transaction or invalidation is added; a later deletion failure can follow
        earlier successful removals.

        Example:
            >>> repository._delete_matching(table, id_column, where)  # doctest: +SKIP


        :param table: Table containing rows to remove.
        :param id_column: Identity column read from each selected row and passed to deletion.
        :param where: Column/value predicate forwarded to get_rows.
        :return: None after all selected deletions return.
        """

        for row in self.macros.get_rows(table, where=where):
            self.macros.delete_row(
                table, row[id_column], id_column=id_column
            )

    def _delete_item_target(self, item_id: api.ItemID, role: str) -> None:
        """
        Visit both Item-link tables and delete rows whose resolved role equals the supplied text.
        Scratch role takes precedence over scalar type and primary_payload fallback, with no
        whitespace normalization. No transaction, cache invalidation, or target-existence check is
        added; errors can follow partial deletion.

        Example:
            >>> repository._delete_item_target(api.ItemID(7), "cover")  # doctest: +SKIP


        :param item_id: Item identity int-converted for both link-table predicates.
        :param role: Exact resolved role text identifying links to delete.
        :return: None after all matching links are deleted.
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
        Query Store rows by the string form of the public UUID. Absence raises
        StoreConfigurationNotFound; multiple matches are accepted and the first row ID is returned
        without ordering or duplicate validation. Shared cache and live Store facades are not
        consulted.

        Example:
            >>> store_id = repository._store_id(store_uuid)  # doctest: +SKIP


        :param store_ref: Public Store UUID used for exact scalar lookup.
        :return: The first matching Store row ID converted to int.
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
        Load a Store row directly through macros. Missing rows or None/empty UUID values raise
        StoreConfigurationNotFound; other values pass through str and UUID parsing, whose errors
        propagate. No facade is attached or probed.

        Example:
            >>> store_uuid = repository._store_uuid(7)  # doctest: +SKIP


        :param store_id: Database Store row identity used for lookup.
        :return: The parsed public Store UUID.
        """

        row = self.macros.get_row("stores", store_id, id_column="store_id")
        if row is None or row.get("store_uuid") in (None, ""):
            raise api.StoreConfigurationNotFound(
                f"Store row {store_id} has no durable UUID."
            )
        return UUID(str(row["store_uuid"]))

    def _journal_row(self, operation_id: UUID) -> Mapping[str, Any] | None:
        """
        Find journal rows by the UUID's string form. Return the sole row or None, and raise
        StorageManagementError for duplicates. No journal-support flag, payload/state validation, or
        transaction lock is checked here.

        Example:
            >>> row = repository._journal_row(operation_id)  # doctest: +SKIP


        :param operation_id: Operation UUID whose scalar journal identity is queried.
        :return: The unique raw row mapping, or None.
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
        Require a unique existing row with a dictionary payload, shallow-merge values, then store
        the requested state and re-encoded payload. Project location/Asset scalars only when their
        retained values have the expected runtime types. Missing/invalid entries raise
        StorageManagementError; no transition guard, last-error clearing, or independent transaction
        is added.

        Example:
            >>> repository._update_journal_payload(operation_id, state="published", values={})  # doctest: +SKIP


        :param operation_id: UUID of the existing journal entry.
        :param state: State text assigned without validating an allowed transition.
        :param values: Recovery entries overwriting same-named payload keys; absent keys remain retained.
        :return: None after the journal row update.
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
        Ignore unrelated scratch content and decode only an envelope marked with this format.

        Missing/empty text, invalid JSON, and unmarked JSON return None for legacy fallback. Once
        the marker matches, decoding failures deriving from Exception become StorageManagementError
        with the original cause; BaseException propagates. No expected record type or embedded
        identity is checked. A valid marked envelope without payload can decode to None, including
        an allocation reservation.

        Example:
            >>> value = repository._load_optional_record(row, "digital_asset_scratch")  # doctest: +SKIP


        :param row: Raw database row whose scratch value is inspected.
        :param scratch_column: Column containing an optional marked storage envelope.
        :return: The decoded value, or None for unrelated/absent content or a None payload.
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
        Validate and rewrite a recognized version-zero envelope to the current wrapper.

        Ignore absent, malformed/unmarked JSON and already-current versions; current payloads are
        not validated here. Missing version counts as zero. Any other version raises
        StorageManagementError, including values that are not actually newer. For version zero,
        prefer an existing payload key over record, decode it for validation, then preserve that
        encoded payload in canonical ASCII JSON. Ordinary decode failures are wrapped with their
        cause; source data is not mutated.

        Example:
            >>> migrated = repository._migrate_envelope(old_scratch)  # doctest: +SKIP


        :param raw: Optional scratch scalar whose string form may contain a legacy envelope.
        :return: The upgraded JSON text, or None when no rewrite is selected.
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
        Encode a supported value in the version-one storage wrapper as compact ASCII JSON.

        Sorted object keys and ASCII escapes keep surrogate-containing text representable in
        database scratch columns. This does not make every Python value round-trip: codec markers,
        constructor fields, and registry coverage still matter. Default JSON handling permits
        nonfinite floats; enum values must themselves serialize. Unsupported values and
        recursive/serialization failures propagate.

        Example:
            >>> scratch = repository._dump(record)  # doctest: +SKIP


        :param value: Supported typed value or container to encode; its decode types must be registered separately.
        :return: Compact marked JSON text containing format, version, and encoded payload.
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
        Parse str(raw), require a dictionary with the current format/version values, then decode its
        payload through the registry. Missing payload decodes as None. JSON/shape/version and
        constructor errors propagate directly; this method does not perform legacy migration or
        compare a decoded record with row scalars.

        Example:
            >>> restored = repository._load(scratch)  # doctest: +SKIP


        :param raw: Scratch value whose string representation is a current storage envelope.
        :return: The decoded payload, possibly None; not restricted to a particular record class.
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
    Retain valid UTF-8 text and visibly escape unencodable surrogate code points.

    None passes through. On UnicodeEncodeError, UTF-8 backslashreplace produces printable escapes
    for scalar database columns; the scratch envelope can retain the original string separately.
    This is not secret redaction or general control-character filtering, and escaped output can be
    longer than its input.

    Example:
        >>> _database_scalar_text("book")
        'book'
        >>> _database_scalar_text(None) is None
        True


    :param value: Optional scalar text, potentially containing surrogateescaped filename code points.
    :return: The original text or None when encodable, otherwise text with unencodable code points escaped.
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
    Build the decoder registry from explicit types and storage API dataclasses/enums.

    Additional entries are set-collected without verifying their category. API exports are scanned
    without importing types by serialized name; keys use each type's actual module and qualified
    name. Distinct types sharing a name collide with set-iteration-dependent precedence.
    Constructors remain responsible for decoded field/value validation.

    Example:
        >>> types = _storage_value_types(())
        >>> types[_type_name(api.Digest)] is api.Digest
        True


    :param additional: Additional constructor types, such as private journal request/result classes, accepted during decoding.
    :return: A fresh qualified-name-to-type dictionary used for explicit constructor lookup.
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
    Combine the supplied type's current module and qualified name. No importability or name
    stability is checked; explicit __module__ assignments affect persisted identities.

    Example:
        >>> _type_name(int)
        'builtins.int'


    :param value: Type whose module and qualified name identify it in an envelope.
    :return: The dotted module-qualified type name.
    """

    return f"{value.__module__}.{value.__qualname__}"


def _encode(value: Any) -> Any:
    """
    Recursively convert supported values into tagged JSON-compatible structures.

    Enums retain their raw value; dataclass instances encode every field, including fields their
    constructor may not accept on decode. UUIDs, datetimes, tuples, and frozensets receive markers;
    frozensets sort by str without a tie-breaker. Lists recurse, and dictionaries with any
    non-string key use paired mappings. Other string-key dictionaries remain ordinary objects,
    including reserved marker keys that the decoder may reinterpret.

    None, text, numeric values, and booleans pass through. Unsupported values raise TypeError;
    cycles are not detected independently of Python recursion errors. This helper neither checks the
    type registry nor guarantees every encoded shape will round-trip. Nonfinite floats are left for
    the JSON encoder.

    Example:
        >>> _encode((1, "book"))
        {'$tuple': [1, 'book']}


    :param value: Supported storage value or recursively composed container to encode.
    :return: Tagged containers or primitive values for JSON serialization.
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
    Reconstruct tagged values using explicit constructors from the supplied registry.

    Lists recurse and non-dictionary values pass through. Dictionary markers are tested in UUID,
    datetime, tuple, frozenset, mapping, enum, then dataclass order; the first recognized marker
    wins even when extra keys exist. Ordinary string-key dictionaries containing reserved markers
    may therefore change shape.

    Enum/dataclass names must exist in types; no dynamic import is performed. Dataclass fields must
    be a dictionary and are passed as keyword arguments to the registered callable. Constructor,
    malformed-container, hashability, and recursion errors propagate. The registry does not
    independently enforce the named type category or restrict its constructor behavior.

    Example:
        >>> _decode({'$tuple': [1, "book"]}, {})
        (1, 'book')


    :param value: Parsed JSON payload, possibly containing the codec's reserved marker dictionaries.
    :param types: Qualified-name registry of constructors permitted for enum/dataclass markers.
    :return: The reconstructed value, or unchanged untagged primitive.
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
    Consume digests until the first exact algorithm-name match. No case normalization, duplicate
    rejection, or digest verification is performed; later matches are ignored.

    Example:
        >>> _digest_value((api.Digest("sha256", "aa"),), "sha256")
        'aa'


    :param digests: Iterable of digest values consumed until a match or exhaustion.
    :param algorithm: Exact algorithm string to compare against each digest.
    :return: The first matching digest value, or None.
    """

    return next(
        (digest.value for digest in digests if digest.algorithm == algorithm), None
    )


def _row_digests(row: Mapping[str, Any], prefix: str) -> tuple[api.Digest, ...]:
    """
    Read only SHA-256 and BLAKE3 scalar columns in that order. Nonempty values are stringified
    without stripping; other algorithms and absent/empty columns are ignored. Digest construction
    errors propagate.

    Example:
        >>> _row_digests({"hash_sha256": "aa"}, "hash_")
        (Digest(algorithm='sha256', value='aa'),)


    :param row: Legacy row mapping containing optional digest scalar columns.
    :param prefix: Column-name prefix before sha256 and blake3.
    :return: A tuple of supplied digest evidence in SHA-256/BLAKE3 order.
    """

    values: list[api.Digest] = []
    for algorithm in ("sha256", "blake3"):
        value = _optional_text(row.get(f"{prefix}{algorithm}"))
        if value is not None:
            values.append(api.Digest(algorithm, value))
    return tuple(values)


def _epoch_ms(value: datetime | None) -> int | None:
    """
    Convert datetime.timestamp seconds to integer milliseconds by truncating toward zero. None
    passes through; naive datetimes use the host's local-time interpretation, and no awareness check
    is added.

    Example:
        >>> _epoch_ms(datetime(1970, 1, 1, tzinfo=UTC))
        0


    :param value: Optional datetime to project into an integer database timestamp.
    :return: Unix milliseconds truncated toward zero, or None.
    """

    return None if value is None else int(value.timestamp() * 1000)


def _datetime_from_epoch(value: Any) -> datetime | None:
    """
    Convert an optional int-coercible millisecond scalar to an aware UTC datetime. None and empty
    text yield None; fractional inputs first follow int truncation. Conversion and timestamp-range
    errors propagate.

    Example:
        >>> _datetime_from_epoch(0).isoformat()
        '1970-01-01T00:00:00+00:00'


    :param value: Nullable Unix-millisecond scalar accepted by _optional_int.
    :return: An aware UTC datetime, or None for absent input.
    """

    parsed = _optional_int(value)
    return None if parsed is None else datetime.fromtimestamp(parsed / 1000, UTC)


def _optional_text(value: Any) -> str | None:
    """
    Return str(value) unless value is None or its string representation is empty. Whitespace is
    retained, and false numeric/boolean values become their ordinary nonempty text.

    Example:
        >>> _optional_text("  ") == "  "
        True


    :param value: Nullable legacy scalar to stringify without whitespace normalization.
    :return: Nonempty text, or None for None/empty text.
    """

    if value is None:
        return None
    text = str(value)
    return text if text else None


def _optional_int(value: Any) -> int | None:
    """
    Return None for None or empty text, otherwise apply int directly. This accepts numeric strings
    and booleans and truncates convertible fractional numbers; it does not enforce positivity.
    Conversion errors propagate.

    Example:
        >>> (_optional_int("7"), _optional_int(""), _optional_int(2.9))
        (7, None, 2)


    :param value: Nullable legacy scalar to convert using int.
    :return: The converted integer, or None for an absent scalar.
    """

    if value in (None, ""):
        return None
    return int(value)


def _optional_id(value: Any, constructor: Callable[[int], Any]) -> Any | None:
    """
    Normalize a nullable integer scalar, then call the supplied constructor only when present. The
    helper adds no identity/reference validation beyond the constructor and propagates conversion
    errors.

    Example:
        >>> _optional_id("7", api.DigitalAssetID)
        7


    :param value: Nullable int-coercible legacy identity.
    :param constructor: Callable receiving the converted integer to produce the desired identity type.
    :return: The constructed identity, or None for absent input.
    """

    parsed = _optional_int(value)
    return None if parsed is None else constructor(parsed)


def _json_list(value: Any, default: list[str]) -> list[str]:
    """
    Parse JSON and stringify each element only when the result is a list. Absent or non-list data
    returns a fresh shallow copy of default; malformed JSON raises rather than using the default.
    Ordering and duplicate values are retained.

    Example:
        >>> _json_list('[1, "store"]', [])
        ['1', 'store']


    :param value: JSON text or a value whose string representation is JSON; None/empty text selects the default.
    :param default: Fallback list copied for absent input or valid JSON of another shape.
    :return: The list of stringified decoded elements, or a shallow default copy.
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
