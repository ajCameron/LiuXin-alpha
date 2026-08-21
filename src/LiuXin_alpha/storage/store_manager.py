"""Application-facing manager built on the second-generation storage API."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, override
from uuid import UUID

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.backend_registry import (
    DEFAULT_BACKEND_REGISTRY,
    StoreConstructionContext,
)
from LiuXin_alpha.storage.storage_manager import InMemoryStorageManager
from LiuXin_alpha.storage.store_factory import build_store
from LiuXin_alpha.storage.store_spec_utils import store_configuration_from_row


class StorageManager(InMemoryStorageManager):
    """Usable reference manager with a default Store factory and DB bootstrap.

    Metadata remains in memory after bootstrap; callers needing durable asset,
    replica, and workflow repositories should inject those production adapters
    behind the same API. Store configurations loaded from the database retain
    stable UUID identity and all byte routing uses opaque ``Location`` values.

    Example:
        >>> from LiuXin_alpha.storage.stores import FilesystemStore
        >>> store = FilesystemStore(  # doctest: +SKIP
        ...     "/tmp/liuxin-example", name="primary",
        ... )
        >>> with StorageManager(stores=[store]) as manager:  # doctest: +SKIP
        ...     asset = manager.store_bytes(b"book")
        ...     manager.read_asset(asset)
        b'book'
    """

    def __init__(
        self,
        *,
        stores: Iterable[api.StoreAPI] = (),
        store_registrations: Iterable[
            tuple[api.StoreConfiguration, api.StoreAPI]
        ] = (),
        store_factory=None,
        backend_context: StoreConstructionContext | None = None,
        s3_client: Any | None = None,
        encryption_key_provider: Any | None = None,
        db: Any | None = None,
        startup_on_add: bool = True,
        default_store_ref: api.StoreUUID | None = None,
        **kwargs,
    ) -> None:
        registrations = list(store_registrations)
        for store in stores:
            if not isinstance(store, api.StoreAPI):
                raise TypeError("stores must contain StoreAPI instances.")
            registrations.append((store.configuration, store))
        self.db = db
        self.startup_on_add = bool(startup_on_add)
        supplied_context = backend_context or StoreConstructionContext()
        self.backend_context = StoreConstructionContext(
            s3_client=(
                supplied_context.s3_client
                if s3_client is None
                else s3_client
            ),
            store_resolver=(
                supplied_context.store_resolver or self.get_store
            ),
            encryption_key_provider=(
                supplied_context.encryption_key_provider
                if encryption_key_provider is None
                else encryption_key_provider
            ),
        )
        selected_factory = store_factory or (
            lambda configuration: build_store(
                configuration,
                context=self.backend_context,
            )
        )
        super().__init__(
            store_registrations=(),
            store_factory=selected_factory,
            default_store_ref=None,
            **kwargs,
        )
        for configuration, store in registrations:
            self.attach_store(
                configuration,
                store,
                startup=self.startup_on_add,
            )
        if default_store_ref is not None:
            self.set_default_store(default_store_ref)

    def add_store(
        self,
        store_or_name: api.StoreAPI | str | None = None,
        *args: Any,
        configuration: api.StoreConfiguration | None = None,
        startup: bool | None = None,
        **kwargs: Any,
    ) -> api.StoreConfiguration:
        """Add configured backend details or attach an existing Store object.

        ``add_store(name, kind, root, ...)`` is the ordinary configuration
        form defined by ``StorageManagerAPI``. Passing a ``StoreAPI`` retains
        the object-oriented attachment form; ``configuration`` may override
        the object's own configuration in that form.

        Example:
            >>> configured = manager.add_store(  # doctest: +SKIP
            ...     "primary", "filesystem", "/srv/liuxin",
            ... )
            >>> attached = manager.add_store(store)  # doctest: +SKIP
        """

        if store_or_name is None:
            supplied_keys = [
                key for key in ("name", "store") if key in kwargs
            ]
            if len(supplied_keys) != 1:
                raise TypeError(
                    "add_store requires exactly one Store object or Store name."
                )
            store_or_name = kwargs.pop(supplied_keys[0])

        if isinstance(store_or_name, api.StoreAPI):
            if args or kwargs:
                raise TypeError(
                    "Store object attachment accepts only configuration and "
                    "startup keyword arguments."
                )
            return self.add_store_instance(
                store_or_name,
                configuration=configuration,
                startup=startup,
            )
        if not isinstance(store_or_name, str):
            raise TypeError(
                "add_store expects a StoreAPI instance or a Store name."
            )
        if configuration is not None:
            raise TypeError(
                "configuration is only valid when attaching a Store object."
            )
        if startup is not None:
            if "start" in kwargs:
                raise TypeError("Pass only one of start and startup.")
            kwargs["start"] = startup
        return super().add_store(store_or_name, *args, **kwargs)

    def add_store_instance(
        self,
        store: api.StoreAPI,
        *,
        configuration: api.StoreConfiguration | None = None,
        startup: bool | None = None,
    ) -> api.StoreConfiguration:
        """Attach one already-constructed configured Store facade."""

        return self.attach_store(
            configuration or store.configuration,
            store,
            startup=self.startup_on_add if startup is None else startup,
        )

    def get_store_configuration_from_db(
        self,
        store_id: int,
    ) -> api.StoreConfiguration:
        if self.db is None:
            raise RuntimeError("StorageManager is not bound to a database.")
        if "stores" not in set(self.db.get_tables()):
            raise KeyError("Database has no stores table.")
        row = self.db.get_row_from_id("stores", int(store_id))
        if row is None:
            raise KeyError(f"Unknown Store row: {store_id}")
        return store_configuration_from_row(
            row,
            fallback_store_id=int(store_id),
        )

    def load_from_database(
        self,
        db: Any | None = None,
        *,
        include_offline: bool = False,
        clear_existing: bool = True,
        startup: bool | None = None,
    ) -> api.StorageBootstrapReport:
        """Reconcile live Store facades with durable database rows.

        ``clear_existing=True`` treats the database as authoritative: Stores
        removed from the table, or explicitly marked offline/retired, are
        unloaded after all usable rows have been considered. A configuration
        needed by an existing Replica claim is retained without a live facade.

        Replacements are prepared and optionally started before the old Store
        is swapped out. If construction or startup fails, an existing facade
        for that UUID remains available and the failure is returned in the
        bootstrap report.

        With ``clear_existing=False``, existing live Stores are left alone and
        only newly discovered or currently unavailable configurations load.

        Example:
            >>> report = manager.load_from_database(  # doctest: +SKIP
            ...     startup=False,
            ... )
        """

        database = self.db if db is None else db
        if database is None:
            raise RuntimeError("StorageManager is not bound to a database.")
        self.db = database
        if "stores" not in set(database.get_tables()):
            if clear_existing:
                self._unload_database_stores(
                    tuple(
                        configuration.store_uuid
                        for configuration in self.iter_store_configurations()
                    )
                )
            return api.StorageBootstrapReport()

        rows = tuple(
            database.get_all_rows("stores", iterator_return=False) or ()
        )
        existing_refs = {
            configuration.store_uuid
            for configuration in self.iter_store_configurations()
        }
        active_database_refs: set[api.StoreUUID] = set()
        issues: list[api.StorageBootstrapIssue] = []
        loaded = skipped = failed = 0
        should_start = self.startup_on_add if startup is None else startup
        ordered_rows = sorted(rows, key=_is_encrypted_row)

        for row in ordered_rows:
            store_id = _row_int(row, "store_id")
            store_name = _row_text(row, "store_name")
            declared_store_ref = _row_uuid(row, "store_uuid")
            configuration: api.StoreConfiguration | None = None
            candidate: api.StoreAPI | None = None
            attached = False
            try:
                online = (
                    _row_text(row, "store_online_status") or ""
                ).lower()
                if (
                    declared_store_ref is not None
                    and (include_offline or online not in {"offline", "retired"})
                ):
                    # A malformed changed row must not make the last known-good
                    # facade disappear merely because translation fails below.
                    active_database_refs.add(declared_store_ref)
                configuration = store_configuration_from_row(
                    row,
                    fallback_store_id=store_id,
                )
                if online in {"offline", "retired"} and not include_offline:
                    skipped += 1
                    issues.append(
                        api.StorageBootstrapIssue(
                            configuration.store_uuid,
                            configuration.store_name,
                            f"Store is marked {online}.",
                        )
                    )
                    continue

                active_database_refs.add(configuration.store_uuid)
                _persist_derived_store_uuid(
                    database,
                    row=row,
                    store_id=store_id,
                    store_ref=configuration.store_uuid,
                )
                with self._lock:
                    already_live = configuration.store_uuid in self._stores
                    already_configured = (
                        configuration.store_uuid
                        in self._store_configurations
                    )
                if already_live and not clear_existing:
                    skipped += 1
                    continue

                candidate = self._require_store_factory()(configuration)
                if should_start:
                    status = candidate.startup()
                    if not status.available and not include_offline:
                        candidate.close()
                        candidate = None
                        skipped += 1
                        issues.append(
                            api.StorageBootstrapIssue(
                                configuration.store_uuid,
                                configuration.store_name,
                                status.message or "Store is unavailable.",
                            )
                        )
                        continue

                self.attach_store(
                    configuration,
                    candidate,
                    startup=False,
                    replace_existing=already_configured,
                )
                attached = True
                loaded += 1
            except Exception as error:
                if candidate is not None and not attached:
                    try:
                        candidate.close()
                    except Exception:
                        pass
                failed += 1
                issues.append(
                    api.StorageBootstrapIssue(
                        (
                            declared_store_ref
                            if configuration is None
                            else configuration.store_uuid
                        ),
                        (
                            store_name
                            if configuration is None
                            else configuration.store_name
                        ),
                        str(error) or type(error).__name__,
                    )
                )

        if clear_existing:
            self._unload_database_stores(
                tuple(existing_refs - active_database_refs)
            )

        return api.StorageBootstrapReport(
            discovered_configurations=len(rows),
            loaded_stores=loaded,
            skipped_configurations=skipped,
            failed_configurations=failed,
            issues=tuple(issues),
        )

    @override
    def reload_stores(
        self,
        *,
        include_offline: bool = False,
        replace_existing: bool = True,
    ) -> api.StorageBootstrapReport:
        """Reload database rows when bound, otherwise in-memory configuration.

        Example:
            >>> report = manager.reload_stores()  # doctest: +SKIP
        """

        if self.db is None:
            return super().reload_stores(
                include_offline=include_offline,
                replace_existing=replace_existing,
            )
        return self.load_from_database(
            self.db,
            include_offline=include_offline,
            clear_existing=replace_existing,
            startup=True,
        )

    def _unload_database_stores(
        self,
        store_refs: tuple[api.StoreUUID, ...],
    ) -> None:
        """Unload inactive rows while retaining referenced Store identities.

        Example:
            >>> manager._unload_database_stores(())
        """

        for store_ref in sorted(store_refs, key=lambda value: value.int):
            try:
                self.remove_store(store_ref, forget_configuration=True)
            except api.StoragePreconditionFailed:
                self.remove_store(store_ref, forget_configuration=False)

    @classmethod
    def from_database(cls, db: Any, **kwargs):
        manager = cls(db=db, **kwargs)
        report = manager.load_from_database(db)
        return manager, report


StoreManager = StorageManager
StorageBootstrapIssue = api.StorageBootstrapIssue
StorageBootstrapReport = api.StorageBootstrapReport


def _row_value(row: Any, key: str):
    try:
        return row[key]
    except Exception:
        return getattr(row, key, None)


def _row_int(row: Any, key: str) -> int | None:
    try:
        value = _row_value(row, key)
        if value is None or value == "":
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _persist_derived_store_uuid(
    database: Any,
    *,
    row: Any,
    store_id: int | None,
    store_ref: api.StoreUUID,
) -> None:
    """Backfill stable identity when bootstrapping a legacy Store row."""

    if store_id is None or _row_text(row, "store_uuid") is not None:
        return
    allowed_columns = getattr(row, "allowed_columns", None)
    if allowed_columns is not None and "store_uuid" not in set(allowed_columns):
        return
    macros = getattr(database, "macros", None)
    update_row = getattr(macros, "update_row", None)
    if not callable(update_row):
        return
    update_row(
        "stores",
        store_id,
        {"store_uuid": str(store_ref)},
        id_column="store_id",
    )


def _row_text(row: Any, key: str) -> str | None:
    value = _row_value(row, key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _row_uuid(row: Any, key: str) -> UUID | None:
    value = _row_value(row, key)
    if isinstance(value, UUID):
        return value
    if value is None or value == "":
        return None
    try:
        return UUID(str(value))
    except ValueError:
        return None


def _row_kind(row: Any) -> str:
    return (_row_text(row, "store_kind") or "").lower().replace("-", "_")


def _is_encrypted_row(row: Any) -> bool:
    kind = _row_kind(row)
    try:
        return DEFAULT_BACKEND_REGISTRY.canonical_kind(kind) == "encrypted"
    except (ValueError, api.StoreUnsupportedOperation):
        return False


__all__ = [
    "StorageBootstrapIssue",
    "StorageBootstrapReport",
    "StorageManager",
    "StoreManager",
]
