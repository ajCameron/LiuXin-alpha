"""Reconcile configured Store facades from one bound database snapshot."""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any, Protocol

from LiuXin_alpha.storage.api import models as storage_models
from LiuXin_alpha.storage.api import storage_manager_api as manager_api
from LiuXin_alpha.storage.api import store_api
from LiuXin_alpha.storage.utils.store_configuration import store_configuration_from_row
from LiuXin_alpha.storage.utils.store_rows import (
    _order_store_rows,
    _persist_derived_store_uuid,
    _row_int,
    _row_text,
    _row_uuid,
)


class StoreBootstrapHost(Protocol):
    """Declare manager state and operations required for Store reconciliation.

    Example:
        >>> bootstrapper = StoreBootstrapper(manager)  # doctest: +SKIP
    """

    startup_on_add: bool
    _lock: Any
    _stores: dict[storage_models.StoreUUID, store_api.StoreAPI]
    _store_configurations: dict[
        storage_models.StoreUUID, manager_api.StoreConfiguration
    ]

    def iter_store_configurations(self) -> Iterable[manager_api.StoreConfiguration]:
        """Enumerate currently retained Store configurations.

        :return: Iterable of retained portable configurations.
        """
        ...

    def get_replica_record(
        self,
        replica_id: manager_api.ReplicaID,
    ) -> manager_api.ReplicaRecord:
        """Return a Replica so backed Store dependencies can be ordered.

        :param replica_id: Preferred Replica identity retained by the backing reference.
        :return: Current Replica record, or raise when the claim is absent.
        """
        ...

    def _unload_database_stores(
        self, store_refs: tuple[storage_models.StoreUUID, ...]
    ) -> None:
        """Unload database-owned Store identities absent from the new snapshot."""
        ...

    def _require_store_factory(
        self,
    ) -> Callable[[manager_api.StoreConfiguration], store_api.StoreAPI]:
        """Return the configured backend factory or raise."""
        ...

    def attach_store(
        self,
        configuration: manager_api.StoreConfiguration,
        store: store_api.StoreAPI,
        *,
        startup: bool = True,
        replace_existing: bool = False,
    ) -> manager_api.StoreConfiguration:
        """Install one constructed facade and portable configuration.

        :param configuration: Portable configuration to retain.
        :param store: Constructed facade with the same UUID.
        :param startup: Whether the host starts the candidate.
        :param replace_existing: Whether an existing identity may be replaced.
        :return: Installed configuration.
        """
        ...

    def recover_pending_ingests(
        self, operation_id: object | None = None
    ) -> tuple[str, ...]:
        """Reconcile durable ingest state after Store loading.

        :param operation_id: Optional exact operation UUID; None processes the pending set.
        :return: Current recovery issue messages.
        """
        ...


class StoreBootstrapper:
    """Load, construct, replace, and unload Store facades for a bound manager.

    Example:
        >>> report = StoreBootstrapper(manager).load(database, startup=False)  # doctest: +SKIP
    """

    def __init__(self, host: StoreBootstrapHost) -> None:
        """Retain the manager host without inspecting its Store registry.

        Example:
            >>> bootstrapper = StoreBootstrapper(manager)  # doctest: +SKIP
        """
        self.host = host

    def load(
        self,
        database: Any,
        *,
        include_offline: bool = False,
        clear_existing: bool = True,
        startup: bool | None = None,
    ) -> manager_api.StorageBootstrapReport:
        """Reconcile the host against the database's materialized Store rows.

        Example:
            >>> report = bootstrapper.load(database, startup=False)  # doctest: +SKIP

        :param database: Bound database supplying the authoritative Store-row snapshot.
        :param include_offline: Whether offline, retired, or unavailable candidates remain eligible.
        :param clear_existing: Whether absent and changed facades are unloaded or replaced.
        :param startup: Candidate startup policy; None uses the host default.
        :return: Counts and issues for the complete reconciliation pass.
        """
        if "stores" not in set(database.get_tables()):
            if clear_existing:
                self.host._unload_database_stores(
                    tuple(
                        configuration.store_uuid
                        for configuration in self.host.iter_store_configurations()
                    )
                )
            return manager_api.StorageBootstrapReport()

        rows = tuple(database.get_all_rows("stores", iterator_return=False) or ())
        existing_refs = {
            configuration.store_uuid
            for configuration in self.host.iter_store_configurations()
        }
        active_database_refs: set[storage_models.StoreUUID] = set()
        issues: list[manager_api.StorageBootstrapIssue] = []
        loaded = skipped = failed = 0
        should_start = self.host.startup_on_add if startup is None else startup
        ordered_rows = _order_store_rows(self.host, rows)

        for row in ordered_rows:
            store_id = _row_int(row, "store_id")
            store_name = _row_text(row, "store_name")
            declared_store_ref = _row_uuid(row, "store_uuid")
            configuration: manager_api.StoreConfiguration | None = None
            candidate: store_api.StoreAPI | None = None
            attached = False
            try:
                online = (_row_text(row, "store_online_status") or "").lower()
                if declared_store_ref is not None and (
                    include_offline or online not in {"offline", "retired"}
                ):
                    # A malformed changed row must not make the last known-good
                    # facade disappear merely because translation fails below.
                    active_database_refs.add(declared_store_ref)
                configuration = store_configuration_from_row(
                    row,
                    fallback_store_id=store_id,
                )
                _persist_derived_store_uuid(
                    database,
                    row=row,
                    store_id=store_id,
                    store_ref=configuration.store_uuid,
                )
                if online in {"offline", "retired"} and not include_offline:
                    skipped += 1
                    issues.append(
                        manager_api.StorageBootstrapIssue(
                            configuration.store_uuid,
                            configuration.store_name,
                            f"Store is marked {online}.",
                        )
                    )
                    continue

                active_database_refs.add(configuration.store_uuid)
                with self.host._lock:
                    already_live = configuration.store_uuid in self.host._stores
                    already_configured = (
                        configuration.store_uuid in self.host._store_configurations
                    )
                if already_live and not clear_existing:
                    skipped += 1
                    continue

                candidate = self.host._require_store_factory()(configuration)
                if should_start:
                    status = candidate.startup()
                    if not status.available and not include_offline:
                        candidate.close()
                        candidate = None
                        skipped += 1
                        issues.append(
                            manager_api.StorageBootstrapIssue(
                                configuration.store_uuid,
                                configuration.store_name,
                                status.message or "Store is unavailable.",
                            )
                        )
                        continue

                self.host.attach_store(
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
                    manager_api.StorageBootstrapIssue(
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
            self.host._unload_database_stores(
                tuple(existing_refs - active_database_refs)
            )
        self.host.recover_pending_ingests()

        return manager_api.StorageBootstrapReport(
            discovered_configurations=len(rows),
            loaded_stores=loaded,
            skipped_configurations=skipped,
            failed_configurations=failed,
            issues=tuple(issues),
        )


__all__ = ["StoreBootstrapHost", "StoreBootstrapper"]
