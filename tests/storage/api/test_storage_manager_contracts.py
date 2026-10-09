"""
Exercise application Store registration, configuration bootstrap, and database-row reload.

Filesystem cases use real temporary paths and bytes. Minimal row-list doubles model
Store-table reads and updates without a SQL engine, durable transactions, or a full
Asset catalogue. Tests distinguish unknown Stores from retained offline configurations,
replacement construction failures from successful attachment, and unbound manager
metadata from database-backed application behavior.
"""

from __future__ import annotations

from pathlib import Path
from uuid import UUID, uuid4

import pytest

from LiuXin_alpha.storage.api import (
    Digest,
    StorageBootstrapReport,
    StorageManagementError,
    StoreAlreadyExists,
    StoreConfiguration,
    StoreConfigurationNotFound,
    StoreUnavailable,
)
from LiuXin_alpha.storage.durable_manager import StorageManager
from LiuXin_alpha.storage.storage_manager import TransientStorageManager
from LiuXin_alpha.storage.stores import FilesystemStore, MemoryStore


def test_storage_developer_guide_quickstart_is_executable() -> None:
    """Keep the in-memory introductory workflow aligned with Replica-mode policy.

    Example:
        >>> test_storage_developer_guide_quickstart_is_executable()

    :return: None after the documented payload is stored and read through a transient Replica.
    """

    with StorageManager(stores=(MemoryStore(name="scratch"),)) as manager:
        asset = manager.store_bytes(
            b"book",
            name="book.epub",
            replica_mode="transient",
        )
        payload = manager.read_asset(asset, replica_mode="transient")

    assert payload == b"book"


def _store(path: Path, name: str) -> FilesystemStore:
    """
    Construct a configured filesystem Store without starting it or registering manager metadata.

    Example:
        >>> store = _store(Path("archive"), "archive")  # doctest: +SKIP


    :param path: Filesystem root passed to the Store constructor.
    :param name: Human-readable name retained by the Store configuration.
    :return: New FilesystemStore using the constructor defaults and a generated Store UUID.
    """
    return FilesystemStore(path, name=name)


def test_application_manager_does_not_inherit_transient_state_manager() -> None:
    """
    Protect the application manager composition boundary from inheritance of the transient manager.

    This checks class ancestry without constructing a manager or exercising database persistence.

    Example:
        >>> test_application_manager_does_not_inherit_transient_state_manager()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    assert not issubclass(StorageManager, TransientStorageManager)


def test_manager_initialization_accepts_only_new_store_api_and_starts_stores(
    tmp_path: Path,
) -> None:
    """
    Check startup, default routing, and object identity for an initial filesystem Store.

    An unrelated object is rejected as an invalid StoreAPI input. Availability is observed on the
    temporary filesystem; the manager has no database binding.

    Example:
        >>> test_manager_initialization_accepts_only_new_store_api_and_starts_stores(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary root for filesystem Store paths; catalogue rows and manager metadata are disposable test state.
    :return: None after the stated regression assertions pass.
    """
    first = _store(tmp_path / "first", "first")
    manager = StorageManager(stores=[first], startup_on_add=True)

    assert manager.get_store(first.store_ref) is first
    assert manager.get_default_store_ref() == first.store_ref
    assert first.status().available is True
    with pytest.raises(TypeError, match="StoreAPI"):
        StorageManager(stores=[object()])  # type: ignore[list-item]


def test_manager_registration_is_uuid_routed_and_duplicate_safe(tmp_path: Path) -> None:
    """
    Reject a second Store with an existing UUID and distinguish an unknown UUID lookup.

    The two configured Stores share a name but use different roots; registration is attempted
    without startup.

    Example:
        >>> test_manager_registration_is_uuid_routed_and_duplicate_safe(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary root for filesystem Store paths; catalogue rows and manager metadata are disposable test state.
    :return: None after the stated regression assertions pass.
    """
    first = _store(tmp_path / "first", "same-name")
    second = FilesystemStore(
        tmp_path / "second",
        name="same-name",
        uuid=first.store_ref,
    )
    manager = StorageManager(stores=[first], startup_on_add=False)

    with pytest.raises(StoreAlreadyExists):
        manager.add_store_instance(second, startup=False)
    with pytest.raises(StoreConfigurationNotFound):
        manager.get_store(uuid4())


def test_manager_adds_filesystem_store_from_a_path_without_configuration_boilerplate(
    tmp_path: Path,
) -> None:
    """
    Exercise filesystem configuration, startup, ingest, and readback through public convenience
    calls.

    The Unicode root becomes a file URI and a real directory. Tags, backend protocol, and default
    selection are checked alongside temporary destination bytes and in-memory Asset metadata. The
    context closes the manager.

    Example:
        >>> test_manager_adds_filesystem_store_from_a_path_without_configuration_boilerplate(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary root for filesystem Store paths; catalogue rows and manager metadata are disposable test state.
    :return: None after the stated regression assertions pass.
    """
    root = tmp_path / "primary 😀 store"

    with StorageManager() as manager:
        configuration = manager.add_filesystem_store(
            "primary",
            root,
            tags={"local", "fast"},
            operational_role="live",
        )
        asset = manager.store_bytes(
            b"configured through the public convenience API",
            original_name="book.epub",
        )

        assert configuration.store_root_uri == root.resolve().as_uri()
        assert configuration.store_kind == "filesystem"
        assert configuration.store_access_protocol == "file"
        assert set(configuration.store_tags) == {"local", "fast"}
        assert manager.get_default_store_ref() == configuration.store_uuid
        assert manager.read_asset(asset) == (
            b"configured through the public convenience API"
        )
        assert root.is_dir()


def test_concrete_manager_separates_store_creation_from_object_attachment(
    tmp_path: Path,
) -> None:
    """
    Check positional and keyword configuration forms alongside explicit object attachment.

    Configured filesystem Stores start successfully, while the attached object retains identity.
    The distinct method names keep configuration creation and dependency injection unambiguous. The
    manager context closes the attached Stores.

    Example:
        >>> test_concrete_manager_separates_store_creation_from_object_attachment(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary root for filesystem Store paths; catalogue rows and manager metadata are disposable test state.
    :return: None after the stated regression assertions pass.
    """
    attached = FilesystemStore(tmp_path / "attached", name="attached")

    with StorageManager() as manager:
        generic = manager.add_store(
            "generic",
            "filesystem",
            tmp_path / "generic",
            protocol="file",
            start=True,
        )
        keyword = manager.add_store(
            name="keyword",
            kind="filesystem",
            root=tmp_path / "keyword",
        )
        attached_configuration = manager.add_store_instance(attached)

        assert generic.store_root_uri == (tmp_path / "generic").resolve().as_uri()
        assert manager.get_store(generic.store_uuid).status().available
        assert keyword.store_root_uri == (tmp_path / "keyword").resolve().as_uri()
        assert manager.get_store(keyword.store_uuid).status().available
        assert attached_configuration == attached.configuration
        assert manager.get_store(attached.store_ref) is attached


def test_manager_convenience_stores_and_reads_by_asset_id_or_hash(
    tmp_path: Path,
) -> None:
    """
    Round-trip real filesystem bytes through Asset ID, Digest, digest-text, and range lookups.

    The manager computes the ingested digest and verifies storage with disposable in-memory
    metadata. The offset and length select the seven-byte payload suffix.

    Example:
        >>> test_manager_convenience_stores_and_reads_by_asset_id_or_hash(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary root for filesystem Store paths; catalogue rows and manager metadata are disposable test state.
    :return: None after the stated regression assertions pass.
    """
    store = _store(tmp_path / "managed", "managed")
    manager = StorageManager(stores=[store], startup_on_add=True)

    asset = manager.store_bytes(
        b"manager-payload",
        original_name="book.epub",
        verify=True,
    )
    digest = next(digest for digest in asset.digests if digest.algorithm == "sha256")

    assert manager.read_file(asset.digital_asset_id) == b"manager-payload"
    assert manager.read_file(digest) == b"manager-payload"
    assert manager.read_file(digest.value) == b"manager-payload"
    assert manager.read_file(asset.digital_asset_id, offset=8, length=7) == b"payload"


def test_manager_routes_locations_and_changes_default_store_explicitly(
    tmp_path: Path,
) -> None:
    """
    Select the second Store as default and check an explicitly targeted write reaches it.

    The recorded Replica Location and Asset readback agree with that destination. Because the write
    also supplies a Store UUID, this assertion does not isolate implicit-default placement.

    Example:
        >>> test_manager_routes_locations_and_changes_default_store_explicitly(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary root for filesystem Store paths; catalogue rows and manager metadata are disposable test state.
    :return: None after the stated regression assertions pass.
    """
    first = _store(tmp_path / "first", "first")
    second = _store(tmp_path / "second", "second")
    manager = StorageManager(stores=[first, second], startup_on_add=True)
    manager.set_default_store(second.store_ref)

    asset = manager.store_bytes(b"two", store=second.store_ref)
    replica = next(
        manager.iter_replica_records(digital_asset_id=asset.digital_asset_id)
    )

    assert replica.location.store_ref == second.store_ref
    assert manager.read_file(asset.digital_asset_id) == b"two"


class _RowsDatabase:
    """
    Provide only the Store-table read surface over a retained mutable list.

    This double has no SQL engine, durable writes, transactions, or complete Asset catalogue.
    Returning the same rows lets reload tests change their source in place.

    Example:
        >>> database = _RowsDatabase([{ "store_id": 1 }])
        >>> database.get_row_from_id("stores", 1)
        {'store_id': 1}
    """

    def __init__(self, rows):
        """
        Retain the supplied rows without copying their list or dictionaries.

        Example:
            >>> rows = []
            >>> _RowsDatabase(rows).rows is rows
            True


        :param rows: Mutable row list shared by the fake and test.
        :return: None after retaining the caller-owned rows.
        """
        self.rows = rows

    def get_tables(self):
        """
        Advertise only the Store table so the fake does not claim an Asset catalogue.

        Example:
            >>> _RowsDatabase([]).get_tables()
            ['stores']


        :return: Fresh list containing the stores table name.
        """
        return ["stores"]

    def get_all_rows(self, table: str, *, iterator_return: bool):
        """
        Require the expected materialized Store-table request and return the shared rows.

        Unexpected table names or iterator requests fail assertions instead of being emulated.

        Example:
            >>> database = _RowsDatabase([])
            >>> database.get_all_rows("stores", iterator_return=False) is database.rows
            True


        :param table: Table name, required to equal stores.
        :param iterator_return: Requested iterator mode, required to be the singleton False.
        :return: Original row list, with no snapshot or copy.
        """
        assert table == "stores"
        assert iterator_return is False
        return self.rows

    def get_row_from_id(self, table: str, row_id: int):
        """
        Find the first Store row whose int-converted ID equals the requested value.

        Missing matches return None. Missing or non-convertible row IDs propagate their errors; only
        the table name is asserted.

        Example:
            >>> _RowsDatabase([{ "store_id": "7" }]).get_row_from_id("stores", 7)
            {'store_id': '7'}


        :param table: Table name, required to equal stores.
        :param row_id: ID compared against each row after converting that row ID to int.
        :return: First matching original row dictionary, or None.
        """
        assert table == "stores"
        return next(
            (row for row in self.rows if int(row["store_id"]) == row_id),
            None,
        )


class _RowsMacros:
    """
    Emulate the Store-row update hook by changing a shared dictionary in memory.

    The fake validates the requested table and ID column but provides no database durability or
    transaction behavior.

    Example:
        >>> rows = [{"store_id": 1}]
        >>> _RowsMacros(rows).update_row("stores", 1, {"store_name": "archive"}, id_column="store_id")
        >>> rows[0]["store_name"]
        'archive'
    """

    def __init__(self, rows):
        """
        Retain the same mutable row list exposed by the companion database double.

        Example:
            >>> rows = []
            >>> _RowsMacros(rows).rows is rows
            True


        :param rows: Shared Store rows whose dictionaries receive updates.
        :return: None after storing the original list reference.
        """
        self.rows = rows

    def update_row(self, table, row_id, values, *, id_column=None):
        """
        Merge values into the first row with an exactly equal stored ID.

        Unlike the read helper, this lookup does not convert IDs. Missing matches raise
        StopIteration, and unexpected table or ID-column arguments fail assertions.

        Example:
            >>> rows = [{"store_id": 2}]
            >>> _RowsMacros(rows).update_row("stores", 2, {"store_uuid": "assigned"}, id_column="store_id")
            >>> rows[0]["store_uuid"]
            'assigned'


        :param table: Table name, required to equal stores.
        :param row_id: Value compared directly with each stored store_id.
        :param values: Mapping merged into the matching dictionary with update.
        :param id_column: Lookup column, required to equal store_id despite the None default.
        :return: None after mutating the shared row dictionary.
        """
        assert table == "stores"
        assert id_column == "store_id"
        row = next(row for row in self.rows if row["store_id"] == row_id)
        row.update(values)


class _WritableRowsDatabase(_RowsDatabase):
    """
    Add an in-memory update hook to the minimal Store-table read double.

    Both interfaces share the original rows, allowing tests to observe a generated UUID written
    through the application database adapter.

    Example:
        >>> rows = [{"store_id": 1}]
        >>> database = _WritableRowsDatabase(rows)
        >>> database.macros.rows is database.rows is rows
        True
    """

    def __init__(self, rows):
        """
        Initialize the read interface and attach macros over the same row list.

        Example:
            >>> database = _WritableRowsDatabase([])
            >>> database.macros.rows is database.rows
            True


        :param rows: Shared mutable Store-table rows.
        :return: None after wiring read and update hooks to the same test state.
        """
        super().__init__(rows)
        self.macros = _RowsMacros(rows)


class _IncompleteCatalogueDatabase(_RowsDatabase):
    """
    Advertise an Asset table without the remaining catalogue schema or capabilities.

    This deliberately incomplete database shape drives the refusal of implicit volatile metadata
    fallback.

    Example:
        >>> _IncompleteCatalogueDatabase([]).get_tables()
        ['stores', 'digital_assets']
    """

    def get_tables(self):
        """
        Report Store and Asset table names while leaving the catalogue incomplete.

        Example:
            >>> _IncompleteCatalogueDatabase([]).get_tables()
            ['stores', 'digital_assets']


        :return: Fresh list naming stores and digital_assets only.
        """
        return ["stores", "digital_assets"]


def test_database_catalogue_never_silently_falls_back_to_volatile_metadata(
    tmp_path: Path,
) -> None:
    """
    Require application construction to reject an advertised but incomplete Asset catalogue.

    The minimal table-list fake triggers StorageManagementError rather than silently selecting
    in-memory metadata; no live database is involved.

    Example:
        >>> test_database_catalogue_never_silently_falls_back_to_volatile_metadata(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary-directory fixture retained in the signature but unused by this constructor-only regression.
    :return: None after the stated regression assertions pass.
    """
    database = _IncompleteCatalogueDatabase([])

    with pytest.raises(
        StorageManagementError,
        match="refusing an implicit in-memory fallback",
    ):
        StorageManager(db=database, startup_on_add=False)


def test_database_bootstrap_reports_loaded_skipped_and_failed_configurations(
    tmp_path: Path,
) -> None:
    """
    Account separately for a valid Store, an offline row, and an unknown backend kind.

    The fake rows yield one loaded, one skipped, and one failed configuration in encounter order.
    Startup is disabled; this checks bootstrap reporting and configuration routing without a live
    database.

    Example:
        >>> test_database_bootstrap_reports_loaded_skipped_and_failed_configurations(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary root for filesystem Store paths; catalogue rows and manager metadata are disposable test state.
    :return: None after the stated regression assertions pass.
    """
    good_uuid = uuid4()
    rows = [
        {
            "store_id": 1,
            "store_uuid": str(good_uuid),
            "store_name": "good",
            "store_kind": "filesystem",
            "store_root_uri": (tmp_path / "good").resolve().as_uri(),
            "store_online_status": "online",
        },
        {
            "store_id": 2,
            "store_uuid": str(uuid4()),
            "store_name": "offline",
            "store_kind": "filesystem",
            "store_root_uri": (tmp_path / "offline").resolve().as_uri(),
            "store_online_status": "offline",
        },
        {
            "store_id": 3,
            "store_name": "broken",
            "store_kind": "unknown-kind",
            "store_root_uri": "unknown://broken",
        },
    ]
    manager = StorageManager(db=_RowsDatabase(rows), startup_on_add=False)

    report = manager.load_from_database(startup=False)

    assert report == StorageBootstrapReport(
        discovered_configurations=3,
        loaded_stores=1,
        skipped_configurations=1,
        failed_configurations=1,
        issues=report.issues,
    )
    assert report.ok is False
    assert manager.get_store(good_uuid).configuration.store_uuid == good_uuid
    assert [issue.store_name for issue in report.issues] == ["offline", "broken"]


def test_database_rows_without_uuid_get_stable_derived_identity(tmp_path: Path) -> None:
    """
    Check repeated reads of the same legacy row derive the same UUID.

    The read-only Store-table fake starts without a UUID column value. Two configuration conversions
    establish repeatability, without asserting a particular derivation algorithm or durable write.

    Example:
        >>> test_database_rows_without_uuid_get_stable_derived_identity(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary root for filesystem Store paths; catalogue rows and manager metadata are disposable test state.
    :return: None after the stated regression assertions pass.
    """
    row = {
        "store_id": 42,
        "store_name": "legacy-row",
        "store_kind": "filesystem",
        "store_root_uri": (tmp_path / "legacy").resolve().as_uri(),
    }
    database = _RowsDatabase([row])
    manager = StorageManager(db=database, startup_on_add=False)

    first = manager.get_store_configuration_from_db(42)
    second = manager.get_store_configuration_from_db(42)

    assert isinstance(first.store_uuid, UUID)
    assert first.store_uuid == second.store_uuid


def test_database_bootstrap_persists_a_derived_legacy_store_uuid(
    tmp_path: Path,
) -> None:
    """
    Observe bootstrap write a derived UUID through the fake Store-row update hook.

    The mutated row parses as a UUID and resolves to the loaded Store. Persistence here means the
    shared test dictionary changed; the test does not open or commit a real database.

    Example:
        >>> test_database_bootstrap_persists_a_derived_legacy_store_uuid(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary root for filesystem Store paths; catalogue rows and manager metadata are disposable test state.
    :return: None after the stated regression assertions pass.
    """
    row = {
        "store_id": 43,
        "store_uuid": None,
        "store_name": "legacy-row",
        "store_kind": "filesystem",
        "store_root_uri": (tmp_path / "legacy-persisted").resolve().as_uri(),
    }
    database = _WritableRowsDatabase([row])
    manager = StorageManager(db=database, startup_on_add=False)

    report = manager.load_from_database(startup=False)

    assert report.loaded_stores == 1
    persisted_ref = UUID(str(row["store_uuid"]))
    assert manager.get_store(persisted_ref).store_ref == persisted_ref


def test_database_bound_reload_reconciles_added_changed_and_removed_rows(
    tmp_path: Path,
) -> None:
    """
    Follow reload as mutable Store rows are edited, appended, removed, and marked offline.

    Replacement changes the live facade and configuration; new rows load, deleted rows disappear,
    and the final offline Store becomes unknown. This scenario has no recorded Replicas requiring
    configuration retention.

    Example:
        >>> test_database_bound_reload_reconciles_added_changed_and_removed_rows(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary root for filesystem Store paths; catalogue rows and manager metadata are disposable test state.
    :return: None after the stated regression assertions pass.
    """
    primary_ref = uuid4()
    archive_ref = uuid4()
    primary_row = {
        "store_id": 1,
        "store_uuid": str(primary_ref),
        "store_name": "primary",
        "store_kind": "filesystem",
        "store_root_uri": (tmp_path / "primary-v1").resolve().as_uri(),
        "store_online_status": "online",
    }
    archive_row = {
        "store_id": 2,
        "store_uuid": str(archive_ref),
        "store_name": "archive",
        "store_kind": "filesystem",
        "store_root_uri": (tmp_path / "archive").resolve().as_uri(),
        "store_online_status": "online",
    }
    database = _RowsDatabase([primary_row])
    manager = StorageManager(db=database, startup_on_add=False)

    assert manager.load_from_database(startup=False).loaded_stores == 1
    first_primary = manager.get_store(primary_ref)

    primary_row["store_name"] = "primary-renamed"
    primary_row["store_root_uri"] = (tmp_path / "primary-v2").resolve().as_uri()
    database.rows.append(archive_row)
    changed = manager.reload_stores()

    assert changed.discovered_configurations == 2
    assert changed.loaded_stores == 2
    assert changed.ok
    replacement = manager.get_store(primary_ref)
    assert replacement is not first_primary
    assert replacement.configuration.store_name == "primary-renamed"
    assert replacement.configuration.store_root_uri.endswith("/primary-v2")
    assert manager.get_store(archive_ref).store_ref == archive_ref

    database.rows[:] = [archive_row]
    removed = manager.reload_stores()

    assert removed.loaded_stores == 1
    with pytest.raises(StoreConfigurationNotFound):
        manager.get_store(primary_ref)

    archive_row["store_online_status"] = "offline"
    offline = manager.reload_stores()

    assert offline.skipped_configurations == 1
    assert offline.issues[0].store_ref == archive_ref
    with pytest.raises(StoreConfigurationNotFound):
        manager.get_store(archive_ref)


def test_database_reload_without_replacement_only_loads_new_rows(
    tmp_path: Path,
) -> None:
    """
    Preserve an existing facade and configuration while loading a newly appended row.

    With replacement disabled, an edited existing row contributes a skip and its new name remains
    unapplied. The fake list supplies the database changes.

    Example:
        >>> test_database_reload_without_replacement_only_loads_new_rows(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary root for filesystem Store paths; catalogue rows and manager metadata are disposable test state.
    :return: None after the stated regression assertions pass.
    """
    primary_ref = uuid4()
    archive_ref = uuid4()
    primary_row = {
        "store_id": 1,
        "store_uuid": str(primary_ref),
        "store_name": "primary",
        "store_kind": "filesystem",
        "store_root_uri": (tmp_path / "primary").resolve().as_uri(),
    }
    database = _RowsDatabase([primary_row])
    manager = StorageManager(db=database, startup_on_add=False)
    manager.load_from_database(startup=False)
    original = manager.get_store(primary_ref)

    primary_row["store_name"] = "ignored-until-replacement"
    database.rows.append(
        {
            "store_id": 2,
            "store_uuid": str(archive_ref),
            "store_name": "archive",
            "store_kind": "filesystem",
            "store_root_uri": (tmp_path / "archive").resolve().as_uri(),
        }
    )
    report = manager.reload_stores(replace_existing=False)

    assert report.loaded_stores == 1
    assert report.skipped_configurations == 1
    assert manager.get_store(primary_ref) is original
    assert manager.get_store_configuration(primary_ref).store_name == "primary"
    assert manager.get_store(archive_ref).store_ref == archive_ref


def test_failed_database_replacement_keeps_existing_live_store(
    tmp_path: Path,
) -> None:
    """
    Keep the previous facade and configuration when replacement construction raises.

    A name-selective factory rejects the changed fake row before attachment. The bootstrap report
    retains that failure; this case does not establish rollback after a replacement has been
    attached.

    Example:
        >>> test_failed_database_replacement_keeps_existing_live_store(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary root for filesystem Store paths; catalogue rows and manager metadata are disposable test state.
    :return: None after the stated regression assertions pass.
    """
    store_ref = uuid4()
    row = {
        "store_id": 1,
        "store_uuid": str(store_ref),
        "store_name": "healthy",
        "store_kind": "filesystem",
        "store_root_uri": (tmp_path / "healthy").resolve().as_uri(),
    }
    database = _RowsDatabase([row])

    def factory(configuration: StoreConfiguration) -> FilesystemStore:
        """
        Inject a construction failure for the replacement name and build other filesystem Stores.

        The exception happens before any replacement Store is returned to the manager.

        Example:
            >>> candidate = factory(configuration)  # doctest: +SKIP


        :param configuration: Store configuration whose name selects normal construction or the injected RuntimeError.
        :return: New FilesystemStore for accepted names; the deliberately broken name raises.
        """
        if configuration.store_name == "broken-replacement":
            raise RuntimeError("replacement construction failed")
        return FilesystemStore.from_configuration(configuration)

    manager = StorageManager(
        db=database,
        store_factory=factory,
        startup_on_add=False,
    )
    manager.load_from_database(startup=False)
    original = manager.get_store(store_ref)

    row["store_name"] = "broken-replacement"
    report = manager.reload_stores()

    assert report.failed_configurations == 1
    assert "replacement construction failed" in report.issues[0].reason
    assert manager.get_store(store_ref) is original
    assert manager.get_store_configuration(store_ref).store_name == "healthy"


def test_malformed_database_replacement_keeps_existing_live_store(
    tmp_path: Path,
) -> None:
    """
    Retain an existing Store when its changed database row has no usable root URI.

    The row-to-configuration failure is reported against the Store UUID. The previous facade and
    healthy configuration remain installed.

    Example:
        >>> test_malformed_database_replacement_keeps_existing_live_store(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary root for filesystem Store paths; catalogue rows and manager metadata are disposable test state.
    :return: None after the stated regression assertions pass.
    """
    store_ref = uuid4()
    row = {
        "store_id": 1,
        "store_uuid": str(store_ref),
        "store_name": "healthy",
        "store_kind": "filesystem",
        "store_root_uri": (tmp_path / "healthy").resolve().as_uri(),
    }
    manager = StorageManager(
        db=_RowsDatabase([row]),
        startup_on_add=False,
    )
    manager.load_from_database(startup=False)
    original = manager.get_store(store_ref)

    row["store_root_uri"] = None
    report = manager.reload_stores()

    assert report.failed_configurations == 1
    assert report.issues[0].store_ref == store_ref
    assert manager.get_store(store_ref) is original
    assert manager.get_store_configuration(store_ref).store_name == "healthy"


def test_offline_database_row_retains_configuration_for_live_replica(
    tmp_path: Path,
) -> None:
    """
    Retain configuration metadata but detach the facade for an offline Store with claimed bytes.

    The initial write creates real temporary bytes and an in-memory Replica record. Reload skips the
    offline row, preserves its configuration, and makes Store lookup raise StoreUnavailable.

    Example:
        >>> test_offline_database_row_retains_configuration_for_live_replica(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary root for filesystem Store paths; catalogue rows and manager metadata are disposable test state.
    :return: None after the stated regression assertions pass.
    """
    store_ref = uuid4()
    row = {
        "store_id": 1,
        "store_uuid": str(store_ref),
        "store_name": "primary",
        "store_kind": "filesystem",
        "store_root_uri": (tmp_path / "primary").resolve().as_uri(),
        "store_online_status": "online",
    }
    manager = StorageManager(
        db=_RowsDatabase([row]),
        startup_on_add=True,
    )
    manager.load_from_database(startup=True)
    manager.store_bytes(b"claimed bytes")

    row["store_online_status"] = "offline"
    report = manager.reload_stores()

    assert report.skipped_configurations == 1
    assert manager.get_store_configuration(store_ref).store_uuid == store_ref
    with pytest.raises(StoreUnavailable):
        manager.get_store(store_ref)


def test_unbound_storage_manager_reload_uses_in_memory_configurations(
    tmp_path: Path,
) -> None:
    """
    Recreate a registered Store from in-memory configuration when no database is bound.

    Reload reports one successful load and replaces the original facade object, retaining its
    configured UUID.

    Example:
        >>> test_unbound_storage_manager_reload_uses_in_memory_configurations(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary root for filesystem Store paths; catalogue rows and manager metadata are disposable test state.
    :return: None after the stated regression assertions pass.
    """
    store_ref = uuid4()
    configuration = StoreConfiguration(
        store_uuid=store_ref,
        store_name="primary",
        store_kind="filesystem",
        store_root_uri=(tmp_path / "primary").resolve().as_uri(),
    )
    manager = StorageManager(startup_on_add=False)
    manager.create_store(configuration, startup=False)
    original = manager.get_store(store_ref)

    report = manager.reload_stores()

    assert report.loaded_stores == 1
    assert report.ok
    assert manager.get_store(store_ref) is not original


def test_manager_factory_can_create_configured_store_without_manual_construction(
    tmp_path: Path,
) -> None:
    """
    Create a Store through the manager factory and write seven bytes through the resulting facade.

    The direct Store write supplies a fixed SHA-256 expectation and reports the expected size.
    Creation disables explicit startup; this test checks the subsequent write path, not a manager
    Asset registration.

    Example:
        >>> test_manager_factory_can_create_configured_store_without_manual_construction(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary root for filesystem Store paths; catalogue rows and manager metadata are disposable test state.
    :return: None after the stated regression assertions pass.
    """
    configuration = StoreConfiguration(
        store_uuid=uuid4(),
        store_name="created",
        store_kind="filesystem",
        store_root_uri=(tmp_path / "created").resolve().as_uri(),
    )
    manager = StorageManager(startup_on_add=False)

    manager.create_store(configuration, startup=False)

    store = manager.get_store(configuration.store_uuid)
    assert store.store_ref == configuration.store_uuid
    assert (
        store.store_bytes(
            b"created",
            location="created.bin",
            expected_digest=Digest(
                "sha256",
                "406effb1e9c59672c66a598c2b21e331b23b16c54024e96d6df3e7c173549791",
            ),
        ).size
        == 7
    )
