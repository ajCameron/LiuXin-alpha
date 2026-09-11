"""
Exercise backend discovery, runtime construction dependencies, and row translation.

Pure descriptor/serialization checks are combined with inert S3 injection,
synthetic Store-row enumeration, and real local encryption/archive operations.
Readback tests reopen backend objects within the same process. The RAR builder
case checks staged bytes rather than sealing an archive or invoking its tool.
"""

from __future__ import annotations

import json

from uuid import uuid4

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.backend_registry import (
    DEFAULT_BACKEND_REGISTRY,
    StorageBackendDescriptor,
    StorageBackendRegistry,
    StoreConstructionContext,
)
from LiuXin_alpha.storage.store_factory import build_store
from LiuXin_alpha.storage.store_manager import StorageManager
from LiuXin_alpha.storage.store_backend_plugins.iso_readonly import (
    IsoReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.iso_writable import (
    IsoWritableStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.rar_build import (
    RarBuildStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.sevenzip_readonly import (
    SevenZipReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.tar_readonly import (
    TarReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.tar_writable import (
    TarWritableStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.zip_readonly import (
    ZipReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.zip_writable import (
    ZipWritableStorageBackend,
)
from LiuXin_alpha.storage.store_spec_utils import (
    store_configuration_from_row,
    store_configuration_to_row_dict,
)
from LiuXin_alpha.storage.stores import (
    EncryptedStore,
    FilesystemStore,
    S3Store,
    StaticEncryptionKeyProvider,
)
from tests.fixtures.iso_image import build_joliet_iso


def _configuration(kind: str, root: str, **kwargs) -> api.StoreConfiguration:
    """
    Build a test StoreConfiguration with an optional supplied UUID/name and forwarded fields.
    Defaults use a fresh UUID and the kind as name; construction applies ordinary configuration
    validation without creating a backend.

    Example:
        >>> _configuration("filesystem", "file:///srv").store_name
        'filesystem'


    :param kind: Backend kind or alias retained in the configuration.
    :param root: Endpoint URI/text passed as store_root_uri.
    :param kwargs: Additional configuration fields, with store_uuid and store_name consumed as optional overrides.
    :return: A new StoreConfiguration with the requested endpoint and test defaults.
    """
    return api.StoreConfiguration(
        store_uuid=kwargs.pop("store_uuid", uuid4()),
        store_name=kwargs.pop("store_name", kind),
        store_kind=kind,
        store_root_uri=root,
        **kwargs,
    )


def test_default_registry_contains_every_supported_backend_family() -> None:
    """
    Check that the canonical registry contains the expected backend families and resolves selected
    historical/case/hyphen aliases. The family assertion permits additional descriptors; this test
    does not construct Stores or prove backend availability.

    Example:
        >>> test_default_registry_contains_every_supported_backend_family()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    kinds = {descriptor.kind for descriptor in DEFAULT_BACKEND_REGISTRY}

    assert {
        "filesystem",
        "on_disk_existing_managed_drive",
        "on_disk_existing_unmanaged_drive",
        "on_disk_flat",
        "on_disk_calibre_like",
        "single_file_sqlite",
        "http_readonly",
        "native_html_readonly",
        "wget_html_readonly",
        "ftp_readonly",
        "rclone_http_readonly",
        "rclone_writable",
        "s3",
        "squashfs_build",
        "squashfs_readonly",
        "iso_readonly",
        "iso_writable",
        "zip_readonly",
        "zip_writable",
        "tar_readonly",
        "tar_writable",
        "rar_build",
        "rar_readonly",
        "sevenzip_readonly",
        "encrypted",
    } <= kinds
    assert DEFAULT_BACKEND_REGISTRY.canonical_kind("managed-drive") == (
        "on_disk_existing_managed_drive"
    )
    assert DEFAULT_BACKEND_REGISTRY.canonical_kind("S3-compatible") == "s3"
    assert DEFAULT_BACKEND_REGISTRY.canonical_kind("ISO9660") == "iso_readonly"
    assert DEFAULT_BACKEND_REGISTRY.canonical_kind("iso-rw") == "iso_writable"
    assert DEFAULT_BACKEND_REGISTRY.canonical_kind("zip") == "zip_readonly"
    assert DEFAULT_BACKEND_REGISTRY.canonical_kind("tar-rw") == "tar_writable"
    assert DEFAULT_BACKEND_REGISTRY.canonical_kind("rar-seal") == "rar_build"
    assert DEFAULT_BACKEND_REGISTRY.canonical_kind("rar") == "rar_readonly"
    assert DEFAULT_BACKEND_REGISTRY.canonical_kind("7z") == "sevenzip_readonly"
    assert DEFAULT_BACKEND_REGISTRY.canonical_kind("udf") == "iso_readonly"


def test_every_default_backend_advertises_storage_characteristics() -> None:
    """
    Check the exact default backend set and its declared publication, staging, and usage profiles.
    Require archive expansion-limit notes and selected rclone/S3/encryption caveats. These are
    descriptor-contract assertions, not measured capabilities of live endpoints.

    Example:
        >>> test_every_default_backend_advertises_storage_characteristics()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    profiles = {
        descriptor.kind: descriptor.characteristics
        for descriptor in DEFAULT_BACKEND_REGISTRY
    }
    expected_publication_models = {
        "filesystem": api.StoragePublicationModel.PER_OBJECT,
        "on_disk_existing_managed_drive": api.StoragePublicationModel.PER_OBJECT,
        "on_disk_existing_unmanaged_drive": api.StoragePublicationModel.READ_ONLY,
        "on_disk_flat": api.StoragePublicationModel.PER_OBJECT,
        "on_disk_calibre_like": api.StoragePublicationModel.PER_OBJECT,
        "single_file_sqlite": api.StoragePublicationModel.PER_OBJECT,
        "http_readonly": api.StoragePublicationModel.READ_ONLY,
        "native_html_readonly": api.StoragePublicationModel.READ_ONLY,
        "wget_html_readonly": api.StoragePublicationModel.READ_ONLY,
        "ftp_readonly": api.StoragePublicationModel.READ_ONLY,
        "rclone_http_readonly": api.StoragePublicationModel.READ_ONLY,
        "rclone_writable": api.StoragePublicationModel.PER_OBJECT,
        "s3": api.StoragePublicationModel.PER_OBJECT,
        "squashfs_build": api.StoragePublicationModel.STAGING_THEN_SEAL,
        "squashfs_readonly": api.StoragePublicationModel.READ_ONLY,
        "iso_writable": api.StoragePublicationModel.WHOLE_STORE_REBUILD,
        "iso_readonly": api.StoragePublicationModel.READ_ONLY,
        "zip_writable": api.StoragePublicationModel.WHOLE_STORE_REBUILD,
        "zip_readonly": api.StoragePublicationModel.READ_ONLY,
        "tar_writable": api.StoragePublicationModel.WHOLE_STORE_REBUILD,
        "tar_readonly": api.StoragePublicationModel.READ_ONLY,
        "rar_build": api.StoragePublicationModel.STAGING_THEN_SEAL,
        "rar_readonly": api.StoragePublicationModel.READ_ONLY,
        "sevenzip_readonly": api.StoragePublicationModel.READ_ONLY,
        # Wrapper publication mechanics are inherited from the configured inner Store.
        "encrypted": api.StoragePublicationModel.UNKNOWN,
    }

    assert set(profiles) == set(expected_publication_models)
    assert {
        kind: profile.publication_model for kind, profile in profiles.items()
    } == expected_publication_models
    assert all(profile != api.StorageCharacteristics() for profile in profiles.values())

    for descriptor in DEFAULT_BACKEND_REGISTRY:
        profile = descriptor.characteristics
        if descriptor.read_only_default:
            expected_temporary_space = (
                api.StorageTemporarySpaceRequirement.OBJECT_STAGE
                if descriptor.kind
                in {
                    "iso_readonly",
                    "rar_readonly",
                    "sevenzip_readonly",
                    "squashfs_readonly",
                }
                else api.StorageTemporarySpaceRequirement.NONE
            )
            assert profile.temporary_space is expected_temporary_space
            assert profile.recommended_write_usage is api.StorageWriteUsage.NOT_APPLICABLE
        elif descriptor.kind != "encrypted":
            assert profile.temporary_space is not api.StorageTemporarySpaceRequirement.UNKNOWN
            assert profile.recommended_write_usage is not api.StorageWriteUsage.UNKNOWN

    for kind in {
        "iso_readonly",
        "iso_writable",
        "rar_build",
        "rar_readonly",
        "sevenzip_readonly",
        "squashfs_build",
        "squashfs_readonly",
        "tar_readonly",
        "tar_writable",
        "zip_readonly",
        "zip_writable",
    }:
        assert profiles[kind].limitation("nested_expansion_budget_external")

    assert profiles["rclone_writable"].limitation(
        "rclone_backend_dependent_limits"
    ) is not None
    assert profiles["s3"].limitation("s3_service_limits_apply") is not None
    assert profiles["encrypted"].limitation("inner_store_dependent") is not None


def test_factory_constructs_filesystem_through_registry(tmp_path) -> None:
    """
    Resolve the file alias through the factory, retain configuration identity, and publish seven
    bytes into a real temporary filesystem Store. The assertion checks the returned size, without an
    independent readback in this test.

    Example:
        >>> test_factory_constructs_filesystem_through_registry(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary root for the real filesystem Store and object bytes.
    :return: None after the stated regression assertions pass.
    """
    configuration = _configuration(
        "file",
        (tmp_path / "files").resolve().as_uri(),
    )

    store = build_store(configuration)

    assert isinstance(store, FilesystemStore)
    assert store.configuration is configuration
    assert store.store_bytes(b"content", location="object.bin").size == 7


def test_s3_factory_uses_injected_client_and_persisted_non_secret_options() -> None:
    """
    Construct an S3 Store with an inert injected client and configuration options. Verify
    Store/configuration/client identity and endpoint projection without invoking S3 methods, making
    a network request, or persisting a database row.

    Example:
        >>> test_s3_factory_uses_injected_client_and_persisted_non_secret_options()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    client = object()
    configuration = _configuration(
        "s3",
        "s3://library/books",
        backend_options=(
            ("endpoint_url", "https://objects.example"),
            ("region_name", "eu-west-2"),
        ),
    )

    store = build_store(
        configuration,
        context=StoreConstructionContext(s3_client=client),
    )

    assert isinstance(store, S3Store)
    assert store.configuration is configuration
    assert store.options.endpoint_url == "https://objects.example"
    assert store.driver._client is client


def test_encrypted_factory_requires_runtime_dependencies(tmp_path) -> None:
    """
    Reject missing inner-Store and key-provider dependencies, then construct an encrypted wrapper
    over a real filesystem Store. Publish a payload and check its reported size and prefixed
    physical-object existence; this test does not independently decrypt it.

    Example:
        >>> test_encrypted_factory_requires_runtime_dependencies(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary root for the inner Store and encrypted bytes.
    :return: None after the stated regression assertions pass.
    """
    inner = FilesystemStore(tmp_path / "inner")
    configuration = _configuration(
        "encrypted",
        f"encrypted://{inner.store_ref}",
        backend_options=(
            ("inner_store_uuid", str(inner.store_ref)),
            ("key_id", "primary"),
            ("chunk_size", 4096),
            ("inner_prefix", "private"),
        ),
    )
    provider = StaticEncryptionKeyProvider(
        {"primary": b"p" * 32},
        active_key_id="primary",
    )

    with pytest.raises(api.StoreUnsupportedOperation, match="inner-Store resolver"):
        build_store(configuration)
    with pytest.raises(api.StoreUnsupportedOperation, match="key provider"):
        build_store(
            configuration,
            context=StoreConstructionContext(store_resolver=lambda _ref: inner),
        )

    store = build_store(
        configuration,
        context=StoreConstructionContext(
            store_resolver=lambda store_ref: (
                inner if store_ref == inner.store_ref else None
            ),
            encryption_key_provider=provider,
        ),
    )
    assert isinstance(store, EncryptedStore)
    assert store.configuration is configuration
    assert store.store_bytes(b"private", location="book.epub").size == 7
    assert inner.file_exists("private/book.epub")


def test_backend_policy_round_trip_handles_new_backends_and_strips_secrets() -> None:
    """
    Round-trip selected backend option sets through row dictionaries and policy JSON. Verify the
    supplied secret_access_key/session_token values are absent and supported options survive across
    the backend matrix. This is in-memory serialization coverage, not exhaustive secret detection or
    a database/backend operation.

    Example:
        >>> test_backend_policy_round_trip_handles_new_backends_and_strips_secrets()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    for kind, options in (
        (
            "s3",
            (
                ("endpoint_url", "https://objects.example"),
                ("region_name", "eu-west-2"),
                ("secret_access_key", "must-not-persist"),
                ("session_token", "must-not-persist-either"),
            ),
        ),
        (
            "rclone_writable",
            (("rclone_exe", "rclone-custom"), ("timeout_s", 20.0)),
        ),
        (
            "http_readonly",
            (("max_inventory_entries", 4321), ("timeout_s", 12.0)),
        ),
        (
            "ftp_readonly",
            (
                ("max_directory_entries", 321),
                ("max_inventory_entries", 654),
            ),
        ),
        (
            "iso_writable",
            (
                ("volume_id", "BOOK_ARCHIVE"),
                ("include_joliet", True),
                ("deterministic", True),
                ("max_inventory_entries", 4321),
                ("max_udf_member_bytes", 16 * 1024 * 1024),
                ("max_total_uncompressed_bytes", 64 * 1024 * 1024),
                ("max_logical_expansion_ratio", 250.0),
                ("max_path_bytes", 8192),
            ),
        ),
        (
            "iso_readonly",
            (
                ("max_inventory_entries", 4321),
                ("max_directory_bytes", 8 * 1024 * 1024),
                ("max_depth", 64),
                ("max_susp_bytes", 256 * 1024),
                ("max_udf_member_bytes", 16 * 1024 * 1024),
                ("max_total_uncompressed_bytes", 64 * 1024 * 1024),
                ("max_logical_expansion_ratio", 250.0),
                ("max_path_bytes", 8192),
                ("enable_udf", True),
            ),
        ),
        (
            "zip_writable",
            (
                ("compression", "deflated"),
                ("compresslevel", 6),
                ("deterministic", True),
                ("max_inventory_entries", 4321),
                ("max_total_uncompressed_bytes", 8 * 1024 * 1024),
                ("max_compression_ratio", 250.0),
                ("max_central_directory_bytes", 1024 * 1024),
            ),
        ),
        (
            "zip_readonly",
            (
                ("max_inventory_entries", 4321),
                ("max_depth", 64),
                ("max_total_uncompressed_bytes", 8 * 1024 * 1024),
                ("max_compression_ratio", 250.0),
                ("max_central_directory_bytes", 1024 * 1024),
            ),
        ),
        (
            "tar_writable",
            (
                ("compression", "xz"),
                ("deterministic", True),
                ("max_inventory_entries", 4321),
                ("max_member_bytes", 16 * 1024 * 1024),
                ("max_total_uncompressed_bytes", 64 * 1024 * 1024),
                ("max_compression_ratio", 250.0),
                ("max_metadata_bytes", 8 * 1024 * 1024),
                ("max_single_metadata_record_bytes", 1024 * 1024),
            ),
        ),
        (
            "tar_readonly",
            (
                ("max_inventory_entries", 4321),
                ("max_depth", 64),
                ("max_member_bytes", 16 * 1024 * 1024),
                ("max_total_uncompressed_bytes", 64 * 1024 * 1024),
                ("max_compression_ratio", 250.0),
                ("max_metadata_bytes", 8 * 1024 * 1024),
                ("max_single_metadata_record_bytes", 1024 * 1024),
            ),
        ),
        (
            "rar_build",
            (
                ("rar_exe", "rar-custom"),
                ("compression_level", 4),
                ("command_timeout_s", 90.0),
                ("staging_root", "/durable/rar-stage"),
                ("max_inventory_entries", 4321),
                ("max_member_bytes", 16 * 1024 * 1024),
                ("max_total_uncompressed_bytes", 64 * 1024 * 1024),
                ("max_compression_ratio", 250.0),
                ("max_path_bytes", 8192),
            ),
        ),
        (
            "rar_readonly",
            (
                ("extractor_exe", "unrar"),
                ("extract_timeout_s", 45.0),
                ("max_inventory_entries", 4321),
                ("max_member_bytes", 16 * 1024 * 1024),
                ("max_total_uncompressed_bytes", 64 * 1024 * 1024),
                ("max_compression_ratio", 250.0),
                ("max_path_bytes", 8192),
            ),
        ),
        (
            "sevenzip_readonly",
            (
                ("max_inventory_entries", 4321),
                ("max_member_bytes", 16 * 1024 * 1024),
                ("max_depth", 64),
                ("max_total_uncompressed_bytes", 64 * 1024 * 1024),
                ("max_compression_ratio", 250.0),
                ("max_header_bytes", 8 * 1024 * 1024),
                ("max_path_bytes", 8192),
            ),
        ),
        (
            "encrypted",
            (("inner_store_uuid", str(uuid4())), ("key_id", "key-v1")),
        ),
    ):
        configuration = _configuration(kind, f"test://{kind}", backend_options=options)
        row = store_configuration_to_row_dict(configuration)
        policy = json.loads(row["store_policy_json"])
        rendered = json.dumps(policy)

        assert "must-not-persist" not in rendered
        restored = store_configuration_from_row(row)
        assert dict(restored.backend_options) == {
            key: value
            for key, value in options
            if key not in {"secret_access_key", "session_token"}
        }


def test_store_configuration_roundtrips_backing_and_extended_replica_modes() -> None:
    """
    Round-trip an Asset-backed archive configuration through scalar fields and the version-1 manager
    policy extension. Assert complete configuration equality and exact backing/mode JSON while
    leaving referenced catalogue identities unresolved.

    Example:
        >>> test_store_configuration_roundtrips_backing_and_extended_replica_modes()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    materialization_ref = uuid4()
    backing = api.StoreBackingReference(
        api.DigitalAssetID(7),
        preferred_replica_id=api.ReplicaID(12),
        materialization_store_ref=materialization_ref,
    )
    configuration = api.StoreConfiguration.for_backend(
        "nested archive",
        "zip_readonly",
        "asset://digital-asset/7",
        read_only=True,
        modes=(api.ReplicaMode.ARCHIVE, api.ReplicaMode.UNMANAGED),
        options={"max_inventory_entries": 123},
        backing=backing,
    )

    row = store_configuration_to_row_dict(configuration)
    restored = store_configuration_from_row(row)

    assert restored == configuration
    policy = json.loads(row["store_policy_json"])
    assert policy["_liuxin_storage"] == {
        "version": 1,
        "replica_modes": ["archive", "unmanaged"],
        "backing": {
            "digital_asset_id": 7,
            "preferred_replica_id": 12,
            "materialization_store_uuid": str(materialization_ref),
        },
    }


def test_registry_rejects_writable_backend_over_a_catalogued_asset() -> None:
    """
    Reject a writable ZIP descriptor for a read-only Asset-backed configuration even with a supplied
    backing-path resolver. The expected registry error precedes image access; no archive or
    catalogue Asset is created.

    Example:
        >>> test_registry_rejects_writable_backend_over_a_catalogued_asset()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    configuration = api.StoreConfiguration.for_backed_backend(
        "mutable archive",
        "zip_writable",
        api.DigitalAssetID(7),
    )

    with pytest.raises(
        api.StoreUnsupportedOperation,
        match="cannot expose a read-only Store backed by a Digital Asset",
    ):
        build_store(
            configuration,
            context=StoreConstructionContext(
                backing_path_resolver=lambda _configuration: "/tmp/archive.zip"
            ),
        )


class _RowsDatabase:
    """
    Expose borrowed Store rows through the minimal manager-bootstrap interface. This double
    advertises only the stores table and returns the same row collection on each non-iterator read.
    It supplies no real database, transaction, schema migration, or durable manager metadata.
    Archive tests pair these synthetic rows with real temporary local bytes.

    Example:
        >>> rows = [{"store_id": 1}]
        >>> _RowsDatabase(rows).get_all_rows("stores", iterator_return=False) is rows
        True
    """
    def __init__(self, rows):
        """
        Retain the supplied row collection by reference without copying or validation. Mutations to
        that collection remain visible to later manager reads.

        Example:
            >>> database = _RowsDatabase([])


        :param rows: Synthetic stores-row collection returned unchanged by get_all_rows.
        :return: None after retaining rows.
        """
        self.rows = rows

    def get_tables(self):
        """
        Advertise the stores-only schema used by these bootstrap tests. The returned list is fresh;
        no backend introspection or migration occurs.

        Example:
            >>> _RowsDatabase([]).get_tables()
            ['stores']


        :return: A new list containing only the stores table name.
        """
        return ["stores"]

    def get_all_rows(self, table: str, *, iterator_return: bool):
        """
        Assert that the manager requests stores with iterator_return=False, then return the borrowed
        row collection. This checks the selected enumeration contract without reading a database or
        creating a snapshot.

        Example:
            >>> _RowsDatabase([]).get_all_rows("stores", iterator_return=False)
            []


        :param table: Requested table name, asserted to equal stores.
        :param iterator_return: Requested iterator flag, asserted to be exactly False.
        :return: The original rows object after assertions pass.
        """
        assert table == "stores"
        assert iterator_return is False
        return self.rows


def test_manager_bootstraps_encrypted_wrapper_after_its_inner_store(tmp_path) -> None:
    """
    Load synthetic rows listed wrapper-first and verify dependency-aware construction. Real
    filesystem encryption must round-trip the plaintext through the wrapper, publish under its
    configured prefix, and omit that plaintext from the physical representation. The row double
    supplies no durable database or process-restart evidence.

    Example:
        >>> test_manager_bootstraps_encrypted_wrapper_after_its_inner_store(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary physical Store root used for actual encrypted writes and reads.
    :return: None after the stated regression assertions pass.
    """
    inner_ref = uuid4()
    encrypted_ref = uuid4()
    rows = [
        {
            "store_id": 2,
            "store_uuid": str(encrypted_ref),
            "store_name": "encrypted",
            "store_kind": "encrypted",
            "store_root_uri": f"encrypted://{inner_ref}",
            "store_policy_json": json.dumps(
                {
                    "backend": "encrypted",
                    "encrypted": {
                        "inner_store_uuid": str(inner_ref),
                        "key_id": "primary",
                        "chunk_size": 4096,
                        "inner_prefix": "vault",
                    },
                }
            ),
        },
        {
            "store_id": 1,
            "store_uuid": str(inner_ref),
            "store_name": "physical",
            "store_kind": "filesystem",
            "store_root_uri": (tmp_path / "physical").resolve().as_uri(),
        },
    ]
    manager = StorageManager(
        db=_RowsDatabase(rows),
        startup_on_add=False,
        encryption_key_provider=StaticEncryptionKeyProvider(
            {"primary": b"p" * 32},
            active_key_id="primary",
        ),
    )

    report = manager.load_from_database(startup=False)

    assert report.ok
    encrypted = manager.get_store(encrypted_ref)
    assert isinstance(encrypted, EncryptedStore)
    stored = encrypted.store_bytes(b"secret book", location="books/book.epub")
    assert encrypted.read_file(stored) == b"secret book"
    physical = manager.get_store(inner_ref)
    assert physical.file_exists("vault/books/book.epub")
    assert b"secret book" not in physical.read_file("vault/books/book.epub")


def test_manager_bootstraps_iso_backend_from_database_row(tmp_path) -> None:
    """
    Construct a read-only ISO Store from a synthetic row and read known member bytes from a real
    generated Joliet image. Manager bootstrap uses the row double without a real database, while
    archive parsing and byte readback use the actual backend.

    Example:
        >>> test_manager_bootstraps_iso_backend_from_database_row(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary directory holding the generated Joliet image.
    :return: None after the stated regression assertions pass.
    """
    store_ref = uuid4()
    image = build_joliet_iso(
        tmp_path / "database-library.iso",
        {"books/database.epub": b"database ISO"},
    )
    manager = StorageManager(
        db=_RowsDatabase(
            [
                {
                    "store_id": 1,
                    "store_uuid": str(store_ref),
                    "store_name": "ISO archive",
                    "store_kind": "iso_readonly",
                    "store_root_uri": image.resolve().as_uri(),
                    "store_access_protocol": "iso",
                    "store_is_read_only": 1,
                }
            ]
        ),
        startup_on_add=False,
    )

    report = manager.load_from_database(startup=False)
    store = manager.get_store(store_ref)

    assert report.ok
    assert isinstance(store, IsoReadOnlyStorageBackend)
    assert store.read_file("books/database.epub") == b"database ISO"


def test_manager_bootstraps_sevenzip_backend_from_database_row(tmp_path) -> None:
    """
    Build a real 7z fixture, bootstrap its Store from a synthetic row, and verify member readback
    plus retained parser limits. Skip when py7zr is unavailable; successful execution exercises
    local archive bytes without a real database or external service.

    Example:
        >>> test_manager_bootstraps_sevenzip_backend_from_database_row(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary directory for the 7z archive created by py7zr.
    :return: None after the stated regression assertions pass.
    """
    py7zr = pytest.importorskip("py7zr")
    store_ref = uuid4()
    archive = tmp_path / "database-library.7z"
    with py7zr.SevenZipFile(archive, mode="w") as target:
        target.writestr(b"database 7z", "books/database.epub")
    manager = StorageManager(
        db=_RowsDatabase(
            [
                {
                    "store_id": 1,
                    "store_uuid": str(store_ref),
                    "store_name": "7z archive",
                    "store_kind": "sevenzip_readonly",
                    "store_root_uri": archive.resolve().as_uri(),
                    "store_access_protocol": "7z",
                    "store_is_read_only": 1,
                    "store_policy_json": json.dumps(
                        {
                            "backend": "sevenzip_readonly",
                            "sevenzip_readonly": {
                                "max_inventory_entries": 123,
                                "max_member_bytes": 1024,
                                "max_depth": 16,
                            },
                        }
                    ),
                }
            ]
        ),
        startup_on_add=False,
    )

    report = manager.load_from_database(startup=False)
    store = manager.get_store(store_ref)

    assert report.ok
    assert isinstance(store, SevenZipReadOnlyStorageBackend)
    assert store.read_file("books/database.epub") == b"database 7z"
    assert dict(store.configuration.backend_options) == {
        "max_depth": 16,
        "max_inventory_entries": 123,
        "max_member_bytes": 1024,
    }


def test_manager_bootstraps_writable_iso_backend_from_database_row(tmp_path) -> None:
    """
    Bootstrap a writable ISO Store from a synthetic row, publish a member, and read it through both
    the original Store and a newly constructed read-only backend. This verifies real image
    persistence across backend reopen within one process.

    Example:
        >>> test_manager_bootstraps_writable_iso_backend_from_database_row(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary directory for the new ISO image and actual publication/readback.
    :return: None after the stated regression assertions pass.
    """
    store_ref = uuid4()
    image = tmp_path / "database-writable.iso"
    manager = StorageManager(
        db=_RowsDatabase(
            [
                {
                    "store_id": 1,
                    "store_uuid": str(store_ref),
                    "store_name": "Writable ISO archive",
                    "store_kind": "iso_writable",
                    "store_root_uri": image.resolve().as_uri(),
                    "store_access_protocol": "iso-write",
                    "store_is_read_only": 0,
                    "store_policy_json": json.dumps(
                        {
                            "backend": "iso_writable",
                            "iso_writable": {
                                "volume_id": "DATABASE_BOOKS",
                                "deterministic": True,
                            },
                        }
                    ),
                }
            ]
        ),
        startup_on_add=False,
    )

    report = manager.load_from_database(startup=False)
    store = manager.get_store(store_ref)

    assert report.ok
    assert isinstance(store, IsoWritableStorageBackend)
    stored = store.store_bytes(b"database ISO", location="books/database.epub")
    assert store.read_file(stored) == b"database ISO"
    reopened = IsoReadOnlyStorageBackend(str(image))
    assert reopened.read_file("books/database.epub") == b"database ISO"


@pytest.mark.parametrize(
    ("kind", "protocol", "backend_type", "suffix", "policy"),
    (
        (
            "zip_writable",
            "zip-write",
            ZipWritableStorageBackend,
            ".zip",
            {"compression": "stored", "deterministic": True},
        ),
        (
            "tar_writable",
            "tar-write",
            TarWritableStorageBackend,
            ".tar.gz",
            {"compression": "gz", "deterministic": True},
        ),
    ),
)
def test_manager_bootstraps_writable_archive_backends_from_database_rows(
    tmp_path,
    kind,
    protocol,
    backend_type,
    suffix,
    policy,
) -> None:
    """
    Bootstrap each parametrized writable ZIP/TAR backend from a synthetic row and policy. Publish
    bytes, read through the writer, then reopen a separate read-only backend and verify the member
    again. Real archive persistence is tested within one process; the rows themselves are supplied
    by a non-durable database double.

    Example:
        >>> test_manager_bootstraps_writable_archive_backends_from_database_rows(tmp_path, kind, protocol, backend_type, suffix, policy)  # doctest: +SKIP


    :param tmp_path: Temporary root for the actual archive file.
    :param kind: Parametrized zip_writable or tar_writable registry key.
    :param protocol: Persisted access-protocol label used in the synthetic row.
    :param backend_type: Expected concrete writable Store class after bootstrap.
    :param suffix: Archive filename suffix selecting the case's local output path.
    :param policy: Backend-specific compression/determinism options encoded into row policy JSON.
    :return: None after the stated regression assertions pass.
    """
    store_ref = uuid4()
    archive = tmp_path / f"database{suffix}"
    manager = StorageManager(
        db=_RowsDatabase(
            [
                {
                    "store_id": 1,
                    "store_uuid": str(store_ref),
                    "store_name": f"{kind} archive",
                    "store_kind": kind,
                    "store_root_uri": archive.resolve().as_uri(),
                    "store_access_protocol": protocol,
                    "store_is_read_only": 0,
                    "store_policy_json": json.dumps(
                        {"backend": kind, kind: policy}
                    ),
                }
            ]
        ),
        startup_on_add=False,
    )

    report = manager.load_from_database(startup=False)
    store = manager.get_store(store_ref)

    assert report.ok
    assert isinstance(store, backend_type)
    stored = store.store_bytes(b"database archive", location="books/book.epub")
    assert store.read_file(stored) == b"database archive"
    reader_type = (
        ZipReadOnlyStorageBackend
        if kind == "zip_writable"
        else TarReadOnlyStorageBackend
    )
    reopened = reader_type(str(archive))
    assert reopened.read_file("books/book.epub") == b"database archive"


def test_manager_bootstraps_durable_rar_builder_from_database_row(tmp_path) -> None:
    """
    Bootstrap a RAR build Store with explicit staging and tool options from a synthetic row. Verify
    identity/path/options and real staged member write/readback. The configured rar-custom
    executable is not invoked and no sealed RAR image is produced here.

    Example:
        >>> test_manager_bootstraps_durable_rar_builder_from_database_row(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary archive pathname and persistent staging root used by the builder.
    :return: None after the stated regression assertions pass.
    """
    store_ref = uuid4()
    archive = tmp_path / "database-backup.rar"
    staging = tmp_path / "database-backup-stage"
    manager = StorageManager(
        db=_RowsDatabase(
            [
                {
                    "store_id": 1,
                    "store_uuid": str(store_ref),
                    "store_name": "RAR build archive",
                    "store_kind": "rar_build",
                    "store_root_uri": archive.resolve().as_uri(),
                    "store_access_protocol": "rar-build",
                    "store_is_read_only": 0,
                    "store_policy_json": json.dumps(
                        {
                            "backend": "rar_build",
                            "rar_build": {
                                "rar_exe": "rar-custom",
                                "compression_level": 4,
                                "command_timeout_s": 90.0,
                                "staging_root": str(staging),
                            },
                        }
                    ),
                }
            ]
        ),
        startup_on_add=False,
    )

    report = manager.load_from_database(startup=False)
    store = manager.get_store(store_ref)

    assert report.ok
    assert isinstance(store, RarBuildStorageBackend)
    assert store.staging_root == staging.resolve()
    assert store.archive_path == archive.resolve()
    assert store.configuration.store_uuid == store_ref
    assert dict(store.configuration.backend_options) == {
        "command_timeout_s": 90.0,
        "compression_level": 4,
        "rar_exe": "rar-custom",
        "staging_root": str(staging),
    }
    stored = store.store_bytes(b"database RAR", location="books/book.epub")
    assert store.read_file(stored) == b"database RAR"


def test_registry_rejects_duplicate_aliases() -> None:
    """
    Reject registration of a second descriptor claiming an existing alias. An always-failing builder
    ensures descriptor registration does not construct a Store; this assertion checks the raised
    collision error without testing later registry reuse.

    Example:
        >>> test_registry_rejects_duplicate_aliases()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    def builder(configuration, context):
        """
        Fail if a construction callback is invoked during alias registration. Registration should
        inspect descriptor names without constructing a Store; the ignored arguments only satisfy
        the builder signature.

        Example:
            >>> builder(configuration, context)  # doctest: +SKIP


        :param configuration: Unused Store configuration that would indicate unexpected construction.
        :param context: Unused runtime construction context.
        :return: Never returns normally; always raises AssertionError.
        """
        del configuration, context
        raise AssertionError

    descriptor = StorageBackendDescriptor("one", "One", builder, aliases=("shared",))
    registry = StorageBackendRegistry((descriptor,))

    with pytest.raises(ValueError, match="already registered"):
        registry.register(StorageBackendDescriptor("two", "Two", builder, aliases=("shared",)))
