"""Canonical registry, construction-context, and persistence coverage."""

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


def _configuration(kind: str, root: str, **kwargs) -> api.StoreConfiguration:
    return api.StoreConfiguration(
        store_uuid=kwargs.pop("store_uuid", uuid4()),
        store_name=kwargs.pop("store_name", kind),
        store_kind=kind,
        store_root_uri=root,
        **kwargs,
    )


def test_default_registry_contains_every_supported_backend_family() -> None:
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
        "encrypted",
    } <= kinds
    assert DEFAULT_BACKEND_REGISTRY.canonical_kind("managed-drive") == (
        "on_disk_existing_managed_drive"
    )
    assert DEFAULT_BACKEND_REGISTRY.canonical_kind("S3-compatible") == "s3"


def test_factory_constructs_filesystem_through_registry(tmp_path) -> None:
    configuration = _configuration(
        "file",
        (tmp_path / "files").resolve().as_uri(),
    )

    store = build_store(configuration)

    assert isinstance(store, FilesystemStore)
    assert store.configuration is configuration
    assert store.store_bytes(b"content", location="object.bin").size == 7


def test_s3_factory_uses_injected_client_and_persisted_non_secret_options() -> None:
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


class _RowsDatabase:
    def __init__(self, rows):
        self.rows = rows

    def get_tables(self):
        return ["stores"]

    def get_all_rows(self, table: str, *, iterator_return: bool):
        assert table == "stores"
        assert iterator_return is False
        return self.rows


def test_manager_bootstraps_encrypted_wrapper_after_its_inner_store(tmp_path) -> None:
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


def test_registry_rejects_duplicate_aliases() -> None:
    def builder(configuration, context):
        del configuration, context
        raise AssertionError

    descriptor = StorageBackendDescriptor("one", "One", builder, aliases=("shared",))
    registry = StorageBackendRegistry((descriptor,))

    with pytest.raises(ValueError, match="already registered"):
        registry.register(StorageBackendDescriptor("two", "Two", builder, aliases=("shared",)))
