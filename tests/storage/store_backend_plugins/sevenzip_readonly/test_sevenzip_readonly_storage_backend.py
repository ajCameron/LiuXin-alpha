"""Read-only 7z driver, Store, registry, and Unicode contracts."""

from __future__ import annotations

import pathlib
import os

from uuid import uuid4

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.backend_registry import DEFAULT_BACKEND_REGISTRY
from LiuXin_alpha.storage.store_backend_plugins.sevenzip_readonly import (
    SevenZipReadOnlyStorageBackend,
)
from tests.fixtures.storage_unicode import TORTURED_UNICODE_PATH_CASES
from tests.storage.contracts.unicode_paths import exercise_unicode_path_cases


def _write_7z(path: pathlib.Path, members: dict[str, bytes]) -> None:
    py7zr = pytest.importorskip("py7zr")
    with py7zr.SevenZipFile(path, mode="w") as archive:
        for key, payload in members.items():
            archive.writestr(payload, key)


def test_sevenzip_readonly_preserves_unicode_ranges_and_metadata(tmp_path) -> None:
    path = tmp_path / "unicode.7z"
    members = {case.key: case.payload for case in TORTURED_UNICODE_PATH_CASES}
    _write_7z(path, members)
    store = SevenZipReadOnlyStorageBackend(str(path), name="7z archive")

    results = exercise_unicode_path_cases(store, TORTURED_UNICODE_PATH_CASES)
    status = store.startup()

    assert len(results) == len(TORTURED_UNICODE_PATH_CASES)
    assert status.available
    assert status.object_count == len(members)
    assert dict(status.details)["format"] == "7z"
    assert store.configuration.store_access_protocol == "7z"
    assert store.capabilities.enumeration is api.EnumerationCompleteness.COMPLETE
    assert store.characteristics.limitation("sevenzip_member_reads_spooled")
    with pytest.raises(api.StoreReadOnly):
        store.store_bytes(b"forbidden", location="new.epub")


def test_sevenzip_readonly_enforces_inventory_and_member_limits(tmp_path) -> None:
    path = tmp_path / "bounded.7z"
    _write_7z(path, {"one": b"1", "two": b"22"})

    with pytest.raises(api.StorageUnsupportedOperation, match="inventory exceeds"):
        SevenZipReadOnlyStorageBackend(
            str(path), max_inventory_entries=1
        ).startup()
    with pytest.raises(api.StorageUnsupportedOperation, match="declared size"):
        SevenZipReadOnlyStorageBackend(str(path), max_member_bytes=1).startup()


def test_sevenzip_bounds_total_ratio_and_header_before_extraction(tmp_path) -> None:
    path = tmp_path / "expansion.7z"
    _write_7z(
        path,
        {
            "one.bin": b"0" * 128 * 1024,
            "two.bin": b"1" * 128 * 1024,
        },
    )

    with pytest.raises(api.StorageUnsupportedOperation, match="total expanded size"):
        SevenZipReadOnlyStorageBackend(
            str(path),
            max_total_uncompressed_bytes=200 * 1024,
            max_compression_ratio=10_000,
        ).startup()
    with pytest.raises(api.StorageUnsupportedOperation, match="expansion ratio"):
        SevenZipReadOnlyStorageBackend(
            str(path),
            max_compression_ratio=10,
        ).startup()
    with pytest.raises(api.StorageUnsupportedOperation, match="header exceeds 1"):
        SevenZipReadOnlyStorageBackend(
            str(path),
            max_header_bytes=1,
            max_compression_ratio=10_000,
        ).startup()


@pytest.mark.parametrize(
    "members",
    (
        (("same", b"one"), ("same", b"two")),
        (("node", b"file"), ("node/child", b"child")),
        (("node/child", b"child"), ("node", b"file")),
    ),
)
def test_sevenzip_rejects_duplicate_and_overwrite_topology(
    tmp_path: pathlib.Path,
    members: tuple[tuple[str, bytes], ...],
) -> None:
    py7zr = pytest.importorskip("py7zr")
    path = tmp_path / "ambiguous.7z"
    with py7zr.SevenZipFile(path, mode="w") as archive:
        for key, payload in members:
            archive.writestr(payload, key)

    with pytest.raises(api.StorageIntegrityError, match="duplicate|descends|overwrite"):
        SevenZipReadOnlyStorageBackend(str(path)).startup()


@pytest.mark.skipif(os.name != "posix", reason="symbolic links are a POSIX contract")
def test_sevenzip_rejects_symbolic_link_members(tmp_path: pathlib.Path) -> None:
    py7zr = pytest.importorskip("py7zr")
    source = tmp_path / "source"
    source.mkdir()
    (source / "target").write_bytes(b"target")
    (source / "link").symlink_to("target")
    path = tmp_path / "linked.7z"
    with py7zr.SevenZipFile(path, mode="w") as archive:
        archive.write(source / "link", "link")
        archive.write(source / "target", "target")

    with pytest.raises(api.StorageUnsupportedOperation, match="symbolic-link"):
        SevenZipReadOnlyStorageBackend(str(path)).startup()


def test_sevenzip_safety_policy_is_durable(tmp_path: pathlib.Path) -> None:
    path = tmp_path / "durable.7z"
    _write_7z(path, {"book": b"book"})
    store = SevenZipReadOnlyStorageBackend(
        str(path),
        max_member_bytes=1024,
        max_total_uncompressed_bytes=4096,
        max_compression_ratio=50,
        max_header_bytes=2048,
        max_path_bytes=512,
    )

    options = dict(store.configuration.backend_options)
    assert options["max_total_uncompressed_bytes"] == 4096
    assert options["max_compression_ratio"] == 50.0
    assert options["max_header_bytes"] == 2048
    assert options["max_path_bytes"] == 512
    assert store.characteristics.temporary_space is api.StorageTemporarySpaceRequirement.OBJECT_STAGE
    assert store.characteristics.limitation("bounded_sevenzip_expansion") is not None
    assert store.characteristics.limitation("nested_expansion_budget_external")


def test_sevenzip_missing_optional_dependency_is_actionable(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "dependency.7z"
    path.write_bytes(b"7z\xbc\xaf'\x1c" + bytes(32))

    def unavailable(name: str):
        if name == "py7zr":
            raise ImportError("not installed")
        raise AssertionError(name)

    monkeypatch.setattr(
        "LiuXin_alpha.storage.drivers.sevenzip.importlib.import_module",
        unavailable,
    )

    with pytest.raises(api.StorageUnsupportedOperation, match="archives"):
        SevenZipReadOnlyStorageBackend(str(path)).startup()


def test_sevenzip_rejects_corrupt_and_encrypted_archives(tmp_path) -> None:
    py7zr = pytest.importorskip("py7zr")
    corrupt = tmp_path / "corrupt.7z"
    corrupt.write_bytes(b"7z\xbc\xaf'\x1c" + bytes(32))
    encrypted = tmp_path / "encrypted.7z"
    with py7zr.SevenZipFile(encrypted, mode="w", password="secret") as archive:
        archive.writestr(b"private", "book.epub")

    with pytest.raises(api.StorageIntegrityError):
        SevenZipReadOnlyStorageBackend(str(corrupt)).startup()
    with pytest.raises(api.StorageUnsupportedOperation, match="encrypted"):
        SevenZipReadOnlyStorageBackend(str(encrypted)).startup()


def test_sevenzip_builds_from_registry_with_durable_policy(tmp_path) -> None:
    path = tmp_path / "registry.7z"
    _write_7z(path, {"books/book.epub": b"book"})
    configuration = api.StoreConfiguration(
        store_uuid=uuid4(),
        store_name="7z",
        store_kind="sevenzip_readonly",
        store_root_uri=path.resolve().as_uri(),
        read_only=True,
        backend_options=(
            ("max_inventory_entries", 321),
            ("max_member_bytes", 1024),
            ("max_depth", 12),
        ),
    )

    store = DEFAULT_BACKEND_REGISTRY.build(configuration)

    assert isinstance(store, SevenZipReadOnlyStorageBackend)
    assert store.configuration is configuration
    assert store.read_file("books/book.epub") == b"book"
    assert DEFAULT_BACKEND_REGISTRY.canonical_kind("7z") == "sevenzip_readonly"
