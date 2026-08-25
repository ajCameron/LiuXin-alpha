"""Atomic writable ISO driver, Store, interoperability, and Unicode coverage."""

from __future__ import annotations

import hashlib
import os
import pathlib
import shutil
import subprocess

from concurrent.futures import ThreadPoolExecutor
from uuid import UUID

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.backend_registry import DEFAULT_BACKEND_REGISTRY
from LiuXin_alpha.storage.drivers.iso import IsoStorageDriver
from LiuXin_alpha.storage.drivers.iso_writer import (
    WritableIsoStorageDriver,
    _IsoImageWriter,
)
from LiuXin_alpha.storage.store_backend_plugins.iso_readonly import (
    IsoReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.iso_writable import (
    IsoWritableStorageBackend,
)
from tests.fixtures.iso_image import (
    BLOCK_SIZE,
    build_iso9660_iso,
    build_joliet_iso,
    build_rock_ridge_iso,
)
from tests.fixtures.storage_unicode import (
    POSIX_BAD_BYTES_FILENAME,
    POSIX_BAD_BYTES_FILENAME_BYTES,
    POSIX_BAD_BYTES_PAYLOAD,
    TORTURED_UNICODE_PATH_CASES,
)
from tests.storage.contracts.unicode_paths import exercise_unicode_path_cases


def test_writable_iso_creates_valid_empty_image_and_reports_capabilities(tmp_path) -> None:
    image = tmp_path / "library.iso"
    store = IsoWritableStorageBackend(
        str(image),
        name="Mutable archive",
        volume_id="Book archive",
        deterministic=True,
    )

    status = store.startup()
    descriptor = DEFAULT_BACKEND_REGISTRY.descriptor("iso-rw")

    assert image.is_file()
    assert status.available is True
    assert status.writable is True
    assert status.object_count == 0
    assert dict(status.details)["namespace"] == "rock-ridge"
    assert dict(status.details)["publication"] == "atomic_whole_image_rebuild"
    assert store.capabilities.create is True
    assert store.capabilities.replace is True
    assert store.capabilities.delete is True
    assert store.capabilities.atomic_publish is True
    assert store.configuration.store_kind == "iso_writable"
    assert store.configuration.store_access_protocol == "iso-write"
    assert dict(store.configuration.backend_options)["volume_id"] == "BOOK_ARCHIVE"
    assert descriptor.kind == "iso_writable"
    assert descriptor.read_only_default is False
    assert descriptor.supports_delete is True
    assert descriptor.supports_random_write is False
    assert (
        descriptor.characteristics.publication_model
        is api.StoragePublicationModel.WHOLE_STORE_REBUILD
    )
    assert store.characteristics == store.driver.storage_characteristics
    assert store.characteristics.max_object_bytes == (1 << 32) - 1
    assert store.characteristics.max_component_bytes == 255
    assert store.characteristics.max_path_depth == 256
    assert (
        store.characteristics.recommended_write_usage
        is api.StorageWriteUsage.ARCHIVAL_SNAPSHOT
    )
    assert store.characteristics.limitation("whole_store_rebuild") is not None
    assert store.characteristics.limitation("nested_expansion_budget_external")


def test_registry_preserves_configured_read_only_policy_for_writable_iso(tmp_path) -> None:
    image = build_joliet_iso(
        tmp_path / "policy.iso",
        {"book.epub": b"book"},
    )
    configuration = api.StoreConfiguration(
        store_uuid=UUID(int=2),
        store_name="Policy-pinned ISO",
        store_kind="iso_writable",
        store_root_uri=image.resolve().as_uri(),
        store_access_protocol="iso-write",
        read_only=True,
    )

    store = DEFAULT_BACKEND_REGISTRY.build(configuration)

    assert isinstance(store, IsoWritableStorageBackend)
    assert store.configuration is configuration
    assert store.capabilities.create is False
    assert (
        store.characteristics.publication_model
        is api.StoragePublicationModel.READ_ONLY
    )
    assert (
        store.characteristics.temporary_space
        is api.StorageTemporarySpaceRequirement.NONE
    )
    assert store.startup().writable is False
    assert store.read_file("book.epub") == b"book"
    with pytest.raises(api.StoreReadOnly):
        store.store_bytes(b"new", location="new.epub")


def test_registry_restores_durable_lossy_rebuild_policy(tmp_path) -> None:
    image = tmp_path / "configured.iso"
    configuration = api.StoreConfiguration(
        store_uuid=UUID(int=3),
        store_name="Configured ISO",
        store_kind="iso_writable",
        store_root_uri=image.resolve().as_uri(),
        backend_options=(
            ("create_image", True),
            ("allow_lossy_rebuild", True),
        ),
    )

    store = DEFAULT_BACKEND_REGISTRY.build(configuration)

    assert store.configuration is configuration
    assert dict(store.startup().details)["allow_lossy_rebuild"] == "true"


def test_writable_iso_commit_reads_ranges_and_survives_reopen(tmp_path) -> None:
    image = tmp_path / "library.iso"
    store = IsoWritableStorageBackend(str(image))

    stored = store.store_bytes(b"EPUB-DATA", location="books/novel.epub")
    store.store_bytes(b"", location="books/empty.bin")
    store.store_bytes(b"x" * 5000, location="books/multiblock.bin")
    store.store_bytes(b"literal", location="folder;1/book;1")

    assert stored.size == 9
    assert stored.version is not None and stored.version.startswith("iso:")
    assert store.read_file(stored) == b"EPUB-DATA"
    assert store.read_file(stored, offset=2, length=4) == b"UB-D"
    assert {location.key for location in store.iter_locations()} == {
        "books/empty.bin",
        "books/multiblock.bin",
        "books/novel.epub",
        "folder;1/book;1",
    }
    assert store.read_file("books/empty.bin") == b""
    assert store.read_file("books/multiblock.bin", offset=2040, length=32) == b"x" * 32
    assert store.read_file("folder;1/book;1") == b"literal"
    readonly = IsoReadOnlyStorageBackend(str(image))
    assert readonly.read_file("books/novel.epub") == b"EPUB-DATA"
    assert readonly.read_file("folder;1/book;1") == b"literal"
    writable = IsoWritableStorageBackend(str(image), create_image=False)
    assert writable.read_file("books/novel.epub") == b"EPUB-DATA"


def test_writable_iso_supports_generic_copy_move_and_empty_snapshot(tmp_path) -> None:
    image = tmp_path / "library.iso"
    store = IsoWritableStorageBackend(str(image))
    source = store.store_bytes(b"book", location="source/book.epub")

    copied = store.copy(source.location, store.locate("copies/book.epub"))
    moved = store.move(copied.location, store.locate("moved/book.epub"))

    assert store.read_file(source) == b"book"
    assert store.read_file(moved) == b"book"
    assert store.file_exists(copied.location) is False
    store.delete_file(source.location)
    store.delete_file(moved.location)
    assert list(store.iter_locations()) == []
    assert list(IsoReadOnlyStorageBackend(str(image)).iter_locations()) == []


def test_writable_iso_enforces_create_replace_upsert_and_delete(tmp_path) -> None:
    store = IsoWritableStorageBackend(str(tmp_path / "library.iso"))
    original = store.store_bytes(b"one", location="book.txt")

    with pytest.raises(api.StoreAlreadyExists):
        store.store_bytes(b"two", location="book.txt")
    with pytest.raises(api.StoreNotFound):
        store.store_bytes(
            b"missing",
            location="missing.txt",
            write_mode=api.WriteMode.REPLACE,
        )

    replaced = store.store_bytes(
        b"two",
        location="book.txt",
        write_mode=api.WriteMode.REPLACE,
    )
    inserted = store.store_bytes(
        b"three",
        location="other.txt",
        write_mode=api.WriteMode.UPSERT,
    )
    assert store.read_file(replaced) == b"two"
    assert store.read_file(inserted) == b"three"

    with pytest.raises(api.StorePreconditionFailed):
        store.delete_file(replaced.location, if_version=original.version)
    store.delete_file(replaced.location, if_version=store.stat_file(replaced).version)
    assert store.file_exists("book.txt") is False
    store.delete_file("book.txt", missing_ok=True)
    with pytest.raises(api.StoreNotFound):
        store.delete_file("book.txt")


def test_writable_iso_abort_and_integrity_failure_leave_image_unchanged(tmp_path) -> None:
    image = tmp_path / "library.iso"
    store = IsoWritableStorageBackend(str(image), deterministic=True)
    store.store_bytes(b"stable", location="stable.bin")
    original = image.read_bytes()

    with store.begin_write(store.locate("partial.bin"), expected_size=8) as session:
        session.write(b"partial")
    assert image.read_bytes() == original
    assert store.file_exists("partial.bin") is False

    wrong = api.Digest("sha256", hashlib.sha256(b"other").hexdigest())
    with pytest.raises(api.StoreIntegrityError):
        store.store_bytes(
            b"payload",
            location="bad.bin",
            expected_digest=wrong,
        )
    assert image.read_bytes() == original
    assert store.file_exists("bad.bin") is False


def test_writable_iso_enforces_streamed_member_limit_before_publication(tmp_path) -> None:
    image = tmp_path / "bounded-member.iso"
    store = IsoWritableStorageBackend(
        str(image),
        allocation_prefix="o",
        max_udf_member_bytes=4,
        max_total_uncompressed_bytes=20,
    )
    original = image.read_bytes()

    with pytest.raises(api.StorageUnsupportedOperation, match="write-size limit"):
        store.store_bytes(b"12345", location="large.bin")

    assert image.read_bytes() == original
    assert store.file_exists("large.bin") is False


def test_writable_iso_preflights_total_logical_size_and_persists_policy(tmp_path) -> None:
    image = tmp_path / "bounded-total.iso"
    store = IsoWritableStorageBackend(
        str(image),
        allocation_prefix="o",
        max_udf_member_bytes=6,
        max_total_uncompressed_bytes=6,
        max_logical_expansion_ratio=50,
        max_path_bytes=512,
    )
    store.store_bytes(b"1234", location="first.bin")
    original = image.read_bytes()

    with pytest.raises(api.StorageUnsupportedOperation, match="total logical-size"):
        store.store_bytes(b"567", location="second.bin")

    assert image.read_bytes() == original
    assert store.read_file("first.bin") == b"1234"
    assert store.file_exists("second.bin") is False
    options = dict(store.configuration.backend_options)
    assert options["max_udf_member_bytes"] == 6
    assert options["max_total_uncompressed_bytes"] == 6
    assert options["max_logical_expansion_ratio"] == 50.0
    assert options["max_path_bytes"] == 512


def test_failed_iso_rebuild_preserves_published_image(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    image = tmp_path / "library.iso"
    store = IsoWritableStorageBackend(str(image), deterministic=True)
    store.store_bytes(b"stable", location="stable.bin")
    original = image.read_bytes()

    def fail_build(self, destination, sources):
        del self, sources
        destination.write_bytes(b"PARTIAL")
        raise RuntimeError("synthetic ISO builder failure")

    monkeypatch.setattr(_IsoImageWriter, "build", fail_build)

    with pytest.raises(RuntimeError, match="synthetic ISO builder failure"):
        store.store_bytes(b"new", location="new.bin")

    assert image.read_bytes() == original
    assert store.read_file("stable.bin") == b"stable"
    assert not list(image.parent.glob(f".{image.name}.*.part"))


def test_writable_iso_does_not_overwrite_an_external_image_change(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    image = tmp_path / "library.iso"
    store = IsoWritableStorageBackend(str(image), deterministic=True)
    store.store_bytes(b"stable", location="stable.bin")
    original_build = _IsoImageWriter.build

    def race_build(self, destination, sources):
        original_build(self, destination, sources)
        image.write_bytes(b"EXTERNAL-REPLACEMENT")

    monkeypatch.setattr(_IsoImageWriter, "build", race_build)

    with pytest.raises(api.StorePreconditionFailed, match="changed"):
        store.store_bytes(b"new", location="new.bin")

    assert image.read_bytes() == b"EXTERNAL-REPLACEMENT"
    assert not list(image.parent.glob(f".{image.name}.*.part"))


def test_writable_iso_can_require_an_existing_image(tmp_path) -> None:
    missing = tmp_path / "missing.iso"

    with pytest.raises(api.StorageNotFound, match="does not exist"):
        IsoWritableStorageBackend(str(missing), create_image=False)

    assert missing.exists() is False


def test_writable_iso_rejects_file_directory_collisions_without_publication(tmp_path) -> None:
    image = tmp_path / "library.iso"
    store = IsoWritableStorageBackend(str(image), deterministic=True)
    store.store_bytes(b"file", location="collision")
    original = image.read_bytes()

    with pytest.raises(api.StorageInvalidAddress, match="collides with a file"):
        store.store_bytes(b"nested", location="collision/nested.bin")

    assert image.read_bytes() == original
    assert store.read_file("collision") == b"file"
    assert store.file_exists("collision/nested.bin") is False


def test_writable_iso_inventory_limit_fails_before_rebuild(tmp_path) -> None:
    image = tmp_path / "limited.iso"
    store = IsoWritableStorageBackend(str(image), max_inventory_entries=1)
    store.store_bytes(b"one", location="one.bin")
    original = image.read_bytes()

    with pytest.raises(api.StorageUnavailable, match="entry limit"):
        store.store_bytes(b"two", location="two.bin")

    assert image.read_bytes() == original
    assert store.read_file("one.bin") == b"one"


def test_writable_iso_imports_existing_joliet_image_before_mutation(tmp_path) -> None:
    image = build_joliet_iso(
        tmp_path / "existing.iso",
        {
            "legacy/Café.epub": b"legacy",
            "keep/second.bin": b"second",
        },
    )
    store = IsoWritableStorageBackend(str(image), create_image=False)

    store.store_bytes(b"new", location="new/東京.epub")

    assert store.read_file("legacy/Café.epub") == b"legacy"
    assert store.read_file("keep/second.bin") == b"second"
    assert store.read_file("new/東京.epub") == b"new"
    assert dict(store.startup().details)["namespace"] == "rock-ridge"


def test_writable_iso_blocks_detected_lossy_rebuild_without_explicit_approval(
    tmp_path,
) -> None:
    image = build_rock_ridge_iso(
        tmp_path / "foreign.iso",
        {b"legacy-link": b"link target"},
    )
    payload = bytearray(image.read_bytes())
    name_entry = payload.find(b"NM")
    assert name_entry >= 0
    payload[name_entry : name_entry + 2] = b"SL"
    image.write_bytes(payload)
    original = image.read_bytes()
    store = IsoWritableStorageBackend(str(image), create_image=False)

    status = store.startup()

    assert status.available is True
    assert status.writable is False
    assert dict(status.details)["rebuild_loss_features"] == "1"
    assert any("symbolic-link entry" in warning for warning in status.warnings)
    with pytest.raises(
        api.StorageUnsupportedOperation,
        match="allow_lossy_rebuild=True",
    ):
        store.store_bytes(b"new", location="new.bin")
    assert image.read_bytes() == original


def test_writable_iso_allows_explicit_lossy_normalization_and_advertises_it(
    tmp_path,
) -> None:
    image = build_rock_ridge_iso(
        tmp_path / "foreign.iso",
        {b"legacy-link": b"link target"},
    )
    payload = bytearray(image.read_bytes())
    name_entry = payload.find(b"NM")
    assert name_entry >= 0
    payload[name_entry : name_entry + 2] = b"SL"
    image.write_bytes(payload)
    store = IsoWritableStorageBackend(
        str(image),
        create_image=False,
        allow_lossy_rebuild=True,
    )

    status = store.startup()

    assert status.writable is True
    assert any("will discard" in warning for warning in status.warnings)
    assert dict(store.configuration.backend_options)["allow_lossy_rebuild"] is True
    store.store_bytes(b"new", location="new.bin")
    assert store.read_file("new.bin") == b"new"
    assert [location.key for location in store.iter_locations()] == ["new.bin"]


def test_writable_iso_detects_boot_and_udf_bridge_features_before_rebuild(
    tmp_path,
) -> None:
    image = build_iso9660_iso(
        tmp_path / "hybrid.iso",
        {"BOOK.BIN": b"book"},
    )
    payload = bytearray(image.read_bytes())
    terminator = bytes(payload[17 * BLOCK_SIZE : 18 * BLOCK_SIZE])
    payload[18 * BLOCK_SIZE : 19 * BLOCK_SIZE] = terminator
    boot = bytearray(BLOCK_SIZE)
    boot[0] = 0
    boot[1:6] = b"CD001"
    boot[6] = 1
    payload[17 * BLOCK_SIZE : 18 * BLOCK_SIZE] = boot
    udf = bytearray(BLOCK_SIZE)
    udf[0] = 0
    udf[1:6] = b"NSR02"
    udf[6] = 1
    last_sector = len(payload) // BLOCK_SIZE - 1
    payload[last_sector * BLOCK_SIZE : (last_sector + 1) * BLOCK_SIZE] = udf
    image.write_bytes(payload)
    store = IsoWritableStorageBackend(str(image), create_image=False)

    status = store.startup()

    assert status.writable is False
    assert any("boot volume descriptor" in warning for warning in status.warnings)
    assert any("UDF bridge markers NSR02" in warning for warning in status.warnings)
    with pytest.raises(api.StorageUnsupportedOperation, match="boot volume descriptor"):
        store.store_bytes(b"new", location="NEW.BIN")


def test_writable_iso_emits_an_independently_readable_joliet_namespace(tmp_path) -> None:
    image = tmp_path / "joliet.iso"
    key = "書庫/東京-Café-👩‍💻.epub"
    store = IsoWritableStorageBackend(str(image), include_joliet=True)
    store.store_bytes(b"joliet", location=key)
    payload = bytearray(image.read_bytes())
    marker = payload.find(b"SP\x07\x01\xbe\xef")
    assert marker >= 0
    payload[marker : marker + 2] = b"XX"
    joliet_only_selection = tmp_path / "joliet-selected.iso"
    joliet_only_selection.write_bytes(payload)

    reopened = IsoReadOnlyStorageBackend(str(joliet_only_selection))

    assert dict(reopened.startup().details)["namespace"] == "joliet"
    assert reopened.read_file(key) == b"joliet"


def test_writable_iso_applies_generic_unicode_torture_contract(tmp_path) -> None:
    store = IsoWritableStorageBackend(
        str(tmp_path / "unicode.iso"),
        deterministic=True,
    )

    results = exercise_unicode_path_cases(
        store,
        TORTURED_UNICODE_PATH_CASES,
        seed=lambda key, payload: store.store_bytes(payload, location=key),
    )

    assert {result.location.key for result in results} == {
        case.key for case in TORTURED_UNICODE_PATH_CASES
    }
    reopened = IsoReadOnlyStorageBackend(str(store.image_path))
    assert {
        location.key for location in reopened.iter_locations()
    } == {case.key for case in TORTURED_UNICODE_PATH_CASES}


@pytest.mark.skipif(os.name != "posix", reason="surrogateescape is a POSIX byte-name contract")
def test_writable_iso_round_trips_surrogateescaped_name_bytes(tmp_path) -> None:
    store = IsoWritableStorageBackend(str(tmp_path / "legacy.iso"))
    key = "legacy/" + POSIX_BAD_BYTES_FILENAME

    store.store_bytes(POSIX_BAD_BYTES_PAYLOAD, location=key)
    reopened = IsoReadOnlyStorageBackend(str(store.image_path))
    location = next(item for item in reopened.iter_locations() if item.key == key)

    assert os.fsencode(location.key) == b"legacy/" + POSIX_BAD_BYTES_FILENAME_BYTES
    assert reopened.read_file(location) == POSIX_BAD_BYTES_PAYLOAD


def test_writable_iso_uses_susp_continuation_for_long_rock_ridge_names(tmp_path) -> None:
    store = IsoWritableStorageBackend(str(tmp_path / "long-name.iso"))
    key = "x" * 250 + ".bin"

    store.store_bytes(b"long", location=key)

    assert store.read_file(key) == b"long"
    assert [location.key for location in store.iter_locations()] == [key]
    with pytest.raises(api.StoreInvalidLocation, match="255 encoded bytes"):
        store.locate("é" * 128)


def test_writable_iso_serializes_concurrent_commits_without_losing_members(tmp_path) -> None:
    store = IsoWritableStorageBackend(str(tmp_path / "concurrent.iso"))

    def publish(index: int) -> None:
        store.store_bytes(
            f"payload-{index}".encode(),
            location=f"objects/{index}.bin",
        )

    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(publish, range(8)))

    assert {location.key for location in store.iter_locations()} == {
        f"objects/{index}.bin" for index in range(8)
    }
    for index in range(8):
        assert store.read_file(f"objects/{index}.bin") == f"payload-{index}".encode()


def test_writable_iso_deterministic_mode_reproduces_identical_image(tmp_path) -> None:
    first = IsoWritableStorageBackend(
        str(tmp_path / "first.iso"),
        deterministic=True,
        volume_id="REPRODUCIBLE",
    )
    second = IsoWritableStorageBackend(
        str(tmp_path / "second.iso"),
        deterministic=True,
        volume_id="REPRODUCIBLE",
    )
    for store in (first, second):
        store.store_bytes(b"one", location="a/one.bin")
        store.store_bytes(b"two", location="b/two.bin")

    assert first.image_path.read_bytes() == second.image_path.read_bytes()


def test_writable_iso_rejects_native_metadata_and_oversized_declared_member(tmp_path) -> None:
    driver = WritableIsoStorageDriver(
        tmp_path / "limits.iso",
        address_space_uuid=UUID(int=1),
    )
    address = driver.parse_object_address("book.epub")

    with pytest.raises(api.StorageUnsupportedOperation, match="metadata"):
        driver.begin_write(address, metadata=(("title", "Book"),))
    with pytest.raises(api.StorageUnsupportedOperation, match="4 GiB"):
        driver.begin_write(address, expected_size=1 << 32)


@pytest.mark.skipif(shutil.which("file") is None, reason="file utility is unavailable")
def test_writable_iso_is_recognized_by_independent_file_utility(tmp_path) -> None:
    store = IsoWritableStorageBackend(
        str(tmp_path / "interoperable.iso"),
        volume_id="BOOKS",
    )
    store.store_bytes(b"book", location="books/book.epub")

    result = subprocess.run(
        ["file", str(store.image_path)],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "ISO 9660 CD-ROM filesystem data 'BOOKS'" in result.stdout


def test_driver_package_exports_writable_iso_driver() -> None:
    from LiuXin_alpha.storage.drivers import WritableIsoStorageDriver as exported

    assert exported is WritableIsoStorageDriver
    assert issubclass(exported, IsoStorageDriver)
