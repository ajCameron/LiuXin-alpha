from __future__ import annotations

import hashlib
import os
import pathlib
import shutil
import subprocess

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from LiuXin_alpha.storage.api import (
    EnumerationCompleteness,
    StorageIntegrityError,
    StorageInvalidAddress,
    StorageNotFound,
    StoragePreconditionFailed,
    StoragePublicationModel,
    StorageTemporarySpaceRequirement,
    StorageTimeout,
    StorageUnsupportedOperation,
    StoreReadOnly,
    StoreStatus,
)
from LiuXin_alpha.storage.drivers.squashfs import SquashfsStorageDriver
from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly import (
    SquashfsReadOnlyStorageBackend,
)
from tests.fixtures.storage_unicode import (
    POSIX_BAD_BYTES_FILENAME,
    POSIX_BAD_BYTES_FILENAME_BYTES,
    POSIX_BAD_BYTES_PAYLOAD,
    TORTURED_UNICODE_PATH_CASES,
    UNICODE_DIRECTORY,
    UNICODE_FILENAME,
    UNICODE_KEY,
    UNICODE_PAYLOAD,
)
from tests.storage.contracts.unicode_paths import exercise_unicode_path_cases


def _build_squashfs(tmp_path: pathlib.Path) -> pathlib.Path:
    if shutil.which("mksquashfs") is None or shutil.which("unsquashfs") is None:
        pytest.skip("squashfs-tools not available in environment")
    source = tmp_path / "src"
    (source / "nested").mkdir(parents=True, exist_ok=True)
    (source / "book one.txt").write_text("hello", encoding="utf-8")
    (source / "nested" / "book_two.epub").write_bytes(b"EPUB-DATA")
    image = tmp_path / "library.squashfs"
    subprocess.run(
        ["mksquashfs", str(source), str(image), "-noappend", "-quiet"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return image


def test_squashfs_readonly_preserves_unicode_archive_paths_and_bytes(
    tmp_path: pathlib.Path,
) -> None:
    if shutil.which("mksquashfs") is None or shutil.which("unsquashfs") is None:
        pytest.skip("squashfs-tools not available in environment")
    source = tmp_path / "unicode-src"
    path = source.joinpath(*UNICODE_KEY.split("/"))
    path.parent.mkdir(parents=True)
    path.write_bytes(UNICODE_PAYLOAD)
    image = tmp_path / "unicode-library.squashfs"
    subprocess.run(
        ["mksquashfs", str(source), str(image), "-noappend", "-quiet"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    store = SquashfsReadOnlyStorageBackend(str(image))

    [location] = list(
        store.iter_locations(prefix=store.locate(UNICODE_DIRECTORY))
    )
    info = store.stat_file(location)

    assert location.key == UNICODE_KEY
    assert info.hints.suggested_filename == UNICODE_FILENAME
    assert store.read_file(info) == UNICODE_PAYLOAD


@pytest.mark.skipif(os.name != "posix", reason="surrogateescape is a POSIX filename contract")
def test_squashfs_readonly_reads_tortured_and_undecodable_archive_paths(
    tmp_path: pathlib.Path,
) -> None:
    if shutil.which("mksquashfs") is None or shutil.which("unsquashfs") is None:
        pytest.skip("squashfs-tools not available in environment")
    source = tmp_path / "tortured-src"
    source.mkdir()
    expected = {
        case.key: case.payload for case in TORTURED_UNICODE_PATH_CASES
    }
    for case in TORTURED_UNICODE_PATH_CASES:
        path = source.joinpath(*case.key.split("/"))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(case.payload)
    raw_path = os.path.join(os.fsencode(source), POSIX_BAD_BYTES_FILENAME_BYTES)
    with open(raw_path, "wb") as handle:
        handle.write(POSIX_BAD_BYTES_PAYLOAD)
    expected[POSIX_BAD_BYTES_FILENAME] = POSIX_BAD_BYTES_PAYLOAD
    image = tmp_path / "tortured-library.squashfs"
    subprocess.run(
        ["mksquashfs", str(source), str(image), "-noappend", "-quiet"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    store = SquashfsReadOnlyStorageBackend(str(image))

    results = exercise_unicode_path_cases(store, TORTURED_UNICODE_PATH_CASES)

    discovered = {result.location.key for result in results}
    assert discovered == {case.key for case in TORTURED_UNICODE_PATH_CASES}
    bad_location = next(
        location
        for location in store.iter_locations()
        if location.key == POSIX_BAD_BYTES_FILENAME
    )
    assert store.read_file(bad_location) == POSIX_BAD_BYTES_PAYLOAD


def test_squashfs_readonly_init_and_status(tmp_path: pathlib.Path) -> None:
    image = _build_squashfs(tmp_path)
    store = SquashfsReadOnlyStorageBackend(url=str(image))

    status = store.startup()

    assert isinstance(status, StoreStatus)
    assert status.available is True
    assert status.writable is False
    assert status.object_count == 2
    assert store.configuration.store_root_uri == image.resolve().as_uri()
    assert store.capabilities.enumeration is EnumerationCompleteness.COMPLETE
    assert store.characteristics.publication_model is StoragePublicationModel.READ_ONLY
    assert (
        store.characteristics.temporary_space
        is StorageTemporarySpaceRequirement.OBJECT_STAGE
    )
    assert store.characteristics.limitation("regular_files_only") is not None
    assert store.characteristics.limitation("bounded_squashfs_expansion") is not None
    assert store.characteristics.limitation("nested_expansion_budget_external")
    options = dict(store.configuration.backend_options)
    assert options["max_compression_ratio"] == 200.0
    assert options["max_header_bytes"] == 128 * 1024 * 1024


def test_squashfs_readonly_locate_stat_read_and_range(tmp_path: pathlib.Path) -> None:
    image = _build_squashfs(tmp_path)
    store = SquashfsReadOnlyStorageBackend(url=str(image))
    canonical = str(image.resolve()) + "/nested/book_two.epub"

    location = store.locate(canonical)

    assert location.key == "nested/book_two.epub"
    assert location.store_ref == store.store_ref
    assert store.file_exists(location) is True
    assert store.file_exists("missing.txt") is False
    assert store.stat_file(location).size == len(b"EPUB-DATA")
    assert store.read_file(location) == b"EPUB-DATA"
    assert store.read_file(location, offset=2, length=4) == b"UB-D"


def test_squashfs_readonly_iter_locations_and_prefix_lists_files(tmp_path: pathlib.Path) -> None:
    image = _build_squashfs(tmp_path)
    store = SquashfsReadOnlyStorageBackend(url=str(image))

    assert {location.key for location in store.iter_locations()} == {
        "book one.txt",
        "nested/book_two.epub",
    }
    assert [
        location.key
        for location in store.iter_locations(prefix=store.locate("nested"))
    ] == ["nested/book_two.epub"]


def test_squashfs_readonly_prefix_enumeration_obeys_path_boundaries(
    tmp_path: pathlib.Path,
) -> None:
    if shutil.which("mksquashfs") is None or shutil.which("unsquashfs") is None:
        pytest.skip("squashfs-tools not available in environment")
    source = tmp_path / "prefix-src"
    (source / "books").mkdir(parents=True)
    (source / "bookstore").mkdir()
    (source / "books" / "inside.epub").write_bytes(b"inside")
    (source / "bookstore" / "outside.epub").write_bytes(b"outside")
    image = tmp_path / "prefix-library.squashfs"
    subprocess.run(
        ["mksquashfs", str(source), str(image), "-noappend", "-quiet"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    store = SquashfsReadOnlyStorageBackend(str(image))

    assert [
        location.key
        for location in store.iter_locations(prefix=store.locate("books"))
    ] == ["books/inside.epub"]


@pytest.mark.skipif(
    os.name != "posix",
    reason="control characters are a POSIX filename contract",
)
def test_squashfs_readonly_reads_control_characters_in_archive_paths(
    tmp_path: pathlib.Path,
) -> None:
    if shutil.which("mksquashfs") is None or shutil.which("unsquashfs") is None:
        pytest.skip("squashfs-tools not available in environment")
    source = tmp_path / "control-src"
    source.mkdir()
    expected = {
        "line\nbreak.epub": b"newline payload",
        "carriage\rreturn.epub": b"carriage-return payload",
        "tab\tname-[*]?.epub": b"tab and punctuation payload",
    }
    for key, payload in expected.items():
        (source / key).write_bytes(payload)
    image = tmp_path / "control-library.squashfs"
    subprocess.run(
        ["mksquashfs", str(source), str(image), "-noappend", "-quiet"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    store = SquashfsReadOnlyStorageBackend(str(image))

    discovered = {location.key: location for location in store.iter_locations()}

    assert set(discovered) == set(expected)
    for key, payload in expected.items():
        assert store.read_file(discovered[key]) == payload


def test_squashfs_readonly_range_boundaries_and_empty_member(
    tmp_path: pathlib.Path,
) -> None:
    if shutil.which("mksquashfs") is None or shutil.which("unsquashfs") is None:
        pytest.skip("squashfs-tools not available in environment")
    source = tmp_path / "range-src"
    source.mkdir()
    (source / "payload.bin").write_bytes(bytes(range(256)) * 8192)
    (source / "empty.bin").write_bytes(b"")
    image = tmp_path / "range-library.squashfs"
    subprocess.run(
        ["mksquashfs", str(source), str(image), "-noappend", "-quiet"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    store = SquashfsReadOnlyStorageBackend(str(image))

    assert store.read_file("empty.bin") == b""
    assert store.read_file("payload.bin", offset=1_048_573, length=9) == (
        bytes(range(256)) * 8192
    )[1_048_573:1_048_582]
    assert store.read_file("payload.bin", offset=2_097_152) == b""
    assert store.read_file("payload.bin", offset=5, length=0) == b""


def test_squashfs_readonly_supports_concurrent_indexed_reads(
    tmp_path: pathlib.Path,
) -> None:
    image = _build_squashfs(tmp_path)
    store = SquashfsReadOnlyStorageBackend(str(image))
    requests = [
        ("book one.txt", 0, None, b"hello"),
        ("nested/book_two.epub", 0, None, b"EPUB-DATA"),
        ("nested/book_two.epub", 2, 4, b"UB-D"),
    ] * 8

    def read_one(request: tuple[str, int, int | None, bytes]) -> bytes:
        key, offset, length, _expected = request
        return store.read_file(key, offset=offset, length=length)

    with ThreadPoolExecutor(max_workers=8) as executor:
        observed = list(executor.map(read_one, requests))

    assert observed == [expected for _key, _offset, _length, expected in requests]


def test_squashfs_readonly_rejects_mutating_ops(tmp_path: pathlib.Path) -> None:
    image = _build_squashfs(tmp_path)
    store = SquashfsReadOnlyStorageBackend(url=str(image))

    with pytest.raises(StoreReadOnly):
        store.store_bytes(b"new data", location="new.bin")
    with pytest.raises(StoreReadOnly):
        store.delete_file("book one.txt")


def test_squashfs_readonly_hash_streaming_matches_sha256(tmp_path: pathlib.Path) -> None:
    image = _build_squashfs(tmp_path)
    store = SquashfsReadOnlyStorageBackend(url=str(image))

    digest = store.compute_digest(store.locate("nested/book_two.epub"), "sha256")

    assert digest.value == hashlib.sha256(b"EPUB-DATA").hexdigest()


@pytest.mark.parametrize(
    "invalid",
    ["", "/absolute", "../escape", "nested/../escape", "a//b", "a\\b"],
)
def test_squashfs_readonly_rejects_noncanonical_internal_paths(
    tmp_path: pathlib.Path,
    invalid: str,
) -> None:
    image = _build_squashfs(tmp_path)
    store = SquashfsReadOnlyStorageBackend(url=str(image))
    with pytest.raises((StorageInvalidAddress, ValueError)):
        store.locate(invalid)


def test_squashfs_readonly_missing_stat_remains_typed(tmp_path: pathlib.Path) -> None:
    image = _build_squashfs(tmp_path)
    store = SquashfsReadOnlyStorageBackend(url=str(image))
    with pytest.raises(StorageNotFound):
        store.stat_file("missing.epub")


@pytest.mark.parametrize(
    ("header", "message"),
    (
        (
            b"node R 0 600 0 0 1 0 0\nnode/child R 0 600 0 0 1 0 0\n",
            "descends through file",
        ),
        (
            b"node/child R 0 600 0 0 1 0 0\nnode R 0 600 0 0 1 0 0\n",
            "overwrite a required directory",
        ),
        (
            b"same R 0 600 0 0 1 0 0\nsame R 0 600 0 0 1 0 0\n",
            "duplicate or conflicting",
        ),
    ),
)
def test_squashfs_rejects_ambiguous_member_topology(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
    header: bytes,
    message: str,
) -> None:
    image = _build_squashfs(tmp_path)
    driver = SquashfsStorageDriver(image, address_space_uuid=uuid4())
    monkeypatch.setattr(driver, "_read_pseudo_header", lambda: header)

    with pytest.raises(StorageIntegrityError, match=message):
        driver.startup()


def test_squashfs_rejects_non_regular_members(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    image = _build_squashfs(tmp_path)
    driver = SquashfsStorageDriver(image, address_space_uuid=uuid4())
    monkeypatch.setattr(
        driver,
        "_read_pseudo_header",
        lambda: b"unsafe S 0 777 0 0 target\n",
    )

    with pytest.raises(StorageUnsupportedOperation, match="non-regular"):
        driver.startup()


def test_squashfs_bounds_member_total_ratio_entry_count_and_header(
    tmp_path: pathlib.Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    image = _build_squashfs(tmp_path)
    records = (
        b"first R 0 600 0 0 8 0 0\n"
        b"second R 0 600 0 0 8 0 0\n"
    )

    member_driver = SquashfsStorageDriver(
        image,
        address_space_uuid=uuid4(),
        max_member_bytes=4,
        max_compression_ratio=10_000,
    )
    monkeypatch.setattr(member_driver, "_read_pseudo_header", lambda: records)
    with pytest.raises(StorageUnsupportedOperation, match="member.*exceeds 4"):
        member_driver.startup()

    total_driver = SquashfsStorageDriver(
        image,
        address_space_uuid=uuid4(),
        max_total_uncompressed_bytes=12,
        max_compression_ratio=10_000,
    )
    monkeypatch.setattr(total_driver, "_read_pseudo_header", lambda: records)
    with pytest.raises(StorageUnsupportedOperation, match="total expanded size"):
        total_driver.startup()

    ratio_driver = SquashfsStorageDriver(
        image,
        address_space_uuid=uuid4(),
        max_compression_ratio=1,
    )
    huge_record = f"bomb R 0 600 0 0 {image.stat().st_size + 1} 0 0\n".encode()
    monkeypatch.setattr(ratio_driver, "_read_pseudo_header", lambda: huge_record)
    with pytest.raises(StorageUnsupportedOperation, match="expansion ratio"):
        ratio_driver.startup()

    count_driver = SquashfsStorageDriver(
        image,
        address_space_uuid=uuid4(),
        max_inventory_entries=1,
    )
    monkeypatch.setattr(
        count_driver,
        "_read_pseudo_header",
        lambda: b"one D 0 755 0 0\ntwo D 0 755 0 0\n",
    )
    with pytest.raises(StorageUnsupportedOperation, match="inventory exceeds 1"):
        count_driver.startup()

    with pytest.raises(StorageUnsupportedOperation, match="header exceeds 1"):
        SquashfsStorageDriver(
            image,
            address_space_uuid=uuid4(),
            max_header_bytes=1,
        ).startup()


def test_squashfs_extraction_rejects_output_beyond_indexed_size(
    tmp_path: pathlib.Path,
) -> None:
    image = _build_squashfs(tmp_path)
    driver = SquashfsStorageDriver(image, address_space_uuid=uuid4())
    driver.startup()
    fake = tmp_path / "oversized-unsquashfs"
    fake.write_text("#!/bin/sh\nprintf 'hello!'\n", encoding="utf-8")
    fake.chmod(0o700)
    driver._unsquashfs_exe = str(fake)

    with pytest.raises(StorageIntegrityError, match="more bytes"):
        driver.open_read(driver.parse_object_address("book one.txt"))


def test_squashfs_extraction_timeout_is_enforced(tmp_path: pathlib.Path) -> None:
    image = _build_squashfs(tmp_path)
    driver = SquashfsStorageDriver(
        image,
        address_space_uuid=uuid4(),
        timeout_s=0.05,
    )
    driver.startup()
    fake = tmp_path / "slow-unsquashfs"
    fake.write_text("#!/bin/sh\nsleep 2\nprintf 'hello'\n", encoding="utf-8")
    fake.chmod(0o700)
    driver._unsquashfs_exe = str(fake)

    with pytest.raises(StorageTimeout, match="timed out"):
        driver.open_read(driver.parse_object_address("book one.txt"))


def test_squashfs_conditional_read_rejects_archive_replacement(
    tmp_path: pathlib.Path,
) -> None:
    image = _build_squashfs(tmp_path)
    store = SquashfsReadOnlyStorageBackend(str(image))
    info = store.stat_file("book one.txt")
    replacement_root = tmp_path / "replacement"
    replacement_root.mkdir()
    (replacement_root / "book one.txt").write_bytes(b"other")
    replacement = tmp_path / "replacement.squashfs"
    subprocess.run(
        ["mksquashfs", str(replacement_root), str(replacement), "-noappend", "-quiet"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    os.replace(replacement, image)

    with pytest.raises(StoragePreconditionFailed, match="version changed"):
        store.open_read(info.location, if_version=info.version)
