from __future__ import annotations

import hashlib
import os
import pathlib
import shutil
import subprocess

import pytest

from LiuXin_alpha.storage.api import (
    EnumerationCompleteness,
    StorageInvalidAddress,
    StorageNotFound,
    StoreReadOnly,
    StoreStatus,
)
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

    discovered = {location.key: location for location in store.iter_locations()}

    assert set(discovered) == set(expected)
    for key, payload in expected.items():
        assert store.read_file(discovered[key]) == payload


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
