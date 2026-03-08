from __future__ import annotations

import hashlib
import pathlib
import shutil
import subprocess

import pytest

from LiuXin_alpha.storage.api.storage_api import StoreStatus
from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly import (
    SquashfsReadOnlyStorageBackend,
)


def _build_squashfs(tmp_path: pathlib.Path) -> pathlib.Path:
    if shutil.which("mksquashfs") is None or shutil.which("unsquashfs") is None:
        pytest.skip("squashfs-tools not available in environment")

    src = tmp_path / "src"
    (src / "nested").mkdir(parents=True, exist_ok=True)
    (src / "book one.txt").write_text("hello", encoding="utf-8")
    (src / "nested" / "book_two.epub").write_bytes(b"EPUB-DATA")

    image = tmp_path / "library.squashfs"
    subprocess.run(
        ["mksquashfs", str(src), str(image), "-noappend", "-quiet"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    assert image.exists() is True
    return image


def test_squashfs_readonly_init_and_status(tmp_path: pathlib.Path) -> None:
    image = _build_squashfs(tmp_path)
    store = SquashfsReadOnlyStorageBackend(url=str(image))
    status = store.startup()

    assert isinstance(status, StoreStatus)
    assert status.url == str(image.resolve())
    assert status.check_status.read is True
    assert status.check_status.write is False
    assert status.details.get("mode") == "read_only"
    assert status.details.get("container") == "squashfs_archive"


def test_squashfs_readonly_file_lookup_and_read(tmp_path: pathlib.Path) -> None:
    image = _build_squashfs(tmp_path)
    store = SquashfsReadOnlyStorageBackend(url=str(image))

    canonical = str(image.resolve()) + "/nested/book_two.epub"
    assert store.file_exists(canonical) is True
    assert store.file_exists("nested/book_two.epub") is True
    assert store.file_exists("missing.txt") is False

    file_obj = store.get_file(canonical)
    assert file_obj.store == store.name
    assert file_obj.as_bytes() == b"EPUB-DATA"


def test_squashfs_readonly_true_files_lists_archive_files(tmp_path: pathlib.Path) -> None:
    image = _build_squashfs(tmp_path)
    store = SquashfsReadOnlyStorageBackend(url=str(image))

    urls = {f.file_url for f in store.true_files()}
    assert str(image.resolve()) + "/book one.txt" in urls
    assert str(image.resolve()) + "/nested/book_two.epub" in urls


def test_squashfs_readonly_rejects_mutating_ops(tmp_path: pathlib.Path) -> None:
    image = _build_squashfs(tmp_path)
    store = SquashfsReadOnlyStorageBackend(url=str(image))

    with pytest.raises(PermissionError):
        store.add_file(b"new data")
    with pytest.raises(PermissionError):
        store.delete_file(str(image.resolve()) + "/book one.txt")


def test_squashfs_readonly_hash_streaming_matches_sha256(tmp_path: pathlib.Path) -> None:
    if shutil.which("mksquashfs") is None or shutil.which("unsquashfs") is None:
        pytest.skip("squashfs-tools not available in environment")

    src = tmp_path / "src"
    src.mkdir(parents=True, exist_ok=True)
    payload = (b"abcdefgh01234567" * (1024 * 256))  # 4 MiB
    target = src / "big.bin"
    target.write_bytes(payload)

    image = tmp_path / "library.squashfs"
    subprocess.run(
        ["mksquashfs", str(src), str(image), "-noappend", "-quiet"],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    store = SquashfsReadOnlyStorageBackend(url=str(image))
    status = store.get_file_status("big.bin")
    status.recheck_self(hash=True)
    expected = hashlib.sha256(payload).hexdigest()
    assert status.hash == expected
