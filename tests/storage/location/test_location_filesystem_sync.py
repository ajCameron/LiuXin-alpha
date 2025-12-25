from __future__ import annotations

import os
import pathlib

import pytest

from LiuXin_alpha.storage.store_backend_plugins.on_disk_unmanaged_drive.on_disk_unmanaged_location import (
    OnDiskUnmanagedStoreLocation,
)

from .conftest import fs_path


class TestLocationFilesystemSync:
    def test_exists_is_file_is_dir(self, store) -> None:
        fs_path(store, "dir").mkdir(parents=True, exist_ok=True)
        fs_path(store, "dir", "file.txt").write_text("hello", encoding="utf-8")

        root = OnDiskUnmanagedStoreLocation(store=store)
        d = OnDiskUnmanagedStoreLocation("dir", store=store)
        f = OnDiskUnmanagedStoreLocation("dir", "file.txt", store=store)

        assert root.exists() is True
        assert root.is_dir() is True

        assert d.exists() is True
        assert d.is_dir() is True
        assert d.is_file() is False

        assert f.exists() is True
        assert f.is_file() is True
        assert f.is_dir() is False

    def test_mkdir_parents_exist_ok(self, store) -> None:
        d = OnDiskUnmanagedStoreLocation("a", "b", "c", store=store)
        d.mkdir(parents=True)
        assert fs_path(store, "a", "b", "c").is_dir()

        # exist_ok=False should fail if directory exists
        with pytest.raises(FileExistsError):
            d.mkdir(parents=True, exist_ok=False)

        # exist_ok=True should be fine
        d.mkdir(parents=True, exist_ok=True)

    def test_touch_and_open_text_roundtrip(self, store) -> None:
        f = OnDiskUnmanagedStoreLocation("note.txt", store=store)
        assert f.exists() is False

        f.touch()
        assert f.exists() is True
        assert f.is_file() is True

        with f.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write("line1\nline2\n")

        assert f.read_text(encoding="utf-8") == "line1\nline2\n"

    def test_open_binary_roundtrip(self, store) -> None:
        f = OnDiskUnmanagedStoreLocation("blob.bin", store=store)
        data = b"\x00\x01\x02hello\xff"
        n = f.write_bytes(data)
        assert n == len(data)
        assert f.read_bytes() == data

    def test_stat_size_matches_content(self, store) -> None:
        f = OnDiskUnmanagedStoreLocation("sized.txt", store=store)
        payload = "abcd" * 100
        f.write_text(payload, encoding="utf-8")
        st = f.stat()
        assert isinstance(st, os.stat_result)
        assert st.st_size == len(payload.encode("utf-8"))

    def test_unlink_missing_ok_semantics(self, store) -> None:
        f = OnDiskUnmanagedStoreLocation("gone.txt", store=store)
        with pytest.raises(FileNotFoundError):
            f.unlink(missing_ok=False)

        # missing_ok=True should not raise
        f.unlink(missing_ok=True)

        f.write_text("x", encoding="utf-8")
        assert f.exists() is True
        f.unlink()
        assert f.exists() is False

    def test_rmdir_requires_empty_dir(self, store) -> None:
        d = OnDiskUnmanagedStoreLocation("d", store=store)
        d.mkdir()
        (fs_path(store, "d") / "child.txt").write_text("x", encoding="utf-8")
        with pytest.raises(OSError):
            d.rmdir()

        # empty then delete
        (fs_path(store, "d") / "child.txt").unlink()
        d.rmdir()
        assert d.exists() is False

    def test_rename_within_same_dir_and_store_binding(self, store) -> None:
        f = OnDiskUnmanagedStoreLocation("folder", "old.txt", store=store)
        fs_path(store, "folder").mkdir(parents=True, exist_ok=True)
        f.write_text("payload", encoding="utf-8")

        new_loc = f.rename("new.txt")
        assert isinstance(new_loc, OnDiskUnmanagedStoreLocation)
        assert new_loc.store is store
        assert fs_path(store, "folder", "old.txt").exists() is False
        assert fs_path(store, "folder", "new.txt").exists() is True
        assert new_loc.read_text(encoding="utf-8") == "payload"

    def test_replace_moves_and_overwrites(self, store) -> None:
        src = OnDiskUnmanagedStoreLocation("src.txt", store=store)
        dst = OnDiskUnmanagedStoreLocation("dst.txt", store=store)

        src.write_text("SRC", encoding="utf-8")
        dst.write_text("DST", encoding="utf-8")

        moved = src.replace("dst.txt")
        assert moved.store is store
        assert moved.read_text(encoding="utf-8") == "SRC"
        assert src.exists() is False
