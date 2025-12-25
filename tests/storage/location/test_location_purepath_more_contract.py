from __future__ import annotations

import os

import pytest

from LiuXin_alpha.storage.store_backend_plugins.on_disk_unmanaged_drive.on_disk_unmanaged_storage_backend import (
    OnDiskUnmanagedStorageBackend,
)


class TestLocationMorePureContract:
    def test_constructor_splits_slashes_and_ignores_dots(self, loc_cls, store) -> None:
        loc = loc_cls("a/b", "./c", "d//e", store=store)
        assert loc.parts == ("a", "b", "c", "d", "e")

        root = loc_cls(store=store)
        assert str(root) == "."

    def test_constructor_refuses_absolute(self, loc_cls, store) -> None:
        with pytest.raises(ValueError):
            _ = loc_cls("/a/b", store=store)

    def test_constructor_refuses_dotdot(self, loc_cls, store) -> None:
        with pytest.raises(ValueError):
            _ = loc_cls("a/../b", store=store)

    def test_drive_root_anchor_and_absolute_flags(self, loc_cls, store) -> None:
        loc = loc_cls("a", "b", store=store)
        assert loc.drive == ""
        assert loc.root == ""
        assert loc.anchor == ""
        assert loc.is_absolute() is False
        assert loc.is_reserved() is False

    def test_bytes_matches_os_fsencode(self, loc_cls, store) -> None:
        loc = loc_cls("a", "b.txt", store=store)
        assert bytes(loc) == os.fsencode("a/b.txt")

        root = loc_cls(store=store)
        assert bytes(root) == os.fsencode(".")

    def test_rtruediv_composes(self, loc_cls, store) -> None:
        loc = loc_cls("b", "c.txt", store=store)

        out = "a" / loc
        assert out.parts == ("a", "b", "c.txt")

        out2 = b"a" / loc
        assert out2.parts == ("a", "b", "c.txt")

    def test_rtruediv_refuses_absolute_and_dotdot(self, loc_cls, store) -> None:
        loc = loc_cls("b", store=store)

        with pytest.raises(ValueError):
            _ = "/a" / loc

        with pytest.raises(ValueError):
            _ = "../a" / loc

    def test_with_stem_preserves_suffix(self, loc_cls, store) -> None:
        loc = loc_cls("dir", "file.tar.gz", store=store)
        out = loc.with_stem("changed")
        # pathlib semantics: with_stem replaces the *stem* but keeps only the final suffix
        assert out.parts == ("dir", "changed.gz")

        root = loc_cls(store=store)
        with pytest.raises(ValueError):
            _ = root.with_stem("x")

    def test_as_uri_raises(self, loc_cls, store) -> None:
        loc = loc_cls("a", store=store)
        with pytest.raises(ValueError):
            _ = loc.as_uri()

    def test_ordering_within_same_store(self, loc_cls, store) -> None:
        a = loc_cls("a", store=store)
        b = loc_cls("b", store=store)
        assert a < b

    def test_ordering_across_stores_raises(self, tmp_path, loc_cls) -> None:
        s1 = tmp_path / "s1"
        s2 = tmp_path / "s2"
        s1.mkdir()
        s2.mkdir()

        store1 = OnDiskUnmanagedStorageBackend(url=str(s1))
        store2 = OnDiskUnmanagedStorageBackend(url=str(s2))

        a1 = loc_cls("a", store=store1)
        a2 = loc_cls("a", store=store2)

        assert (a1 == a2) is False
        with pytest.raises(TypeError):
            _ = a1 < a2
