from __future__ import annotations

import pytest

from LiuXin_alpha.storage.store_backend_plugins.on_disk_unmanaged_drive.on_disk_unmanaged_storage_backend import (
    OnDiskUnmanagedStorageBackend,
)


class TestCrossStoreBehavior:
    def test_relative_to_across_stores_raises(self, tmp_path, loc_cls) -> None:
        s1 = tmp_path / "s1"
        s2 = tmp_path / "s2"
        s1.mkdir()
        s2.mkdir()

        store1 = OnDiskUnmanagedStorageBackend(url=str(s1))
        store2 = OnDiskUnmanagedStorageBackend(url=str(s2))

        a1 = loc_cls("a", "b", store=store1)
        a2 = loc_cls("a", store=store2)

        with pytest.raises(ValueError):
            _ = a1.relative_to(a2)

        assert a1.is_relative_to(a2) is False
        assert (a1 == a2) is False
