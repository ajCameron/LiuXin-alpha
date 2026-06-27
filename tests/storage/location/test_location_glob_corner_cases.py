from __future__ import annotations

import pytest

from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive.on_disk_existing_managed_drive_location import (
    OnDiskExistingManagedStoreLocation,
)

from .conftest import fs_path


class TestGlobCornerCases:
    def test_glob_rejects_absolute_patterns(self, store) -> None:
        loc = OnDiskExistingManagedStoreLocation("g", store=store)
        with pytest.raises(ValueError):
            _ = list(loc.glob("/etc/*"))

    def test_rglob_rejects_absolute_patterns(self, store) -> None:
        loc = OnDiskExistingManagedStoreLocation("g", store=store)
        with pytest.raises(ValueError):
            _ = list(loc.rglob("/etc/*"))

    def test_glob_supports_double_star(self, store) -> None:
        fs_path(store, "ds", "a").mkdir(parents=True, exist_ok=True)
        fs_path(store, "ds", "b").mkdir(parents=True, exist_ok=True)
        fs_path(store, "ds", "a", "one.txt").write_text("1", encoding="utf-8")
        fs_path(store, "ds", "b", "two.txt").write_text("2", encoding="utf-8")

        root = OnDiskExistingManagedStoreLocation("ds", store=store)
        matches = {m.as_store_key() for m in root.glob("**/*.txt")}
        assert matches == {
            str(fs_path(store, "ds", "a", "one.txt")),
            str(fs_path(store, "ds", "b", "two.txt")),
        }
