from __future__ import annotations

import asyncio

import pytest

from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive.on_disk_existing_managed_drive_location import (
    OnDiskExistingManagedStoreLocation,
)

from .conftest import fs_path

pytestmark = pytest.mark.usefixtures("require_asyncio_thread_bridge")


class TestSyncNativePretendAsyncBridge:
    def test_async_rename_and_replace(self, store) -> None:
        src = OnDiskExistingManagedStoreLocation("ar", "src.txt", store=store)
        fs_path(store, "ar").mkdir(parents=True, exist_ok=True)
        src.write_text("hello", encoding="utf-8")

        async def go() -> None:
            renamed = await src.arename("renamed.txt")
            assert renamed.read_text(encoding="utf-8") == "hello"
            assert fs_path(store, "ar", "src.txt").exists() is False
            assert fs_path(store, "ar", "renamed.txt").exists() is True

            # now move to a different name via replace
            replaced = await renamed.areplace("moved.txt")
            assert replaced.read_text(encoding="utf-8") == "hello"
            assert fs_path(store, "ar", "renamed.txt").exists() is False
            assert fs_path(store, "ar", "moved.txt").exists() is True

        asyncio.run(go())

    def test_async_mkdir_unlink_rmdir(self, store) -> None:
        d = OnDiskExistingManagedStoreLocation("adir", store=store)
        f = OnDiskExistingManagedStoreLocation("adir", "f.txt", store=store)

        async def go() -> None:
            await d.amkdir(parents=True)
            assert d.exists() is True

            await f.atouch()
            assert await f.aexists() is True

            await f.aunlink()
            assert await f.aexists() is False

            await d.armdir()
            assert await d.aexists() is False

        asyncio.run(go())
