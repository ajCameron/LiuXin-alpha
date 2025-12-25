from __future__ import annotations

import asyncio

from LiuXin_alpha.storage.store_backend_plugins.on_disk_unmanaged_drive.on_disk_unmanaged_location import (
    OnDiskUnmanagedStoreLocation,
)

from .conftest import fs_path


class TestSyncNativePretendAsyncBridge:
    def test_async_rename_and_replace(self, store) -> None:
        src = OnDiskUnmanagedStoreLocation("ar", "src.txt", store=store)
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
        d = OnDiskUnmanagedStoreLocation("adir", store=store)
        f = OnDiskUnmanagedStoreLocation("adir", "f.txt", store=store)

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
