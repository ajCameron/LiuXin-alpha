from __future__ import annotations

import asyncio

import pytest

from LiuXin_alpha.storage.store_backend_plugins.on_disk_unmanaged_drive.on_disk_unmanaged_location import (
    OnDiskUnmanagedStoreLocation,
)

from .conftest import fs_path


class TestLocationFilesystemAsync:
    def test_derived_async_exists_is_file_is_dir(self, store) -> None:
        fs_path(store, "d").mkdir(parents=True, exist_ok=True)
        fs_path(store, "d", "f.txt").write_text("hi", encoding="utf-8")

        root = OnDiskUnmanagedStoreLocation(store=store)
        d = OnDiskUnmanagedStoreLocation("d", store=store)
        f = OnDiskUnmanagedStoreLocation("d", "f.txt", store=store)

        async def go() -> None:
            assert await root.aexists() is True
            assert await root.ais_dir() is True

            assert await d.aexists() is True
            assert await d.ais_dir() is True
            assert await d.ais_file() is False

            assert await f.aexists() is True
            assert await f.ais_file() is True
            assert await f.ais_dir() is False

        asyncio.run(go())

    def test_async_open_roundtrip_text(self, store) -> None:
        """
        Roundtrip some text using the async version of writing.

        :param store:
        :return:
        """
        f = OnDiskUnmanagedStoreLocation("async.txt", store=store)

        async def go() -> None:
            async with f.aopen("w", encoding="utf-8", newline="\n") as handle:
                await handle.write("hello\n")
                await handle.flush()

            async with f.aopen("r", encoding="utf-8") as handle:
                got = await handle.read()
            assert got == "hello\n"

        asyncio.run(go())

    def test_async_convenience_helpers(self, store) -> None:
        """
        Tests convenience helpers for just writing text out directly.

        :param store:
        :return:
        """
        f = OnDiskUnmanagedStoreLocation("helpers.txt", store=store)

        async def go() -> None:
            n = await f.awrite_text("abc", encoding="utf-8")
            assert n == 3
            assert await f.aread_text(encoding="utf-8") == "abc"

        asyncio.run(go())

    def test_async_iterdir_streams_results(self, store) -> None:
        """
        Async iterdir and stream the results out.

        :param store:
        :return:
        """
        fs_path(store, "dir").mkdir(parents=True, exist_ok=True)
        fs_path(store, "dir", "a.txt").write_text("a", encoding="utf-8")
        fs_path(store, "dir", "b.txt").write_text("b", encoding="utf-8")

        d = OnDiskUnmanagedStoreLocation("dir", store=store)

        async def go() -> list[str]:
            out: list[str] = []
            async for child in d.aiterdir():
                out.append(child.as_store_key())
            return out

        keys = asyncio.run(go())
        assert set(keys) == {
            str(fs_path(store, "dir", "a.txt")),
            str(fs_path(store, "dir", "b.txt")),
        }
