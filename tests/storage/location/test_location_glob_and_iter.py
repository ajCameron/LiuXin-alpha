from __future__ import annotations

import asyncio

from LiuXin_alpha.storage.store_backend_plugins.on_disk_unmanaged_drive.on_disk_unmanaged_location import (
    OnDiskUnmanagedStoreLocation,
)

from .conftest import AsyncOnDiskLocation, fs_path


class TestLocationGlobAndIter:
    def test_iterdir_is_location_relative(self, store) -> None:
        # store/
        #   top/
        #     a.txt
        #     nested/
        #       b.txt
        fs_path(store, "top", "nested").mkdir(parents=True, exist_ok=True)
        fs_path(store, "top", "a.txt").write_text("a", encoding="utf-8")
        fs_path(store, "top", "nested", "b.txt").write_text("b", encoding="utf-8")

        top = OnDiskUnmanagedStoreLocation("top", store=store)
        children = list(top.iterdir())
        keys = {c.as_store_key() for c in children}

        assert keys == {
            str(fs_path(store, "top", "a.txt")),
            str(fs_path(store, "top", "nested")),
        }

    def test_glob_matches_in_directory(self, store) -> None:
        fs_path(store, "g").mkdir(parents=True, exist_ok=True)
        fs_path(store, "g", "one.txt").write_text("1", encoding="utf-8")
        fs_path(store, "g", "two.md").write_text("2", encoding="utf-8")
        fs_path(store, "g", "three.txt").write_text("3", encoding="utf-8")

        g = OnDiskUnmanagedStoreLocation("g", store=store)
        matches = list(g.glob("*.txt"))
        assert {m.as_store_key() for m in matches} == {
            str(fs_path(store, "g", "one.txt")),
            str(fs_path(store, "g", "three.txt")),
        }

    def test_rglob_recurses(self, store) -> None:
        fs_path(store, "r", "a").mkdir(parents=True, exist_ok=True)
        fs_path(store, "r", "a", "x.txt").write_text("x", encoding="utf-8")
        fs_path(store, "r", "b").mkdir(parents=True, exist_ok=True)
        fs_path(store, "r", "b", "y.txt").write_text("y", encoding="utf-8")
        fs_path(store, "r", "b", "y.md").write_text("z", encoding="utf-8")

        r = OnDiskUnmanagedStoreLocation("r", store=store)
        matches = list(r.rglob("*.txt"))
        assert {m.as_store_key() for m in matches} == {
            str(fs_path(store, "r", "a", "x.txt")),
            str(fs_path(store, "r", "b", "y.txt")),
        }

    def test_async_native_glob_and_iter_sync_facade(self, store) -> None:
        # This validates AsyncNativePretendSyncLocation's derived sync methods.
        fs_path(store, "an", "sub").mkdir(parents=True, exist_ok=True)
        fs_path(store, "an", "a.txt").write_text("a", encoding="utf-8")
        fs_path(store, "an", "sub", "b.txt").write_text("b", encoding="utf-8")

        an = AsyncOnDiskLocation("an", store=store)

        # sync iterdir (collecting async generator)
        keys = {c.as_store_key() for c in an.iterdir()}
        assert keys == {str(fs_path(store, "an", "sub")), str(fs_path(store, "an", "a.txt"))}

        # sync rglob (collecting async generator)
        txt = {c.as_store_key() for c in an.rglob("*.txt")}
        assert txt == {str(fs_path(store, "an", "a.txt")), str(fs_path(store, "an", "sub", "b.txt"))}

    def test_async_native_a_glob_streams(self, store) -> None:
        fs_path(store, "agn").mkdir(parents=True, exist_ok=True)
        fs_path(store, "agn", "one.txt").write_text("1", encoding="utf-8")
        fs_path(store, "agn", "two.txt").write_text("2", encoding="utf-8")

        loc = AsyncOnDiskLocation("agn", store=store)

        async def go() -> set[str]:
            out: set[str] = set()
            async for child in loc.aglob("*.txt"):
                out.add(child.as_store_key())
            return out

        assert asyncio.run(go()) == {
            str(fs_path(store, "agn", "one.txt")),
            str(fs_path(store, "agn", "two.txt")),
        }
