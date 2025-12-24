
"""
We are testing our Sync bridge to an Async native library.
"""


from __future__ import annotations

import threading

from .conftest import AsyncOnDiskLocation, fs_path


class TestAsyncNativePretendSyncBridge:
    """
    Tests that the dummy sync methods wrapped around a async lib work.
    """
    def test_sync_facade_exists_and_open_work(self, store) -> None:
        """
        Tests that the sync facade exists and open works through it.

        :param store:
        :return:
        """
        loc = AsyncOnDiskLocation("bridges", store=store)
        fs_path(store, "bridges").mkdir(parents=True, exist_ok=True)

        assert loc.exists() is True
        assert loc.is_dir() is True

        f = AsyncOnDiskLocation("bridges", "hello.txt", store=store)
        with f.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write("hello\n")

        with f.open("r", encoding="utf-8") as handle:
            assert handle.read() == "hello\n"

    def test_sync_facade_from_multiple_threads(self, store) -> None:
        """
        Attempt to access the sync facade through multiple threads.

        :param store:
        :return:
        """
        fs_path(store, "t").mkdir(parents=True, exist_ok=True)
        loc = AsyncOnDiskLocation("t", store=store)

        results: list[bool] = []
        lock = threading.Lock()

        def worker() -> None:
            for _ in range(25):
                ok = loc.exists()
                with lock:
                    results.append(ok)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(results)