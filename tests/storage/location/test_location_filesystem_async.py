"""Concurrency exercises the transactional Store, not Location methods."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor


def test_concurrent_writes_publish_complete_independent_objects(store) -> None:
    def write_one(index: int):
        data = (f"payload-{index}-" * 100).encode()
        return store.store_bytes(
            data,
            location=f"parallel/{index}.bin",
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        infos = list(executor.map(write_one, range(8)))

    assert [store.read_bytes(info.location) for info in infos] == [
        (f"payload-{i}-" * 100).encode() for i in range(8)
    ]


def test_async_reader_observes_old_or_new_complete_value_never_staging(store) -> None:
    location = store.locate("atomic/value.bin")
    store.write_bytes(location, b"old")

    def replace() -> None:
        store.write_bytes(
            location,
            b"new-complete-value",
            mode="replace",
        )

    before = store.read_bytes(location)
    with ThreadPoolExecutor(max_workers=1) as executor:
        executor.submit(replace).result()
    after = store.read_bytes(location)
    assert (before, after) == (b"old", b"new-complete-value")
