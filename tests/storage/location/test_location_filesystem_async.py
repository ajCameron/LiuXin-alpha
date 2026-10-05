"""
Exercise thread-pool writes and before/after replacement reads on local storage.

Independent objects are written concurrently. The replacement case waits for its
worker to finish before the second read, so it does not observe readers racing
publication or establish absence of intermediate states during that interval.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor


def test_concurrent_writes_publish_complete_independent_objects(store) -> None:
    """
    Use eight worker threads to publish distinct indexed payloads at independent keys, then compare
    complete readback in input order. This does not test multiple writers competing for one
    destination.

    Example:
        >>> test_concurrent_writes_publish_complete_independent_objects(store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :return: None after the stated contract assertions pass.
    """
    def write_one(index: int):
        """
        Build the index-specific repeated byte payload and publish it at parallel/index.bin through
        the captured Store. Each worker uses an independent key; default collision policy applies.

        Example:
            >>> info = write_one(3)  # doctest: +SKIP


        :param index: Worker index used in both payload text and destination key.
        :return: FileInfo returned by Store.store_bytes after publication; errors propagate to the future.
        """
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
    """
    Read the old bytes, run replacement on a worker and wait for its result, then read the new
    complete bytes. The assertion checks the two endpoints; no read overlaps staging or publication
    despite the historical test name.

    Example:
        >>> test_async_reader_observes_old_or_new_complete_value_never_staging(store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :return: None after the stated contract assertions pass.
    """
    location = store.locate("atomic/value.bin")
    store.write_bytes(location, b"old")

    def replace() -> None:
        """
        Replace the captured Location with the fixed new payload using the Store's replace mode. The
        caller waits for this worker to finish before its second read.

        Example:
            >>> replace()  # doctest: +SKIP


        :return: None after replacement completes; Store errors propagate to the waiting future.
        """
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
