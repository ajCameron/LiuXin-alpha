"""
Check that backend capabilities and read-only policy belong to Stores.

Address attribute checks are structural. Filesystem capability lookup and an actual
read-only replacement attempt provide the local operational evidence.
"""

from __future__ import annotations

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.stores import FilesystemStore


def test_location_does_not_advertise_backend_capabilities(location) -> None:
    """
    Require the passive Location to lack capabilities, read_only, and can_open_write attributes.
    Backend capability values are not queried by this case.

    Example:
        >>> test_location_does_not_advertise_backend_capabilities(location)  # doctest: +SKIP


    :param location: Primary Store Location for objects/book.epub; resolving it does not publish bytes.
    :return: None after the stated contract assertions pass.
    """
    assert not hasattr(location, "capabilities")
    assert not hasattr(location, "read_only")
    assert not hasattr(location, "can_open_write")


def test_manager_reports_capabilities_for_the_owning_store(manager, store, location) -> None:
    """
    Compare routed manager capabilities with the owning filesystem Store and require create,
    atomic_publish, and range_reads flags. This checks reported capabilities rather than exercising
    each capability here.

    Example:
        >>> test_manager_reports_capabilities_for_the_owning_store(manager, store, location)  # doctest: +SKIP


    :param manager: Fixture providing a transient manager attached to the primary and secondary filesystem Stores.
    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :param location: Primary Store Location for objects/book.epub; resolving it does not publish bytes.
    :return: None after the stated contract assertions pass.
    """
    capabilities = manager.capabilities(location.store_ref)

    assert capabilities == store.capabilities
    assert capabilities.create
    assert capabilities.atomic_publish
    assert capabilities.range_reads


def test_read_only_is_store_configuration_not_location_state(tmp_path) -> None:
    """
    Seed a real file, start a read-only filesystem Store, and verify reading succeeds while writable
    status is false and a replacement raises StoreReadOnly. The Location itself carries no policy
    flag.

    Example:
        >>> test_read_only_is_store_configuration_not_location_state(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for actual local paths and test bytes.
    :return: None after the stated contract assertions pass.
    """
    root = tmp_path / "readonly"
    root.mkdir()
    (root / "book.bin").write_bytes(b"book")
    store = FilesystemStore(root, read_only=True)
    store.startup()
    location = store.locate("book.bin")

    assert store.read_bytes(location) == b"book"
    assert not store.status().writable
    try:
        store.write_bytes(location, b"replacement", mode=api.WriteMode.REPLACE)
    except api.StoreReadOnly:
        pass
    else:  # pragma: no cover - makes the safety invariant explicit
        raise AssertionError("read-only store accepted a mutation")
