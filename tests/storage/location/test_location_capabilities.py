"""Capabilities belong to stores, never to addresses."""

from __future__ import annotations

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.stores import FilesystemStore


def test_location_does_not_advertise_backend_capabilities(location) -> None:
    assert not hasattr(location, "capabilities")
    assert not hasattr(location, "read_only")
    assert not hasattr(location, "can_open_write")


def test_manager_reports_capabilities_for_the_owning_store(manager, store, location) -> None:
    capabilities = manager.capabilities(location.store_ref)

    assert capabilities == store.capabilities
    assert capabilities.create
    assert capabilities.atomic_publish
    assert capabilities.range_reads


def test_read_only_is_store_configuration_not_location_state(tmp_path) -> None:
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
