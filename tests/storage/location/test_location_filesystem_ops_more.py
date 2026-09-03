"""Explicit routed mutation contracts."""

from __future__ import annotations

import pytest

from LiuXin_alpha.storage import api


def test_delete_is_strict_by_default_and_optionally_idempotent(manager, location) -> None:
    with pytest.raises(api.StoreNotFound):
        manager.delete(location)
    manager.delete(location, missing_ok=True)


def test_filesystem_delete_does_not_claim_an_atomic_version_precondition(
    store, location,
) -> None:
    info = store.write_bytes(location, b"first")
    replacement = store.write_bytes(location, b"second", mode=api.WriteMode.REPLACE)

    assert replacement.version != info.version
    with pytest.raises(api.StoreUnsupportedOperation):
        store.delete(location, if_version=info.version)
    assert store.read_bytes(location) == b"second"


def test_stat_and_try_stat_do_not_hide_availability_categories(manager, location) -> None:
    assert manager.try_stat(location) is None
    manager.write_bytes(location, b"present")
    assert manager.stat(location).size == 7
