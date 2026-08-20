"""Shared fixtures for the opaque Location and BoundLocation contracts."""

from __future__ import annotations

import hashlib

from pathlib import Path

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.store_manager import StorageManager
from LiuXin_alpha.storage.stores import FilesystemStore


@pytest.fixture()
def payload() -> bytes:
    return b"location-contract-payload"


@pytest.fixture()
def store(tmp_path: Path) -> FilesystemStore:
    value = FilesystemStore(tmp_path / "primary", name="primary")
    value.startup()
    return value


@pytest.fixture()
def second_store(tmp_path: Path) -> FilesystemStore:
    value = FilesystemStore(tmp_path / "secondary", name="secondary")
    value.startup()
    return value


@pytest.fixture()
def manager(store: FilesystemStore, second_store: FilesystemStore) -> StorageManager:
    return StorageManager(stores=[store, second_store], startup_on_add=True)


@pytest.fixture()
def location(store: FilesystemStore) -> api.Location:
    return store.locate("objects/book.epub")


def sha256(data: bytes) -> api.Digest:
    return api.Digest("sha256", hashlib.sha256(data).hexdigest())
