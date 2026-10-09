"""
Supply local filesystem Stores, a transient manager, and an opaque address.

Store fixtures start their roots and return directly without teardown hooks. The
manager fixture attaches both Stores and starts them again. The location fixture
only resolves an address; it does not publish the payload. Tests own their explicit
stream/session contexts, while pytest owns the temporary filesystem directory.
"""

from __future__ import annotations

import hashlib

from pathlib import Path

import pytest

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.durable_manager import StorageManager
from LiuXin_alpha.storage.stores import FilesystemStore


@pytest.fixture()
def payload() -> bytes:
    """
    Return the fixed nonempty payload used for full and sliced read comparisons. No Store or file is
    created by this fixture.

    Example:
        >>> payload.__wrapped__()
        b'location-contract-payload'


    :return: The bytes literal b"location-contract-payload".
    """
    return b"location-contract-payload"


@pytest.fixture()
def store(tmp_path: Path) -> FilesystemStore:
    """
    Construct the primary filesystem Store, call startup, and return it without a fixture teardown
    hook. The returned availability status is not checked here; later tests exercise the Store.

    Example:
        >>> value = store.__wrapped__(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for actual local paths and test bytes.
    :return: Started FilesystemStore rooted at tmp_path/primary; construction/startup errors propagate.
    """
    value = FilesystemStore(tmp_path / "primary", name="primary")
    value.startup()
    return value


@pytest.fixture()
def second_store(tmp_path: Path) -> FilesystemStore:
    """
    Construct the secondary filesystem Store under its own root, start it, and return it without a
    fixture teardown hook. Its generated UUID distinguishes it from the primary Store.

    Example:
        >>> value = second_store.__wrapped__(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for actual local paths and test bytes.
    :return: Started FilesystemStore rooted at tmp_path/secondary; errors propagate.
    """
    value = FilesystemStore(tmp_path / "secondary", name="secondary")
    value.startup()
    return value


@pytest.fixture()
def manager(store: FilesystemStore, second_store: FilesystemStore) -> StorageManager:
    """
    Construct a transient application manager with both already-started Store fixtures.
    startup_on_add=True calls startup again during attachment. No database/cache is bound and this
    return-only fixture adds no manager close hook.

    Example:
        >>> value = manager.__wrapped__(store, second_store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :param second_store: Fixture providing the separately identified started secondary filesystem Store.
    :return: New StorageManager routing to the two supplied Store facades.
    """
    return StorageManager(stores=[store, second_store], startup_on_add=True)


@pytest.fixture()
def location(store: FilesystemStore) -> api.Location:
    """
    Resolve objects/book.epub through the primary Store's key parser without writing or probing that
    object.

    Example:
        >>> address = location.__wrapped__(store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :return: Opaque Location retaining the primary Store UUID and canonical object key.
    """
    return store.locate("objects/book.epub")


def sha256(data: bytes) -> api.Digest:
    """
    Hash the supplied bytes with SHA-256 and wrap the hexadecimal digest in the storage value type.
    This calculates evidence for write-session expectations without touching a Store.

    Example:
        >>> sha256(b"abc").value
        'ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad'


    :param data: Byte payload consumed by hashlib.sha256.
    :return: Digest with algorithm sha256 and the computed hexadecimal value.
    """
    return api.Digest("sha256", hashlib.sha256(data).hexdigest())
