"""Opt-in read contracts against operator-supplied live storage endpoints.

Set ``LIUXIN_RUN_LIVE_STORAGE_TESTS=1`` and the backend-specific root/key
variables used below. These tests never write or delete remote objects.
"""

from __future__ import annotations

import os

import pytest

from LiuXin_alpha.storage.store_backend_plugins.ftp_readonly import (
    FtpBackendOptions,
    FtpReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import (
    RcloneBackendOptions,
    RcloneHttpReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.stores import HttpReadOnlyStore, S3Store


pytestmark = [pytest.mark.integration, pytest.mark.live_storage]


def _enabled() -> bool:
    return os.environ.get("LIUXIN_RUN_LIVE_STORAGE_TESTS", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


@pytest.fixture(autouse=True)
def _require_live_storage_flag() -> None:
    if not _enabled():
        pytest.skip(
            "Live storage tests disabled; set LIUXIN_RUN_LIVE_STORAGE_TESTS=1."
        )


def _required(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        pytest.skip(f"{name} is not configured.")
    return value


def _assert_read_contract(store, key: str) -> None:
    try:
        location = store.locate(key)
        info = store.stat(location)
        assert info.location == location
        assert info.size is None or info.size >= 0
        with store.open_read(location, length=64) as stream:
            chunk = stream.read(64)
        assert isinstance(chunk, bytes)
        if info.size:
            assert chunk
    finally:
        store.close()


def test_live_http_read_contract() -> None:
    store = HttpReadOnlyStore(
        _required("LIUXIN_LIVE_HTTP_ROOT"),
        timeout_s=20.0,
        max_inventory_entries=1000,
    )
    _assert_read_contract(store, _required("LIUXIN_LIVE_HTTP_KEY"))


def test_live_ftp_read_contract() -> None:
    store = FtpReadOnlyStorageBackend(
        _required("LIUXIN_LIVE_FTP_ROOT"),
        options=FtpBackendOptions(
            timeout_s=20.0,
            max_directory_entries=1000,
            max_inventory_entries=5000,
        ),
    )
    _assert_read_contract(store, _required("LIUXIN_LIVE_FTP_KEY"))


def test_live_rclone_read_contract() -> None:
    store = RcloneHttpReadOnlyStorageBackend(
        _required("LIUXIN_LIVE_RCLONE_ROOT"),
        options=RcloneBackendOptions(
            timeout_s=20.0,
            max_http_requests_per_hour=0,
            max_inventory_entries=5000,
            max_json_token_chars=2 * 1024 * 1024,
        ),
    )
    _assert_read_contract(store, _required("LIUXIN_LIVE_RCLONE_KEY"))


def test_live_s3_read_contract() -> None:
    store = S3Store(_required("LIUXIN_LIVE_S3_ROOT"))
    _assert_read_contract(store, _required("LIUXIN_LIVE_S3_KEY"))
