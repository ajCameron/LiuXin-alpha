"""
Check bounded reads from explicitly configured live storage endpoints.

LIUXIN_RUN_LIVE_STORAGE_TESTS must opt in, and each backend requires its root/key
environment variables. Cases use HTTP, FTP, rclone, or S3 construction followed by
stat and a short read; they do not write or delete remote objects, verify complete
payload identity, or imply coverage when skipped. Store cleanup is provided by the
read helper after entry, not by a module-wide fixture around construction/settings.
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
    """
    Read LIUXIN_RUN_LIVE_STORAGE_TESTS, strip/lowercase it, and recognize only 1, true, yes, or on.
    Other text and absence are false; endpoint configuration is checked separately.

    Example:
        >>> enabled = _enabled()  # doctest: +SKIP


    :return: Boolean opt-in flag from the current environment.
    """
    return os.environ.get("LIUXIN_RUN_LIVE_STORAGE_TESTS", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


@pytest.fixture(autouse=True)
def _require_live_storage_flag() -> None:
    """
    Skip each test in this module unless the live-storage opt-in flag is enabled. This autouse
    fixture does not open an endpoint or validate its root/key variables.

    Example:
        The fixture runs automatically; its underlying check can also be called
        directly when illustrating the configured environment:

        .. code-block:: python

            _require_live_storage_flag.__wrapped__()


    :return: None when enabled; otherwise raises pytest's skip outcome.
    """
    if not _enabled():
        pytest.skip(
            "Live storage tests disabled; set LIUXIN_RUN_LIVE_STORAGE_TESTS=1."
        )


def _required(name: str) -> str:
    """
    Read and strip one environment variable, skipping when absent or blank. The value is not
    otherwise validated, converted, or printed by this helper.

    Example:
        >>> root = _required("LIUXIN_LIVE_HTTP_ROOT")  # doctest: +SKIP


    :param name: Environment variable name identifying a required live-test setting.
    :return: Stripped nonempty setting, or a pytest skip outcome when unconfigured.
    """
    value = os.environ.get(name, "").strip()
    if not value:
        pytest.skip(f"{name} is not configured.")
    return value


def _assert_read_contract(store, key: str) -> None:
    """
    Resolve and stat the configured object, read up to 64 bytes, and always close the Store. Require
    matching Location, absent/nonnegative reported size, a bytes return, and nonempty data when
    reported size is truthy. No known expected content, digest, complete-object length, or version
    snapshot is checked. The reader context closes before Store cleanup. A Store.close failure can
    replace an earlier assertion/read error; no remote write or deletion is requested.

    Example:
        >>> _assert_read_contract(store, configured_key)  # doctest: +SKIP


    :param store: Constructed live Store facade whose close is called even when resolve/stat/read fails.
    :param key: Configured existing object key interpreted by that Store.
    :return: None after read assertions and cleanup; operation/assertion/close errors propagate.
    """
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
    """
    Construct an HTTP Store from the required root with a 20-second timeout and 1,000-entry
    inventory cap, then run the bounded read contract for its required key. A missing key skips
    before entering the helper's Store cleanup block.

    Example:
        >>> test_live_http_read_contract()  # doctest: +SKIP


    :return: None after the opted-in HTTP read contract; missing settings skip and backend failures propagate.
    """
    store = HttpReadOnlyStore(
        _required("LIUXIN_LIVE_HTTP_ROOT"),
        timeout_s=20.0,
        max_inventory_entries=1000,
    )
    _assert_read_contract(store, _required("LIUXIN_LIVE_HTTP_KEY"))


def test_live_ftp_read_contract() -> None:
    """
    Construct the read-only FTP backend from its required root with a 20-second timeout, 1,000-entry
    directory cap, and 5,000-entry inventory cap, then check the required key. No inventory
    traversal is requested here; missing-key skip occurs before helper cleanup.

    Example:
        >>> test_live_ftp_read_contract()  # doctest: +SKIP


    :return: None after the opted-in FTP read contract; missing settings skip and backend failures propagate.
    """
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
    """
    Construct the rclone HTTP backend with its configured root, 20-second timeout, hourly
    request-limit option set to zero, 5,000-entry inventory cap, and 2,097,152-character JSON token
    cap. Run the bounded read contract for its required key; a missing key skips before helper
    cleanup.

    Example:
        >>> test_live_rclone_read_contract()  # doctest: +SKIP


    :return: None after the opted-in rclone read contract; missing settings skip and process/backend failures propagate.
    """
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
    """
    Construct an S3 Store using its required root and default runtime client/credential resolution,
    then check the required object key. This case requests only stat/read operations and closes
    through the shared helper once entered.

    Example:
        >>> test_live_s3_read_contract()  # doctest: +SKIP


    :return: None after the opted-in S3 read contract; missing settings skip and client/backend failures propagate.
    """
    store = S3Store(_required("LIUXIN_LIVE_S3_ROOT"))
    _assert_read_contract(store, _required("LIUXIN_LIVE_S3_KEY"))
