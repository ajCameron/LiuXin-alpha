"""
Check canonical driver address parsing and a local file-URI round trip.

Driver addresses retain a distinct address-space UUID. The persisted generic
Location does not acquire path parsing or URI conversion methods from these tests.
"""

from __future__ import annotations

import pytest
from uuid import uuid4

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.drivers import FilesystemStorageDriver


def test_driver_parses_and_round_trips_its_own_canonical_token(tmp_path) -> None:
    """
    Construct a filesystem driver with an explicit address-space UUID, parse a canonical key, and
    require text rendering/reparsing to preserve the driver address. No file is created.

    Example:
        >>> test_driver_parses_and_round_trips_its_own_canonical_token(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for actual local paths and test bytes.
    :return: None after the stated contract assertions pass.
    """
    driver = FilesystemStorageDriver(tmp_path, address_space_uuid=uuid4())
    address = driver.parse_object_address("a/b/object.bin")

    assert str(address) == "a/b/object.bin"
    assert driver.parse_object_address(str(address)) == address


@pytest.mark.parametrize("raw", ["", "../x", "/x", "a//b", "a/./b", "a\\b"])
def test_driver_rejects_ambiguous_relative_tokens(tmp_path, raw) -> None:
    """
    Require the filesystem driver's parser to reject the selected empty, traversal, absolute,
    repeated-separator, dot, or backslash token with StorageInvalidAddress.

    Example:
        >>> test_driver_rejects_ambiguous_relative_tokens(tmp_path, raw)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for actual local paths and test bytes.
    :param raw: Parameterized invalid driver address text.
    :return: None after the stated contract assertions pass.
    """
    driver = FilesystemStorageDriver(tmp_path, address_space_uuid=uuid4())
    with pytest.raises(api.StorageInvalidAddress):
        driver.parse_object_address(raw)


def test_driver_uri_roundtrip_does_not_leak_path_logic_to_location(tmp_path) -> None:
    """
    Start a filesystem driver, publish bytes, render the returned driver address as a file URI, and
    require URI decoding to recover the same address. This operates on driver addresses, not generic
    Location path methods.

    Example:
        >>> test_driver_uri_roundtrip_does_not_leak_path_logic_to_location(tmp_path)  # doctest: +SKIP


    :param tmp_path: Pytest temporary directory for actual local paths and test bytes.
    :return: None after the stated contract assertions pass.
    """
    driver = FilesystemStorageDriver(tmp_path, address_space_uuid=uuid4())
    driver.startup()
    info = driver.store_bytes(b"object", object_address="objects/value")

    uri = driver.object_uri(info.object_address)
    assert driver.object_address_from_uri(uri) == info.object_address
