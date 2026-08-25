"""Canonical persisted keys replace generic relative-path tokenization."""

from __future__ import annotations

import pytest
from uuid import uuid4

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.drivers import FilesystemStorageDriver


def test_driver_parses_and_round_trips_its_own_canonical_token(tmp_path) -> None:
    driver = FilesystemStorageDriver(tmp_path, address_space_uuid=uuid4())
    address = driver.parse_object_address("a/b/object.bin")

    assert str(address) == "a/b/object.bin"
    assert driver.parse_object_address(str(address)) == address


@pytest.mark.parametrize("raw", ["", "../x", "/x", "a//b", "a/./b", "a\\b"])
def test_driver_rejects_ambiguous_relative_tokens(tmp_path, raw) -> None:
    driver = FilesystemStorageDriver(tmp_path, address_space_uuid=uuid4())
    with pytest.raises(api.StorageInvalidAddress):
        driver.parse_object_address(raw)


def test_driver_uri_roundtrip_does_not_leak_path_logic_to_location(tmp_path) -> None:
    driver = FilesystemStorageDriver(tmp_path, address_space_uuid=uuid4())
    driver.startup()
    info = driver.store_bytes(b"object", object_address="objects/value")

    uri = driver.object_uri(info.object_address)
    assert driver.object_address_from_uri(uri) == info.object_address
