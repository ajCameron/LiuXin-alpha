"""Cooperative cancellation inside real filesystem copy/hash loops preserves sources."""

from uuid import uuid4

import pytest

from LiuXin_alpha.storage.drivers import FilesystemStorageDriver
from LiuXin_alpha.storage.operation_context import operation_scope


@pytest.mark.parametrize("action", ("copy", "hash"))
def test_cancel_between_chunks_does_not_publish_partial_file(tmp_path, action):
    content = b"x" * (3 * 1024 * 1024)
    (tmp_path / "source").write_bytes(content)
    driver = FilesystemStorageDriver(tmp_path, address_space_uuid=uuid4())
    source = driver.parse_object_address("source")
    destination = driver.parse_object_address("destination")
    calls = 0

    def cancel():
        nonlocal calls
        calls += 1
        if calls == 2:
            raise RuntimeError("cancelled between chunks")

    with operation_scope(cancel), pytest.raises(RuntimeError, match="cancelled"):
        if action == "copy":
            driver.native_copy(source, destination)
        else:
            driver.native_compute_digest(source)
    assert (tmp_path / "source").read_bytes() == content
    assert not (tmp_path / "destination").exists()
    assert not list(tmp_path.rglob("*.part"))
