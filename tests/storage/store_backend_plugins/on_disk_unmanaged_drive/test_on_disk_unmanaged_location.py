"""Opaque Location behavior for the unmanaged disk Store."""

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive import (
    OnDiskUnmanagedStorageBackend,
)


class TestOnDiskUnmanagedLocation:
    def test_basic_api(self, tmp_path) -> None:
        (tmp_path / "book").write_bytes(b"book")
        store = OnDiskUnmanagedStorageBackend(tmp_path)
        location = store.locate("book")

        assert isinstance(location, api.Location)
        assert location.store_ref == store.store_ref
        assert location.key == "book"
        assert store.read_file(location) == b"book"
