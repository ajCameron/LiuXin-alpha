"""
Check that unmanaged Store lookup returns the common opaque Location value.

The regression reads a real temporary file through that owned key; it does not
reintroduce a backend-specific filesystem Location type.
"""

from LiuXin_alpha.storage import api
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive import (
    OnDiskUnmanagedStorageBackend,
)


class TestOnDiskUnmanagedLocation:
    """
    Group the basic owned-Location/read regression for an unmanaged filesystem Store.

    Example:
        >>> suite = TestOnDiskUnmanagedLocation()
        >>> suite.test_basic_api(tmp_path)  # doctest: +SKIP
    """
    def test_basic_api(self, tmp_path) -> None:
        """
        Locate an externally created file and verify common Location type, Store ownership, key
        spelling, and readable bytes.

        Example:
            >>> TestOnDiskUnmanagedLocation().test_basic_api(tmp_path)  # doctest: +SKIP


        :param tmp_path: Pytest temporary directory used for real local roots, source files, and committed payloads.
        :return: None after all Location and payload assertions pass.
        """
        (tmp_path / "book").write_bytes(b"book")
        store = OnDiskUnmanagedStorageBackend(tmp_path)
        location = store.locate("book")

        assert isinstance(location, api.Location)
        assert location.store_ref == store.store_ref
        assert location.key == "book"
        assert store.read_file(location) == b"book"
