
from __future__ import annotations

import os
import pathlib

import pytest

from LiuXin_alpha.storage.store_backend_plugins.on_disk_unmanaged_drive.on_disk_unmanaged_location import (
    OnDiskUnmanagedStoreLocation,
)


def test_constructor_refuses_store_escape(store, tmp_path) -> None:
    # '..' is refused by tokenization (defense in depth)
    with pytest.raises(ValueError):
        _ = OnDiskUnmanagedStoreLocation("a/../b", store=store)


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="symlink not supported on this platform")
def test_symlink_escape_is_refused(store, tmp_path) -> None:
    # Create a symlink inside the store that points *outside*, then ensure we refuse it.
    store_root = pathlib.Path(store.url)
    outside = tmp_path.parent / (tmp_path.name + "_outside")
    outside.mkdir(parents=True, exist_ok=True)

    link = store_root / "escape"
    try:
        link.symlink_to(outside, target_is_directory=True)
    except (OSError, NotImplementedError):
        pytest.skip("symlink creation not permitted on this system")

    with pytest.raises(ValueError):
        _ = OnDiskUnmanagedStoreLocation("escape", "x.txt", store=store)
