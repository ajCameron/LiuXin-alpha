"""
Provide test local store properties utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test local store properties through a consuming regression::

        python -m pytest -q tests/utils/storage/local/test_local_store_properties.py
"""
from __future__ import annotations

from dataclasses import dataclass

import os
from pathlib import Path

import pytest


def test_get_free_bytes_accepts_future_paths(tmp_path: Path) -> None:
    """
    Perform the test get free bytes accepts future paths utility operation under explicit compatibility rules.

    Example:
        Exercise test get free bytes accepts future paths through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_local_store_properties.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.storage.local.local_store_properties import get_free_bytes

    future = tmp_path / "does" / "not" / "exist" / "yet" / "file.bin"
    val = get_free_bytes(future)
    assert isinstance(val, int)
    assert val > 0


def test_get_free_bytes_include_reserved_uses_statvfs(monkeypatch, tmp_path):
    """
    Perform the test get free bytes include reserved uses statvfs utility operation under explicit compatibility rules.

    Example:
        Exercise test get free bytes include reserved uses statvfs through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_local_store_properties.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    from dataclasses import dataclass
    import os
    from LiuXin_alpha.utils.storage.local import local_store_properties as mod

    @dataclass
    class _Fake:
        """
        Provide the Fake utility contract with explicit state and cleanup behavior.

        Example:
            Exercise test get free bytes include reserved uses statvfs. Fake through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_local_store_properties.py
        """
        f_frsize: int = 4096
        f_bfree: int = 10

    def fake_statvfs(p: os.PathLike[str]):
        """
        Perform the fake statvfs utility operation under explicit compatibility rules.

        Example:
            Exercise test get free bytes include reserved uses statvfs.fake statvfs through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_local_store_properties.py


        :param p: Path-like value normalized or validated by the operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return _Fake()

    # Key bit: allow creating the attr even if the platform lacks it
    monkeypatch.setattr(mod.os, "statvfs", fake_statvfs, raising=False)

    # Optional: ensure we *don't* fall back to disk_usage
    def boom(_):
        """
        Perform the boom utility operation under explicit compatibility rules.

        Example:
            Exercise test get free bytes include reserved uses statvfs.boom through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_local_store_properties.py


        :param _: Value supplied for operation under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        raise AssertionError("disk_usage should not be called when include_reserved=True and statvfs is present")
    monkeypatch.setattr(mod.shutil, "disk_usage", boom)

    assert mod.get_free_bytes(tmp_path, include_reserved=True) == 4096 * 10

def test_get_free_bytes_raises_if_no_existing_parent(monkeypatch) -> None:
    """
    Perform the test get free bytes raises if no existing parent utility operation under explicit compatibility rules.

    Example:
        Exercise test get free bytes raises if no existing parent through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_local_store_properties.py


    :param monkeypatch: Value supplied for monkeypatch under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    from LiuXin_alpha.utils.storage.local.local_store_properties import get_free_bytes

    # Force Path.exists() to always be False so the upward walk hits the root sentinel.
    monkeypatch.setattr(Path, "exists", lambda self: False)

    with pytest.raises(FileNotFoundError):
        get_free_bytes("/totally/nonexistent/rootless")
