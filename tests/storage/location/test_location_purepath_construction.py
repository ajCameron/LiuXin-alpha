
from __future__ import annotations

import os
import pathlib
import pickle

import pytest

from LiuXin_alpha.storage.store_backend_plugins.on_disk_unmanaged_drive.on_disk_unmanaged_location import (
    OnDiskUnmanagedStoreLocation,
)

from .conftest import AsyncOnDiskLocation, fs_path


@pytest.mark.parametrize("loc_cls", [OnDiskUnmanagedStoreLocation, AsyncOnDiskLocation])
def test_construction_splits_slashes(store, loc_cls) -> None:
    loc = loc_cls("alpha/bravo", "charlie", store=store)
    assert loc.parts == ("alpha", "bravo", "charlie")
    assert loc._tokens == ["alpha", "bravo", "charlie"]
    assert str(loc) == "alpha/bravo/charlie"
    assert loc.as_posix() == "alpha/bravo/charlie"


@pytest.mark.parametrize("loc_cls", [OnDiskUnmanagedStoreLocation, AsyncOnDiskLocation])
def test_construction_strips_dot_segments(store, loc_cls) -> None:
    loc = loc_cls(".", "a", "./b", "c/./d", store=store)
    assert loc.parts == ("a", "b", "c", "d")


@pytest.mark.parametrize("loc_cls", [OnDiskUnmanagedStoreLocation, AsyncOnDiskLocation])
def test_construction_refuses_absolute_and_dotdot(store, loc_cls) -> None:
    with pytest.raises(ValueError):
        loc_cls("/abs/path", store=store)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        loc_cls("..", "x", store=store)


@pytest.mark.parametrize("loc_cls", [OnDiskUnmanagedStoreLocation, AsyncOnDiskLocation])
def test_root_string_and_parts(store, loc_cls) -> None:
    root = loc_cls(store=store)
    assert root.parts == ()
    assert str(root) == "."
    assert root.name == ""


@pytest.mark.parametrize("loc_cls", [OnDiskUnmanagedStoreLocation, AsyncOnDiskLocation])
def test_fspath_integration(store, loc_cls, tmp_path) -> None:
    loc = loc_cls("file.txt", store=store)
    # os.fspath should call __fspath__, which returns as_store_key() (absolute under store root)
    p = pathlib.Path(os.fspath(loc))
    assert p == fs_path(store, "file.txt")
    assert pathlib.Path(loc) == fs_path(store, "file.txt")


@pytest.mark.parametrize("loc_cls", [OnDiskUnmanagedStoreLocation, AsyncOnDiskLocation])
def test_hash_and_equality_are_store_scoped(store, loc_cls, tmp_path) -> None:
    store2 = type(store)(url=str(tmp_path / "other"))
    store2.url  # touch

    a1 = loc_cls("a", "b", store=store)
    a2 = loc_cls("a/b", store=store)
    b_other_store = loc_cls("a", "b", store=store2)

    assert a1 == a2
    assert hash(a1) == hash(a2)
    assert a1 != b_other_store

    s = {a1}
    assert a2 in s
    assert b_other_store not in s


@pytest.mark.parametrize("loc_cls", [OnDiskUnmanagedStoreLocation, AsyncOnDiskLocation])
def test_sorting_requires_same_store(store, loc_cls, tmp_path) -> None:
    store2 = type(store)(url=str(tmp_path / "other2"))
    a = loc_cls("a", store=store)
    b = loc_cls("b", store=store)
    assert sorted([b, a]) == [a, b]
    with pytest.raises(TypeError):
        _ = a < loc_cls("a", store=store2)  # type: ignore[operator]


@pytest.mark.parametrize("loc_cls", [OnDiskUnmanagedStoreLocation, AsyncOnDiskLocation])
def test_pickle_roundtrip_when_store_is_picklable(store, loc_cls) -> None:
    loc = loc_cls("a", "b.txt", store=store)
    blob = pickle.dumps(loc)
    loc2 = pickle.loads(blob)
    # NOTE: pickle round-trips store by value; identity might not be preserved.
    assert loc2.parts == loc.parts
    assert loc2.as_posix() == loc.as_posix()
