"""A Location must never escape Store policy through os.PathLike."""

from __future__ import annotations

import os
import pathlib

import pytest


def test_builtin_open_and_pathlib_reject_location(location) -> None:
    assert not isinstance(location, os.PathLike)
    with pytest.raises(TypeError):
        open(location, "rb")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        pathlib.Path(location)  # type: ignore[arg-type]


def test_store_open_file_is_read_only_and_context_managed(store, location, payload) -> None:
    store.write_bytes(location, payload)

    with store.open_file(location) as source:
        assert source.read() == payload
        assert not source.writable()
