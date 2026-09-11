"""
Check that native path APIs reject Location and Store streams stay read-only.

Actual bytes are opened through the owning Store rather than os.PathLike
conversion; the test closes the returned stream with its context manager.
"""

from __future__ import annotations

import os
import pathlib

import pytest


def test_builtin_open_and_pathlib_reject_location(location) -> None:
    """
    Require Location to be outside os.PathLike and reject both builtin open and pathlib.Path
    conversion with TypeError. These attempts must not route through a native filesystem path.

    Example:
        >>> test_builtin_open_and_pathlib_reject_location(location)  # doctest: +SKIP


    :param location: Primary Store Location for objects/book.epub; resolving it does not publish bytes.
    :return: None after the stated contract assertions pass.
    """
    assert not isinstance(location, os.PathLike)
    with pytest.raises(TypeError):
        open(location, "rb")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        pathlib.Path(location)  # type: ignore[arg-type]


def test_store_open_file_is_read_only_and_context_managed(store, location, payload) -> None:
    """
    Publish the payload and open it through Store.open_file in a context. Require complete readback
    and writable() false; context exit owns stream cleanup.

    Example:
        >>> test_store_open_file_is_read_only_and_context_managed(store, location, payload)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :param location: Primary Store Location for objects/book.epub; resolving it does not publish bytes.
    :param payload: Fixed nonempty byte payload supplied by the shared fixture.
    :return: None after the stated contract assertions pass.
    """
    store.write_bytes(location, payload)

    with store.open_file(location) as source:
        assert source.read() == payload
        assert not source.writable()
