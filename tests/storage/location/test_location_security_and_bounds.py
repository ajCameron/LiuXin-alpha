"""
Exercise filesystem traversal and final-symlink containment rejection.

Direct Location construction may retain traversal text, but bound I/O still goes
through Store validation. The final-symlink case skips when creation is unavailable;
these cases do not simulate concurrent symlink replacement.
"""

from __future__ import annotations

import os

import pytest

from LiuXin_alpha.storage import api


@pytest.mark.parametrize("key", ["../escape", "nested/../../escape", "/absolute"])
def test_filesystem_store_refuses_traversal(store, key) -> None:
    """
    Attempt publication with each parent-traversal or absolute key and require StoreInvalidLocation.
    This tests key rejection through the Store write convenience path.

    Example:
        >>> test_filesystem_store_refuses_traversal(store, key)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :param key: Parameterized key spelling whose exact retention or rejection is asserted.
    :return: None after the stated contract assertions pass.
    """
    with pytest.raises(api.StoreInvalidLocation):
        store.store_bytes(b"escape", location=key)


@pytest.mark.skipif(not hasattr(os, "symlink"), reason="symlinks unavailable")
def test_filesystem_store_refuses_final_symlink_escape(store, tmp_path) -> None:
    """
    Create an outside file and a final symlink under the Store root, then require routed reading to
    reject the escape. Skip when symlink support or creation is unavailable; no concurrent link
    mutation is attempted.

    Example:
        >>> test_filesystem_store_refuses_final_symlink_escape(store, tmp_path)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :param tmp_path: Pytest temporary directory for actual local paths and test bytes.
    :return: None after the stated contract assertions pass.
    """
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"outside")
    link = store.root_path / "linked.bin"
    try:
        link.symlink_to(outside)
    except OSError:
        pytest.skip("symlink creation unavailable")

    with pytest.raises(api.StoreInvalidLocation):
        store.read_bytes(store.locate("linked.bin"))


def test_bound_location_cannot_bypass_store_scope(manager, second_store) -> None:
    """
    Construct an opaque Location containing parent traversal, bind it through the manager, and
    require the owning filesystem Store to reject the read. Binding does not make unsafe backend
    syntax valid.

    Example:
        >>> test_bound_location_cannot_bypass_store_scope(manager, second_store)  # doctest: +SKIP


    :param manager: Fixture providing a transient manager attached to the primary and secondary filesystem Stores.
    :param second_store: Fixture providing the separately identified started secondary filesystem Store.
    :return: None after the stated contract assertions pass.
    """
    unknown = api.Location(second_store.store_ref, "../escape")
    with pytest.raises(api.StoreInvalidLocation):
        manager.bind(unknown).read_bytes()
