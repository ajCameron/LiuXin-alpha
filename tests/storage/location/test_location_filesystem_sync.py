"""
Exercise staged publication, abandoned/invalid writes, and exact range reads.

Real filesystem sessions remain invisible before commit in the selected cases.
Size/digest expectations and context exit are tested at the Store boundary.
"""

from __future__ import annotations

import pytest

from LiuXin_alpha.storage import api

from .conftest import sha256


def test_staged_write_is_invisible_until_commit(store, location, payload) -> None:
    """
    Write with matching size and SHA-256 expectations, assert the destination is absent before
    explicit commit, then require the returned Location and complete published bytes.

    Example:
        >>> test_staged_write_is_invisible_until_commit(store, location, payload)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :param location: Primary Store Location for objects/book.epub; resolving it does not publish bytes.
    :param payload: Fixed nonempty byte payload supplied by the shared fixture.
    :return: None after the stated contract assertions pass.
    """
    with store.begin_write(
        location,
        expected_size=len(payload),
        expected_digest=sha256(payload),
    ) as session:
        session.write(payload)
        assert not store.exists(location)
        info = session.commit()

    assert info.location == location
    assert store.read_bytes(location) == payload


def test_abandoned_and_failed_sessions_leave_no_public_object(store) -> None:
    """
    Leave one partial write context without commit, then fail another commit with a size mismatch.
    Both destination keys must remain absent; the second path must raise StoreIntegrityError.

    Example:
        >>> test_abandoned_and_failed_sessions_leave_no_public_object(store)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :return: None after the stated contract assertions pass.
    """
    abandoned = store.locate("staging/abandoned")
    with store.begin_write(abandoned) as session:
        session.write(b"partial")
    assert not store.exists(abandoned)

    invalid = store.locate("staging/invalid")
    with pytest.raises(api.StoreIntegrityError):
        with store.begin_write(invalid, expected_size=99) as session:
            session.write(b"short")
            session.commit()
    assert not store.exists(invalid)


def test_read_ranges_are_exact(store, location, payload) -> None:
    """
    Publish the fixture payload and require an offset-four, length-eight read to equal bytes 4
    through 11 of the original.

    Example:
        >>> test_read_ranges_are_exact(store, location, payload)  # doctest: +SKIP


    :param store: Fixture providing the started primary filesystem Store under the temporary directory.
    :param location: Primary Store Location for objects/book.epub; resolving it does not publish bytes.
    :param payload: Fixed nonempty byte payload supplied by the shared fixture.
    :return: None after the stated contract assertions pass.
    """
    store.write_bytes(location, payload)
    assert store.read_bytes(location, offset=4, length=8) == payload[4:12]
