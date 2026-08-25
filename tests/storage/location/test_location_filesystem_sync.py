"""Synchronous transactional read/write behavior."""

from __future__ import annotations

import pytest

from LiuXin_alpha.storage import api

from .conftest import sha256


def test_staged_write_is_invisible_until_commit(store, location, payload) -> None:
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
    store.write_bytes(location, payload)
    assert store.read_bytes(location, offset=4, length=8) == payload[4:12]
