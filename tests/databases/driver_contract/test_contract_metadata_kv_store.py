"""Driver contract: metadata key/value store.

This module exercises the database_metadata helpers exposed by drivers.

Covered
-------
- direct_write_metadata
- direct_read_metadata
- direct_get_db_unique_id
- direct_set_db_unique_id

Contract expectations
---------------------
- Field names may be provided with or without the ``database_metadata_`` prefix.
- Unset metadata fields should read back as ``None``.
- Unicode and SQL-injection-shaped payloads must be treated as inert data.
- Invalid field names must raise ``ValueError``.

These tests are intentionally strict; failures indicate driver contract drift.
"""

from __future__ import annotations

from typing import Sequence

import pytest


def _safe_read(driver, field: str):
    """Read metadata and convert unexpected exceptions into a clear assertion."""

    try:
        return driver.direct_read_metadata(field)
    except Exception as e:  # pragma: no cover (we want to surface the exception text)
        raise AssertionError(
            f"direct_read_metadata({field!r}) raised {type(e).__name__}: {e}"
        ) from e


@pytest.mark.parametrize(
    "field",
    [
        "unique_id",
        "parent_LiuXin_instance",
        "db_name",
        "scratch",
    ],
)
def test_metadata_unset_fields_read_as_none(driver, field: str):
    # A freshly provisioned contract DB should treat unset fields as None.
    assert _safe_read(driver, field) is None


@pytest.mark.parametrize(
    "field,idx",
    [
        ("unique_id", 0),
        ("parent_LiuXin_instance", 1),
        ("db_name", 2),
        ("scratch", 3),
    ],
)
def test_metadata_roundtrip_write_and_read_with_unprefixed_and_prefixed_names(
    driver,
    field: str,
    idx: int,
    pick_payload,
):
    value = pick_payload(100 + idx)

    # Unprefixed write, unprefixed read.
    driver.direct_write_metadata(field, value)
    assert _safe_read(driver, field) == value

    # Prefixed read should match.
    prefixed = f"database_metadata_{field}"
    assert _safe_read(driver, prefixed) == value

    # Prefixed write should also work and overwrite.
    value2 = pick_payload(200 + idx)
    driver.direct_write_metadata(prefixed, value2)
    assert _safe_read(driver, field) == value2
    assert _safe_read(driver, prefixed) == value2


def test_metadata_roundtrip_accepts_injection_shaped_values(
    driver,
    sql_injection_payloads: Sequence[str],
):
    payload = sql_injection_payloads[3]
    driver.direct_write_metadata("db_name", payload)

    assert _safe_read(driver, "db_name") == payload

    # Core schema should remain intact.
    assert "titles" in set(driver.direct_get_tables(force_refresh=True))


def test_metadata_writing_none_reads_back_as_none(driver):
    # Contract: writing None should not crash reads.
    driver.direct_write_metadata("parent_LiuXin_instance", None)
    assert _safe_read(driver, "parent_LiuXin_instance") is None


def test_metadata_invalid_field_raises_valueerror(driver, pick_payload):
    bad = "definitely_not_a_real_metadata_field"

    with pytest.raises(ValueError):
        driver.direct_write_metadata(bad, pick_payload(0))

    with pytest.raises(ValueError):
        driver.direct_read_metadata(bad)


def test_db_unique_id_set_and_get_roundtrip(driver):
    forced = "00000000-0000-0000-0000-000000000009"
    assert driver.direct_set_db_unique_id(force_value=forced) is True

    assert driver.direct_get_db_unique_id() == forced
    assert _safe_read(driver, "unique_id") == forced


def test_db_unique_id_multiple_sets_last_write_wins(driver):
    v1 = "00000000-0000-0000-0000-0000000000a1"
    v2 = "00000000-0000-0000-0000-0000000000a2"

    driver.direct_set_db_unique_id(force_value=v1)
    driver.direct_set_db_unique_id(force_value=v2)

    assert driver.direct_get_db_unique_id() == v2
    assert _safe_read(driver, "unique_id") == v2
