"""
Check optional metadata values, prefixed field aliases, UUID accessors, and invalid-field rejection.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_metadata_kv_store.py
"""

from __future__ import annotations

from typing import Sequence

import pytest
import uuid


def _safe_read(driver, field: str):
    """
    Read a metadata field and translate any ordinary exception into an AssertionError retaining its cause.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_metadata_kv_store.py


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param field: Field name forwarded unchanged to direct_read_metadata.
    :return: Driver-returned metadata value, including None.
    """

    try:
        return driver.direct_read_metadata(field)
    except Exception as e:  # pragma: no cover (we want to surface the exception text)
        raise AssertionError(
            f"direct_read_metadata({field!r}) raised {type(e).__name__}: {e}"
        ) from e


@pytest.mark.parametrize(
    "field",
    [
        "parent_LiuXin_instance",
        "db_name",
        "scratch",
    ],
)
def test_metadata_unset_fields_read_as_none(driver, field: str):
    # A freshly provisioned contract DB should treat unset fields as None.
    """
    Require each parametrized optional field to read as None in a freshly provisioned database.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_metadata_kv_store.py::test_metadata_unset_fields_read_as_none


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param field: Unset parent-instance, database-name, or scratch metadata field.
    :return: None; failed expectations raise AssertionError.
    """
    assert _safe_read(driver, field) is None


def test_metadata_unique_id_is_present_and_uuid4(driver):
    """
    Require a nonempty UUID4 metadata string equal to the dedicated unique-ID getter.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_metadata_kv_store.py::test_metadata_unique_id_is_present_and_uuid4


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :return: None; failed expectations raise AssertionError.
    """
    val = _safe_read(driver, "unique_id")
    assert isinstance(val, str) and val, f"Expected non-empty unique_id string; got: {val!r}"
    assert val == driver.direct_get_db_unique_id()
    parsed = uuid.UUID(val)
    assert parsed.version == 4


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
    """
    Write and overwrite a metadata field through both name forms and require exact matching reads through either alias.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_metadata_kv_store.py::test_metadata_roundtrip_write_and_read_with_unprefixed_and_prefixed_names


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param field: Unprefixed metadata field to exercise.
    :param idx: Payload offset distinguishing each parametrized field.
    :param pick_payload: Fixture callable selecting payload strings by a wrapping
        integer index.
    :return: None; failed expectations raise AssertionError.
    """
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
    """
    Store an SQL-shaped database name, require exact readback, and confirm titles survives.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_metadata_kv_store.py::test_metadata_roundtrip_accepts_injection_shaped_values


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param sql_injection_payloads: Ordered corpus of SQL-shaped strings intended as test
        data.
    :return: None; failed expectations raise AssertionError.
    """
    payload = sql_injection_payloads[3]
    driver.direct_write_metadata("db_name", payload)

    assert _safe_read(driver, "db_name") == payload

    # Core schema should remain intact.
    assert "titles" in set(driver.direct_get_tables(force_refresh=True))


def test_metadata_writing_none_reads_back_as_none(driver):
    # Contract: writing None should not crash reads.
    """
    Write None to the parent-instance field and require None on readback.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_metadata_kv_store.py::test_metadata_writing_none_reads_back_as_none


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :return: None; failed expectations raise AssertionError.
    """
    driver.direct_write_metadata("parent_LiuXin_instance", None)
    assert _safe_read(driver, "parent_LiuXin_instance") is None


def test_metadata_invalid_field_raises_valueerror(driver, pick_payload):
    """
    Require ValueError from both read and write when the metadata field is unknown.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_metadata_kv_store.py::test_metadata_invalid_field_raises_valueerror


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :param pick_payload: Fixture callable selecting payload strings by a wrapping
        integer index.
    :return: None; failed expectations raise AssertionError.
    """
    bad = "definitely_not_a_real_metadata_field"

    with pytest.raises(ValueError):
        driver.direct_write_metadata(bad, pick_payload(0))

    with pytest.raises(ValueError):
        driver.direct_read_metadata(bad)


def test_db_unique_id_set_and_get_roundtrip(driver):
    """
    Force a fixed identifier, require a True setter result, and compare both getter paths exactly.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_metadata_kv_store.py::test_db_unique_id_set_and_get_roundtrip


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :return: None; failed expectations raise AssertionError.
    """
    forced = "00000000-0000-0000-0000-000000000009"
    assert driver.direct_set_db_unique_id(force_value=forced) is True

    assert driver.direct_get_db_unique_id() == forced
    assert _safe_read(driver, "unique_id") == forced


def test_db_unique_id_multiple_sets_last_write_wins(driver):
    """
    Set two identifiers in succession and require both read paths to return the second.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_metadata_kv_store.py::test_db_unique_id_multiple_sets_last_write_wins


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :return: None; failed expectations raise AssertionError.
    """
    v1 = "00000000-0000-0000-0000-0000000000a1"
    v2 = "00000000-0000-0000-0000-0000000000a2"

    driver.direct_set_db_unique_id(force_value=v1)
    driver.direct_set_db_unique_id(force_value=v2)

    assert driver.direct_get_db_unique_id() == v2
    assert _safe_read(driver, "unique_id") == v2
