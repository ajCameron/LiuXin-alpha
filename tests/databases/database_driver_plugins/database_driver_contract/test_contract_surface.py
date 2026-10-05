"""
Compare selected drivers’ callable direct_ names with an import-time pure-SQLite baseline and inspect their signatures.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_surface.py
"""

from __future__ import annotations

import inspect

import pytest


def _direct_callable_names(obj) -> set[str]:
    """
    Inspect direct_-prefixed attributes and collect callable names, suppressing ordinary attribute-access exceptions.

    Attribute access may execute descriptors; this helper does not restore resulting
    state.

    Example:
        >>> _direct_callable_names(object())
        set()


    :param obj: Class or instance whose attributes are inspected.
    :return: Set of callable names; errors from dir itself propagate.
    """

    names: set[str] = set()
    for name in dir(obj):
        if not name.startswith("direct_"):
            continue
        try:
            attr = getattr(obj, name)
        except Exception:
            continue
        if callable(attr):
            names.add(name)
    return names


# Baseline surface: the pure sqlite3-backed SQLite driver.
from LiuXin_alpha.databases.database_driver_plugins.SQLite.databasedriver import (  # noqa: E402
    DatabaseDriver as PureSQLiteDriver,
)

_BASELINE_DIRECT = _direct_callable_names(PureSQLiteDriver)


@pytest.mark.parametrize("baseline_size_min", [1])
def test_baseline_has_direct_methods(baseline_size_min: int) -> None:
    """
    Require the import-time pure-SQLite surface to contain at least the parametrized minimum.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_surface.py::test_baseline_has_direct_methods


    :param baseline_size_min: Minimum count of discovered direct_ callables.
    :return: None; failed expectations raise AssertionError.
    """
    assert len(_BASELINE_DIRECT) >= baseline_size_min, (
        "Expected the baseline SQLite driver to expose at least one direct_* method."
    )


def test_direct_surface_is_stable(driver) -> None:
    """
    Require two successive driver introspections to return equal callable-name sets.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_surface.py::test_direct_surface_is_stable


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :return: None; failed expectations raise AssertionError.
    """

    first = _direct_callable_names(driver)
    second = _direct_callable_names(driver)
    assert first == second


def test_driver_module_matches_requested_backend(driver_spec, driver) -> None:
    """
    Check the implementation module for recognized sqlite or apsw fixture IDs.

    Other fixture IDs reach no backend-specific assertion.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_surface.py::test_driver_module_matches_requested_backend


    :param driver_spec: Selected database driver specification, including its ID and
        Database db_type.
    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :return: None; failed expectations raise AssertionError.
    """

    mod = driver.__class__.__module__

    if driver_spec.id == "sqlite":
        assert ".database_driver_plugins.SQLite." in mod and "SQLite_apsw" not in mod, (
            f"Requested sqlite (stdlib) backend, but got driver from module: {mod}"\
            "\nThis usually means load_database_driver still routes SQLite -> SQLite_apsw."
        )

    elif driver_spec.id == "apsw":
        assert "SQLite_apsw" in mod, (
            f"Requested apsw backend, but got driver from module: {mod}"
        )


def test_direct_surface_matches_baseline(driver_spec, driver) -> None:
    """
    Require the selected driver’s callable names to match the baseline exactly, reporting missing names before extras.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_surface.py::test_direct_surface_matches_baseline


    :param driver_spec: Selected database driver specification, including its ID and
        Database db_type.
    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :return: None; failed expectations raise AssertionError.
    """

    got = _direct_callable_names(driver)
    missing = sorted(_BASELINE_DIRECT - got)
    extra = sorted(got - _BASELINE_DIRECT)

    if missing:
        raise AssertionError(
            "Driver is missing baseline direct_* methods:\n"
            + "\n".join("  - " + m for m in missing)
        )

    if extra:
        raise AssertionError(
            "Driver exposes extra direct_* methods not in the baseline:\n"
            + "\n".join("  - " + m for m in extra)
        )


def test_direct_methods_have_inspectable_signatures(driver) -> None:
    """
    Inspect every discovered direct_ method signature and report all caught inspection exceptions.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_surface.py::test_direct_methods_have_inspectable_signatures


    :param driver: Driver supplied by the isolated Database fixture; teardown attempts
        to close it.
    :return: None; failed expectations raise AssertionError.
    """

    bad: list[tuple[str, str]] = []
    for name in sorted(_direct_callable_names(driver)):
        meth = getattr(driver, name)
        try:
            inspect.signature(meth)
        except Exception as e:
            bad.append((name, repr(e)))

    assert not bad, "Some direct_* methods lacked inspectable signatures: " + repr(bad)
