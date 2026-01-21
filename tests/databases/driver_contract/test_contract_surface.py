"""Contract tests: driver surface parity.

These tests enforce a stable "direct_*" API surface across driver backends.

The baseline is the stdlib sqlite3-backed SQLite driver. Other drivers must
match it *exactly* (no missing methods, no extras).

This module focuses on surface/shape; deeper functional semantics are covered by
other contract modules.
"""

from __future__ import annotations

import inspect

import pytest


def _direct_callable_names(obj) -> set[str]:
    """Return the set of callable attributes beginning with 'direct_'."""

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
    assert len(_BASELINE_DIRECT) >= baseline_size_min, (
        "Expected the baseline SQLite driver to expose at least one direct_* method."
    )


def test_direct_surface_is_stable(driver) -> None:
    """Introspection should be deterministic and not mutate driver state."""

    first = _direct_callable_names(driver)
    second = _direct_callable_names(driver)
    assert first == second


def test_driver_module_matches_requested_backend(driver_spec, driver) -> None:
    """Ensure the constructed driver matches the requested backend.

    This catches routing mistakes in loadDatabaseDriver / driver selection.
    """

    mod = driver.__class__.__module__

    if driver_spec.id == "sqlite":
        assert ".database_driver_plugins.SQLite." in mod and "SQLite_apsw" not in mod, (
            f"Requested sqlite (stdlib) backend, but got driver from module: {mod}"\
            "\nThis usually means loadDatabaseDriver still routes SQLite -> SQLite_apsw."
        )

    elif driver_spec.id == "apsw":
        assert "SQLite_apsw" in mod, (
            f"Requested apsw backend, but got driver from module: {mod}"
        )


def test_direct_surface_matches_baseline(driver_spec, driver) -> None:
    """All drivers must match the baseline direct_* surface exactly."""

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
    """All direct_* methods should be introspectable (useful for meta-tests)."""

    bad: list[tuple[str, str]] = []
    for name in sorted(_direct_callable_names(driver)):
        meth = getattr(driver, name)
        try:
            inspect.signature(meth)
        except Exception as e:
            bad.append((name, repr(e)))

    assert not bad, "Some direct_* methods lacked inspectable signatures: " + repr(bad)
