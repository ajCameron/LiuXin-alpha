"""Shared fixture plugin for database driver / database contract suites.

This module exists because modern pytest versions discourage using
``pytest_plugins`` from nested conftest files.

By keeping the shared fixtures in a normal module (not named conftest), we can
load it from the top-level tests/conftest.py once, and reuse it across multiple
suites without triggering plugin double-registration.
"""

from __future__ import annotations

import os
import random
from dataclasses import dataclass
from typing import Sequence

import pytest


# ---------------------------------------------------------------------------
# Driver parametrization
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DriverSpec:
    """A concrete driver backend to test."""

    id: str
    db_type: str
    requires_module: str | None = None


def _can_import(module_name: str) -> bool:
    try:
        __import__(module_name)
        return True
    except Exception:
        return False


def _parse_driver_tokens(raw: str | None) -> set[str]:
    if not raw:
        return {"all"}
    tokens = {t.strip().lower() for t in raw.split(",") if t.strip()}
    return tokens or {"all"}


def _selected_driver_specs() -> list[DriverSpec]:
    """Return the ordered list of DriverSpecs requested by the environment."""

    tokens = _parse_driver_tokens(os.environ.get("LIUXIN_TEST_DB_DRIVERS"))

    want_all = "all" in tokens
    want_sqlite = want_all or bool(tokens & {"sqlite", "pure", "sqlite3"})
    want_apsw = want_all or bool(tokens & {"apsw", "sqlite_apsw"})

    specs: list[DriverSpec] = []
    if want_sqlite:
        specs.append(DriverSpec(id="sqlite", db_type="SQLite"))

    if want_apsw:
        if not _can_import("apsw"):
            # If explicitly requested (not via "all"), fail loudly.
            if not want_all:
                raise RuntimeError(
                    "LIUXIN_TEST_DB_DRIVERS requests APSW, but 'apsw' is not importable. "
                    "Install apsw or remove 'apsw' from LIUXIN_TEST_DB_DRIVERS."
                )
            # Otherwise, quietly omit it.
        else:
            specs.append(DriverSpec(id="apsw", db_type="SQLite_apsw", requires_module="apsw"))

    if not specs:
        raise RuntimeError(
            "No database drivers selected. Set LIUXIN_TEST_DB_DRIVERS to one of: all, sqlite, apsw."
        )

    return specs


_DRIVER_SPECS: Sequence[DriverSpec]
try:
    _DRIVER_SPECS = tuple(_selected_driver_specs())
except RuntimeError as e:
    # Defer raising until pytest config is available (nicer error formatting).
    _DRIVER_SPECS = ()
    _DRIVER_SPEC_ERROR = e
else:
    _DRIVER_SPEC_ERROR = None


def pytest_configure(config) -> None:
    if _DRIVER_SPEC_ERROR is not None:
        raise _DRIVER_SPEC_ERROR


@pytest.fixture(params=_DRIVER_SPECS, ids=[s.id for s in _DRIVER_SPECS])
def driver_spec(request) -> DriverSpec:
    """Parametrized fixture selecting the driver backend under test."""

    return request.param


# ---------------------------------------------------------------------------
# Deterministic randomness
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _seed_random() -> None:
    """Make test randomness deterministic."""

    random.seed(0x5EED)


# ---------------------------------------------------------------------------
# Torture corpora
# ---------------------------------------------------------------------------


def _torture_strings() -> list[str]:
    long_chunk = "x" * 4096
    return [
        "plain-ascii",
        "with spaces and\ttabs",
        "with\nnewlines\r\nwindows",
        "quotes 'single' and \"double\"",
        "backticks `like` these",
        "sql-comment -- not actually a comment",
        "c-style /* comment */ markers",
        "semi;colon;party",
        "nul\x00byte\x00inside",
        "path/like/thing/..//../",
        "emoji 😀🤖🧠",
        "combining e\u0301cole",
        "rtl עברית العربية",
        "cjk 漢字かなカナ",
        "zero-width \u200b\u200d join",
        "mixed ßøđ€ symbols",
        long_chunk,
    ]


def _sql_injection_payloads() -> list[str]:
    """SQL-injection-shaped values that must be treated as inert data.

    NOTE: Updated for FRBR-first schema (works/agents/manifestations...).
    """

    return [
        "' OR '1'='1",
        "' OR 1=1 --",
        "\" OR \"1\"=\"1\" --",
        "'); DROP TABLE works; --",
        "'); DROP TABLE agents; --",
        "'); DROP TABLE manifestations; --",
        "'; ATTACH DATABASE ':memory:' AS evil; --",
        "'; PRAGMA foreign_keys=OFF; --",
        "'||(SELECT name FROM sqlite_master LIMIT 1)||'",
        "%'; UPDATE works SET work_title='pwned' WHERE 1=1; --",
        '"; VACUUM; --',
        "'); SELECT randomblob(1024); --",
    ]


@pytest.fixture(scope="session")
def torture_strings() -> Sequence[str]:
    return tuple(_torture_strings())


@pytest.fixture(scope="session")
def sql_injection_payloads() -> Sequence[str]:
    return tuple(_sql_injection_payloads())


@pytest.fixture(scope="session")
def all_torture_payloads(torture_strings: Sequence[str], sql_injection_payloads: Sequence[str]) -> Sequence[str]:
    """Union of the two corpora."""

    return tuple(list(torture_strings) + list(sql_injection_payloads))


# ---------------------------------------------------------------------------
# Database provisioning + driver construction
# ---------------------------------------------------------------------------


@pytest.fixture
def contract_db_name() -> str:
    return os.environ.get("LIUXIN_TEST_DB_NAME", "test_db_13")


@pytest.fixture
def provisioned_contract_db(provision_test_database, contract_db_name: str):
    return provision_test_database(contract_db_name)


@pytest.fixture
def db_metadata(provisioned_contract_db) -> dict:
    return {"database_path": str(provisioned_contract_db.db_path)}


@pytest.fixture
def db(driver_spec: DriverSpec, db_metadata: dict):
    from LiuXin_alpha.databases.database import Database

    database = Database(metadata=db_metadata, db_type=driver_spec.db_type, create=False, backup=False)
    try:
        yield database
    finally:
        try:
            database.driver.close()
        except Exception:
            pass


@pytest.fixture
def driver(db):
    return db.driver


@pytest.fixture
def driver_wrapper(db):
    return db.driver_wrapper


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def assert_integrity():
    def _assert_integrity(database) -> None:
        conn = getattr(database, "conn", None) or getattr(database, "driver", None).conn
        try:
            rows = conn.execute("PRAGMA integrity_check").fetchall()
        except Exception:
            rows = conn.get("PRAGMA integrity_check")

        flat: list[str] = []
        for r in rows:
            if r is None:
                continue
            if isinstance(r, (list, tuple)):
                flat.append(str(r[0]))
            else:
                flat.append(str(r))

        assert flat and flat[0].lower() == "ok", f"integrity_check failed: {flat}"

    return _assert_integrity


@pytest.fixture
def pick_payload(all_torture_payloads: Sequence[str]):
    def _pick(i: int) -> str:
        return all_torture_payloads[i % len(all_torture_payloads)]

    return _pick
