"""
Provide shared driver selection, isolated database fixtures, deterministic payloads, and integrity helpers.

Backend selection is captured at import from LIUXIN_TEST_DB_DRIVERS. The top-level
test configuration registers this module for both contract suites.

Example:
    Run the shared fixtures through a contract test::

        python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py
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
    """
    Describe a frozen backend choice with a pytest ID, Database db_type, and optional import requirement.

    Example:
        >>> spec = DriverSpec('sqlite', 'SQLite')
        >>> (spec.id, spec.requires_module)
        ('sqlite', None)
    """

    id: str
    db_type: str
    requires_module: str | None = None


def _can_import(module_name: str) -> bool:
    """
    Attempt an ordinary import and report whether it completes without an Exception.

    Import side effects and module-cache changes are retained; a failed import can leave
    partial state.

    Example:
        >>> _can_import('sys')
        True


    :param module_name: Importable module name passed to __import__.
    :return: True on success, False for any caught Exception.
    """
    try:
        __import__(module_name)
        return True
    except Exception:
        return False


def _parse_driver_tokens(raw: str | None) -> set[str]:
    """
    Split a comma-separated backend selector into stripped, lowercase, distinct tokens.

    Example:
        >>> sorted(_parse_driver_tokens(' SQLite, APSW,sqlite '))
        ['apsw', 'sqlite']
        >>> _parse_driver_tokens(' , ') == {'all'}
        True


    :param raw: Optional environment selector string.
    :return: Token set, or {all} for missing, empty, or separator-only input.
    """
    if not raw:
        return {"all"}
    tokens = {t.strip().lower() for t in raw.split(",") if t.strip()}
    return tokens or {"all"}


def _selected_driver_specs() -> list[DriverSpec]:
    """
    Resolve the current backend environment into SQLite-first driver specifications.

    Recognize sqlite/pure/sqlite3 and apsw/sqlite_apsw. The all token requests both and
    silently omits unimportable APSW; explicit APSW raises RuntimeError when
    unavailable. Unknown tokens are ignored if another token selects a backend. No
    selected backend also raises RuntimeError.

    Example:
        Run the shared fixtures through a contract test::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py


    :return: Nonempty ordered list of available requested DriverSpec values.
    """

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
    """
    Raise any backend-selection error saved during module import.

    This hook does not reread the environment or retry imports.

    Example:
        Run the shared fixtures through a contract test::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py


    :param config: Unused pytest configuration object required by the hook signature.
    :return: None when selection succeeded; otherwise raises the stored RuntimeError.
    """
    if _DRIVER_SPEC_ERROR is not None:
        raise _DRIVER_SPEC_ERROR


@pytest.fixture(params=_DRIVER_SPECS, ids=[s.id for s in _DRIVER_SPECS])
def driver_spec(request) -> DriverSpec:
    """
    Expose the import-time backend choice for the current fixture parameter.

    Example:
        Run the shared fixtures through a contract test::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py


    :param request: Pytest fixture request parametrized from the captured driver
        specifications.
    :return: request.param, without additional validation.
    """

    return request.param


# ---------------------------------------------------------------------------
# Deterministic randomness
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _seed_random() -> None:
    """
    Reset the process-wide random generator to 0x5EED before each consuming test.

    The autouse fixture does not restore the previous random state.

    Example:
        Run the shared fixtures through a contract test::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py


    :return: None; mutates the global random generator.
    """

    random.seed(0x5EED)


# ---------------------------------------------------------------------------
# Torture corpora
# ---------------------------------------------------------------------------


def _torture_strings() -> list[str]:
    """
    Build a fresh ordered corpus of seventeen text edge cases, including NUL, Unicode, and a 4096-character value.

    Example:
        >>> values = _torture_strings()
        >>> (len(values), chr(0) in values[8], len(values[-1]))
        (17, True, 4096)


    :return: New list of strings; SQL-looking fragments remain ordinary data.
    """
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
    """
    Build twelve SQL-shaped strings for bound-value tests against the FRBR schema.

    Example:
        >>> len(_sql_injection_payloads())
        12


    :return: Fresh ordered list; this helper executes no SQL.
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
    """
    Freeze the text edge-case corpus for session-scoped fixture consumers.

    Example:
        Run the shared fixtures through a contract test::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py


    :return: Tuple of seventeen text payloads.
    """
    return tuple(_torture_strings())


@pytest.fixture(scope="session")
def sql_injection_payloads() -> Sequence[str]:
    """
    Freeze the SQL-shaped payload corpus for session-scoped fixture consumers.

    Example:
        Run the shared fixtures through a contract test::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py


    :return: Tuple of twelve strings intended as data.
    """
    return tuple(_sql_injection_payloads())


@pytest.fixture(scope="session")
def all_torture_payloads(torture_strings: Sequence[str], sql_injection_payloads: Sequence[str]) -> Sequence[str]:
    """
    Concatenate the text and SQL-shaped fixture corpora in that order.

    Preserve duplicates and each input sequence’s ordering.

    Example:
        Run the shared fixtures through a contract test::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py


    :param torture_strings: First ordered text corpus.
    :param sql_injection_payloads: Second ordered SQL-shaped corpus.
    :return: Tuple containing every supplied payload.
    """

    return tuple(list(torture_strings) + list(sql_injection_payloads))


# ---------------------------------------------------------------------------
# Database provisioning + driver construction
# ---------------------------------------------------------------------------


@pytest.fixture
def contract_db_name() -> str:
    """
    Read LIUXIN_TEST_DB_NAME, defaulting to test_db_13 only when the variable is absent.

    Example:
        Run the shared fixtures through a contract test::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py


    :return: Configured name string; an explicitly empty value is retained.
    """
    return os.environ.get("LIUXIN_TEST_DB_NAME", "test_db_13")


@pytest.fixture
def provisioned_contract_db(provision_test_database, contract_db_name: str):
    """
    Provision an isolated copy of the selected named test database.

    Example:
        Run the shared fixtures through a contract test::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py


    :param provision_test_database: Fixture factory that provisions a named database in
        an isolated location.
    :param contract_db_name: Database fixture name passed through unchanged.
    :return: Resource returned by the provisioning fixture.
    """
    return provision_test_database(contract_db_name)


@pytest.fixture
def db_metadata(provisioned_contract_db) -> dict:
    """
    Build Database metadata pointing at the provisioned database file.

    Example:
        Run the shared fixtures through a contract test::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py


    :param provisioned_contract_db: Isolated provisioned database resource exposing
        db_path.
    :return: Fresh dictionary with database_path converted to str.
    """
    return {"database_path": str(provisioned_contract_db.db_path)}


@pytest.fixture
def db(driver_spec: DriverSpec, db_metadata: dict):
    """
    Open the selected backend without creation or backup and yield the Database.

    On teardown, attempt to close database.driver and suppress ordinary close errors.
    This fixture does not call Database.close.

    Example:
        Run the shared fixtures through a contract test::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py


    :param driver_spec: Selected database driver specification, including its ID and
        Database db_type.
    :param db_metadata: Metadata mapping containing the provisioned SQLite database
        path.
    :return: Generator yielding one Database; its driver is closed on teardown when
        possible.
    """
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
    """
    Expose the existing driver owned by the shared Database fixture.

    Example:
        Run the shared fixtures through a contract test::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py


    :param db: Database yielded by the shared fixture.
    :return: The same db.driver object; no new connection is opened.
    """
    return db.driver


@pytest.fixture
def driver_wrapper(db):
    """
    Expose the existing wrapper attached to the shared Database fixture.

    Example:
        Run the shared fixtures through a contract test::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py


    :param db: Database yielded by the shared fixture.
    :return: The same db.driver_wrapper object.
    """
    return db.driver_wrapper


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def assert_integrity():
    """
    Provide a callable that checks the first retained SQLite integrity result.

    Example:
        Run the shared fixtures through a contract test::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py


    :return: Nested assertion helper; it neither commits nor closes the supplied
        connection.
    """
    def _assert_integrity(database) -> None:
        """
        Assert that the first retained integrity_check result string is ok, ignoring case.

        Use database.conn when truthy, otherwise database.driver.conn. On any
        execute/fetchall Exception, retry through conn.get. Ignore None rows; unwrap the
        first element of list/tuple rows. Later results are not checked and empty row
        sequences can raise IndexError.

        Example:
            Run the shared fixtures through a contract test::

                python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py


        :param database: Driver or Database-like object exposing a usable connection.
        :return: None when the first normalized result is ok; otherwise raises.
        """
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
    """
    Provide a selector that wraps integer indices across the supplied payload sequence.

    Example:
        Run the shared fixtures through a contract test::

            python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py


    :param all_torture_payloads: Ordered combined text and SQL-shaped payload corpus.
    :return: Nested callable retaining the supplied sequence.
    """
    def _pick(i: int) -> str:
        """
        Select a corpus element using index modulo corpus length, including negative indices.

        Example:
            Run the shared fixtures through a contract test::

                python -m pytest -q tests/databases/database_driver_plugins/database_driver_contract/test_contract_basic_crud_roundtrips.py


        :param i: Integer index to wrap across the captured payload corpus.
        :return: Payload at the wrapped index; an empty corpus raises ZeroDivisionError.
        """
        return all_torture_payloads[i % len(all_torture_payloads)]

    return _pick
