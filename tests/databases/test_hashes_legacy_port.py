"""
Check legacy database fingerprint return types, repeatability and helper parity.

Runs against an isolated test_db_1 copy for each selected driver and closes Database
after each test. Equality checks do not pin a particular fingerprint algorithm or
exact token set.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_hashes_legacy_port.py
"""
from __future__ import annotations

import pytest

from LiuXin_alpha.databases.hashes import (
    generate_book_fingerprint,
    generate_title_fingerprint,
)
from LiuXin_alpha.catalog.metadata_tools.fingerprints import (
    generate_title_fingerprint as metadata_tools_generate_title_fingerprint,
)


@pytest.fixture
def db_with_test_db_1(provision_named_test_database, driver_spec):
    """
    Open an isolated test_db_1 database with creation and backup disabled.

    Yields the Database and closes it in finally after the test; setup errors propagate.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_hashes_legacy_port.py


    :param provision_named_test_database: Fixture factory that copies a named test
        database into an isolated location.
    :param driver_spec: Selected database driver specification, including its ID and
        Database db_type.
    :return: Generator yielding one open Database and closing it during fixture
        teardown.
    """
    from LiuXin_alpha.databases.database import Database

    provisioned = provision_named_test_database("test_db_1")
    db = Database(
        metadata={"database_path": str(provisioned.db_path)},
        db_type=driver_spec.db_type,
        create=False,
        backup=False,
    )
    try:
        yield db
    finally:
        db.close()


def test_generate_title_fingerprint_runs_for_all_titles(db_with_test_db_1) -> None:
    """
    Require a nonempty title fixture and a set-valued fingerprint for every title.

    Does not assert that each resulting set is nonempty.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_hashes_legacy_port.py::test_generate_title_fingerprint_runs_for_all_titles


    :param db_with_test_db_1: Open database fixture provisioned from test_db_1; the
        fixture closes it after use.
    :return: None; failed expectations raise AssertionError.
    """
    titles = list(db_with_test_db_1.get_all_rows("titles"))
    assert titles, "test_db_1 should contain title rows for legacy fingerprint tests"

    for title_row in titles:
        fingerprint = generate_title_fingerprint(db=db_with_test_db_1, title_row=title_row)
        assert isinstance(fingerprint, set)


def test_generate_title_fingerprint_is_deterministic(db_with_test_db_1) -> None:
    """
    Compare two fingerprint calls for the first available title.

    Checks equality within one database state, not across processes or catalogue
    changes.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_hashes_legacy_port.py::test_generate_title_fingerprint_is_deterministic


    :param db_with_test_db_1: Open database fixture provisioned from test_db_1; the
        fixture closes it after use.
    :return: None; failed expectations raise AssertionError.
    """
    title_row = next(iter(db_with_test_db_1.get_all_rows("titles")))

    first = generate_title_fingerprint(db=db_with_test_db_1, title_row=title_row)
    second = generate_title_fingerprint(db=db_with_test_db_1, title_row=title_row)
    assert first == second


def test_generate_book_fingerprint_runs_for_all_books(db_with_test_db_1) -> None:
    """
    Require a nonempty book fixture and a set-valued fingerprint for every book.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_hashes_legacy_port.py::test_generate_book_fingerprint_runs_for_all_books


    :param db_with_test_db_1: Open database fixture provisioned from test_db_1; the
        fixture closes it after use.
    :return: None; failed expectations raise AssertionError.
    """
    books = list(db_with_test_db_1.get_all_rows("books"))
    assert books, "test_db_1 should contain book rows for legacy fingerprint tests"

    for book_row in books:
        fingerprint = generate_book_fingerprint(db=db_with_test_db_1, book_row=book_row)
        assert isinstance(fingerprint, set)


def test_hashes_and_metadata_tools_title_fingerprint_parity(db_with_test_db_1) -> None:
    """
    Compare database and metadata-tools fingerprint helpers for the first available title.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_hashes_legacy_port.py::test_hashes_and_metadata_tools_title_fingerprint_parity


    :param db_with_test_db_1: Open database fixture provisioned from test_db_1; the
        fixture closes it after use.
    :return: None; failed expectations raise AssertionError.
    """
    title_row = next(iter(db_with_test_db_1.get_all_rows("titles")))

    from_hashes = generate_title_fingerprint(db=db_with_test_db_1, title_row=title_row)
    from_metadata_tools = metadata_tools_generate_title_fingerprint(db=db_with_test_db_1, title_row=title_row)
    assert from_hashes == from_metadata_tools
