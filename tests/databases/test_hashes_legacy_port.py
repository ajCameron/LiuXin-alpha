from __future__ import annotations

import pytest

from LiuXin_alpha.databases.hashes import (
    generate_book_fingerprint,
    generate_title_fingerprint,
)
from LiuXin_alpha.databases.metadata_tools.fingerprints import (
    generate_title_fingerprint as metadata_tools_generate_title_fingerprint,
)


@pytest.fixture
def db_with_test_db_1(provision_named_test_database, driver_spec):
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
    titles = list(db_with_test_db_1.get_all_rows("titles"))
    assert titles, "test_db_1 should contain title rows for legacy fingerprint tests"

    for title_row in titles:
        fingerprint = generate_title_fingerprint(db=db_with_test_db_1, title_row=title_row)
        assert isinstance(fingerprint, set)


def test_generate_title_fingerprint_is_deterministic(db_with_test_db_1) -> None:
    title_row = next(iter(db_with_test_db_1.get_all_rows("titles")))

    first = generate_title_fingerprint(db=db_with_test_db_1, title_row=title_row)
    second = generate_title_fingerprint(db=db_with_test_db_1, title_row=title_row)
    assert first == second


def test_generate_book_fingerprint_runs_for_all_books(db_with_test_db_1) -> None:
    books = list(db_with_test_db_1.get_all_rows("books"))
    assert books, "test_db_1 should contain book rows for legacy fingerprint tests"

    for book_row in books:
        fingerprint = generate_book_fingerprint(db=db_with_test_db_1, book_row=book_row)
        assert isinstance(fingerprint, set)


def test_hashes_and_metadata_tools_title_fingerprint_parity(db_with_test_db_1) -> None:
    title_row = next(iter(db_with_test_db_1.get_all_rows("titles")))

    from_hashes = generate_title_fingerprint(db=db_with_test_db_1, title_row=title_row)
    from_metadata_tools = metadata_tools_generate_title_fingerprint(db=db_with_test_db_1, title_row=title_row)
    assert from_hashes == from_metadata_tools
