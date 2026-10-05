"""
Build deterministic test-database test legacy objects smoke data and relationships.

The module keeps generated data, ordering and failure modes explicit so consumers
can assert stable behavior.

Example:
    Exercise test legacy objects smoke through a consuming regression::

        python -m pytest -q tests/databases/test_test_resources_manager.py
"""
from __future__ import annotations

from pathlib import Path

from tests.support.test_databases._legacy.objects import TestObjectsHandler as LegacyTestObjectsHandler


def test_test_objects_handler_supports_default_scratch_manager(
    monkeypatch, tmp_path: Path
) -> None:
    """
    Verify test objects handler supports default scratch manager.

    Example:
        Exercise test test objects handler supports default scratch manager through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param monkeypatch: Pytest fixture used to isolate dependencies and mutable process
        state.
    :param tmp_path: Pytest-managed temporary directory for generated fixture data.
    :return: None; completion is expressed through state changes or assertions.
    """
    books = tmp_path / "books"
    covers = tmp_path / "covers"
    books.mkdir()
    covers.mkdir()

    (books / "epub_md_test_file_1.epub").write_bytes(b"book-payload")
    (covers / "book_id_1.jpg").write_bytes(b"cover-payload")

    monkeypatch.setenv("LIUXIN_TEST_BOOKS_DIR", str(books))
    monkeypatch.setenv("LIUXIN_TEST_COVERS_DIR", str(covers))

    handler = LegacyTestObjectsHandler(scratch_file_handler=None, try_for_ramdisk_cache=True)

    cover_path = Path(handler.get_rand_test_cover_path())
    assert cover_path.exists()
    assert cover_path.name == "book_id_1.jpg"
    assert cover_path.read_bytes() == b"cover-payload"
    assert cover_path.parent != covers


def test_test_objects_handler_copies_named_md_fixture_into_scratch_folder(
    monkeypatch, tmp_path: Path
) -> None:
    """
    Verify test objects handler copies named md fixture into scratch folder.

    Example:
        Exercise test test objects handler copies named md fixture into scratch folder through a consuming regression::

            python -m pytest -q tests/databases/test_test_resources_manager.py


    :param monkeypatch: Pytest fixture used to isolate dependencies and mutable process
        state.
    :param tmp_path: Pytest-managed temporary directory for generated fixture data.
    :return: None; completion is expressed through state changes or assertions.
    """
    books = tmp_path / "books"
    covers = tmp_path / "covers"
    books.mkdir()
    covers.mkdir()

    (books / "epub_md_test_file_1.epub").write_bytes(b"book-payload")
    (covers / "book_id_1.jpg").write_bytes(b"cover-payload")

    monkeypatch.setenv("LIUXIN_TEST_BOOKS_DIR", str(books))
    monkeypatch.setenv("LIUXIN_TEST_COVERS_DIR", str(covers))

    handler = LegacyTestObjectsHandler(scratch_file_handler=None, try_for_ramdisk_cache=True)

    md_path = Path(handler.get_test_md_file_path("epub", "1", folder_name="nested"))
    assert md_path.exists()
    assert md_path.name == "epub_md_test_file_1.epub"
    assert md_path.read_bytes() == b"book-payload"
    assert md_path.parent.name == "nested"
    assert md_path.parent.parent != books
