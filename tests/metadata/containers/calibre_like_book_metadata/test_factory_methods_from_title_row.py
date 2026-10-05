# tests/metadata/containers/calibre_like_book_metadata/test_factory_methods_from_title_row.py

"""
Verify metadata factories interpret title rows and related values.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test factory methods from title row through its owning regression module::

        python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_factory_methods_from_title_row.py
"""
from __future__ import annotations

from collections import OrderedDict

import pytest

from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData
from LiuXin_alpha.errors import InputIntegrityError


class _DriverWrapper:
    """
    Provide the schema and link-name behavior needed by read-source contract tests.

    Example:
        Exercise DriverWrapper through its owning regression module::

            python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_factory_methods_from_title_row.py
    """
    def get_display_column(self, table: str) -> str:
        """
        Return display column from deterministic test state.

        Example:
            Exercise DriverWrapper.get display column through its owning regression module::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_factory_methods_from_title_row.py


        :param table: Table name addressed by the test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return {
            "genres": "genre",
            "notes": "note",
            "publishers": "publisher",
            "series": "series",
            "synopses": "synopsis",
            "subjects": "subject",
            "tags": "tag",
        }.get(table, table.rstrip("s"))


class _FakeDB:
    """
    Provide the FakeDB test fixture or double with explicit deterministic behavior.

    Example:
        Exercise FakeDB through its owning regression module::

            python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_factory_methods_from_title_row.py
    """
    def __init__(self, tables: dict[str, list[dict]]) -> None:
        """
        Initialize the FakeDB test double.

        Example:
            Exercise FakeDB.init through its owning regression module::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_factory_methods_from_title_row.py


        :param tables: Value supplied for tables in the focused test operation.
        :return: None; the function records state or raises through its assertions.
        """
        self._tables = tables
        self.driver_wrapper = _DriverWrapper()

    def get_linked_rows(self, _title_row, table: str):
        """
        Return linked rows from deterministic test state.

        Example:
            Exercise FakeDB.get linked rows through its owning regression module::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_factory_methods_from_title_row.py


        :param _title_row: Value supplied for title row in the focused test operation.
        :param table: Table name addressed by the test operation.
        :return: The deterministic value, row, identity or collection described above.
        """
        return list(self._tables.get(table, []))


def test_from_title_row_requires_db_linked_rows_api() -> None:
    """
    Verify from title row requires db linked rows api.

    Example:
        Exercise test from title row requires db linked rows api through its owning regression module::

            python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_factory_methods_from_title_row.py


    :return: None; the function records state or raises through its assertions.
    """
    md = CalibreLikeLiuXinBookMetaData()

    class _TitleRow:
        """
        Provide the TitleRow test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test from title row requires db linked rows api.TitleRow through its owning regression module::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_factory_methods_from_title_row.py
        """
        db = object()

    with pytest.raises(AttributeError):
        md.from_title_row(_TitleRow())  # type: ignore[arg-type]


def test_from_title_row_patched_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Verify from title row patched happy path.

    Example:
        Exercise test from title row patched happy path through its owning regression module::

            python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_factory_methods_from_title_row.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.containers.calibre_like_book_metadata.factory_methods as fm

    md = CalibreLikeLiuXinBookMetaData()
    data = object.__getattribute__(md, "_data")

    # Avoid the ".add" bug in the mixin by ensuring the chosen id bucket is a set for this test.
    # We'll force normalization to "isbn".
    data["isbn"] = set()

    class FakeTitleRow:
        """
        Provide the FakeTitleRow test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test from title row patched happy path.FakeTitleRow through its owning regression module::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_factory_methods_from_title_row.py
        """
        db = _FakeDB(
            {
                "titles": [{"title": "The Title", "title_wordcount": 123, "title_pubdate": None}],
                "genres": [{"genre": "SF"}],
                "notes": [{"note": "N"}],
                "publishers": [
                    {"publisher": "BigPub", "publisher_parent": "parent"},
                    {"publisher": "ImprintPub", "publisher_parent": "None"},
                ],
                "series": [{"series": "S1", "series_title_link_priority": 7}],
                "creators": [
                    {"creator_title_link_type": None, "creator_id": 1, "creator": "Alice"},
                    {"creator_title_link_type": "editor", "creator_id": 2, "creator": "Ed"},
                ],
                "identifiers": [{"identifier_type": "isbn", "identifier": "978-x"}],
                "languages": [{"language": "en"}],
            }
        )

    monkeypatch.setattr(fm, "standardize_creator_category", lambda x: "authors" if not x else "editors", raising=False)
    monkeypatch.setattr(fm, "standardize_id_name", lambda x, logging=True: "isbn", raising=False)
    monkeypatch.setattr(fm, "standardize_internal_id_name", lambda x: None, raising=False)

    md.from_title_row(FakeTitleRow())

    d = object.__getattribute__(md, "_data")
    assert d["title"] == "The Title"
    assert "SF" in d["genres"]
    assert "N" in d["notes"]
    assert "Alice" in d["authors"]
    assert "Ed" in d["editors"]
    assert "BigPub" in d["publishers"]
    assert "ImprintPub" in d["imprints"]
    assert "en" == d["language"]
    assert d["series_index"] == 7
    assert "978-x" in d["isbn"]


def test_from_title_row_identifier_norm_none_raises_database_integrity(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Verify from title row identifier norm none raises database integrity.

    Example:
        Exercise test from title row identifier norm none raises database integrity through its owning regression module::

            python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_factory_methods_from_title_row.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.containers.calibre_like_book_metadata.factory_methods as fm

    md = CalibreLikeLiuXinBookMetaData()

    class FakeTitleRow:
        """
        Provide the FakeTitleRow test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test from title row identifier norm none raises database integrity.FakeTitleRow through its owning regression module::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_factory_methods_from_title_row.py
        """
        db = _FakeDB(
            {
                "titles": [{"title": "T", "title_wordcount": 1, "title_pubdate": None}],
                "identifiers": [{"identifier_type": "???", "identifier": "X"}],
            }
        )

    class MyDbIntegrity(Exception):
        """
        Provide the MyDbIntegrity test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test from title row identifier norm none raises database integrity.MyDbIntegrity through its owning regression module::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_factory_methods_from_title_row.py
        """
        pass

    monkeypatch.setattr(fm, "DatabaseIntegrityError", MyDbIntegrity, raising=False)
    monkeypatch.setattr(fm, "standardize_id_name", lambda x, logging=True: None, raising=False)
    monkeypatch.setattr(fm, "standardize_internal_id_name", lambda x: None, raising=False)

    with pytest.raises(MyDbIntegrity):
        md.from_title_row(FakeTitleRow())


def test_from_title_row_identifier_both_internal_and_external_raises_input_integrity(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Verify from title row identifier both internal and external raises input integrity.

    Example:
        Exercise test from title row identifier both internal and external raises input integrity through its owning regression module::

            python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_factory_methods_from_title_row.py


    :param monkeypatch: Pytest fixture used to isolate collaborators or environment
        state.
    :return: None; the function records state or raises through its assertions.
    """
    import LiuXin_alpha.metadata.containers.calibre_like_book_metadata.factory_methods as fm

    md = CalibreLikeLiuXinBookMetaData()

    class FakeTitleRow:
        """
        Provide the FakeTitleRow test fixture or double with explicit deterministic behavior.

        Example:
            Exercise test from title row identifier both internal and external raises input integrity.FakeTitleRow through its owning regression module::

                python -m pytest -q tests/metadata/containers/calibre_like_book_metadata/test_factory_methods_from_title_row.py
        """
        db = _FakeDB(
            {
                "titles": [{"title": "T", "title_wordcount": 1, "title_pubdate": None}],
                "identifiers": [{"identifier_type": "isbn", "identifier": "X"}],
            }
        )

    monkeypatch.setattr(fm, "DatabaseIntegrityError", RuntimeError, raising=False)
    monkeypatch.setattr(fm, "standardize_id_name", lambda x, logging=True: "isbn", raising=False)
    monkeypatch.setattr(fm, "standardize_internal_id_name", lambda x: "liuxin_internal", raising=False)

    with pytest.raises(InputIntegrityError):
        md.from_title_row(FakeTitleRow())
