# tests/metadata/containers/calibre_like_book_metadata/test_factory_methods_from_title_row.py

from __future__ import annotations

from collections import OrderedDict

import pytest

from LiuXin_alpha.metadata.containers.calibre_like_book_metadata import CalibreLikeLiuXinBookMetaData
from LiuXin_alpha.errors import InputIntegrityError


def test_from_title_row_unpatched_raises_nameerror() -> None:
    """
    Current code references RowCollection without importing it.
    This test both documents and covers that behavior.
    """
    md = CalibreLikeLiuXinBookMetaData()

    class _TitleRow:
        db = object()

    with pytest.raises(NameError):
        md.from_title_row(_TitleRow())  # type: ignore[arg-type]


def test_from_title_row_patched_happy_path(monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Patch RowCollection + DB helpers so we can cover the method without needing the real DB layer.
    """
    import LiuXin_alpha.metadata.containers.calibre_like_book_metadata.factory_methods as fm

    md = CalibreLikeLiuXinBookMetaData()
    data = object.__getattribute__(md, "_data")

    # Avoid the ".add" bug in the mixin by ensuring the chosen id bucket is a set for this test.
    # We'll force normalization to "isbn".
    data["isbn"] = set()

    class FakeDB:
        def get_categorized_tables(self):
            return {"main": ["titles", "genres", "notes", "publishers", "series", "creators", "identifiers", "languages"]}

        def get_display_column(self, table: str) -> str:
            return {
                "genres": "genre",
                "notes": "note",
                "publishers": "publisher",
                "series": "series",
            }.get(table, table.rstrip("s"))

    class FakeTitleRow:
        db = FakeDB()

    class FakeRowCollection:
        def __init__(self, _title_row):
            self._tables = {
                "titles": [
                    {
                        "title": "The Title",
                        "title_wordcount": 123,
                        "title_pubdate": None,
                    }
                ],
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
                "identifiers": [
                    {"identifier_type": "isbn", "identifier": "978-x"},
                ],
                "languages": [
                    {"language": "en"},
                ],
            }

        def __getitem__(self, item: str):
            return list(self._tables.get(item, []))

    # Inject missing globals + deterministic standardizers
    monkeypatch.setattr(fm, "RowCollection", FakeRowCollection, raising=False)
    monkeypatch.setattr(fm, "DatabaseIntegrityError", RuntimeError, raising=False)

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
    assert "en" == d["language"]
    assert d["series_index"] == 7
    assert "978-x" in d["isbn"]


def test_from_title_row_identifier_norm_none_raises_database_integrity(monkeypatch: pytest.MonkeyPatch) -> None:
    import LiuXin_alpha.metadata.containers.calibre_like_book_metadata.factory_methods as fm

    md = CalibreLikeLiuXinBookMetaData()

    class FakeDB:
        def get_categorized_tables(self):
            return {"main": ["titles", "identifiers"]}

        def get_display_column(self, table: str) -> str:
            return table

    class FakeTitleRow:
        db = FakeDB()

    class FakeRowCollection:
        def __init__(self, _title_row):
            self._tables = {
                "titles": [{"title": "T", "title_wordcount": 1, "title_pubdate": None}],
                "identifiers": [{"identifier_type": "???", "identifier": "X"}],
            }

        def __getitem__(self, item: str):
            return list(self._tables.get(item, []))

    class MyDbIntegrity(Exception):
        pass

    monkeypatch.setattr(fm, "RowCollection", FakeRowCollection, raising=False)
    monkeypatch.setattr(fm, "DatabaseIntegrityError", MyDbIntegrity, raising=False)
    monkeypatch.setattr(fm, "standardize_id_name", lambda x, logging=True: None, raising=False)
    monkeypatch.setattr(fm, "standardize_internal_id_name", lambda x: None, raising=False)

    with pytest.raises(MyDbIntegrity):
        md.from_title_row(FakeTitleRow())


def test_from_title_row_identifier_both_internal_and_external_raises_input_integrity(monkeypatch: pytest.MonkeyPatch) -> None:
    import LiuXin_alpha.metadata.containers.calibre_like_book_metadata.factory_methods as fm

    md = CalibreLikeLiuXinBookMetaData()

    class FakeDB:
        def get_categorized_tables(self):
            return {"main": ["titles", "identifiers"]}

        def get_display_column(self, table: str) -> str:
            return table

    class FakeTitleRow:
        db = FakeDB()

    class FakeRowCollection:
        def __init__(self, _title_row):
            self._tables = {
                "titles": [{"title": "T", "title_wordcount": 1, "title_pubdate": None}],
                "identifiers": [{"identifier_type": "isbn", "identifier": "X"}],
            }

        def __getitem__(self, item: str):
            return list(self._tables.get(item, []))

    monkeypatch.setattr(fm, "RowCollection", FakeRowCollection, raising=False)
    monkeypatch.setattr(fm, "DatabaseIntegrityError", RuntimeError, raising=False)
    monkeypatch.setattr(fm, "standardize_id_name", lambda x, logging=True: "isbn", raising=False)
    monkeypatch.setattr(fm, "standardize_internal_id_name", lambda x: "liuxin_internal", raising=False)

    with pytest.raises(InputIntegrityError):
        md.from_title_row(FakeTitleRow())
