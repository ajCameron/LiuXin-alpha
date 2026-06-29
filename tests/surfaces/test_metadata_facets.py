from __future__ import annotations

import pytest

from LiuXin_alpha.surfaces.metadata_facets import (
    build_tag_row_payload,
    preferred_tag_table,
    resolve_tag_or_label_table_token,
    search_tag_rows,
    tag_row_text,
    tag_search_column_and_value,
    tag_search_value,
)


class _FakeDb:
    def __init__(self, *, tables, counts=None, columns=None, rows=None) -> None:
        self._tables = set(tables)
        self._counts = dict(counts or {})
        self._columns = {table: tuple(values) for table, values in dict(columns or {}).items()}
        self._rows = {key: list(value) for key, value in dict(rows or {}).items()}
        self.search_calls: list[tuple[str, str, str]] = []

    def get_tables(self):
        return sorted(self._tables)

    def get_record_count(self, table: str) -> int:
        return int(self._counts[table])

    def get_column_headings(self, table: str):
        return self._columns[table]

    def search(self, table: str, column: str, value: str):
        self.search_calls.append((table, column, value))
        return list(self._rows.get((table, column, value), []))


def test_preferred_tag_table_uses_real_tags_for_write_like_flows() -> None:
    db = _FakeDb(tables={"tags", "labels"}, counts={"tags": 0})

    assert preferred_tag_table(db) == "tags"


def test_preferred_tag_table_can_fall_back_to_labels_for_browse_categories() -> None:
    empty_tags_db = _FakeDb(tables={"tags", "labels"}, counts={"tags": 0})
    populated_tags_db = _FakeDb(tables={"tags", "labels"}, counts={"tags": 2})
    tags_only_db = _FakeDb(tables={"tags"}, counts={"tags": 0})
    labels_only_db = _FakeDb(tables={"labels"})

    assert preferred_tag_table(empty_tags_db, prefer_populated_tags=True) == "labels"
    assert preferred_tag_table(populated_tags_db, prefer_populated_tags=True) == "tags"
    assert preferred_tag_table(tags_only_db, prefer_populated_tags=True) == "tags"
    assert preferred_tag_table(labels_only_db, prefer_populated_tags=True) == "labels"


def test_preferred_tag_table_treats_count_errors_as_usable_tags() -> None:
    db = _FakeDb(tables={"tags", "labels"})

    assert preferred_tag_table(db, prefer_populated_tags=True) == "tags"


@pytest.mark.parametrize(
    ("token", "tables", "expected"),
    [
        ("tag", {"tags", "labels"}, "tags"),
        ("tags", {"labels"}, "labels"),
        ("label", {"tags", "labels"}, "labels"),
        ("labels", {"tags"}, "tags"),
        ("genre", {"tags", "labels"}, None),
    ],
)
def test_resolve_tag_or_label_table_token(token: str, tables: set[str], expected: str | None) -> None:
    assert resolve_tag_or_label_table_token(token, tables) == expected


def test_tag_search_column_and_value_prefers_normalized_columns() -> None:
    normalized = tag_search_value("Arabian Frights")

    assert tag_search_column_and_value("tags", {"tag", "tag_phash"}, "Arabian Frights") == ("tag_phash", normalized)
    assert tag_search_column_and_value("tags", {"tag"}, "Arabian Frights") == ("tag", "Arabian Frights")
    assert tag_search_column_and_value("labels", {"label_text", "label_text_norm"}, "Arabian Frights") == ("label_text_norm", normalized)
    assert tag_search_column_and_value("labels", {"label", "label_phash"}, "Arabian Frights") == ("label_phash", normalized)
    assert tag_search_column_and_value("labels", {"label_text"}, "Arabian Frights") == ("label_text", "Arabian Frights")


def test_search_tag_rows_uses_shared_column_selection() -> None:
    normalized = tag_search_value("Arabian Frights")
    rows = [{"tag": "Arabian Frights"}]
    db = _FakeDb(
        tables={"tags"},
        columns={"tags": ("tag", "tag_phash")},
        rows={("tags", "tag_phash", normalized): rows},
    )

    assert search_tag_rows(db, "tags", "Arabian Frights") == rows
    assert db.search_calls == [("tags", "tag_phash", normalized)]


def test_build_tag_row_payload_matches_current_tags_and_labels_columns() -> None:
    assert build_tag_row_payload("tags", {"tag", "tag_phash"}, "Arabian Frights") == {
        "tag": "Arabian Frights",
        "tag_phash": tag_search_value("Arabian Frights"),
    }
    assert build_tag_row_payload("labels", {"label_text", "label_text_norm"}, "Arabian Frights") == {
        "label_text": "Arabian Frights",
        "label_text_norm": tag_search_value("Arabian Frights"),
    }
    assert build_tag_row_payload("labels", {"label", "label_phash"}, "Arabian Frights") == {
        "label": "Arabian Frights",
        "label_phash": tag_search_value("Arabian Frights"),
    }


def test_build_tag_row_payload_requires_legacy_label_text_column() -> None:
    with pytest.raises(ValueError, match="supported text column"):
        build_tag_row_payload("labels", {"label_text_norm"}, "Arabian Frights")


def test_tag_row_text_keeps_legacy_display_precedence() -> None:
    assert tag_row_text({"label_text": "Label Text", "label": "Label", "tag": "Tag"}) == "Label Text"
    assert tag_row_text({"label": "Label", "tag": "Tag"}) == "Label"
    assert tag_row_text({"tag": "Tag"}) == "Tag"
    assert tag_row_text({"tag": "  "}) == ""
