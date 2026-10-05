"""
Check tag/label compatibility policies with in-memory schema, count, and search fixtures.

These tests cover write-like versus browse-like table preference, normalized
column selection, payload shape, and display precedence without opening a database.
"""

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
    """
    Provide deterministic table metadata and exact search results while recording query arguments.

    Schema and result collections are copied on construction; returned lists are
    copied again so callers cannot replace the stored collections through them.

    Example:
        >>> db = _FakeDb(tables={"tags"}, counts={"tags": 2})
        >>> db.get_tables(), db.get_record_count("tags")
        (['tags'], 2)
    """

    def __init__(self, *, tables, counts=None, columns=None, rows=None) -> None:
        """
        Copy configured schema/count/search fixtures and start an empty search-call log.

        Example:
            >>> db = _FakeDb(tables={"tags"}, columns={"tags": ["tag"]})
            >>> db.get_column_headings("tags")
            ('tag',)


        :param tables: Iterable of advertised table names, stored as a set.
        :param counts: Optional table-to-count mapping; absent entries raise on lookup.
        :param columns: Optional table-to-column iterable mapping, copied into tuples.
        :param rows: Optional table/column/value tuple keys mapped to row iterables.
        :return: ``None`` after storing independent fixture containers.
        """
        self._tables = set(tables)
        self._counts = dict(counts or {})
        self._columns = {
            table: tuple(values) for table, values in dict(columns or {}).items()
        }
        self._rows = {key: list(value) for key, value in dict(rows or {}).items()}
        self.search_calls: list[tuple[str, str, str]] = []

    def get_tables(self):
        """
        Return advertised table names in deterministic sorted order.

        Example:
            >>> _FakeDb(tables={"tags", "labels"}).get_tables()
            ['labels', 'tags']


        :return: New sorted table-name list.
        """
        return sorted(self._tables)

    def get_record_count(self, table: str) -> int:
        """
        Retrieve and integer-convert a configured count, retaining lookup/conversion failures.

        Example:
            >>> _FakeDb(tables={"tags"}, counts={"tags": "2"}).get_record_count("tags")
            2


        :param table: Exact key in the configured count mapping.
        :return: Configured count converted to an integer.
        :raises KeyError: If no count was configured for the table.
        """
        return int(self._counts[table])

    def get_column_headings(self, table: str):
        """
        Return a table's configured immutable column sequence.

        Example:
            >>> _FakeDb(tables={"tags"}, columns={"tags": ["tag"]}).get_column_headings("tags")
            ('tag',)


        :param table: Exact table key in the configured schema mapping.
        :return: Stored column tuple in fixture order.
        :raises KeyError: If columns were not configured for the table.
        """
        return self._columns[table]

    def search(self, table: str, column: str, value: str):
        """
        Record an exact search and return a fresh list of its configured matching rows.

        Example:
            >>> db = _FakeDb(tables={"tags"})
            >>> db.search("tags", "tag", "missing")
            []
            >>> db.search_calls
            [('tags', 'tag', 'missing')]


        :param table: Table component of the exact fixture lookup key.
        :param column: Column component of the lookup key.
        :param value: Unnormalized value component of the lookup key.
        :return: Copied row list, or an empty list when no matching fixture was configured.
        """
        self.search_calls.append((table, column, value))
        return list(self._rows.get((table, column, value), []))


def test_preferred_tag_table_uses_real_tags_for_write_like_flows() -> None:
    """
    Verify default table selection prefers tags even when its configured row count is zero.

    Example:
        >>> test_preferred_tag_table_uses_real_tags_for_write_like_flows()


    :return: ``None`` when empty tags still win over labels in the default policy.
    """
    db = _FakeDb(tables={"tags", "labels"}, counts={"tags": 0})

    assert preferred_tag_table(db) == "tags"


def test_preferred_tag_table_can_fall_back_to_labels_for_browse_categories() -> None:
    """
    Cover populated preference across mixed, tags-only, and labels-only schemas.

    Example:
        >>> test_preferred_tag_table_can_fall_back_to_labels_for_browse_categories()


    :return: ``None`` when each population/schema combination selects the expected table.
    """
    empty_tags_db = _FakeDb(tables={"tags", "labels"}, counts={"tags": 0})
    populated_tags_db = _FakeDb(tables={"tags", "labels"}, counts={"tags": 2})
    tags_only_db = _FakeDb(tables={"tags"}, counts={"tags": 0})
    labels_only_db = _FakeDb(tables={"labels"})

    assert preferred_tag_table(empty_tags_db, prefer_populated_tags=True) == "labels"
    assert preferred_tag_table(populated_tags_db, prefer_populated_tags=True) == "tags"
    assert preferred_tag_table(tags_only_db, prefer_populated_tags=True) == "tags"
    assert preferred_tag_table(labels_only_db, prefer_populated_tags=True) == "labels"


def test_preferred_tag_table_treats_count_errors_as_usable_tags() -> None:
    """
    Verify an unavailable tag count does not force a fallback to labels.

    The fake raises ``KeyError`` for its unconfigured count, exercising the helper's
    count-error policy rather than simulating a genuine zero count.

    Example:
        >>> test_preferred_tag_table_treats_count_errors_as_usable_tags()


    :return: ``None`` when count failure still selects tags.
    """
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
def test_resolve_tag_or_label_table_token(
    token: str, tables: set[str], expected: str | None
) -> None:
    """
    Check requested-family precedence, cross-family fallback, and unknown-token rejection.

    Example:
        >>> test_resolve_tag_or_label_table_token("tag", {"labels"}, "labels")


    :param token: Parametrized user-facing tag/label or unsupported kind spelling.
    :param tables: Parametrized available table-name set.
    :param expected: Expected concrete table or ``None`` for an unsupported token.
    :return: ``None`` when resolution agrees with the selected case.
    """
    assert resolve_tag_or_label_table_token(token, tables) == expected


def test_tag_search_column_and_value_prefers_normalized_columns() -> None:
    """
    Check normalized tag/label column precedence and raw-text fallbacks for representative schemas.

    Example:
        >>> test_tag_search_column_and_value_prefers_normalized_columns()


    :return: ``None`` when each tested schema chooses the intended column and value representation.
    """
    normalized = tag_search_value("Arabian Frights")

    assert tag_search_column_and_value(
        "tags", {"tag", "tag_phash"}, "Arabian Frights"
    ) == ("tag_phash", normalized)
    assert tag_search_column_and_value("tags", {"tag"}, "Arabian Frights") == (
        "tag",
        "Arabian Frights",
    )
    assert tag_search_column_and_value(
        "labels", {"label_text", "label_text_norm"}, "Arabian Frights"
    ) == ("label_text_norm", normalized)
    assert tag_search_column_and_value(
        "labels", {"label", "label_phash"}, "Arabian Frights"
    ) == ("label_phash", normalized)
    assert tag_search_column_and_value("labels", {"label_text"}, "Arabian Frights") == (
        "label_text",
        "Arabian Frights",
    )


def test_search_tag_rows_uses_shared_column_selection() -> None:
    """
    Verify tag searching returns configured rows and sends the normalized hash-column query.

    Example:
        >>> test_search_tag_rows_uses_shared_column_selection()


    :return: ``None`` when both returned rows and the recorded search call match expectations.
    """
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
    """
    Verify exact creation payloads for current tags and both supported legacy label text layouts.

    Example:
        >>> test_build_tag_row_payload_matches_current_tags_and_labels_columns()


    :return: ``None`` when original text and schema-selected normalized fields are preserved.
    """
    assert build_tag_row_payload("tags", {"tag", "tag_phash"}, "Arabian Frights") == {
        "tag": "Arabian Frights",
        "tag_phash": tag_search_value("Arabian Frights"),
    }
    assert build_tag_row_payload(
        "labels", {"label_text", "label_text_norm"}, "Arabian Frights"
    ) == {
        "label_text": "Arabian Frights",
        "label_text_norm": tag_search_value("Arabian Frights"),
    }
    assert build_tag_row_payload(
        "labels", {"label", "label_phash"}, "Arabian Frights"
    ) == {
        "label": "Arabian Frights",
        "label_phash": tag_search_value("Arabian Frights"),
    }


def test_build_tag_row_payload_requires_legacy_label_text_column() -> None:
    """
    Require a clear error when a labels schema advertises normalization but no usable display text column.

    Example:
        >>> test_build_tag_row_payload_requires_legacy_label_text_column()


    :return: ``None`` when payload construction raises the expected supported-column error.
    """
    with pytest.raises(ValueError, match="supported text column"):
        build_tag_row_payload("labels", {"label_text_norm"}, "Arabian Frights")


def test_tag_row_text_keeps_legacy_display_precedence() -> None:
    """
    Check label-text, label, and tag display precedence plus the blank-text fallback.

    Example:
        >>> test_tag_row_text_keeps_legacy_display_precedence()


    :return: ``None`` when the first available nonblank display value is selected in each case.
    """
    assert (
        tag_row_text({"label_text": "Label Text", "label": "Label", "tag": "Tag"})
        == "Label Text"
    )
    assert tag_row_text({"label": "Label", "tag": "Tag"}) == "Label"
    assert tag_row_text({"tag": "Tag"}) == "Tag"
    assert tag_row_text({"tag": "  "}) == ""
