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

    def __init__(
        self, *, tables, counts=None, columns=None, rows=None, identities=None
    ) -> None:
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
        self._identities = dict(identities or {})
        self.identity_calls: list[tuple[str, str, str]] = []

    def derive_identity_value(self, table, column, value):
        """Return an explicitly configured policy result; missing declarations fail."""
        self.identity_calls.append((table, column, value))
        return self._identities[(table, column, value)]

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


@pytest.mark.parametrize("tables,expected", [({"tags"}, "tags"), ({"labels"}, "labels"), (set(), None)])
def test_write_tag_target_depends_only_on_schema(tables, expected) -> None:
    """Write selection does not ask for a count or apply the browse-source policy."""
    assert preferred_tag_table(_FakeDb(tables=tables)) == expected


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
    """Every key comes from the advertised policy, while raw searches stay exact."""
    text = "Arabian Frights"
    normalized = "custom declared key"
    sources = [("tags", "tag"), ("labels", "label_text"), ("labels", "label")]
    db = _FakeDb(
        tables={"tags", "labels"},
        identities={(table, column, text): normalized for table, column in sources},
    )
    for table, source, identity in (
        ("tags", "tag", "tag_phash"),
        ("labels", "label_text", "label_text_norm"),
        ("labels", "label", "label_phash"),
    ):
        assert tag_search_column_and_value(db, table, {source, identity}, text) == (
            identity,
            normalized,
        )
        assert tag_search_column_and_value(db, table, {source}, text) == (source, text)
    assert db.identity_calls == [(table, source, text) for table, source in sources]


def test_search_tag_rows_uses_shared_column_selection() -> None:
    """The declared identity drives the exact lookup without a second transformation."""
    normalized = "custom declared key"
    rows = [{"tag": "Arabian Frights"}]
    db = _FakeDb(
        tables={"tags"},
        columns={"tags": ("tag", "tag_phash")},
        identities={("tags", "tag", "Arabian Frights"): normalized},
        rows={("tags", "tag_phash", normalized): rows},
    )
    assert search_tag_rows(db, "tags", "Arabian Frights") == rows
    assert db.identity_calls == [("tags", "tag", "Arabian Frights")]
    assert db.search_calls == [("tags", "tag_phash", normalized)]


def test_search_tag_rows_propagates_missing_identity_policy() -> None:
    """A missing declaration must not silently become a miss or a local rule."""
    db = _FakeDb(tables={"tags"}, columns={"tags": ("tag", "tag_phash")})
    with pytest.raises(KeyError):
        search_tag_rows(db, "tags", "Arabian Frights")
    assert db.search_calls == []


def test_build_tag_row_payload_matches_current_tags_and_labels_columns() -> None:
    """Creation payloads contain source text and leave derived keys to Catalog."""
    for table, source, identity in (
        ("tags", "tag", "tag_phash"),
        ("labels", "label_text", "label_text_norm"),
        ("labels", "label", "label_phash"),
    ):
        assert build_tag_row_payload(table, {source, identity}, "Arabian Frights") == {
            source: "Arabian Frights"
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
