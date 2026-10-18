"""
Adapt tag-like lookup, display, and creation payloads to current tags and legacy labels schemas.

Write table selection prefers tags whenever present. Catalogue browsing selects
its source through Core browse queries. Helpers only select fields
or perform reads; payload construction does not insert a record.
"""

from __future__ import annotations

from typing import Optional


TAG_TOKENS = frozenset({"tag", "tags"})
LABEL_TOKENS = frozenset({"label", "labels"})


def database_table_names(db) -> set[str]:
    """
    Return the catalogue table names exposed by a database facade.

    Names are stringified and deduplicated, without case normalization or sorting.

    Example:
        >>> from unittest.mock import Mock
        >>> db = Mock()
        >>> db.get_tables.return_value = ["tags", "tags", "labels"]
        >>> sorted(database_table_names(db))
        ['labels', 'tags']


    :param db: Facade providing an iterable of table names through ``get_tables``.
    :return: New set of advertised table-name strings; enumeration errors propagate.
    """
    return {str(table) for table in db.get_tables()}


def preferred_tag_table(db, *, tables: Optional[set[str]] = None) -> Optional[str]:
    """Choose a write target, preferring an existing tags table regardless of count.

    Labels remain the target only for a labels-only schema. This helper does not
    select browse sources; Core owns that policy and its read failures.
    """
    available = set(tables) if tables is not None else database_table_names(db)
    return next((table for table in ("tags", "labels") if table in available), None)


def resolve_tag_or_label_table_token(token: str, tables: set[str]) -> Optional[str]:
    """
    Resolve a user tag or label token against available tables.

    The requested family wins when both tables exist; the other family is a
    compatibility fallback. Unknown tokens do not invoke a general table resolver.

    Example:
        >>> resolve_tag_or_label_table_token(" label ", {"tags", "labels"})
        'labels'
        >>> resolve_tag_or_label_table_token("tags", {"labels"})
        'labels'


    :param token: Singular/plural tag or label spelling, stripped and lowercased.
    :param tables: Available exact table names, not modified or normalized here.
    :return: Matching/fallback table name, or ``None`` for unknown tokens or absent families.
    """
    text = str(token).strip().lower()
    if text in TAG_TOKENS:
        if "tags" in tables:
            return "tags"
        if "labels" in tables:
            return "labels"
    if text in LABEL_TOKENS:
        if "labels" in tables:
            return "labels"
        if "tags" in tables:
            return "tags"
    return None


def tag_search_column_and_value(
    db, table: str, columns: set[str], tag_text: str
) -> tuple[str, str]:
    """Select a tag field and ask the database to derive any stored identity key.

    Raw text schemas use exact display matching. When an identity column exists,
    a missing or invalid declaration is an error; no local policy substitutes for it.
    """
    if table == "tags":
        if "tag_phash" in columns:
            return "tag_phash", db.derive_identity_value("tags", "tag", tag_text)
        return "tag", tag_text
    if table == "labels":
        source = "label_text" if "label_text" in columns else "label"
        for identity in ("label_text_norm", "label_phash"):
            if identity in columns:
                return identity, db.derive_identity_value("labels", source, tag_text)
        return source, tag_text
    raise ValueError("Unsupported tag table: {!r}".format(table))


def tag_row_text(row) -> str:
    """
    Read the display text from a tag- or label-shaped row.

    Try ``label_text``, ``label``, then ``tag``. Lookup exceptions become missing
    values; string conversion errors still propagate. Non-``None`` falsey values
    such as zero can produce display text.

    Example:
        >>> tag_row_text({"label_text": " ", "label": " Legacy ", "tag": "Modern"})
        'Legacy'


    :param row: Mapping-like row supporting column-name subscription.
    :return: First nonblank stripped text in display precedence order, or an empty string.
    """
    for column in ("label_text", "label", "tag"):
        try:
            value = row[column]
        except Exception:
            value = None
        if value is not None:
            text = str(value).strip()
            if text:
                return text
    return ""


def tag_row_identity_column(table: str) -> str:
    """
    Return the identity-column name for a tag-like table.

    This is a fixed naming convention, not a live schema lookup.

    Example:
        >>> tag_row_identity_column("tags"), tag_row_identity_column("labels")
        ('tag_id', 'label_id')


    :param table: Exact supported table name, ``tags`` or ``labels``.
    :return: Conventional primary identity-column name for the selected family.
    :raises ValueError: If the table name is unsupported.
    """
    if table == "tags":
        return "tag_id"
    if table == "labels":
        return "label_id"
    raise ValueError("Unsupported tag table: {!r}".format(table))


def build_tag_row_payload(
    table: str, columns: set[str], tag_text: str
) -> dict[str, object]:
    """Supply display text; Catalog owns derived identity fields on insertion."""
    if table == "tags":
        return {"tag": tag_text}
    if table == "labels":
        for column in ("label_text", "label"):
            if column in columns:
                return {column: tag_text}
        raise ValueError(
            "`labels` table has no supported text column (`label_text`/`label`)."
        )
    raise ValueError("Unsupported tag table: {!r}".format(table))


def search_tag_rows(db, table: str, tag_text: str) -> list[object]:
    """
    Find rows matching tag text in a tag-like table.

    Choose one column from the advertised schema and materialize that search's
    results. A miss does not retry other columns; schema/search errors propagate.

    Example:
        >>> from unittest.mock import Mock
        >>> db = Mock()
        >>> db.get_column_headings.return_value = ["tag", "tag_phash"]
        >>> db.derive_identity_value.return_value = "science fiction"
        >>> db.search.return_value = []
        >>> search_tag_rows(db, "tags", "Science Fiction")
        []
        >>> db.search.assert_called_once_with("tags", "tag_phash", "science fiction")


    :param db: Facade exposing column headings and column/value search.
    :param table: Supported tag-like table to search.
    :param tag_text: Display text used directly or normalized according to the chosen column.
    :return: List of all returned matching rows in backend order.
    """
    columns = set(db.get_column_headings(table))
    search_column, search_value = tag_search_column_and_value(
        db, table, columns, tag_text
    )
    return list(db.search(table, search_column, search_value))


__all__ = [
    "LABEL_TOKENS",
    "TAG_TOKENS",
    "build_tag_row_payload",
    "database_table_names",
    "preferred_tag_table",
    "resolve_tag_or_label_table_token",
    "search_tag_rows",
    "tag_row_identity_column",
    "tag_row_text",
    "tag_search_column_and_value",
]
