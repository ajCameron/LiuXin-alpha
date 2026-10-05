"""
Adapt tag-like lookup, display, and creation payloads to current tags and legacy labels schemas.

Table selection is policy-specific: writes normally prefer tags, while callers
can request a populated-tags preference for browsing. Helpers only select fields
or perform reads; payload construction does not insert a record.
"""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.metadata.standardization import make_tag_search_term


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


def preferred_tag_table(
    db,
    *,
    prefer_populated_tags: bool = False,
    tables: Optional[set[str]] = None,
) -> Optional[str]:
    """
    Choose the usable tag-like table for the current catalogue schema.

    Tags win by default without a count query. With the populated preference,
    a positive tag count or a count/conversion error still selects tags; otherwise
    labels win when available. Neither choice verifies readable text columns.

    Example:
        >>> preferred_tag_table(None, tables={"tags", "labels"})
        'tags'
        >>> preferred_tag_table(None, tables={"labels"})
        'labels'


    :param db: Facade supplying table names and, when requested, the tag record count.
    :param prefer_populated_tags: Permit labels when tags have a nonpositive count and labels exist.
    :param tables: Optional precomputed table-name set; ``None`` triggers facade enumeration.
    :return: ``tags``, ``labels``, or ``None`` when neither is advertised.
    """
    available_tables = set(tables) if tables is not None else database_table_names(db)

    if "tags" in available_tables:
        if prefer_populated_tags:
            try:
                if int(db.get_record_count("tags")) > 0:
                    return "tags"
            except Exception:
                return "tags"
            if "labels" not in available_tables:
                return "tags"
        else:
            return "tags"

    if "labels" in available_tables:
        return "labels"
    if "tags" in available_tables:
        return "tags"
    return None


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


def tag_search_value(tag_text: str) -> str:
    """
    Normalize tag text for schema-compatible searching.

    The shared normalization removes whitespace and lowercases text; it is not a
    cryptographic hash despite legacy column names such as ``tag_phash``.

    Example:
        >>> tag_search_value("Science Fiction")
        'sciencefiction'


    :param tag_text: Tag display text accepted by the shared string normalizer.
    :return: Whitespace-free lowercase search key.
    """
    return make_tag_search_term(tag_text)


def tag_search_column_and_value(
    table: str, columns: set[str], tag_text: str
) -> tuple[str, str]:
    """
    Choose the schema-specific column and value for a tag search.

    Tags use ``tag_phash`` when available, otherwise raw ``tag``. Labels prefer
    normalized text, then hash, then raw text, then raw ``label``. The final raw
    fallback column is assumed rather than checked; this is not schema validation.

    Example:
        >>> tag_search_column_and_value("tags", {"tag_phash"}, "Science Fiction")
        ('tag_phash', 'sciencefiction')


    :param table: Exact supported table name, ``tags`` or ``labels``.
    :param columns: Available column names used to select the preferred search representation.
    :param tag_text: Display text, normalized only for normalized/hash-column searches.
    :return: Search column and normalized or original value appropriate to that choice.
    :raises ValueError: If the table name is unsupported.
    """
    normalized = tag_search_value(tag_text)
    if table == "tags":
        if "tag_phash" in columns:
            return "tag_phash", normalized
        return "tag", tag_text

    if table == "labels":
        if "label_text_norm" in columns:
            return "label_text_norm", normalized
        if "label_phash" in columns:
            return "label_phash", normalized
        if "label_text" in columns:
            return "label_text", tag_text
        return "label", tag_text

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
    """
    Build a schema-compatible payload for a new tag-like row.

    Tags always receive raw ``tag`` text and optionally a normalized hash field.
    Labels require ``label_text`` or ``label`` and receive any supported normalized
    fields. Input text is retained for display; no uniqueness/existence check or
    database write is performed.

    Example:
        >>> build_tag_row_payload("labels", {"label", "label_phash"}, "Science Fiction")
        {'label': 'Science Fiction', 'label_phash': 'sciencefiction'}


    :param table: Exact target family, ``tags`` or ``labels``.
    :param columns: Schema columns controlling optional normalized fields and label text selection.
    :param tag_text: Original display text and input to shared search normalization.
    :return: New field-value mapping ready for the caller's insertion operation.
    :raises ValueError: For an unsupported table or a labels schema lacking a supported text column.
    """
    normalized = tag_search_value(tag_text)
    if table == "tags":
        row_dict: dict[str, object] = {"tag": tag_text}
        if "tag_phash" in columns:
            row_dict["tag_phash"] = normalized
        return row_dict

    if table == "labels":
        row_dict = {}
        if "label_text" in columns:
            row_dict["label_text"] = tag_text
        elif "label" in columns:
            row_dict["label"] = tag_text
        else:
            raise ValueError(
                "`labels` table has no supported text column (`label_text`/`label`)."
            )
        if "label_text_norm" in columns:
            row_dict["label_text_norm"] = normalized
        if "label_phash" in columns:
            row_dict["label_phash"] = normalized
        return row_dict

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
        >>> db.search.return_value = []
        >>> search_tag_rows(db, "tags", "Science Fiction")
        []
        >>> db.search.assert_called_once_with("tags", "tag_phash", "sciencefiction")


    :param db: Facade exposing column headings and column/value search.
    :param table: Supported tag-like table to search.
    :param tag_text: Display text used directly or normalized according to the chosen column.
    :return: List of all returned matching rows in backend order.
    """
    columns = set(db.get_column_headings(table))
    search_column, search_value = tag_search_column_and_value(table, columns, tag_text)
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
    "tag_search_value",
]
