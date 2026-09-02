"""Resolve tag-like metadata facets across compatible catalogue schemas."""

from __future__ import annotations

from typing import Optional

from LiuXin_alpha.metadata.standardization import make_tag_search_term


TAG_TOKENS = frozenset({"tag", "tags"})
LABEL_TOKENS = frozenset({"label", "labels"})


def database_table_names(db) -> set[str]:
    """
    Return the catalogue table names exposed by a database facade.


    :param db:
    :return:
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


    :param db:
    :param prefer_populated_tags:
    :param tables:
    :return:
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


    :param token:
    :param tables:
    :return:
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


    :param tag_text:
    :return:
    """
    return make_tag_search_term(tag_text)


def tag_search_column_and_value(table: str, columns: set[str], tag_text: str) -> tuple[str, str]:
    """
    Choose the schema-specific column and value for a tag search.


    :param table:
    :param columns:
    :param tag_text:
    :return:
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


    :param row:
    :return:
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


    :param table:
    :return:
    """
    if table == "tags":
        return "tag_id"
    if table == "labels":
        return "label_id"
    raise ValueError("Unsupported tag table: {!r}".format(table))


def build_tag_row_payload(table: str, columns: set[str], tag_text: str) -> dict[str, object]:
    """
    Build a schema-compatible payload for a new tag-like row.


    :param table:
    :param columns:
    :param tag_text:
    :return:
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
            raise ValueError("`labels` table has no supported text column (`label_text`/`label`).")
        if "label_text_norm" in columns:
            row_dict["label_text_norm"] = normalized
        if "label_phash" in columns:
            row_dict["label_phash"] = normalized
        return row_dict

    raise ValueError("Unsupported tag table: {!r}".format(table))


def search_tag_rows(db, table: str, tag_text: str) -> list[object]:
    """
    Find rows matching tag text in a tag-like table.


    :param db:
    :param table:
    :param tag_text:
    :return:
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
