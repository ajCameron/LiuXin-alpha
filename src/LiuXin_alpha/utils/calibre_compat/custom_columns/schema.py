"""Explicit Calibre category views over native LiuXin field storage.

These optional projections are not part of native storage. Refresh them after
native schema changes when a Calibre consumer needs the SQL view interface.
"""

from __future__ import annotations

import re

from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
from LiuXin_alpha.databases.custom_columns.native_repository import (
    CustomFieldRepository,
    names,
    stem,
)
from LiuXin_alpha.databases.custom_columns.native_service import CustomFieldService
from LiuXin_alpha.databases.custom_columns.native_types import FieldDefinition
from LiuXin_alpha.databases.db_types.driver_types import SQLConnection
from LiuXin_alpha.databases.schema_readers import quote_identifier as q
from LiuXin_alpha.databases.schema_readers import rows


def refresh_category_views(db: DatabaseAPI) -> None:
    """Replace the optional SQLite views atomically, including obsolete definitions."""
    service = CustomFieldService(db)
    if service.repository.postgres:
        raise NotImplementedError("Calibre SQL category views require SQLite")
    with service.write() as connection:
        for (view,) in rows(
            connection, "SELECT name FROM sqlite_master WHERE type='view'"
        ):
            if re.fullmatch(r"tag_browser_(?:filtered_)?custom_column_\d+", view):
                connection.execute(f"DROP VIEW {q(view)}").close()
        for definition in service.list_fields():
            _install(service.repository, connection, definition)


def _install(
    repository: CustomFieldRepository, conn: SQLConnection, definition: FieldDefinition
) -> None:
    """Maintain the SQLite category-view shape using distinct owner usage."""
    if repository.postgres or not definition.normalized:
        return
    table, link = names(definition)
    ts, ls = stem(table), stem(link)
    ratings = (
        definition.attachment == "books"
        and repository.table_exists(conn, "book_rating_links")
        and "rating" in repository.columns(conn, "ratings")
    )
    for filtered in (False, True):
        view = ("tag_browser_filtered_" if filtered else "tag_browser_") + table
        predicate = f"e.{q(ls + '_value')}=v.{q(ts + '_id')}"
        if filtered:
            predicate += f" AND books_list_filter(e.{q(ls + '_book')})"
        average = "NULL"
        if ratings:
            average = f"(SELECT AVG(r.rating) FROM ratings r JOIN book_rating_links bl ON r.rating_id=bl.book_rating_link_rating_id WHERE r.rating<>0 AND bl.book_rating_link_book_id IN (SELECT e.{q(ls + '_book')} FROM {q(link)} e WHERE {predicate}))"
        conn.execute(
            f"CREATE VIEW {q(view)} AS SELECT v.{q(ts + '_id')} AS id, v.{q(ts + '_value')} AS value, (SELECT COUNT(DISTINCT e.{q(ls + '_book')}) FROM {q(link)} e WHERE {predicate}) AS count, {average} AS avg_rating, v.{q(ts + '_value')} AS sort FROM {q(table)} v"
        ).close()
