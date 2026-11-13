"""Project native custom rows through the legacy Database lookup signature."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
from LiuXin_alpha.databases.custom_columns.native_repository import names, stem
from LiuXin_alpha.databases.custom_columns.native_service import CustomFieldService
from LiuXin_alpha.databases.custom_columns.native_types import (
    CustomFieldError,
    UnsupportedStorage,
)
from LiuXin_alpha.databases.schema_readers import quote_identifier as q
from LiuXin_alpha.errors import InputIntegrityError

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.row_api import RowAPI


class CustomColumnRows:
    """Read native rows in entry order; reject mismatched compatibility traversal."""

    def __init__(self, db: DatabaseAPI):
        self.db = db

    def get_interlinked_rows_cc(
        self, primary_row: RowAPI, custom_column: str, link_table: bool = True
    ) -> list[RowAPI | None] | list[dict[str, Any]]:
        """Return normalized Rows or direct dictionaries in native ordinal order."""
        from LiuXin_alpha.databases.row import Row

        match = re.fullmatch(r"custom_column_(\d+)", custom_column)
        if match is None:
            raise InputIntegrityError(
                "Expected a registered native custom-column table"
            )
        service = CustomFieldService(self.db)
        with service.read() as connection:
            try:
                definition = service.repository.resolve(connection, int(match[1]))
            except UnsupportedStorage:
                raise
            except CustomFieldError as error:
                raise InputIntegrityError(str(error)) from error
            if (
                definition.attachment != primary_row.table
                or definition.normalized != link_table
            ):
                raise InputIntegrityError(
                    "Custom field attachment or storage shape does not match the requested traversal"
                )
            table, link = names(definition)
            base, membership = stem(table), stem(link)
            if definition.normalized:
                sql = f"SELECT v.* FROM {q(table)} v JOIN {q(link)} e ON v.{q(base + '_id')}=e.{q(membership + '_value')} WHERE e.{q(membership + '_book')}=? ORDER BY e.{q(membership + '_ordinal')}"
            else:
                sql = f"SELECT * FROM {q(table)} WHERE {q(base + '_book')}=? ORDER BY {q(base + '_ordinal')}"
            cursor = connection.execute(sql, (primary_row.row_id,))
            try:
                headings = [column[0] for column in cursor.description or ()]
                records = [
                    dict(zip(headings, row, strict=True)) for row in cursor.fetchall()
                ]
            finally:
                cursor.close()
        if definition.normalized:
            result: list[RowAPI | None] = [
                Row(self.db, row_dict=record) for record in records
            ]
            return result
        return records
