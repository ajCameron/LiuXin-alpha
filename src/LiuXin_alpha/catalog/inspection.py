"""Bound complete Catalog inspections using the existing portable row reader."""

from __future__ import annotations

from LiuXin_alpha.catalog.api.common import CatalogNotFoundError


class InspectionLimitExceeded(ValueError):
    """An inspection cannot return a complete result within its explicit budget."""


class BoundedInspection:
    """Read complete row selections and count graph visits against one budget.

    The caller owns the snapshot/transaction. No page is returned partially and
    no SQL or alternate database connection is created by this Catalog helper.
    """

    def __init__(self, db, max_rows=10000):
        if type(max_rows) is not int or not 1 <= max_rows <= 100000:
            raise ValueError("Inspection max_rows must be between 1 and 100000")
        self.db = db
        self.max_rows = max_rows
        self.scanned = 0

    def consume(self, count=1):
        """Charge graph visits as well as rows so repeated paths remain bounded."""
        self.scanned += count
        if self.scanned > self.max_rows:
            raise InspectionLimitExceeded(
                "Complete inspection exceeds max_rows; no partial result is returned"
            )

    def rows(self, table, where):
        """Read every matching row or reject a selection larger than the budget."""
        selected, complete = self.db.macros.get_rows_page(
            table,
            where=where,
            limit=self.max_rows - self.scanned,
        )
        if not complete:
            raise InspectionLimitExceeded(
                "Complete inspection exceeds max_rows; no partial result is returned"
            )
        self.consume(len(selected))
        return tuple(dict(row) for row in selected)

    def require(self, table, row_id):
        """Require one exact positive identity, charging its inspected row."""
        if type(row_id) is not int or row_id < 1:
            raise ValueError("Catalog inspection requires a positive integer identity")
        column = self.db.driver_wrapper.get_id_column(table)
        selected = self.rows(table, {column: row_id})
        if not selected:
            raise CatalogNotFoundError(f"Missing {table} identity {row_id}")
        return selected[0]
