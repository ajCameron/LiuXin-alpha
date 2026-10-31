"""Read physical Work rows through declared Expression relationships."""

from __future__ import annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.databases.db_types.macro_types import LINK_TYPE_UNSET, LinkedTableRow

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class WorksSQLMixin:
    """Own SQL reads returning Works, without choosing a preferred Work.

    Example:
        ``db.metadata_sql.get_works_for_expression(expression_id)`` retains
        every stored Work link and its metadata, including dangling links.
    """

    db: DatabaseAPI

    def get_works_for_expression(
        self, expression_id: int, *, link_type: object = LINK_TYPE_UNSET
    ) -> tuple[LinkedTableRow, ...]:
        """Read Expression-to-Work links and batch their physical Work rows.

        Preserve link order and metadata. An absent destination has row=None;
        source existence and interpretation remain the caller's responsibility.
        The omitted filter includes all types. Supplied filters, including None,
        require a typed relation; the current core WEMI relation is untyped.
        """
        spec = self.db.driver_wrapper.get_link_spec("expressions", "works")
        return self.db.macros.get_linked_rows(spec, expression_id, link_type=link_type)
