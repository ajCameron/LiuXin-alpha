"""Read physical Expression rows through Work and Manifestation relationships."""

from __future__ import annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.databases.db_types.macro_types import LINK_TYPE_UNSET, LinkedTableRow

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class ExpressionsSQLMixin:
    """Own SQL reads returning Expressions without language/edition selection.

    Example:
        ``db.metadata_sql.get_expressions_for_manifestation(manifestation_id)``
        returns every embodied Expression, including an introduction and novel.
    """

    db: DatabaseAPI

    def get_expressions_for_work(
        self, work_id: int, *, link_type: object = LINK_TYPE_UNSET
    ) -> tuple[LinkedTableRow, ...]:
        """Read Work-to-Expression links and batch their destination rows.

        Preserve link order and metadata. An absent destination has row=None;
        no preferred Expression is selected. Supplied type filters require a
        typed relation; the current core WEMI relation is untyped.
        """
        spec = self.db.driver_wrapper.get_link_spec("works", "expressions")
        return self.db.macros.get_linked_rows(spec, work_id, link_type=link_type)

    def get_expressions_for_manifestation(
        self, manifestation_id: int, *, link_type: object = LINK_TYPE_UNSET
    ) -> tuple[LinkedTableRow, ...]:
        """Read Manifestation-to-Expression links and batch destination rows.

        Preserve all embodied Expressions in link order, including repeated
        destinations. Missing rows remain None; no singular book is projected.
        """
        spec = self.db.driver_wrapper.get_link_spec("manifestations", "expressions")
        return self.db.macros.get_linked_rows(
            spec, manifestation_id, link_type=link_type
        )
