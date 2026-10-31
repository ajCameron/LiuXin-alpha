"""Read physical Manifestation rows through declared Expression relationships."""

from __future__ import annotations

from typing import TYPE_CHECKING

from LiuXin_alpha.databases.db_types.macro_types import LINK_TYPE_UNSET, LinkedTableRow

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api import DatabaseAPI


class ManifestationsSQLMixin:
    """Own SQL reads returning Manifestations without publication selection.

    Example:
        ``db.metadata_sql.get_manifestations_for_expression(expression_id)``
        reads every stored embodiment link without choosing an edition.
    """

    db: DatabaseAPI

    def get_manifestations_for_expression(
        self, expression_id: int, *, link_type: object = LINK_TYPE_UNSET
    ) -> tuple[LinkedTableRow, ...]:
        """Read Expression-to-Manifestation links and batch destination rows.

        Keep link metadata and order; an absent destination has row=None. This
        read uses the current connection without opening a snapshot transaction.
        """
        spec = self.db.driver_wrapper.get_link_spec("expressions", "manifestations")
        return self.db.macros.get_linked_rows(spec, expression_id, link_type=link_type)
