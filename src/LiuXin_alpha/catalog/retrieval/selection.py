"""Own Catalog's preferred WEM path ordering, mirrored by SQL primary rays.

Complete Work–Expression–Manifestation paths precede partial paths. Among equal
length paths, rank Work–Expression primary descending/priority ascending, then
Expression–Manifestation primary descending/priority ascending, then Expression,
Manifestation and Work IDs ascending. Null link values mean zero. Item presence
does not rank a bibliographic path.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..api.common import RowMapping


def _link_rank(row: RowMapping | None, primary_column: str) -> tuple[int, int]:
    """Read a traversed link's primary/priority rank; missing values mean zero."""
    link = (row or {}).get("_catalog_link", {})
    primary = link.get("extra", {}).get(primary_column)
    return -int(primary or 0), int(link.get("priority") or 0)


@dataclass(frozen=True)
class WemiPath:
    """Retain one candidate and the linked rows carrying its edge metadata.

    Traversal direction determines which endpoint carries ``_catalog_link``.
    Explicit edge carriers keep that direction out of the selection policy.
    This internal value neither reads the database nor changes graph membership.

    Example:
        >>> WemiPath(work={"work_id": 1}).selection_key()
        (-1, 0, 0, 0, 0, -1, -1, 1)
    """

    work: RowMapping | None = None
    expression: RowMapping | None = None
    manifestation: RowMapping | None = None
    work_link_row: RowMapping | None = None
    manifestation_link_row: RowMapping | None = None

    def selection_key(self) -> tuple[int, ...]:
        """Rank candidates without changing their shallow row mappings.

        SQL ``wemi_primary_rays_v`` contains only complete paths, so its ordering
        omits the constant completeness and Work-ID terms. A bundle retains the
        fullest available partial path when no complete candidate exists.

        Example:
            >>> WemiPath(expression={"expression_id": 11}).selection_key()[5]
            11
        """
        return (
            -sum(
                row is not None
                for row in (self.work, self.expression, self.manifestation)
            ),
            *_link_rank(self.work_link_row, "expression_work_link_primary"),
            *_link_rank(
                self.manifestation_link_row, "expression_manifestation_link_primary"
            ),
            int((self.expression or {}).get("expression_id", -1)),
            int((self.manifestation or {}).get("manifestation_id", -1)),
            int((self.work or {}).get("work_id", -1)),
        )
