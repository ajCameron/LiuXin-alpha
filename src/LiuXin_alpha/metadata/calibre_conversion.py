"""Define the Expression-centred Calibre projection policy without persistence.

This is a versioned local policy, not a claim that every external Calibre row
describes one Expression. Source-library IDs, SQL rays and web Work tokens keep
their separate identities. Catalog validates references against the live graph.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from LiuXin_alpha.metadata.ratings import normalise_rating_pair


@dataclass(frozen=True)
class CalibreBookRef:
    """Identify an Expression with optional, explicitly selected path context.

    A bare integer is deliberately not a compatibility reference. Construction
    checks ID types; Catalog checks existence and relationships before use.

    Example:
        >>> CalibreBookRef.from_sql_ray("3:13:105")
        CalibreBookRef(expression_id=13, work_id=3, manifestation_id=105)
    """

    expression_id: int
    work_id: int | None = None
    manifestation_id: int | None = None

    def __post_init__(self) -> None:
        """Reject booleans, negative IDs and a missing Expression identity."""
        for name in ("expression_id", "work_id", "manifestation_id"):
            value = getattr(self, name)
            if value is None and name != "expression_id":
                continue
            if not isinstance(value, int) or isinstance(value, bool) or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")

    @classmethod
    def from_sql_ray(cls, ray_id: str) -> CalibreBookRef:
        """Parse an exact W:E:M SQL ray; this alone does not validate its edges."""
        if (
            not isinstance(ray_id, str)
            or re.fullmatch(r"(0|[1-9][0-9]*):(0|[1-9][0-9]*):(0|[1-9][0-9]*)", ray_id)
            is None
        ):
            raise ValueError(
                "expected a SQL ray ID in Work:Expression:Manifestation form"
            )
        work_id, expression_id, manifestation_id = map(int, ray_id.split(":"))
        return cls(expression_id, work_id, manifestation_id)


class CalibreConversionPolicy(StrEnum):
    """Name implemented conversion semantics; future changes need a new revision."""

    EXPRESSION_V1 = "expression-v1"

    def owner(self, field: str) -> str:
        """Select a semantic owner, refusing fields without a defined write policy."""
        if field in {"title", "language", "comments", "last_modified"}:
            return "expression"
        if field in {"author_sort", "rating"}:
            return "work"
        raise ValueError(
            f"{field!r} has no write rule in {self.value}; "
            "covers require asset attachments, series position needs a schema rule, "
            "and edition/storage fields need explicit owners"
        )

    def prepare(self, field: str, value: object) -> object:
        """Validate compatibility values before Catalog resolves or writes rows.

        Ratings use a ten-point scale with zero/None meaning unrated. Integer
        timestamps are milliseconds; ISO datetimes and datetime objects are
        accepted, with naive datetimes interpreted as UTC. No unit guessing is
        performed. Text normalization remains with the canonical repositories.
        """
        self.owner(field)
        if field in {"title", "author_sort", "comments"}:
            if value is not None and not isinstance(value, str):
                raise TypeError(f"{field} must be text or None")
            return value
        if field == "language":
            if value is not None and (
                not isinstance(value, int) or isinstance(value, bool) or value < 0
            ):
                raise ValueError("language must be a Language ID or None")
            return value
        if field == "rating":
            rating, _ = normalise_rating_pair(value, 10)
            return None if rating in (None, 0) else rating
        if isinstance(value, bool):
            raise TypeError("last_modified cannot be a boolean")
        if isinstance(value, int):
            if not -(2**63) <= value < 2**63:
                raise ValueError("timestamp is outside the signed 64-bit range")
            return value
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not isinstance(value, datetime):
            raise TypeError(
                "last_modified requires epoch milliseconds or an ISO datetime"
            )
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        return (value.astimezone(UTC) - epoch) // timedelta(milliseconds=1)


DEFAULT_CALIBRE_CONVERSION_POLICY = CalibreConversionPolicy.EXPRESSION_V1

__all__ = [
    "CalibreBookRef",
    "CalibreConversionPolicy",
    "DEFAULT_CALIBRE_CONVERSION_POLICY",
]
