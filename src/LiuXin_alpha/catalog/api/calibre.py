"""Contract for explicit Expression-centred Calibre projections and writes."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from LiuXin_alpha.metadata.calibre_conversion import (
        CalibreBookRef,
        CalibreConversionPolicy,
    )


@runtime_checkable
class CalibreCompatibilityAPI(Protocol):
    """Project canonical Expressions and apply a versioned compatibility write policy.

    Accept explicit Expression references or validated SQL ray strings. Bare
    integers cannot establish which ID space the caller means. Reads retain
    alternative Work/edition IDs; writes resolve their declared field owner,
    reject ambiguous targets and share one portable transaction per batch.

    Example:
        ``catalog.calibre.project(CalibreBookRef(expression_id=13))`` reads the
        preferred projection without assigning a source Calibre identity.
    """

    policy: CalibreConversionPolicy

    def project(self, reference: CalibreBookRef | str) -> dict[str, object]:
        """Read the six supported fields with selected and alternative path IDs."""
        ...

    def resolve_field_target(
        self, field: str, reference: CalibreBookRef | str
    ) -> tuple[str, int]:
        """Return the validated WEMI owner; ambiguous Work parents raise."""
        ...

    def write_field(
        self, field: str, values: Mapping[CalibreBookRef | str, object]
    ) -> dict[CalibreBookRef | str, int | None]:
        """Write atomically, returning owner IDs or created comment/rating IDs.

        Clear comments/ratings return None. Input keys are retained. Conflicting
        writes through aliases reject the batch; failures roll back its effects.
        """
        ...


__all__ = ["CalibreCompatibilityAPI"]
