"""Typed native search requests and pages, independent of physical query storage.

Requests are immutable values; the bound service validates their meaning before
execution. Filters are ANDed with the Boolean text query. Discovery distinguishes
relationship-aware filters from scalar sort keys on the returned WEMI level.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api.common import WemiLevel


@dataclass(frozen=True, slots=True)
class MetadataReference:
    """Identify a WEMI entity in the bound database without a physical table name.

    The service validates identity and existence. This passive value retains no
    database binding and performs no runtime validation of its constructor input.
    """

    level: WemiLevel
    entity_id: int


type MetadataFilterOperator = Literal[
    "contains", "eq", "ne", "lt", "lte", "gt", "gte", "in", "is_null"
]
type MetadataFilterValue = str | int | bool | None | tuple[int, ...]


@dataclass(frozen=True, slots=True)
class MetadataSearchFilter:
    """Constrain a discovered semantic field; text values are always literals."""

    field: str
    operator: MetadataFilterOperator
    value: MetadataFilterValue


@dataclass(frozen=True, slots=True)
class MetadataSearchSort:
    """Order a discovered scalar key; missing values remain last either way."""

    field: str
    ascending: bool = True


@dataclass(frozen=True, slots=True)
class MetadataSearchRequest:
    """Search one WEMI level with exact totals and an offset/limit result window.

    Empty sort uses ascending IDs; IDs also break complete ties. A zero limit
    returns no references but still computes the total. None requests every hit.
    Separate requests do not pin a snapshot or guarantee stable offset pages.
    """

    query: str = ""
    level: WemiLevel = "work"
    filters: tuple[MetadataSearchFilter, ...] = ()
    sort: tuple[MetadataSearchSort, ...] = ()
    limit: int | None = 100
    offset: int = 0

    def __post_init__(self) -> None:
        """Copy supplied outer sequences; entries are immutable search values."""
        object.__setattr__(self, "filters", tuple(self.filters))
        object.__setattr__(self, "sort", tuple(self.sort))


@dataclass(frozen=True, slots=True)
class MetadataSearchField:
    """Describe a supported filter, its aliases, value type and traversal scope."""

    name: str
    value_type: Literal["text", "integer"]
    operators: tuple[MetadataFilterOperator, ...]
    aliases: tuple[str, ...]
    description: str


@dataclass(frozen=True, slots=True)
class MetadataSortField:
    """Describe one scalar ordering key on the requested result level."""

    name: str
    value_type: Literal["text", "integer"]
    description: str


class MetadataSearchValidationError(ValueError):
    """Report an invalid request with a parameter path suitable for a form error."""

    def __init__(self, parameter: str, message: str) -> None:
        """Retain the failing parameter, for example filters[0].value."""
        self.parameter = parameter
        self.message = message
        super().__init__(f"{parameter}: {message}")


@dataclass(frozen=True, slots=True)
class MetadataSearchPage:
    """Return complete reference coverage and provenance for one execution.

    total_count precedes pagination. source identifies actual execution, including
    explicit database fallback. generation belongs only to the bound cache; None
    means database execution. A live cache generation is an invalidation counter,
    not a durable database revision. Neither consistency nor generation promises
    transaction isolation against external writers or across separate requests.
    """

    level: WemiLevel
    references: tuple[MetadataReference, ...]
    total_count: int
    offset: int
    limit: int | None
    source: Literal["cache", "database"]
    consistency: Literal["snapshot", "live"]
    generation: int | None

    @property
    def has_more(self) -> bool:
        """Report whether matches remain beyond the returned window."""
        return self.offset + len(self.references) < self.total_count


__all__ = [
    "HydratedMetadataSearchPage",
    "MetadataReference",
    "MetadataFilterOperator",
    "MetadataFilterValue",
    "MetadataSearchFilter",
    "MetadataSearchSort",
    "MetadataSearchRequest",
    "MetadataSearchField",
    "MetadataSortField",
    "MetadataSearchValidationError",
    "MetadataSearchPage",
]


@dataclass(frozen=True, slots=True)
class HydratedMetadataSearchPage[MetadataT](MetadataSearchPage):
    """Return detached metadata aligned with references from the same captured read."""

    records: tuple[MetadataT, ...]
