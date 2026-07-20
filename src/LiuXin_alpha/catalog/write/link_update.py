"""Value objects passed from catalog writers to database link operations."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Iterable, Mapping

from LiuXin_alpha.databases.macro_types import LINK_TYPE_UNSET, LinkValue
from LiuXin_alpha.databases.schema_specs import StorageLinkSpec


@dataclass(frozen=True, slots=True)
class LinkUpdate:
    """An authoritative update to links between two database tables.

    ``replacements`` is keyed by an id in ``link_spec.primary_table``. Each
    value is the complete desired set of links for that row:

    * an omitted primary id is left untouched;
    * an empty iterable removes all links in scope for that primary id; and
    * each :class:`LinkValue` preserves type, priority, and extra link-column
      data required by the database writer.

    ``link_type`` optionally limits a replacement to one type on a link whose
    type is part of its identity. Other link types are then left untouched.
    """

    link_spec: StorageLinkSpec
    replacements: Mapping[Any, Iterable[LinkValue]]
    link_type: Any = LINK_TYPE_UNSET

    def __post_init__(self) -> None:
        """Materialise caller-owned iterables into a stable update request."""

        if not isinstance(self.link_spec, StorageLinkSpec):
            raise TypeError("link_spec must be a StorageLinkSpec")
        if not isinstance(self.replacements, Mapping):
            raise TypeError("replacements must be a mapping")

        materialised: dict[Any, tuple[LinkValue, ...]] = {}
        for primary_id, links in self.replacements.items():
            if links is None:
                raise TypeError(
                    "replacement links cannot be None; use an empty iterable to clear links"
                )
            values = tuple(links)
            if not all(isinstance(link, LinkValue) for link in values):
                raise TypeError("replacement links must be LinkValue instances")
            materialised[primary_id] = values

        object.__setattr__(
            self,
            "replacements",
            MappingProxyType(materialised),
        )


__all__ = ["LinkUpdate"]
