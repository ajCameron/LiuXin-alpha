"""
Present embedded WEMI title columns as logical records and route writes to their owners.

The titles relation is a compatibility view. Unqualified CRUD targets Works;
level-aware operations target Work, Expression, or Manifestation columns. Items
have no owned title columns. Only replace_for_wemi adds an outer transaction.
"""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from ..common import EntityId, RowInput, RowMapping, WemiLevel
from .base import BaseRepositoryAPI


@runtime_checkable
class TitleRepositoryAPI(BaseRepositoryAPI, Protocol):
    """
    Specify logical access to title-bearing Work, Expression, and Manifestation columns.

    The titles view is not writable. Owner IDs identify logical records within a
    level. Items can be read but have no title columns to modify. Preference skips
    only None/empty values; replacement clears and writes transactionally.

    Example:
        >>> title = catalog.titles.preferred_for_wemi(level="expression", entity_id=expression_id)  # doctest: +SKIP
    """

    def add_for_wemi(self, *, level: WemiLevel, entity_id: EntityId, data: RowInput) -> EntityId:
        """
        Update accepted title columns on a supported existing WEMI row.

        Ignore unrelated keys. A non-None title overrides the first declared storage
        column; explicit None under a storage column can clear it. Generic title=None
        alone yields no changes and raises. Downstream repositories own value validation.

        Example:
            On an Expression, title writes expression_title_override while expression_subtitle can be supplied separately.


        :param level: WEMI level; Item and unknown levels are rejected.
        :param entity_id: Existing owner ID validated by its repository update.
        :param data: Items/get-compatible mapping of logical title and level-specific storage columns.
        :return: Owner ID identifying the logical title record.
        :raises CatalogMutationError: If Items are targeted or no writable values remain.
        """

    def list_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> Sequence[RowMapping]:
        """
        Require a WEMI row and return its preferred logical title when present.

        Only None and empty string mean absent; whitespace is retained. The projection
        uses this row alone, without ancestor fallback. Unknown levels raise ValueError.

        Example:
            An existing Item returns (), whereas a missing Item raises.


        :param level: WEMI level whose embedded columns should be inspected.
        :param entity_id: Existing owner ID, required even for an Item with no title columns.
        :return: Empty tuple or one logical title mapping.
        """

    def preferred_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> RowMapping | None:
        """
        Return the single logical title from level-aware listing, if available.

        Preference follows declared column order. Whitespace-only strings count as
        present, unlike display-title projection; no ancestor fallback is performed.

        Example:
            An Expression with only a subtitle returns that subtitle as its logical title.


        :param level: WEMI level to inspect.
        :param entity_id: Existing owner ID required by list_for_wemi.
        :return: First logical title mapping or None when no owned column has a value.
        """

    def clear_for_wemi(self, *, level: WemiLevel, entity_id: EntityId) -> None:
        """
        Set every declared title column to None through the owner repository.

        No title view rows or owner entities are deleted. Repository validation and
        additional update behavior propagate; this method adds no outer transaction.

        Example:
            Clearing a Work also clears work_canonical_title and work_sort_title.


        :param level: Work, Expression, or Manifestation; Items and unknown levels are rejected.
        :param entity_id: Existing owner ID passed to its repository update.
        :return: None after clearing the owner columns.
        """

    def replace_for_wemi(
        self,
        *,
        level: WemiLevel,
        entity_id: EntityId,
        data: RowInput | str | None,
    ) -> EntityId | None:
        """
        Clear then optionally write title values in one macro transaction.

        Clear first, before validating data type or replacement content. Invalid data
        or later update errors roll back the earlier clear through the transaction. A
        string becomes a title payload; Items are rejected even for clear-only requests.

        Example:
            >>> catalog.titles.replace_for_wemi(level="work", entity_id=work_id, data=None)  # doctest: +SKIP


        :param level: Supported title-owning WEMI level.
        :param entity_id: Existing owner ID checked during clearing.
        :param data: String for the preferred column, copied Mapping of values, or None to leave all cleared.
        :return: Owner ID after replacement, or None after a clear-only request.
        """
