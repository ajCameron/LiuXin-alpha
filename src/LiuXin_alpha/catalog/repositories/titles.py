"""
Present embedded WEMI title columns as logical records and route writes to their owners.

The titles relation is a compatibility view. Unqualified CRUD targets Works;
level-aware operations target Work, Expression, or Manifestation columns. Items
have no owned title columns. Only replace_for_wemi adds an outer transaction.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar, Sequence, cast

from ..api.common import CatalogMutationError, EntityId, RowInput, RowMapping, WemiLevel
from .base import BaseRepository, WEMI_TABLES


class TitleRepository(BaseRepository):
    """
    Read logical title records while writing the owning WEMI base rows.

    title_id is the owner ID, so IDs are not globally unique across WEMI levels.
    Preference is first non-None/nonempty value: Work title/canonical/sort, Expression
    override/subtitle, Manifestation subtitle. Whitespace and non-string values are
    not excluded by this projection. Writes require a bound repository group.

    Example:
        >>> TitleRepository._logical_row("item", {"item_id": 4})["title_values"]
        {}
    """

    table_name = "titles"
    id_column = "title_id"

    _TITLE_COLUMNS: ClassVar[dict[WemiLevel, tuple[str, ...]]] = {
        "work": ("work_title", "work_canonical_title", "work_sort_title"),
        "expression": ("expression_title_override", "expression_subtitle"),
        "manifestation": ("manifestation_subtitle",),
        "item": (),
    }

    @staticmethod
    def _logical_row(level: WemiLevel, row: RowMapping) -> dict[str, Any]:
        """
        Project an owner row into the logical title shape without database access.

        Missing title columns become None. Only None and the empty string are skipped;
        no stripping, type validation or fallback to ancestor rows occurs.

        Example:
            >>> TitleRepository._logical_row("work", {"work_id": 2, "work_title": "", "work_canonical_title": "Canonical"})["title"]
            'Canonical'


        :param level: WEMI level indexing the declared title-column mapping.
        :param row: Owner mapping containing the corresponding level_id key.
        :return: New dict with owner IDs/type, first nonempty title, and all declared title values.
        """

        entity_id = row[f"{level}_id"]
        columns = TitleRepository._TITLE_COLUMNS[level]
        values = tuple(row.get(column) for column in columns)
        preferred = next((value for value in values if value not in (None, "")), None)
        return {
            "title_id": entity_id,
            "title_entity_type": level,
            "title_entity_id": entity_id,
            "title": preferred,
            "title_values": dict(zip(columns, values)),
        }

    def get(self, entity_id: EntityId) -> RowMapping | None:
        """
        Read one Work and render its logical title, including an empty title.

        Read directly through macros; no repository-group binding is needed. Unlike
        list_for_wemi, an existing Work with no title still returns a logical record.

        Example:
            >>> title = catalog.titles.get(work_id)  # doctest: +SKIP


        :param entity_id: Work ID forwarded to get_row without a separate repository ID check.
        :return: Logical Work title mapping, or None for a false-valued database result.
        """

        row = self._macros.get_row("works", entity_id, id_column="work_id")
        return None if not row else self._logical_row("work", self._as_mapping(row))

    def list(self, *, limit: int = 100, offset: int = 0) -> Sequence[RowMapping]:
        """
        Read every Work in ID order, then slice its logical title projections.

        Zero still reads/projects all Works. Only negativity is checked before reads;
        booleans act as integer bounds and unsupported slice types may fail afterward.

        Example:
            limit=0 returns an empty tuple after the full Work scan.


        :param limit: Page size; negative values are rejected.
        :param offset: Start offset; negative values are rejected.
        :return: Tuple slice of logical Work titles, including records with no preferred title.
        """

        if limit < 0 or offset < 0:
            raise ValueError("limit and offset cannot be negative")
        works = self._macros.get_rows("works", order_by=("work_id",))
        rows = tuple(self._logical_row("work", self._as_mapping(row)) for row in works)
        return rows[offset : offset + limit]

    def create(self, data: RowInput) -> EntityId:
        """
        Create a Work from a nonblank logical title through the bound Work repository.

        Require a string whose stripped value is nonempty, but preserve the supplied
        spelling. Other input fields are ignored; this does not write the titles view.

        Example:
            >>> title_id = catalog.titles.create({"title": "Frankenstein"})  # doctest: +SKIP


        :param data: Get-compatible title input; title takes precedence over work_title, including None.
        :return: New Work ID, also the logical Work title ID.
        :raises CatalogMutationError: If the selected title is not a nonblank string.
        """

        title = data.get("title", data.get("work_title"))
        if not isinstance(title, str) or not title.strip():
            raise CatalogMutationError("a non-empty title is required")
        return cast(
            EntityId,
            self.repositories.works.create({"work_title": title}),
        )

    def update(self, entity_id: EntityId, data: RowInput) -> None:
        """
        Translate the title key and delegate all supplied changes to the Work repository.

        If title and work_title both occur, the later iteration entry wins in the new
        dict. Validation and canonical-title side effects belong to the Work repository.

        Example:
            >>> catalog.titles.update(work_id, {"title": "Revised"})  # doctest: +SKIP


        :param entity_id: Existing Work ID.
        :param data: Items-compatible mapping; title is renamed to work_title and other keys pass through.
        :return: None after the delegated Work update.
        """

        mapped = {
            "work_title" if key == "title" else key: value
            for key, value in data.items()
        }
        self.repositories.works.update(entity_id, mapped)

    def delete(self, entity_id: EntityId) -> None:
        """
        Clear all three Work title columns without deleting the Work.

        Example:
            Deleting a logical title preserves the Work and its relationships.


        :param entity_id: Existing Work ID passed to the bound Work repository.
        :return: None after setting title, canonical title, and sort title to None.
        """

        self.repositories.works.update(
            entity_id,
            {column: None for column in self._TITLE_COLUMNS["work"]},
        )

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

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        columns = self._TITLE_COLUMNS[level]
        if not columns:
            raise CatalogMutationError("Items do not own title columns")
        title = data.get("title")
        changes = {
            key: value
            for key, value in data.items()
            if key in columns
        }
        if title is not None:
            changes[columns[0]] = title
        if not changes:
            raise CatalogMutationError(
                f"no writable {level} title values were supplied"
            )
        repository = getattr(self.repositories, f"{level}s")
        repository.update(entity_id, changes)
        return entity_id

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

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        row = self._require_table_row(WEMI_TABLES[level], entity_id)
        logical = self._logical_row(level, row)
        return () if logical["title"] in (None, "") else (logical,)

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

        titles = self.list_for_wemi(level=level, entity_id=entity_id)
        return titles[0] if titles else None

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

        if level not in WEMI_TABLES:
            raise ValueError(f"unknown WEMI level: {level!r}")
        columns = self._TITLE_COLUMNS[level]
        if not columns:
            raise CatalogMutationError("Items do not own title columns")
        repository = getattr(self.repositories, f"{level}s")
        repository.update(entity_id, {column: None for column in columns})

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

        with self._macros.transaction():
            self.clear_for_wemi(level=level, entity_id=entity_id)
            if data is None:
                return None
            if isinstance(data, str):
                payload: RowInput = {"title": data}
            elif isinstance(data, Mapping):
                payload = dict(data)
            else:
                raise TypeError("title data must be a string, mapping, or None")
            return self.add_for_wemi(
                level=level,
                entity_id=entity_id,
                data=payload,
            )


__all__ = ["TitleRepository"]
