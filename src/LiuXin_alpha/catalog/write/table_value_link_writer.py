"""
Schema-backed link writer for values stored in a destination-table column.
"""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import nullcontext
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, cast

from LiuXin_alpha.catalog.write.link_update import LinkUpdate
from LiuXin_alpha.catalog.write.link_writer import (
    CatalogLinkMap,
    CatalogLinkTypeScope,
    CatalogLinkWriter,
)
from LiuXin_alpha.databases.db_types import DstTableID, SrcTableID
from LiuXin_alpha.databases.macro_types import LINK_TYPE_UNSET, LinkRow
from LiuXin_alpha.databases.schema_specs import (
    StorageColumnSpec,
    StorageLinkSpec,
    StorageTableSpec,
)

if TYPE_CHECKING:
    from LiuXin_alpha.catalog.api import CatalogAPI


@dataclass(frozen=True, slots=True)
class _TableValueReference:
    """Hashable, non-persistent reference to one raw destination value."""

    identity: tuple[str, str, str]
    value: Any = field(compare=False, hash=False, repr=False)

    @classmethod
    def from_value(cls, value: Any) -> "_TableValueReference":
        value_type = type(value)
        return cls(
            (
                value_type.__module__,
                value_type.__qualname__,
                repr(value),
            ),
            value,
        )


class CatalogTableValueLinkWriter(CatalogLinkWriter[Any, Any]):
    """
    Link source rows to values ensured in a destination-table column.

    The database's column metadata owns matching and normalization. Missing
    logical values in replacements and additions are created by
    ``ensure_table_value``. Deletions use ``find_table_value`` so an unknown
    value is a no-op rather than a newly created orphan. Field-specific
    subclasses may override adaptation and validation while retaining this
    operation-aware destination-resolution policy.

    :param catalog: Catalog facade used to apply normalized link updates.
    :param link_spec: Directed source-to-destination link specification.
    :param destination_table: Table containing the destination value.
    :param destination_column: Column containing the destination value.
    """

    def __init__(
        self,
        catalog: CatalogAPI,
        link_spec: StorageLinkSpec,
        destination_table: StorageTableSpec,
        destination_column: StorageColumnSpec,
    ) -> None:
        """
        Validate and store the schema-backed destination configuration.

        :param catalog: Catalog facade used to apply normalized link updates.
        :param link_spec: Directed source-to-destination link specification.
        :param destination_table: Table containing the destination value.
        :param destination_column: Column containing the destination value.
        :return: None.
        """

        if not isinstance(destination_table, StorageTableSpec):
            raise TypeError("destination_table must be a StorageTableSpec")
        if not isinstance(destination_column, StorageColumnSpec):
            raise TypeError("destination_column must be a StorageColumnSpec")
        if destination_table.name != link_spec.secondary_table:
            raise ValueError(
                "destination_table must match link_spec.secondary_table"
            )
        if destination_column not in destination_table.columns:
            raise ValueError("destination_column must belong to destination_table")
        if destination_column.is_primary_key:
            raise ValueError("destination value column cannot be a primary key")
        super().__init__(catalog, link_spec)
        self._destination_table = destination_table
        self._destination_column = destination_column

    @property
    def destination_table(self) -> StorageTableSpec:
        """
        Return the destination-table specification.

        :return: Configured destination-table specification.
        """

        return self._destination_table

    @property
    def destination_column(self) -> StorageColumnSpec:
        """
        Return the destination value-column specification.

        :return: Configured destination value-column specification.
        """

        return self._destination_column

    def adapt(self, raw_value: Any) -> Any:
        """
        Preserve one raw destination value by default.

        :param raw_value: Raw metadata value supplied by the caller.
        :return: Unchanged metadata value.
        """

        return raw_value

    def resolve_destination(self, value: Any) -> DstTableID:
        """
        Match or create one destination value through portable macros.

        :param value: Adapted and validated destination value.
        :return: Matched or created destination-table ID.
        """

        return self.catalog.db.macros.ensure_table_value(
            self.destination_table.name,
            self.destination_column.name,
            value,
            id_column=self.link_spec.secondary_id_col,
        )

    def find_destination(self, value: Any) -> DstTableID | None:
        """
        Find one destination value without creating a database row.

        This lookup is used for deletions, where a missing value is already a
        no-op. Matching follows the same database-owned value policy as
        :meth:`resolve_destination`.

        :param value: Adapted and validated destination value.
        :return: Existing destination-table ID, or ``None`` when absent.
        """

        return self.catalog.db.macros.find_table_value(
            self.destination_table.name,
            self.destination_column.name,
            value,
            id_column=self.link_spec.secondary_id_col,
        )

    def _existing_destination_id_for(self, raw_value: Any) -> DstTableID:
        """
        Resolve a deletion value while retaining a missing-value sentinel.

        :param raw_value: Raw metadata value to process.
        :return: Existing destination ID, or an internal ``None`` sentinel.
        """

        return cast(
            DstTableID,
            self.find_destination(self.prepare_value(raw_value)),
        )

    def _reference_for(self, raw_value: Any) -> _TableValueReference:
        """Adapt a value into a pure, lazily resolved reference."""

        return _TableValueReference.from_value(self.prepare_value(raw_value))

    def build_update(
        self,
        replacements: CatalogLinkMap[Any] | None = None,
        *,
        additions: CatalogLinkMap[Any] | None = None,
        deletions: CatalogLinkMap[Any] | None = None,
        link_type: CatalogLinkTypeScope = LINK_TYPE_UNSET,
    ) -> LinkUpdate:
        """
        Build an update with operation-aware destination-value resolution.

        Unlike the legacy link-writer convention, integer scalars are treated
        as values in :attr:`destination_column`, not as destination IDs. Pass
        an explicit :class:`LinkValue` when an ID is already resolved.
        Replacement and addition values are ensured. Deletion values are
        found without creation and omitted when they do not exist.

        :param replacements: Authoritative destination values keyed by source
            ID.
        :param additions: Destination values to add, keyed by source ID.
        :param deletions: Destination values to remove, keyed by source ID.
        :param link_type: Optional link-type scope.
        :return: Immutable normalized link update.
        """

        replacements, additions, deletions = self._validate_link_type_inputs(
            replacements,
            additions=additions,
            deletions=deletions,
            link_type=link_type,
        )
        update = LinkUpdate.from_values(
            self.link_spec,
            replacements,
            additions=additions,
            deletions=deletions,
            secondary_id_for=self._reference_for,
            link_type=link_type,
        )
        self._validate_cardinality(update)
        return update

    def _resolve_update(self, update: LinkUpdate) -> LinkUpdate:
        """Resolve raw value references inside the active write transaction."""

        resolved: dict[_TableValueReference, DstTableID] = {}

        def operation(
            links_by_source: Any,
            *,
            create: bool,
        ) -> dict[Any, tuple[Any, ...]]:
            result: dict[Any, tuple[Any, ...]] = {}
            for source_id, links in links_by_source.items():
                resolved_links = []
                for link in links:
                    reference = link.secondary_id
                    if not isinstance(reference, _TableValueReference):
                        resolved_links.append(link)
                        continue
                    destination_id = resolved.get(reference)
                    if destination_id is None:
                        destination_id = (
                            self.resolve_destination(reference.value)
                            if create
                            else self.find_destination(reference.value)
                        )
                        if destination_id is not None:
                            resolved[reference] = destination_id
                    if destination_id is not None:
                        resolved_links.append(
                            replace(link, secondary_id=destination_id)
                        )
                result[source_id] = tuple(resolved_links)
            return result

        return LinkUpdate(
            link_spec=self.link_spec,
            replacements=operation(update.replacements, create=True),
            additions=operation(update.additions, create=True),
            deletions=operation(update.deletions, create=False),
            link_type=update.link_type,
        )

    def apply_update(
        self,
        update: LinkUpdate,
    ) -> Mapping[SrcTableID, tuple[LinkRow, ...]]:
        """Resolve values and apply links in one portable transaction."""

        if not isinstance(update, LinkUpdate):
            raise TypeError("update must be a LinkUpdate")
        if update.link_spec != self.link_spec:
            raise ValueError("update link_spec does not match writer link_spec")
        macros = self.catalog.db.macros
        transaction = getattr(macros, "transaction", None)
        context = transaction() if callable(transaction) else nullcontext()
        with context:
            resolved = self._resolve_update(update)
            replacement = resolved.as_replacement_update(macros)
            self._validate_cardinality(replacement)
            return self.catalog.write_link_update(replacement)


__all__ = ["CatalogTableValueLinkWriter"]
