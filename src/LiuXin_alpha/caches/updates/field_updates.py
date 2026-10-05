"""
Carry mutable field-value and relation replacement intentions for cache backends.

These dataclasses preserve supplied dictionaries, sets and sequences
without copying, validation or ID normalization. Field deletion means clearing
values or detaching relationships, while owner-row lifecycle stays with
table/database APIs. Consumers apply creation, uniqueness and refresh policy.
"""

from __future__ import annotations

import dataclasses
from typing import Generic, Sequence, Optional, TypeVar

from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.util_mixins import LinkDstUpdate
from LiuXin_alpha.databases.db_types import MainTableName, MainTableColumnName, MainTableID


T = TypeVar("T")


@dataclasses.dataclass
class ManyManyInTwoTableFieldUpdate(Generic[T]):
    """
    Describe plural source-keyed relation changes without deleting owners.

    Table labels and target column describe the destination projection.
    added_maps and updated_maps are keyed by source IDs and carry sequences
    of optional destination values. deleted_ids requests clearing/unlinking
    source mappings; dirtied carries refresh hints. unique defaults false but
    is not enforced by construction. All supplied containers remain shared.

    link_replacements has an independent empty dict default for each instance.
    Its per-source sequence describes desired destination links and metadata;
    the consumer validates conflicts with value maps/deletions and controls
    shared-destination reuse. Construction creates no rows or links.

    Example:
        >>> change = ManyManyInTwoTableFieldUpdate("books", "tags", "name", {}, {}, {1}, set())
        >>> change.deleted_ids, change.unique
        ({1}, False)
    """

    src_table: MainTableName
    dst_table: MainTableName

    dst_table_target_column: MainTableColumnName

    added_maps: dict[MainTableID, Sequence[Optional[T]]]
    updated_maps: dict[MainTableID, Sequence[Optional[T]]]
    deleted_ids: set[MainTableID]
    dirtied: set[MainTableID]

    unique: bool = False

    # Explicit per-src replacement payload for link-oriented operations.
    # When provided for a src id, implementations should treat the sequence as
    # the authoritative desired set of linked dst rows for that src.
    link_replacements: dict[MainTableID, Sequence[LinkDstUpdate[T]]] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class ManyOneInTwoTableFieldUpdate(Generic[T]):
    """
    Describe scalar source-keyed relation changes without deleting owners.

    Table labels and target column describe the destination projection.
    added_maps and updated_maps are keyed by source IDs and carry optional
    destination values. deleted_ids requests clearing/unlinking
    source mappings; dirtied carries refresh hints. unique defaults false but
    is not enforced by construction. All supplied containers remain shared.

    create_missing_links and create_missing_related_rows default false.
    Related-row creation requires link creation under the intended policy,
    but this dataclass does not validate that combination. A concrete updater
    can reuse an existing shared destination or create one when permitted.

    Example:
        >>> change = ManyOneInTwoTableFieldUpdate("books", "tags", "name", {}, {}, {1}, set())
        >>> change.deleted_ids, change.unique
        ({1}, False)
    """

    src_table: MainTableName
    dst_table: MainTableName

    dst_table_target_column: MainTableColumnName

    added_maps: dict[MainTableID, Optional[T]]
    updated_maps: dict[MainTableID, Optional[T]]
    deleted_ids: set[MainTableID]
    dirtied: set[MainTableID]

    # Are the values in this field unique?
    unique: bool = False

    # If True, missing src->dst links may be created when a src row currently
    # has no linked dst row for this field.
    create_missing_links: bool = False

    # If True, and no existing dst row can be matched for a missing link, a new
    # dst row may be created and then linked. This requires
    # ``create_missing_links=True``.
    create_missing_related_rows: bool = False


@dataclasses.dataclass
class OneManyInTwoTableFieldUpdate(Generic[T]):
    """
    Describe plural source-keyed relation changes without deleting owners.

    Table labels and target column describe the destination projection.
    added_maps and updated_maps are keyed by source IDs and carry sequences
    of optional destination values. deleted_ids requests clearing/unlinking
    source mappings; dirtied carries refresh hints. unique defaults false but
    is not enforced by construction. All supplied containers remain shared.

    link_replacements has an independent empty dict default for each instance.
    Its per-source sequence describes desired destination links and metadata;
    the consumer validates conflicts with value maps/deletions and controls
    exclusive-destination ownership. Construction creates no rows or links.

    Example:
        >>> change = OneManyInTwoTableFieldUpdate("books", "tags", "name", {}, {}, {1}, set())
        >>> change.deleted_ids, change.unique
        ({1}, False)
    """

    src_table: MainTableName
    dst_table: MainTableName

    dst_table_target_column: MainTableColumnName

    added_maps: dict[MainTableID, Sequence[Optional[T]]]
    updated_maps: dict[MainTableID, Sequence[Optional[T]]]
    deleted_ids: set[MainTableID]
    dirtied: set[MainTableID]

    # Are the values in this field unique?
    unique: bool = False

    # Explicit per-src replacement payload for link-oriented operations.
    # When provided for a src id, implementations should treat the sequence as
    # the authoritative desired set of linked dst rows for that src.
    link_replacements: dict[MainTableID, Sequence[LinkDstUpdate[T]]] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class OneOneInOneTableFieldUpdate(Generic[T]):
    """
    Describe scalar value writes and clears for existing owner rows.

    Required added_maps and updated_maps carry ID-to-optional-value mappings;
    deleted_ids means clearing/nullifying the column rather than deleting rows.
    dirtied carries owner refresh hints and unique defaults false. This mutable
    dataclass retains the supplied containers without checking nullability, row
    existence or uniqueness; concrete field updates enforce those constraints.

    Example:
        >>> change = OneOneInOneTableFieldUpdate({}, {}, {7}, set())
        >>> change.deleted_ids
        {7}
    """

    added_maps: dict[MainTableID, Optional[T]]
    updated_maps: dict[MainTableID, Optional[T]]
    deleted_ids: set[MainTableID]
    dirtied: set[MainTableID]

    # Are the values in this field unique?
    unique: bool = False
