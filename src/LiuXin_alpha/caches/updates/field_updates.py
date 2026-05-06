from __future__ import annotations

import dataclasses
from typing import Generic, Sequence, Optional

from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.many_many_field_api import T
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.many_one_field_api import T
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.one_many_field_api import T
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.one_one_field_api import T
from LiuXin_alpha.caches.api.storage_cache_api.storage_fields_api.util_mixins import LinkDstUpdate
from LiuXin_alpha.databases.db_types import MainTableName, MainTableColumnName, MainTableID


@dataclasses.dataclass
class ManyManyInTwoTableFieldUpdate(Generic[T]):
    """
    Update for a many-to-many field stored across a link table and a dst table.

    The mapping is keyed by the src table id and valued with the values to be
    written into the dst table target column.

    ``deleted_ids`` means "detach/clear this field from these src rows", not
    "delete the src rows themselves". Implementations may mutate links and, if
    explicitly supported, create or remove related dst rows.
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
    Update for a many-to-one field stored across a link table and a dst table.

    The mapping is keyed by the src table id and valued with the value to be
    written into the dst table target column.

    ``deleted_ids`` means "detach/clear this field from these src rows", not
    "delete the src rows themselves". Implementations may mutate links and, if
    explicitly supported, create or remove related dst rows.
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
    Update for a one-to-many field stored across a link table and a dst table.

    The mapping is keyed by the src table id and valued with the values to be
    written into the dst table target column.

    ``deleted_ids`` means "detach/clear this field from these src rows", not
    "delete the src rows themselves". Implementations may mutate links and, if
    explicitly supported, create or remove related dst rows.
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
    Update for a one-to-one field stored in a single table.

    This update is field-oriented, not row-lifecycle-oriented:
    ``deleted_ids`` means "clear/nullify this field for these ids", not
    "delete the owning rows".
    """

    added_maps: dict[MainTableID, Optional[T]]
    updated_maps: dict[MainTableID, Optional[T]]
    deleted_ids: set[MainTableID]
    dirtied: set[MainTableID]

    # Are the values in this field unique?
    unique: bool = False
