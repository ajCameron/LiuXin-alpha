
"""
Classes which contain update information for tables.
"""

import dataclasses

from typing import Any, Dict, List, Optional, Tuple, Union, TYPE_CHECKING

if TYPE_CHECKING:

    from LiuXin_alpha.databases.db_types import (
        MainTableName,
        SrcTableID,
        DstTableID,
        InterlinkTableID,
        MainTableColumnName,
        InterLinkTableName,
        MainTableID)


@dataclasses.dataclass
class MainTableUpdate:
    """
    Contains all the information needed to update a main table.

    This has the nature of a request for update.
    There's a fair amount of matching and completion to do before the update can go through.
    The result of that effort is a MainTableUpdateResult class.
    """
    # We're targeting this table
    main_table: MainTableName

    # These columns will be created
    create_these_row_dicts: List[Dict[str, Any]]

    # These columns will be "updated" (with checks to see if they need to be)
    update_these_row_dicts: List[Dict[str, Any]]

    # These ids will be removed from the table.
    delete_these_ids: set[MainTableID]


@dataclasses.dataclass
class MainTableUpdateResults:
    """
    Results of updating a main table.
    """
    main_table: MainTableName

    # The ids of rows affected by this update
    dirtied_ids: set[MainTableID]

    changed_row_dicts: List[Dict[str, Any]]


@dataclasses.dataclass
class OneOneInterLinkTableUpdate:
    """
    Updates a basic one-to-one table.

    Update a one-to-one link between two tables.
    """
    interlink_table: InterLinkTableName

    # Keyed with the src table id and valued with the dst table id
    create_these_links: dict[SrcTableID, Union[DstTableID, Any]]

    update_for_dst: dict[str, Any]

    delete_these_src_ids: set[MainTableID]
    delete_these_dst_ids: set[MainTableID]

    delete_these_link_ids: set[InterlinkTableID]
    delete_links_with_this_src_id: set[SrcTableID]
    delete_links_with_this_dst_id: set[DstTableID]

    dirtied_src_ids: set[MainTableID]
    dirtied_dst_ids: set[MainTableID]


@dataclasses.dataclass
class OneOneInterLinkTableUpdateResults:
    """
    Result of updating a one-to-one link between two tables.
    """
    interlink_table: InterLinkTableName

    src_ids_deleted: set[MainTableID]
    dst_ids_deleted: set[MainTableID]

    dst_ids_added: set[MainTableID]
    dst_ids_updated: set[MainTableID]

    src_values_changed: dict[SrcTableID, DstTableID]

    dirtied_src_ids: set[MainTableID]
    dirtied_dst_ids: set[MainTableID]


@dataclasses.dataclass
class OneManyInterlinkTableUpdate:
    """
    We're updating a one-to-many table.

    (Primary example of these is x-notes).
    """
    interlink_table: InterLinkTableName

    src_ids_deleted: set[MainTableID]
    dst_ids_deleted: set[MainTableID]

    dirtied_src_ids: set[MainTableID]
    dirtied_dst_ids: set[MainTableID]

    # Priority update
    src_dst_priority_update: dict[SrcTableID, list[DstTableID]]
    set_link_priority: dict[tuple[SrcTableID, DstTableID], int]

    # Type update
    src_dst_type_update: dict[SrcTableID, dict[str, set[DstTableID]]]
    set_link_type: dict[tuple[SrcTableID, DstTableID], str]

    # Priority-type update
    src_dst_priority_type_update: dict[SrcTableID, dict[str, list[DstTableID]]]

    # Primary update
    set_these_dst_as_primary: set[DstTableID]

    # Origin
    set_link_origin: dict[tuple[SrcTableID, DstTableID], str]

    # Policy
    set_link_policy: dict[tuple[SrcTableID, DstTableID], str]

    # data
    set_link_data: Optional[dict[tuple[SrcTableID, DstTableID], str]] = None

    # index
    set_link_index: Optional[dict[tuple[SrcTableID, DstTableID], Optional[Union[int, float]]]] = None




@dataclasses.dataclass
class OneManyInterLinkTableUpdateResults:
    """
    Result of updating a one-to-one link between two tables.
    """
    interlink_table: InterLinkTableName

    src_ids_deleted: set[MainTableID]
    dst_ids_deleted: set[MainTableID]

    dst_ids_added: set[MainTableID]
    dst_ids_updated: set[MainTableID]

    dirtied_src_ids: set[MainTableID]
    dirtied_dst_ids: set[MainTableID]

    src_values_changed: dict[SrcTableID, DstTableID]

    # Priority updates
    priority_updates: dict[tuple[SrcTableID, DstTableID], int]

    # type updates
    type_updates: dict[tuple[SrcTableID, DstTableID], str]

    # primary updates
    primary_updates: dict[tuple[SrcTableID, DstTableID], bool]

    # origin updates
    origin_updates: dict[tuple[SrcTableID, DstTableID], str]

    # policy updates
    policy_updates: dict[tuple[SrcTableID, DstTableID], str]

    # data updates
    data_updates: dict[tuple[SrcTableID, DstTableID], str]

    # index
    index_updates: dict[tuple[SrcTableID, DstTableID], Union[int, float]]


@dataclasses.dataclass
class ManyOneInterlinkTableUpdate:
    """
    We're updating a many-one table.

    (Primary example of these is notes-x).
    """
    interlink_table: InterLinkTableName

    src_ids_deleted: set[MainTableID]
    dst_ids_deleted: set[MainTableID]

    dirtied_src_ids: set[MainTableID]
    dirtied_dst_ids: set[MainTableID]

    # Priority update
    src_dst_priority_update: dict[tuple[SrcTableID, ...], DstTableID]
    set_link_priority: dict[tuple[SrcTableID, DstTableID], int]

    # Type update
    # - In this case, priority info is present, just ignored
    src_dst_type_update: dict[tuple[str, tuple[SrcTableID, ...]], DstTableID]
    set_link_type: dict[tuple[SrcTableID, DstTableID], str]

    # Priority-type update
    src_dst_priority_type_update: dict[tuple[str, tuple[SrcTableID, ...]], DstTableID]

    # Primary update
    set_these_src_as_primary: set[SrcTableID]

    # Origin
    set_link_origin: dict[tuple[SrcTableID, DstTableID], str]

    # Policy
    set_link_policy: dict[tuple[SrcTableID, DstTableID], str]

    # data
    set_link_data: Optional[dict[tuple[SrcTableID, DstTableID], str]] = None

    # index
    set_link_index: Optional[dict[tuple[SrcTableID, DstTableID], Optional[Union[int, float]]]] = None


class ManyOneInterLinkTableUpdateResults(OneManyInterLinkTableUpdateResults):
    """Result aliases for a many-to-one update viewed from its source side."""



@dataclasses.dataclass
class ManyManyInterlinkTableUpdate:
    """
    We're updating a many-many table.

    E.g. tags-works.
    """
    interlink_table: InterLinkTableName

    dirtied_src_ids: set[MainTableID]
    dirtied_dst_ids: set[MainTableID]

    src_ids_deleted: set[MainTableID]
    dst_ids_deleted: set[MainTableID]

    # Priority update
    src_dst_priority_update: dict[SrcTableID, list[DstTableID]]
    dst_src_priority_update: dict[tuple[SrcTableID], DstTableID]
    set_link_priority: dict[tuple[SrcTableID, DstTableID], int]

    # Type update
    src_dst_type_update: dict[SrcTableID, dict[str, set[DstTableID]]]
    # - Has priority info due to nature of dict, but is ignored
    dst_src_type_update: dict[tuple[str, tuple[SrcTableID]], DstTableID]
    set_link_type: dict[tuple[SrcTableID, DstTableID], str]

    # Priority-type update
    src_dst_priority_type_update: dict[SrcTableID, dict[str, list[DstTableID]]]
    dst_src_priority_type_update: dict[tuple[str, tuple[SrcTableID]], DstTableID]

    # Primary update
    set_these_src_as_primary: set[SrcTableID]
    set_these_dst_as_primary: set[DstTableID]

    # Origin
    set_link_origin: dict[tuple[SrcTableID, DstTableID], str]

    # Policy
    set_link_policy: dict[tuple[SrcTableID, DstTableID], str]

    # data
    set_link_data: Optional[dict[tuple[SrcTableID, DstTableID], str]] = None

    # index
    set_link_index: Optional[dict[tuple[SrcTableID, DstTableID], Optional[Union[int, float]]]] = None


class ManyManyInterLinkTableUpdateResults(OneManyInterLinkTableUpdateResults):
    """
    The results of updating a Many to Many table.
    """











