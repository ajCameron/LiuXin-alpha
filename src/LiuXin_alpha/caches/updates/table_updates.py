
"""
Describe legacy main-table and association update requests and result payloads.

These mutable payloads describe intentions/results only. They retain
supplied containers, add no validation or normalization, and neither execute
writes nor certify that a database transaction succeeded.

Runtime import currently depends on annotation names that are imported only
under TYPE_CHECKING without postponed annotation evaluation. The first
MainTableName annotation therefore cannot resolve in an ordinary import.
The declarations are retained for compatibility/source review; this module
does not provide executable update coordination.
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
    Request row creation, row updates and ID deletion for one main table.

    main_table names the target. create_these_row_dicts and
    update_these_row_dicts carry ordered row dictionaries; delete_these_ids
    selects owner rows to remove. All fields are required. Matching/completion
    and persistence belong to a consumer, not this dataclass.

    Example:
        A request can carry one new row dictionary and a set of obsolete row IDs
        for a consumer to validate before writing.
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
    Report dirty identities and changed row payloads for a main-table operation.

    Require the target table, dirtied_ids and changed_row_dicts. Supplied
    containers are retained without deriving IDs from payloads or checking that
    the described changes were persisted.

    Example:
        A caller can report IDs {1, 2} as dirty with its corresponding changed row dictionaries.
    """
    main_table: MainTableName

    # The ids of rows affected by this update
    dirtied_ids: set[MainTableID]

    changed_row_dicts: List[Dict[str, Any]]


@dataclasses.dataclass
class OneOneInterLinkTableUpdate:
    """
    Request pair creation and several legacy deletion selectors for a one-to-one link table.

    Require the table name, create_these_links, update_for_dst, source and
    destination deletion sets, explicit link-row deletion IDs, endpoint-based
    link deletion sets and dirty endpoint hints. Values in create_these_links
    are broadly annotated; the dataclass does not resolve destination values.
    Consumers decide which selectors they support and whether update_for_dst
    requires a separate destination-table operation.

    Example:
        A create_these_links entry {1: 7} describes a source/destination association
        for the concrete updater to validate and persist.
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
    Carry endpoint and value-change results for a one-to-one association update.

    Require table identity, deleted source/destination IDs, added/updated
    destination IDs, src_values_changed and dirty endpoint sets. The fields
    are consumer-reported facts; construction does not infer them from a request
    or validate their mutual consistency.

    Example:
        A caller can record source 1 mapping to destination 7 in src_values_changed.
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
    Carry legacy one-to-many ordering, typing and link-property intentions.

    Require the association table, dirty/deleted endpoint sets and maps
    for priority, type, combined priority/type, primary flags, origin and policy.
    Pair-property maps use (source_id, destination_id) keys. Optional data/index
    maps default to None; construction does not turn them into empty mappings.
    A consumer expecting .items() on those attributes needs actual mappings.

    Source-keyed priority sequences contain ordered destination IDs, while
    source/type maps contain destination sets. set_these_dst_as_primary selects
    destination identities; enforcement remains the consumer's responsibility.

    Example:
        An explicit set_link_priority mapping {(1, 7): 3} describes priority 3
        for that directed pair; it does not write the physical link by itself.
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
    Report endpoint changes and per-pair property updates for a one-to-many operation.

    All fields are required: table, deleted/added/updated/dirty endpoint IDs,
    source-to-destination value changes, and priority/type/primary/origin/policy/
    data/index update maps. No defaults or consistency checks synthesize
    missing results. Supplied sets/maps remain mutable and shared.

    Example:
        A priority_updates entry {(1, 7): 3} records the reported change for
        one directed association.
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
    Carry legacy many-to-one ordering, typing and link-property intentions.

    Require the association table, dirty/deleted endpoint sets and maps
    for priority, type, combined priority/type, primary flags, origin and policy.
    Pair-property maps use (source_id, destination_id) keys. Optional data/index
    maps default to None; construction does not turn them into empty mappings.
    A consumer expecting .items() on those attributes needs actual mappings.

    Despite their src_dst_* names, the declared priority/type maps use
    tuples of source IDs (optionally with a type) as keys and one destination ID
    as value. This shape is not adapted automatically to the concrete link
    updater's source-keyed maps. set_these_src_as_primary selects source IDs.

    Example:
        An explicit set_link_priority mapping {(1, 7): 3} describes priority 3
        for that directed pair; it does not write the physical link by itself.
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
    """
    Reuse the one-to-many result payload shape for a many-to-one operation.

    This distinct subclass inherits the dataclass constructor and fields
    without adding fields, converting orientation or validating cardinality.
    Callers must populate inherited endpoint/property maps consistently with
    their operation; the class is not an assignment alias.

    Example:
        Construct this result with the same required endpoint and property
        arguments as OneManyInterLinkTableUpdateResults.
    """



@dataclasses.dataclass
class ManyManyInterlinkTableUpdate:
    """
    Carry legacy many-to-many ordering, typing and link-property intentions.

    Require the association table, dirty/deleted endpoint sets and maps
    for priority, type, combined priority/type, primary flags, origin and policy.
    Pair-property maps use (source_id, destination_id) keys. Optional data/index
    maps default to None; construction does not turn them into empty mappings.
    A consumer expecting .items() on those attributes needs actual mappings.

    Separate src_dst_* and dst_src_* maps describe both orientations.
    Source-oriented priority maps contain destination lists; reverse maps use
    source-ID tuples as keys and one destination as value, optionally with type.
    Both source and destination primary selectors are required.

    Example:
        An explicit set_link_priority mapping {(1, 7): 3} describes priority 3
        for that directed pair; it does not write the physical link by itself.
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
    Reuse the one-to-many result payload shape for a many-to-many operation.

    This distinct subclass inherits the dataclass constructor and fields
    without adding fields, converting orientation or validating cardinality.
    Callers must populate inherited endpoint/property maps consistently with
    their operation; the class is not an assignment alias.

    Example:
        Construct this result with the same required endpoint and property
        arguments as OneManyInterLinkTableUpdateResults.
    """











