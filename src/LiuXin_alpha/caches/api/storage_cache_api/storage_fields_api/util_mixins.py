
"""
Define mutable endpoint, property and destination-replacement payloads.

Dataclass mixins compose association identities, optional metadata and
destination-value intentions. They preserve supplied objects without
validation or copying; concrete field setters interpret supported properties
and decide whether None means no update or an explicit clear.
"""


from __future__ import annotations

import dataclasses
from typing import Optional, TypeVar, TYPE_CHECKING, Generic

if TYPE_CHECKING:
    from LiuXin_alpha.databases.db_types import (
        MainTableName,
        MainTableColumnName,
        MainTableID,
    )

T = TypeVar("T")


@dataclasses.dataclass
class SrcDstIDMixin:
    """
    Identify a directed association by table labels and endpoint IDs.

    All four values are required. The mutable dataclass does not resolve
    table names, coerce IDs or verify that a physical link exists.

    Example:
        >>> SrcDstIDMixin("books", 1, "tags", 7).dst_table_id
        7
    """

    src_table: MainTableName
    src_table_id: MainTableID

    dst_table: MainTableName
    dst_table_id: MainTableID


@dataclasses.dataclass
class LinkPropertiesMixin:
    """
    Carry optional priority, type and provenance properties for a link.

    priority, primary, type, origin, policy, data and index all default to
    None. Type annotations add no runtime validation; consumers decide which
    columns are supported and how absent values are handled.

    Example:
        >>> LinkPropertiesMixin(priority=0).primary is None
        True
    """

    priority: Optional[int] = None
    primary: Optional[bool] = None
    type: Optional[str] = None
    origin: Optional[str] = None
    policy: Optional[str] = None
    data: Optional[str] = None
    index: Optional[int] = None


@dataclasses.dataclass
class IndividualLinkProperties(LinkPropertiesMixin, SrcDstIDMixin):
    """
    Combine directed endpoint identities with optional association metadata.

    Dataclass inheritance places required endpoint fields before optional
    property fields in construction. The value is mutable and performs no link
    lookup or metadata validation.

    Example:
        >>> props = IndividualLinkProperties("books", 1, "tags", 7, priority=3)
        >>> props.src_table_id, props.priority
        (1, 3)
    """


@dataclasses.dataclass
class LinkDstUpdateMixin(Generic[T]):
    """
    Describe a destination column value with an optional explicit row identity.

    Require destination table, target column and desired optional value.
    The destination ID defaults to None; consumers choose whether to match an
    existing row or create one. Construction itself resolves nothing.

    Example:
        >>> LinkDstUpdateMixin("tags", "name", "Science Fiction").dst_table_id is None
        True
    """

    dst_table: MainTableName
    dst_table_target_column: MainTableColumnName
    dst_col_val: Optional[T]
    dst_table_id: Optional[MainTableID] = None


@dataclasses.dataclass
class LinkDstUpdate(LinkPropertiesMixin, LinkDstUpdateMixin[T]):
    """
    Combine destination replacement intentions with optional link metadata.

    Required destination labels/value precede the optional destination ID
    and link properties. No copying or validation is performed. A concrete
    replacement helper may select by explicit ID or value, write the destination
    value and apply supported non-None metadata.

    Example:
        >>> update = LinkDstUpdate("tags", "name", "Science Fiction", priority=2)
        >>> update.dst_col_val, update.priority
        ('Science Fiction', 2)
    """
