
"""
Generic objects to characterize links.
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
    Identify one concrete src/dst edge.
    """

    src_table: MainTableName
    src_table_id: MainTableID

    dst_table: MainTableName
    dst_table_id: MainTableID


@dataclasses.dataclass
class LinkPropertiesMixin:
    """
    Link-level properties for many-many relationships.
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
    Properties of one concrete link between two tables.
    """


@dataclasses.dataclass
class LinkDstUpdateMixin(Generic[T]):
    """
    We're adding/updating a dst row with optional link properties.
    """

    dst_table: MainTableName
    dst_table_target_column: MainTableColumnName
    dst_col_val: Optional[T]
    dst_table_id: Optional[MainTableID] = None


@dataclasses.dataclass
class LinkDstUpdate(LinkPropertiesMixin, LinkDstUpdateMixin[T]):
    """
    Update for one concrete linked dst row.
    """
