"""Core WEMI item identity implementation containers.

Category: core WEMI identity object.
This module implements the item entity itself, not the editable metadata bundle
and not a read-side query result.
"""
from __future__ import annotations

from typing import Any, Mapping, Optional

from LiuXin_alpha.metadata.api.metadata_container_api.wemi_containers_api import ItemIdentityAPI


class ItemIdentity(ItemIdentityAPI):
    """
    Lightweight concrete container for one ``items`` row.

    This intentionally mirrors the style of :class:`WorkIdentity`: it is only
    the row itself plus a couple of construction/serialization helpers.
    """

    def __init__(
        self,
        *,
        item_id: Optional[int] = None,
        item_manifestation_id: Optional[int] = None,
        item_flags: Optional[str] = None,
        item_type: Optional[str] = None,
        item_location: Optional[str] = None,
        item_inventory_code: Optional[str] = None,
        item_source: Optional[str] = None,
        item_source_detail: Optional[str] = None,
        item_source_path: Optional[str] = None,
        item_source_name: Optional[str] = None,
        item_acquired_date: Optional[str] = None,
        item_acquired_price_minor: Optional[int | float] = None,
        item_lifecycle_status: Optional[str] = None,
        item_condition: Optional[str] = None,
        item_original_date: Optional[str] = None,
        item_original_copyright_date: Optional[str] = None,
        item_created_timestamp_ep_k: Optional[int] = None,
        item_modified_timestamp_ep_k: Optional[int] = None,
        item_source_created_datestamp_ep_k: Optional[int] = None,
        item_source_modified_datestamp_ep_k: Optional[int] = None,
        item_scratch: Optional[str] = None,
    ) -> None:
        self._item_id = item_id
        self._item_manifestation_id = item_manifestation_id
        self._item_flags = item_flags
        self._item_type = item_type
        self._item_location = item_location
        self._item_inventory_code = item_inventory_code
        self._item_source = item_source
        self._item_source_detail = item_source_detail
        self._item_source_path = item_source_path
        self._item_source_name = item_source_name
        self._item_acquired_date = item_acquired_date
        self._item_acquired_price_minor = item_acquired_price_minor
        self._item_lifecycle_status = item_lifecycle_status
        self._item_condition = item_condition

        # Stored as extras for callers that want the fuller table row.
        self.item_original_date = item_original_date
        self.item_original_copyright_date = item_original_copyright_date
        self.item_created_timestamp_ep_k = item_created_timestamp_ep_k
        self.item_modified_timestamp_ep_k = item_modified_timestamp_ep_k
        self.item_source_created_datestamp_ep_k = item_source_created_datestamp_ep_k
        self.item_source_modified_datestamp_ep_k = item_source_modified_datestamp_ep_k
        self.item_scratch = item_scratch

    @classmethod
    def from_mapping(cls, row: Mapping[str, Any]) -> "ItemIdentity":
        return cls(
            item_id=row.get("item_id"),
            item_manifestation_id=row.get("item_manifestation_id"),
            item_flags=row.get("item_flags"),
            item_type=row.get("item_type"),
            item_location=row.get("item_location"),
            item_inventory_code=row.get("item_inventory_code"),
            item_source=row.get("item_source"),
            item_source_detail=row.get("item_source_detail"),
            item_source_path=row.get("item_source_path"),
            item_source_name=row.get("item_source_name"),
            item_acquired_date=row.get("item_acquired_date"),
            item_acquired_price_minor=row.get("item_acquired_price_minor"),
            item_lifecycle_status=row.get("item_lifecycle_status"),
            item_condition=row.get("item_condition"),
            item_original_date=row.get("item_original_date"),
            item_original_copyright_date=row.get("item_original_copyright_date"),
            item_created_timestamp_ep_k=row.get("item_created_timestamp_ep_k"),
            item_modified_timestamp_ep_k=row.get("item_modified_timestamp_ep_k"),
            item_source_created_datestamp_ep_k=row.get("item_source_created_datestamp_ep_k"),
            item_source_modified_datestamp_ep_k=row.get("item_source_modified_datestamp_ep_k"),
            item_scratch=row.get("item_scratch"),
        )

    def to_mapping(self) -> dict[str, Any]:
        return {
            "item_id": self.item_id,
            "item_manifestation_id": self.item_manifestation_id,
            "item_flags": self.item_flags,
            "item_type": self.item_type,
            "item_location": self.item_location,
            "item_inventory_code": self.item_inventory_code,
            "item_original_date": self.item_original_date,
            "item_original_copyright_date": self.item_original_copyright_date,
            "item_source": self.item_source,
            "item_source_detail": self.item_source_detail,
            "item_source_path": self.item_source_path,
            "item_source_name": self.item_source_name,
            "item_acquired_date": self.item_acquired_date,
            "item_acquired_price_minor": self.item_acquired_price_minor,
            "item_lifecycle_status": self.item_lifecycle_status,
            "item_condition": self.item_condition,
            "item_created_timestamp_ep_k": self.item_created_timestamp_ep_k,
            "item_modified_timestamp_ep_k": self.item_modified_timestamp_ep_k,
            "item_source_created_datestamp_ep_k": self.item_source_created_datestamp_ep_k,
            "item_source_modified_datestamp_ep_k": self.item_source_modified_datestamp_ep_k,
            "item_scratch": self.item_scratch,
        }

    @property
    def item_id(self) -> Optional[int]:
        return self._item_id

    @item_id.setter
    def item_id(self, item_id: Optional[int]) -> None:
        if self._item_id is None:
            self._item_id = item_id
        else:
            raise AttributeError("Item id is already set.")

    @property
    def item_manifestation_id(self) -> Optional[int]:
        return self._item_manifestation_id

    @item_manifestation_id.setter
    def item_manifestation_id(self, item_manifestation_id: Optional[int]) -> None:
        self._item_manifestation_id = item_manifestation_id

    @property
    def item_flags(self) -> Optional[str]:
        return self._item_flags

    @item_flags.setter
    def item_flags(self, item_flags: Optional[str]) -> None:
        self._item_flags = item_flags

    @property
    def item_type(self) -> Optional[str]:
        return self._item_type

    @item_type.setter
    def item_type(self, item_type: Optional[str]) -> None:
        self._item_type = item_type

    @property
    def item_location(self) -> Optional[str]:
        return self._item_location

    @item_location.setter
    def item_location(self, item_location: Optional[str]) -> None:
        self._item_location = item_location

    @property
    def item_inventory_code(self) -> Optional[str]:
        return self._item_inventory_code

    @item_inventory_code.setter
    def item_inventory_code(self, item_inventory_code: Optional[str]) -> None:
        self._item_inventory_code = item_inventory_code

    @property
    def item_source(self) -> Optional[str]:
        return self._item_source

    @item_source.setter
    def item_source(self, item_source: Optional[str]) -> None:
        self._item_source = item_source

    @property
    def item_source_detail(self) -> Optional[str]:
        return self._item_source_detail

    @item_source_detail.setter
    def item_source_detail(self, item_source_detail: Optional[str]) -> None:
        self._item_source_detail = item_source_detail

    @property
    def item_source_path(self) -> Optional[str]:
        return self._item_source_path

    @item_source_path.setter
    def item_source_path(self, item_source_path: Optional[str]) -> None:
        self._item_source_path = item_source_path

    @property
    def item_source_name(self) -> Optional[str]:
        return self._item_source_name

    @item_source_name.setter
    def item_source_name(self, item_source_name: Optional[str]) -> None:
        self._item_source_name = item_source_name

    @property
    def item_acquired_date(self) -> Optional[str]:
        return self._item_acquired_date

    @item_acquired_date.setter
    def item_acquired_date(self, item_acquired_date: Optional[str]) -> None:
        self._item_acquired_date = item_acquired_date

    @property
    def item_acquired_price_minor(self) -> Optional[int | float]:
        return self._item_acquired_price_minor

    @item_acquired_price_minor.setter
    def item_acquired_price_minor(self, item_acquired_price_minor: Optional[int | float]) -> None:
        self._item_acquired_price_minor = item_acquired_price_minor

    @property
    def item_lifecycle_status(self) -> Optional[str]:
        return self._item_lifecycle_status

    @item_lifecycle_status.setter
    def item_lifecycle_status(self, item_lifecycle_status: Optional[str]) -> None:
        self._item_lifecycle_status = item_lifecycle_status

    @property
    def item_condition(self) -> Optional[str]:
        return self._item_condition

    @item_condition.setter
    def item_condition(self, item_condition: Optional[str]) -> None:
        self._item_condition = item_condition


__all__ = ["ItemIdentity"]
