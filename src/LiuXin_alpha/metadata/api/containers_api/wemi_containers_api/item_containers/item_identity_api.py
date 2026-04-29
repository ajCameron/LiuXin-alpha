"""Core WEMI identity API contract for item entities.

Category: core WEMI identity object.
This module defines the smallest stable API for the item entity itself, not the
editable metadata bundle and not a read-side query result.
"""
from __future__ import annotations

import abc
import dataclasses

from typing import ClassVar, Iterable, Mapping, Optional, Self


class ItemIdentityPropertiesAPI(metaclass=abc.ABCMeta):
    """
    Lightweight API for one row from the ``items`` table.

    This mirrors the style of :mod:`work_container_api`, but keeps the item
    surface focused on fields that are operationally useful for storage,
    provenance, and copy-level handling.
    """

    # ------------------------------------------------------------------
    # Primary key
    # ------------------------------------------------------------------

    @property
    def id(self) -> Optional[int]:
        return self.item_id

    @id.setter
    def id(self, value: Optional[int]) -> None:
        self.item_id = value

    @property
    @abc.abstractmethod
    def item_id(self) -> Optional[int]:
        ...

    @item_id.setter
    @abc.abstractmethod
    def item_id(self, item_id: Optional[int]) -> None:
        ...

    # ------------------------------------------------------------------
    # Parent manifestation
    # ------------------------------------------------------------------

    @property
    def manifestation_id(self) -> Optional[int]:
        return self.item_manifestation_id

    @manifestation_id.setter
    def manifestation_id(self, value: Optional[int]) -> None:
        self.item_manifestation_id = value

    @property
    @abc.abstractmethod
    def item_manifestation_id(self) -> Optional[int]:
        ...

    @item_manifestation_id.setter
    @abc.abstractmethod
    def item_manifestation_id(self, item_manifestation_id: Optional[int]) -> None:
        ...

    # ------------------------------------------------------------------
    # Core item identity / handling
    # ------------------------------------------------------------------

    @property
    def flags(self) -> Optional[str]:
        return self.item_flags

    @flags.setter
    def flags(self, value: Optional[str]) -> None:
        self.item_flags = value

    @property
    @abc.abstractmethod
    def item_flags(self) -> Optional[str]:
        ...

    @item_flags.setter
    @abc.abstractmethod
    def item_flags(self, item_flags: Optional[str]) -> None:
        ...

    @property
    def type(self) -> Optional[str]:
        return self.item_type

    @type.setter
    def type(self, value: Optional[str]) -> None:
        self.item_type = value

    @property
    @abc.abstractmethod
    def item_type(self) -> Optional[str]:
        ...

    @item_type.setter
    @abc.abstractmethod
    def item_type(self, item_type: Optional[str]) -> None:
        ...

    @property
    def location(self) -> Optional[str]:
        return self.item_location

    @location.setter
    def location(self, value: Optional[str]) -> None:
        self.item_location = value

    @property
    @abc.abstractmethod
    def item_location(self) -> Optional[str]:
        ...

    @item_location.setter
    @abc.abstractmethod
    def item_location(self, item_location: Optional[str]) -> None:
        ...

    @property
    def inventory_code(self) -> Optional[str]:
        return self.item_inventory_code

    @inventory_code.setter
    def inventory_code(self, value: Optional[str]) -> None:
        self.item_inventory_code = value

    @property
    @abc.abstractmethod
    def item_inventory_code(self) -> Optional[str]:
        ...

    @item_inventory_code.setter
    @abc.abstractmethod
    def item_inventory_code(self, item_inventory_code: Optional[str]) -> None:
        ...

    # ------------------------------------------------------------------
    # Provenance / acquisition
    # ------------------------------------------------------------------

    @property
    def source(self) -> Optional[str]:
        return self.item_source

    @source.setter
    def source(self, value: Optional[str]) -> None:
        self.item_source = value

    @property
    @abc.abstractmethod
    def item_source(self) -> Optional[str]:
        ...

    @item_source.setter
    @abc.abstractmethod
    def item_source(self, item_source: Optional[str]) -> None:
        ...

    @property
    def source_detail(self) -> Optional[str]:
        return self.item_source_detail

    @source_detail.setter
    def source_detail(self, value: Optional[str]) -> None:
        self.item_source_detail = value

    @property
    @abc.abstractmethod
    def item_source_detail(self) -> Optional[str]:
        ...

    @item_source_detail.setter
    @abc.abstractmethod
    def item_source_detail(self, item_source_detail: Optional[str]) -> None:
        ...

    @property
    def source_path(self) -> Optional[str]:
        return self.item_source_path

    @source_path.setter
    def source_path(self, value: Optional[str]) -> None:
        self.item_source_path = value

    @property
    @abc.abstractmethod
    def item_source_path(self) -> Optional[str]:
        ...

    @item_source_path.setter
    @abc.abstractmethod
    def item_source_path(self, item_source_path: Optional[str]) -> None:
        ...

    @property
    def source_name(self) -> Optional[str]:
        return self.item_source_name

    @source_name.setter
    def source_name(self, value: Optional[str]) -> None:
        self.item_source_name = value

    @property
    @abc.abstractmethod
    def item_source_name(self) -> Optional[str]:
        ...

    @item_source_name.setter
    @abc.abstractmethod
    def item_source_name(self, item_source_name: Optional[str]) -> None:
        ...

    @property
    def acquired_date(self) -> Optional[str]:
        return self.item_acquired_date

    @acquired_date.setter
    def acquired_date(self, value: Optional[str]) -> None:
        self.item_acquired_date = value

    @property
    @abc.abstractmethod
    def item_acquired_date(self) -> Optional[str]:
        ...

    @item_acquired_date.setter
    @abc.abstractmethod
    def item_acquired_date(self, item_acquired_date: Optional[str]) -> None:
        ...

    @property
    def acquired_price_minor(self) -> Optional[int | float]:
        return self.item_acquired_price_minor

    @acquired_price_minor.setter
    def acquired_price_minor(self, value: Optional[int | float]) -> None:
        self.item_acquired_price_minor = value

    @property
    @abc.abstractmethod
    def item_acquired_price_minor(self) -> Optional[int | float]:
        ...

    @item_acquired_price_minor.setter
    @abc.abstractmethod
    def item_acquired_price_minor(self, item_acquired_price_minor: Optional[int | float]) -> None:
        ...

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    @property
    def lifecycle_status(self) -> Optional[str]:
        return self.item_lifecycle_status

    @lifecycle_status.setter
    def lifecycle_status(self, value: Optional[str]) -> None:
        self.item_lifecycle_status = value

    @property
    @abc.abstractmethod
    def item_lifecycle_status(self) -> Optional[str]:
        ...

    @item_lifecycle_status.setter
    @abc.abstractmethod
    def item_lifecycle_status(self, item_lifecycle_status: Optional[str]) -> None:
        ...

    @property
    def condition(self) -> Optional[str]:
        return self.item_condition

    @condition.setter
    def condition(self, value: Optional[str]) -> None:
        self.item_condition = value

    @property
    @abc.abstractmethod
    def item_condition(self) -> Optional[str]:
        ...

    @item_condition.setter
    @abc.abstractmethod
    def item_condition(self, item_condition: Optional[str]) -> None:
        ...


class ItemIdentityAPI(ItemIdentityPropertiesAPI):
    """
    Marker base class for a concrete item-row container.

    The richer item metadata bundle lives in :class:`ItemMetadataAPI`.
    """

__all__ = ["ItemIdentityPropertiesAPI", "ItemIdentityAPI"]
