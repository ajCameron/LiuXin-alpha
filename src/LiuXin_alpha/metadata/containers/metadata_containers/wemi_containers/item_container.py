"""
Implement intrinsic item identity fields separately from the editable relation bundle.

The identity retains copy-specific location, acquisition and provenance values and
maps them to prefixed row columns. It performs no lookup or persistence.

Example:
    >>> identity = ItemIdentity(item_id=3)
    >>> identity.item_id
    3
"""
from __future__ import annotations

from typing import Any, Mapping, Optional

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api import ItemIdentityAPI
from LiuXin_alpha.metadata.containers.metadata_containers._string_formatting import (
    compact_mapping_string,
)


class ItemIdentity(ItemIdentityAPI):
    """
    Store a item identity and its copy-specific location, acquisition and provenance metadata.

    This mirrors the items row; editable relationships belong to ItemMetadata. Values,
    including flags, are retained as supplied. The public id setter accepts assignments
    only while the stored id is None.

    Example:
        >>> identity = ItemIdentity(item_id=3)
        >>> identity.item_id
        3
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
        """
        Store the supplied item identity fields without coercion or validation.

        Flags remain in their supplied representation. Date, price, extent and foreign-key
        values are not parsed or resolved here.

        Example:
            >>> identity = ItemIdentity(item_id=3)
            >>> identity.to_mapping()['item_id']
            3


        :param item_id: Optional item row id; a non-None value locks the public id setter.
        :param item_manifestation_id: Optional parent manifestation id; retained as
            supplied.
        :param item_flags: Optional flags text; retained as supplied.
        :param item_type: Optional item type; retained as supplied.
        :param item_location: Optional location text; retained as supplied.
        :param item_inventory_code: Optional inventory code; retained as supplied.
        :param item_source: Optional source label; retained as supplied.
        :param item_source_detail: Optional source detail; retained as supplied.
        :param item_source_path: Optional source path text; retained as supplied.
        :param item_source_name: Optional source name; retained as supplied.
        :param item_acquired_date: Optional acquisition date text; retained as supplied.
        :param item_acquired_price_minor: Optional acquisition price in minor currency
            units; retained as supplied.
        :param item_lifecycle_status: Optional lifecycle status; retained as supplied.
        :param item_condition: Optional condition description; retained as supplied.
        :param item_original_date: Optional original date text; retained as supplied.
        :param item_original_copyright_date: Optional original copyright date text; retained
            as supplied.
        :param item_created_timestamp_ep_k: Optional creation timestamp in ep_k form;
            retained as supplied.
        :param item_modified_timestamp_ep_k: Optional modification timestamp in ep_k form;
            retained as supplied.
        :param item_source_created_datestamp_ep_k: Optional source creation datestamp in
            ep_k form; retained as supplied.
        :param item_source_modified_datestamp_ep_k: Optional source modification datestamp
            in ep_k form; retained as supplied.
        :param item_scratch: Optional scratch text; retained as supplied.
        :return: None.
        """
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
        """
        Build a item identity from recognized prefixed row columns.

        Missing columns become None and unknown keys are ignored. All recognized values are
        retained without coercion or normalization.

        Example:
            >>> identity = ItemIdentity.from_mapping({'item_id': 3, 'item_flags': 'draft, draft'})
            >>> identity.item_flags
            'draft, draft'


        :param row: Mapping supplying optional item-prefixed identity columns.
        :return: New ItemIdentity instance of the requested class.
        """
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
        """
        Serialize every supported item identity field to its prefixed column name.

        Stored values, including None and unnormalized flags, are retained.

        Example:
            >>> identity = ItemIdentity(item_id=3)
            >>> identity.to_mapping()['item_id']
            3


        :return: New dictionary of item row fields.
        """
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

    def __str__(self) -> str:
        """
        Render a compact diagnostic summary emphasizing item ids and display fields.

        Example:
            >>> identity = ItemIdentity(item_source_name='notes.epub')
            >>> 'notes.epub' in str(identity)
            True


        :return: Human-readable identity summary.
        """
        return compact_mapping_string(
            self,
            self.to_mapping(),
            id_keys=("item_id", "item_manifestation_id"),
            display_keys=("item_source_name", "item_source_path", "item_type"),
        )

    @property
    def item_id(self) -> Optional[int]:
        """
        Return the stored item row id.

        Example:
            >>> identity = ItemIdentity(item_id=3)
            >>> identity.item_id
            3


        :return: Item row id, or None when unset.
        """
        return self._item_id

    @item_id.setter
    def item_id(self, item_id: Optional[int]) -> None:
        """
        Assign a item id only while the stored id is None.

        A stored non-None id causes AttributeError even for an identical value. Assigning
        None while unset leaves the setter available.

        Example:
            >>> identity = ItemIdentity()
            >>> identity.item_id = 3
            >>> identity.item_id = 3
            Traceback (most recent call last):
            ...
            AttributeError: Item id is already set.


        :param item_id: New item row id, or None to leave or mark it unset.
        :return: None.
        """
        if self._item_id is None:
            self._item_id = item_id
        else:
            raise AttributeError("Item id is already set.")

    @property
    def item_manifestation_id(self) -> Optional[int]:
        """
        Return the stored parent manifestation id.

        Example:
            >>> identity = ItemIdentity(item_manifestation_id=2)
            >>> identity.item_manifestation_id
            2


        :return: Parent manifestation id, or None when unset.
        """
        return self._item_manifestation_id

    @item_manifestation_id.setter
    def item_manifestation_id(self, item_manifestation_id: Optional[int]) -> None:
        """
        Replace the parent manifestation id without coercion, validation or lookup.

        Example:
            >>> identity = ItemIdentity()
            >>> identity.item_manifestation_id = 2
            >>> identity.item_manifestation_id
            2


        :param item_manifestation_id: New parent manifestation id, or None to leave or mark
            it unset.
        :return: None.
        """
        self._item_manifestation_id = item_manifestation_id

    @property
    def item_flags(self) -> Optional[str]:
        """
        Return the stored flags text.

        Example:
            >>> identity = ItemIdentity(item_flags='draft, draft')
            >>> identity.item_flags
            'draft, draft'


        :return: Flags text, or None when unset.
        """
        return self._item_flags

    @item_flags.setter
    def item_flags(self, item_flags: Optional[str]) -> None:
        """
        Replace the flags text without coercion, validation or lookup.

        Example:
            >>> identity = ItemIdentity()
            >>> identity.item_flags = 'draft, draft'
            >>> identity.item_flags
            'draft, draft'


        :param item_flags: New flags text, or None to leave or mark it unset.
        :return: None.
        """
        self._item_flags = item_flags

    @property
    def item_type(self) -> Optional[str]:
        """
        Return the stored item type.

        Example:
            >>> identity = ItemIdentity(item_type='digital')
            >>> identity.item_type
            'digital'


        :return: Item type, or None when unset.
        """
        return self._item_type

    @item_type.setter
    def item_type(self, item_type: Optional[str]) -> None:
        """
        Replace the item type without coercion, validation or lookup.

        Example:
            >>> identity = ItemIdentity()
            >>> identity.item_type = 'digital'
            >>> identity.item_type
            'digital'


        :param item_type: New item type, or None to leave or mark it unset.
        :return: None.
        """
        self._item_type = item_type

    @property
    def item_location(self) -> Optional[str]:
        """
        Return the stored location text.

        Example:
            >>> identity = ItemIdentity(item_location='Shelf A')
            >>> identity.item_location
            'Shelf A'


        :return: Location text, or None when unset.
        """
        return self._item_location

    @item_location.setter
    def item_location(self, item_location: Optional[str]) -> None:
        """
        Replace the location text without coercion, validation or lookup.

        Example:
            >>> identity = ItemIdentity()
            >>> identity.item_location = 'Shelf A'
            >>> identity.item_location
            'Shelf A'


        :param item_location: New location text, or None to leave or mark it unset.
        :return: None.
        """
        self._item_location = item_location

    @property
    def item_inventory_code(self) -> Optional[str]:
        """
        Return the stored inventory code.

        Example:
            >>> identity = ItemIdentity(item_inventory_code='A-001')
            >>> identity.item_inventory_code
            'A-001'


        :return: Inventory code, or None when unset.
        """
        return self._item_inventory_code

    @item_inventory_code.setter
    def item_inventory_code(self, item_inventory_code: Optional[str]) -> None:
        """
        Replace the inventory code without coercion, validation or lookup.

        Example:
            >>> identity = ItemIdentity()
            >>> identity.item_inventory_code = 'A-001'
            >>> identity.item_inventory_code
            'A-001'


        :param item_inventory_code: New inventory code, or None to leave or mark it unset.
        :return: None.
        """
        self._item_inventory_code = item_inventory_code

    @property
    def item_source(self) -> Optional[str]:
        """
        Return the stored source label.

        Example:
            >>> identity = ItemIdentity(item_source='import')
            >>> identity.item_source
            'import'


        :return: Source label, or None when unset.
        """
        return self._item_source

    @item_source.setter
    def item_source(self, item_source: Optional[str]) -> None:
        """
        Replace the source label without coercion, validation or lookup.

        Example:
            >>> identity = ItemIdentity()
            >>> identity.item_source = 'import'
            >>> identity.item_source
            'import'


        :param item_source: New source label, or None to leave or mark it unset.
        :return: None.
        """
        self._item_source = item_source

    @property
    def item_source_detail(self) -> Optional[str]:
        """
        Return the stored source detail.

        Example:
            >>> identity = ItemIdentity(item_source_detail='archive batch')
            >>> identity.item_source_detail
            'archive batch'


        :return: Source detail, or None when unset.
        """
        return self._item_source_detail

    @item_source_detail.setter
    def item_source_detail(self, item_source_detail: Optional[str]) -> None:
        """
        Replace the source detail without coercion, validation or lookup.

        Example:
            >>> identity = ItemIdentity()
            >>> identity.item_source_detail = 'archive batch'
            >>> identity.item_source_detail
            'archive batch'


        :param item_source_detail: New source detail, or None to leave or mark it unset.
        :return: None.
        """
        self._item_source_detail = item_source_detail

    @property
    def item_source_path(self) -> Optional[str]:
        """
        Return the stored source path text.

        Example:
            >>> identity = ItemIdentity(item_source_path='books/notes.epub')
            >>> identity.item_source_path
            'books/notes.epub'


        :return: Source path text, or None when unset.
        """
        return self._item_source_path

    @item_source_path.setter
    def item_source_path(self, item_source_path: Optional[str]) -> None:
        """
        Replace the source path text without coercion, validation or lookup.

        Example:
            >>> identity = ItemIdentity()
            >>> identity.item_source_path = 'books/notes.epub'
            >>> identity.item_source_path
            'books/notes.epub'


        :param item_source_path: New source path text, or None to leave or mark it unset.
        :return: None.
        """
        self._item_source_path = item_source_path

    @property
    def item_source_name(self) -> Optional[str]:
        """
        Return the stored source name.

        Example:
            >>> identity = ItemIdentity(item_source_name='notes.epub')
            >>> identity.item_source_name
            'notes.epub'


        :return: Source name, or None when unset.
        """
        return self._item_source_name

    @item_source_name.setter
    def item_source_name(self, item_source_name: Optional[str]) -> None:
        """
        Replace the source name without coercion, validation or lookup.

        Example:
            >>> identity = ItemIdentity()
            >>> identity.item_source_name = 'notes.epub'
            >>> identity.item_source_name
            'notes.epub'


        :param item_source_name: New source name, or None to leave or mark it unset.
        :return: None.
        """
        self._item_source_name = item_source_name

    @property
    def item_acquired_date(self) -> Optional[str]:
        """
        Return the stored acquisition date text.

        Example:
            >>> identity = ItemIdentity(item_acquired_date='2020-01-01')
            >>> identity.item_acquired_date
            '2020-01-01'


        :return: Acquisition date text, or None when unset.
        """
        return self._item_acquired_date

    @item_acquired_date.setter
    def item_acquired_date(self, item_acquired_date: Optional[str]) -> None:
        """
        Replace the acquisition date text without coercion, validation or lookup.

        Example:
            >>> identity = ItemIdentity()
            >>> identity.item_acquired_date = '2020-01-01'
            >>> identity.item_acquired_date
            '2020-01-01'


        :param item_acquired_date: New acquisition date text, or None to leave or mark it
            unset.
        :return: None.
        """
        self._item_acquired_date = item_acquired_date

    @property
    def item_acquired_price_minor(self) -> Optional[int | float]:
        """
        Return the stored acquisition price in minor currency units.

        Example:
            >>> identity = ItemIdentity(item_acquired_price_minor=1250)
            >>> identity.item_acquired_price_minor
            1250


        :return: Acquisition price in minor currency units, or None when unset.
        """
        return self._item_acquired_price_minor

    @item_acquired_price_minor.setter
    def item_acquired_price_minor(self, item_acquired_price_minor: Optional[int | float]) -> None:
        """
        Replace the acquisition price in minor currency units without coercion, validation or lookup.

        Example:
            >>> identity = ItemIdentity()
            >>> identity.item_acquired_price_minor = 1250
            >>> identity.item_acquired_price_minor
            1250


        :param item_acquired_price_minor: New acquisition price in minor currency units, or
            None to leave or mark it unset.
        :return: None.
        """
        self._item_acquired_price_minor = item_acquired_price_minor

    @property
    def item_lifecycle_status(self) -> Optional[str]:
        """
        Return the stored lifecycle status.

        Example:
            >>> identity = ItemIdentity(item_lifecycle_status='active')
            >>> identity.item_lifecycle_status
            'active'


        :return: Lifecycle status, or None when unset.
        """
        return self._item_lifecycle_status

    @item_lifecycle_status.setter
    def item_lifecycle_status(self, item_lifecycle_status: Optional[str]) -> None:
        """
        Replace the lifecycle status without coercion, validation or lookup.

        Example:
            >>> identity = ItemIdentity()
            >>> identity.item_lifecycle_status = 'active'
            >>> identity.item_lifecycle_status
            'active'


        :param item_lifecycle_status: New lifecycle status, or None to leave or mark it
            unset.
        :return: None.
        """
        self._item_lifecycle_status = item_lifecycle_status

    @property
    def item_condition(self) -> Optional[str]:
        """
        Return the stored condition description.

        Example:
            >>> identity = ItemIdentity(item_condition='good')
            >>> identity.item_condition
            'good'


        :return: Condition description, or None when unset.
        """
        return self._item_condition

    @item_condition.setter
    def item_condition(self, item_condition: Optional[str]) -> None:
        """
        Replace the condition description without coercion, validation or lookup.

        Example:
            >>> identity = ItemIdentity()
            >>> identity.item_condition = 'good'
            >>> identity.item_condition
            'good'


        :param item_condition: New condition description, or None to leave or mark it unset.
        :return: None.
        """
        self._item_condition = item_condition


__all__ = ["ItemIdentity"]
