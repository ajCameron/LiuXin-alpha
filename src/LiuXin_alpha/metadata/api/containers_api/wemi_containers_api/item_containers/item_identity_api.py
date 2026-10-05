"""
Define the row-level item identity contract and its compatibility aliases.

The API covers storage, provenance, acquisition and condition fields for one copy.
Related graph metadata belongs to ItemMetadataAPI.

Example:
    >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
    >>> item = ItemIdentity(item_id=5, item_manifestation_id=4)
    >>> item.WEMI_LEVEL
    'item'
"""
from __future__ import annotations

import abc
import dataclasses

from typing import ClassVar, Iterable, Mapping, Optional, Self

from LiuXin_alpha.metadata.api.containers_api.wemi_containers_api.identity_api import (
    WemiIdentityAPI,
)


class ItemIdentityPropertiesAPI(WemiIdentityAPI, metaclass=abc.ABCMeta):
    """
    Require canonical item-prefixed properties and short compatibility aliases.

    Short aliases delegate directly to their item-prefixed counterparts. Concrete
    identities determine assignment guards, coercion and mapping behavior.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
        >>> item = ItemIdentity(item_id=5, item_manifestation_id=4)
        >>> item.id, item.manifestation_id
        (5, 4)
    """
    WEMI_LEVEL: ClassVar[str] = "item"
    SOURCE_TABLE: ClassVar[str] = "items"
    ID_FIELD: ClassVar[str] = "item_id"

    # ------------------------------------------------------------------
    # Primary key
    # ------------------------------------------------------------------

    @property
    def id(self) -> Optional[int]:
        """
        Return item_id through the compatibility alias.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_id=5)
            >>> item.id
            5


        :return: Current item id, or None.
        """
        return self.item_id

    @id.setter
    def id(self, value: Optional[int]) -> None:
        """
        Assign item_id through the compatibility alias under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.id = 5
            >>> item.item_id
            5


        :param value: New item id, or None.
        :return: None.
        """
        self.item_id = value

    @property
    @abc.abstractmethod
    def item_id(self) -> Optional[int]:
        """
        Return item_id through the canonical item property.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_id=5)
            >>> item.item_id
            5


        :return: Current item id, or None.
        """

    @item_id.setter
    @abc.abstractmethod
    def item_id(self, item_id: Optional[int]) -> None:
        """
        Assign item_id through the canonical item property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.item_id = 5
            >>> item.item_id
            5


        :param item_id: New item id, or None.
        :return: None.
        """

    # ------------------------------------------------------------------
    # Legacy/source-row manifestation hint
    # ------------------------------------------------------------------

    @property
    def manifestation_id(self) -> Optional[int]:
        """
        Return item_manifestation_id through the compatibility alias.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_manifestation_id=4)
            >>> item.manifestation_id
            4


        :return: Current item manifestation id, or None.
        """
        return self.item_manifestation_id

    @manifestation_id.setter
    def manifestation_id(self, value: Optional[int]) -> None:
        """
        Assign item_manifestation_id through the compatibility alias under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.manifestation_id = 4
            >>> item.item_manifestation_id
            4


        :param value: New item manifestation id, or None.
        :return: None.
        """
        self.item_manifestation_id = value

    @property
    @abc.abstractmethod
    def item_manifestation_id(self) -> Optional[int]:
        """
        Return item_manifestation_id through the canonical item property.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_manifestation_id=4)
            >>> item.item_manifestation_id
            4


        :return: Current item manifestation id, or None.
        """

    @item_manifestation_id.setter
    @abc.abstractmethod
    def item_manifestation_id(self, item_manifestation_id: Optional[int]) -> None:
        """
        Assign item_manifestation_id through the canonical item property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.item_manifestation_id = 4
            >>> item.item_manifestation_id
            4


        :param item_manifestation_id: New item manifestation id, or None.
        :return: None.
        """

    # ------------------------------------------------------------------
    # Core item identity / handling
    # ------------------------------------------------------------------

    @property
    def flags(self) -> Optional[str]:
        """
        Return item_flags through the compatibility alias.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_flags='loaned')
            >>> item.flags
            'loaned'


        :return: Current item flags, or None.
        """
        return self.item_flags

    @flags.setter
    def flags(self, value: Optional[str]) -> None:
        """
        Assign item_flags through the compatibility alias under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.flags = 'loaned'
            >>> item.item_flags
            'loaned'


        :param value: New item flags, or None.
        :return: None.
        """
        self.item_flags = value

    @property
    @abc.abstractmethod
    def item_flags(self) -> Optional[str]:
        """
        Return item_flags through the canonical item property.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_flags='loaned')
            >>> item.item_flags
            'loaned'


        :return: Current item flags, or None.
        """

    @item_flags.setter
    @abc.abstractmethod
    def item_flags(self, item_flags: Optional[str]) -> None:
        """
        Assign item_flags through the canonical item property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.item_flags = 'loaned'
            >>> item.item_flags
            'loaned'


        :param item_flags: New item flags, or None.
        :return: None.
        """

    @property
    def type(self) -> Optional[str]:
        """
        Return item_type through the compatibility alias.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_type='ebook')
            >>> item.type
            'ebook'


        :return: Current item type, or None.
        """
        return self.item_type

    @type.setter
    def type(self, value: Optional[str]) -> None:
        """
        Assign item_type through the compatibility alias under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.type = 'ebook'
            >>> item.item_type
            'ebook'


        :param value: New item type, or None.
        :return: None.
        """
        self.item_type = value

    @property
    @abc.abstractmethod
    def item_type(self) -> Optional[str]:
        """
        Return item_type through the canonical item property.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_type='ebook')
            >>> item.item_type
            'ebook'


        :return: Current item type, or None.
        """

    @item_type.setter
    @abc.abstractmethod
    def item_type(self, item_type: Optional[str]) -> None:
        """
        Assign item_type through the canonical item property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.item_type = 'ebook'
            >>> item.item_type
            'ebook'


        :param item_type: New item type, or None.
        :return: None.
        """

    @property
    def location(self) -> Optional[str]:
        """
        Return item_location through the compatibility alias.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_location='/library/book.epub')
            >>> item.location
            '/library/book.epub'


        :return: Current item location, or None.
        """
        return self.item_location

    @location.setter
    def location(self, value: Optional[str]) -> None:
        """
        Assign item_location through the compatibility alias under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.location = '/library/book.epub'
            >>> item.item_location
            '/library/book.epub'


        :param value: New item location, or None.
        :return: None.
        """
        self.item_location = value

    @property
    @abc.abstractmethod
    def item_location(self) -> Optional[str]:
        """
        Return item_location through the canonical item property.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_location='/library/book.epub')
            >>> item.item_location
            '/library/book.epub'


        :return: Current item location, or None.
        """
        ...

    @item_location.setter
    @abc.abstractmethod
    def item_location(self, item_location: Optional[str]) -> None:
        """
        Assign item_location through the canonical item property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.item_location = '/library/book.epub'
            >>> item.item_location
            '/library/book.epub'


        :param item_location: New item location, or None.
        :return: None.
        """
        ...

    @property
    def inventory_code(self) -> Optional[str]:
        """
        Return item_inventory_code through the compatibility alias.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_inventory_code='INV-5')
            >>> item.inventory_code
            'INV-5'


        :return: Current item inventory code, or None.
        """
        return self.item_inventory_code

    @inventory_code.setter
    def inventory_code(self, value: Optional[str]) -> None:
        """
        Assign item_inventory_code through the compatibility alias under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.inventory_code = 'INV-5'
            >>> item.item_inventory_code
            'INV-5'


        :param value: New item inventory code, or None.
        :return: None.
        """
        self.item_inventory_code = value

    @property
    @abc.abstractmethod
    def item_inventory_code(self) -> Optional[str]:
        """
        Return item_inventory_code through the canonical item property.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_inventory_code='INV-5')
            >>> item.item_inventory_code
            'INV-5'


        :return: Current item inventory code, or None.
        """
        ...

    @item_inventory_code.setter
    @abc.abstractmethod
    def item_inventory_code(self, item_inventory_code: Optional[str]) -> None:
        """
        Assign item_inventory_code through the canonical item property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.item_inventory_code = 'INV-5'
            >>> item.item_inventory_code
            'INV-5'


        :param item_inventory_code: New item inventory code, or None.
        :return: None.
        """
        ...

    # ------------------------------------------------------------------
    # Provenance / acquisition
    # ------------------------------------------------------------------

    @property
    def source(self) -> Optional[str]:
        """
        Return item_source through the compatibility alias.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_source='import')
            >>> item.source
            'import'


        :return: Current item source, or None.
        """
        return self.item_source

    @source.setter
    def source(self, value: Optional[str]) -> None:
        """
        Assign item_source through the compatibility alias under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.source = 'import'
            >>> item.item_source
            'import'


        :param value: New item source, or None.
        :return: None.
        """
        self.item_source = value

    @property
    @abc.abstractmethod
    def item_source(self) -> Optional[str]:
        """
        Return item_source through the canonical item property.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_source='import')
            >>> item.item_source
            'import'


        :return: Current item source, or None.
        """
        ...

    @item_source.setter
    @abc.abstractmethod
    def item_source(self, item_source: Optional[str]) -> None:
        """
        Assign item_source through the canonical item property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.item_source = 'import'
            >>> item.item_source
            'import'


        :param item_source: New item source, or None.
        :return: None.
        """
        ...

    @property
    def source_detail(self) -> Optional[str]:
        """
        Return item_source_detail through the compatibility alias.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_source_detail='batch-1')
            >>> item.source_detail
            'batch-1'


        :return: Current item source detail, or None.
        """
        return self.item_source_detail

    @source_detail.setter
    def source_detail(self, value: Optional[str]) -> None:
        """
        Assign item_source_detail through the compatibility alias under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.source_detail = 'batch-1'
            >>> item.item_source_detail
            'batch-1'


        :param value: New item source detail, or None.
        :return: None.
        """
        self.item_source_detail = value

    @property
    @abc.abstractmethod
    def item_source_detail(self) -> Optional[str]:
        """
        Return item_source_detail through the canonical item property.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_source_detail='batch-1')
            >>> item.item_source_detail
            'batch-1'


        :return: Current item source detail, or None.
        """
        ...

    @item_source_detail.setter
    @abc.abstractmethod
    def item_source_detail(self, item_source_detail: Optional[str]) -> None:
        """
        Assign item_source_detail through the canonical item property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.item_source_detail = 'batch-1'
            >>> item.item_source_detail
            'batch-1'


        :param item_source_detail: New item source detail, or None.
        :return: None.
        """
        ...

    @property
    def source_path(self) -> Optional[str]:
        """
        Return item_source_path through the compatibility alias.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_source_path='/incoming/book.epub')
            >>> item.source_path
            '/incoming/book.epub'


        :return: Current item source path, or None.
        """
        return self.item_source_path

    @source_path.setter
    def source_path(self, value: Optional[str]) -> None:
        """
        Assign item_source_path through the compatibility alias under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.source_path = '/incoming/book.epub'
            >>> item.item_source_path
            '/incoming/book.epub'


        :param value: New item source path, or None.
        :return: None.
        """
        self.item_source_path = value

    @property
    @abc.abstractmethod
    def item_source_path(self) -> Optional[str]:
        """
        Return item_source_path through the canonical item property.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_source_path='/incoming/book.epub')
            >>> item.item_source_path
            '/incoming/book.epub'


        :return: Current item source path, or None.
        """
        ...

    @item_source_path.setter
    @abc.abstractmethod
    def item_source_path(self, item_source_path: Optional[str]) -> None:
        """
        Assign item_source_path through the canonical item property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.item_source_path = '/incoming/book.epub'
            >>> item.item_source_path
            '/incoming/book.epub'


        :param item_source_path: New item source path, or None.
        :return: None.
        """
        ...

    @property
    def source_name(self) -> Optional[str]:
        """
        Return item_source_name through the compatibility alias.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_source_name='book.epub')
            >>> item.source_name
            'book.epub'


        :return: Current item source name, or None.
        """
        return self.item_source_name

    @source_name.setter
    def source_name(self, value: Optional[str]) -> None:
        """
        Assign item_source_name through the compatibility alias under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.source_name = 'book.epub'
            >>> item.item_source_name
            'book.epub'


        :param value: New item source name, or None.
        :return: None.
        """
        self.item_source_name = value

    @property
    @abc.abstractmethod
    def item_source_name(self) -> Optional[str]:
        """
        Return item_source_name through the canonical item property.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_source_name='book.epub')
            >>> item.item_source_name
            'book.epub'


        :return: Current item source name, or None.
        """
        ...

    @item_source_name.setter
    @abc.abstractmethod
    def item_source_name(self, item_source_name: Optional[str]) -> None:
        """
        Assign item_source_name through the canonical item property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.item_source_name = 'book.epub'
            >>> item.item_source_name
            'book.epub'


        :param item_source_name: New item source name, or None.
        :return: None.
        """
        ...

    @property
    def acquired_date(self) -> Optional[str]:
        """
        Return item_acquired_date through the compatibility alias.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_acquired_date='2026-01-01')
            >>> item.acquired_date
            '2026-01-01'


        :return: Current item acquired date, or None.
        """
        return self.item_acquired_date

    @acquired_date.setter
    def acquired_date(self, value: Optional[str]) -> None:
        """
        Assign item_acquired_date through the compatibility alias under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.acquired_date = '2026-01-01'
            >>> item.item_acquired_date
            '2026-01-01'


        :param value: New item acquired date, or None.
        :return: None.
        """
        self.item_acquired_date = value

    @property
    @abc.abstractmethod
    def item_acquired_date(self) -> Optional[str]:
        """
        Return item_acquired_date through the canonical item property.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_acquired_date='2026-01-01')
            >>> item.item_acquired_date
            '2026-01-01'


        :return: Current item acquired date, or None.
        """
        ...

    @item_acquired_date.setter
    @abc.abstractmethod
    def item_acquired_date(self, item_acquired_date: Optional[str]) -> None:
        """
        Assign item_acquired_date through the canonical item property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.item_acquired_date = '2026-01-01'
            >>> item.item_acquired_date
            '2026-01-01'


        :param item_acquired_date: New item acquired date, or None.
        :return: None.
        """
        ...

    @property
    def acquired_price_minor(self) -> Optional[int | float]:
        """
        Return item_acquired_price_minor through the compatibility alias.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_acquired_price_minor=999)
            >>> item.acquired_price_minor
            999


        :return: Current item acquired price minor, or None.
        """
        return self.item_acquired_price_minor

    @acquired_price_minor.setter
    def acquired_price_minor(self, value: Optional[int | float]) -> None:
        """
        Assign item_acquired_price_minor through the compatibility alias under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.acquired_price_minor = 999
            >>> item.item_acquired_price_minor
            999


        :param value: New item acquired price minor, or None.
        :return: None.
        """
        self.item_acquired_price_minor = value

    @property
    @abc.abstractmethod
    def item_acquired_price_minor(self) -> Optional[int | float]:
        """
        Return item_acquired_price_minor through the canonical item property.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_acquired_price_minor=999)
            >>> item.item_acquired_price_minor
            999


        :return: Current item acquired price minor, or None.
        """
        ...

    @item_acquired_price_minor.setter
    @abc.abstractmethod
    def item_acquired_price_minor(self, item_acquired_price_minor: Optional[int | float]) -> None:
        """
        Assign item_acquired_price_minor through the canonical item property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.item_acquired_price_minor = 999
            >>> item.item_acquired_price_minor
            999


        :param item_acquired_price_minor: New item acquired price minor, or None.
        :return: None.
        """
        ...

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    @property
    def lifecycle_status(self) -> Optional[str]:
        """
        Return item_lifecycle_status through the compatibility alias.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_lifecycle_status='active')
            >>> item.lifecycle_status
            'active'


        :return: Current item lifecycle status, or None.
        """
        return self.item_lifecycle_status

    @lifecycle_status.setter
    def lifecycle_status(self, value: Optional[str]) -> None:
        """
        Assign item_lifecycle_status through the compatibility alias under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.lifecycle_status = 'active'
            >>> item.item_lifecycle_status
            'active'


        :param value: New item lifecycle status, or None.
        :return: None.
        """
        self.item_lifecycle_status = value

    @property
    @abc.abstractmethod
    def item_lifecycle_status(self) -> Optional[str]:
        """
        Return item_lifecycle_status through the canonical item property.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_lifecycle_status='active')
            >>> item.item_lifecycle_status
            'active'


        :return: Current item lifecycle status, or None.
        """
        ...

    @item_lifecycle_status.setter
    @abc.abstractmethod
    def item_lifecycle_status(self, item_lifecycle_status: Optional[str]) -> None:
        """
        Assign item_lifecycle_status through the canonical item property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.item_lifecycle_status = 'active'
            >>> item.item_lifecycle_status
            'active'


        :param item_lifecycle_status: New item lifecycle status, or None.
        :return: None.
        """
        ...

    @property
    def condition(self) -> Optional[str]:
        """
        Return item_condition through the compatibility alias.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_condition='good')
            >>> item.condition
            'good'


        :return: Current item condition, or None.
        """
        return self.item_condition

    @condition.setter
    def condition(self, value: Optional[str]) -> None:
        """
        Assign item_condition through the compatibility alias under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.condition = 'good'
            >>> item.item_condition
            'good'


        :param value: New item condition, or None.
        :return: None.
        """
        self.item_condition = value

    @property
    @abc.abstractmethod
    def item_condition(self) -> Optional[str]:
        """
        Return item_condition through the canonical item property.

        No conversion, lookup or fallback beyond direct delegation is required by this
        contract.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_condition='good')
            >>> item.item_condition
            'good'


        :return: Current item condition, or None.
        """
        ...

    @item_condition.setter
    @abc.abstractmethod
    def item_condition(self, item_condition: Optional[str]) -> None:
        """
        Assign item_condition through the canonical item property under concrete identity policy.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity()
            >>> item.item_condition = 'good'
            >>> item.item_condition
            'good'


        :param item_condition: New item condition, or None.
        :return: None.
        """
        ...

    def __str__(self) -> str:
        """
        Return a minimal class-name diagnostic when a concrete identity does not override it.

        Example:
            >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
            >>> item = ItemIdentity(item_id=5, item_manifestation_id=4)
            >>> ItemIdentityPropertiesAPI.__str__(item)
            'ItemIdentity()'


        :return: Concrete class name followed by empty parentheses.
        """
        return f"{self.__class__.__name__}()"


class ItemIdentityAPI(ItemIdentityPropertiesAPI):
    """
    Mark a concrete item identity that provides every canonical and compatibility property.

    The marker adds no behavior beyond ItemIdentityPropertiesAPI and serves as the
    public annotation boundary.

    Example:
        >>> from LiuXin_alpha.metadata.containers.metadata_containers.wemi_containers.item_container import ItemIdentity
        >>> item = ItemIdentity(item_id=5, item_manifestation_id=4)
        >>> isinstance(item, ItemIdentityAPI)
        True
    """

__all__ = ["ItemIdentityPropertiesAPI", "ItemIdentityAPI"]
