"""
Configured-store identity facade.
"""

from __future__ import annotations

import abc

from typing import Protocol, runtime_checkable

from LiuXin_alpha.storage.api.errors import StoreInvalidLocation, StoreUnsupportedOperation
from LiuXin_alpha.storage.api.models import Digest, Location, StoreUUID
from LiuXin_alpha.storage.api.placement_hints_api import StoragePlacementHints


@runtime_checkable
class StoreConfigurationAPI(Protocol):
    """
    Store-level view of configured identity and endpoint information.

    Manager-owned configurations may contain policy fields as well; the store
    deliberately depends only on this smaller structural view.

    Example:
        >>> def endpoint(configuration: StoreConfigurationAPI) -> str:
        ...     return configuration.store_root_uri
    """

    @property
    def store_uuid(self) -> StoreUUID:
        """
        Return the stable UUID used in durable Locations.

        Example:
            >>> store_uuid = configuration.store_uuid  # doctest: +SKIP


        :return:
        """
        ...

    @property
    def store_name(self) -> str:
        """
        Return the configured human-readable name.

        Example:
            >>> name = configuration.store_name  # doctest: +SKIP


        :return:
        """
        ...

    @property
    def store_kind(self) -> str:
        """
        Return the driver or backend kind selected by configuration.

        Example:
            >>> kind = configuration.store_kind  # doctest: +SKIP


        :return:
        """
        ...

    @property
    def store_root_uri(self) -> str:
        """
        Return the configured root or endpoint URI.

        Example:
            >>> root_uri = configuration.store_root_uri  # doctest: +SKIP


        :return:
        """
        ...

    @property
    def read_only(self) -> bool:
        """
        Return whether configuration forbids all store mutations.

        Example:
            >>> read_only = configuration.read_only  # doctest: +SKIP


        :return:
        """
        ...


class StoreIdentityAPI(abc.ABC):
    """
    Identity and location ownership for exactly one configured store.

    Store configuration is supplied by the manager-facing configuration
    layer.  Physical backend identity remains an implementation detail for the
    owned ``StorageDriverAPI``.

    Example:
        >>> def display_name(store: StoreIdentityAPI) -> str:
        ...     return store.configuration.store_name
    """

    @property
    @abc.abstractmethod
    def configuration(self) -> StoreConfigurationAPI:
        """
        Return the durable configuration represented by this store.

        Example:
            >>> configured_name = store.configuration.store_name  # doctest: +SKIP


        :return:
        """
        ...

    @property
    def store_ref(self) -> StoreUUID:
        """
        Return the configured store's durable UUID.

        Example:
            >>> store_ref = store.store_ref  # doctest: +SKIP


        :return:
        """
        return self.configuration.store_uuid

    def owns_location(self, location: Location) -> bool:
        """
        Return whether a routed location belongs to this configured store.

        Example:
            >>> store.owns_location(Location(store.store_ref, "objects/42"))  # doctest: +SKIP
            True


        :param location:
        :return:
        """
        return location.store_ref == self.store_ref

    def require_location(self, location: Location) -> Location:
        """
        Return an owned location or raise ``StoreInvalidLocation``.

        Store implementations should call this before passing ``location.key``
        to a low-level driver.

        Example:
            >>> owned = store.require_location(  # doctest: +SKIP
            ...     Location(store.store_ref, "objects/42"),
            ... )


        :param location:
        :return:
        """
        if not self.owns_location(location):
            raise StoreInvalidLocation(
                f"location belongs to store {location.store_ref!r}, "
                f"not {self.store_ref!r}."
            )
        return location

    @abc.abstractmethod
    def location(self, *tokens: str) -> Location:
        """
        Build a location using the owned driver's key-joining semantics.

        Example:
            >>> location = store.location("authors", "book.epub")  # doctest: +SKIP


        :param tokens:
        :return:
        """
        ...

    def locate(self, identifier: str | Location) -> Location:
        """
        Resolve a persisted key or validate an existing routed location.

        Example:
            >>> location = store.locate("authors/book.epub")  # doctest: +SKIP


        :param identifier:
        :return:
        """
        if isinstance(identifier, Location):
            return self.require_location(identifier)
        return self.location(identifier)

    def allocate_location(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        name_hint: str | None = None,
        placement_hints: StoragePlacementHints | None = None,
    ) -> Location:
        """
        Allocate a driver-selected location when inherently supported.

        Writable store implementations override this method by delegating to
        ``ObjectAddressAllocatorStorageDriverAPI.allocate_object_address``.
        This replaces unsafe legacy
        writes whose implicit destination was hidden inside ``write_bytes``.
        ``placement_hints`` is advisory library metadata. Rich Stores may use
        it to choose a meaningful layout; ordinary Stores may ignore it.

        Example:
            >>> location = store.allocate_location(  # doctest: +SKIP
            ...     expected_size=4, name_hint="book.epub",
            ...     placement_hints=ItemStorageHints(work_id=5),
            ... )


        :param expected_size:
        :param expected_digest:
        :param name_hint:
        :param placement_hints:
        :return:
        """
        raise StoreUnsupportedOperation(
            f"{type(self).__name__} does not support driver-selected locations."
        )


__all__ = ["StoreConfigurationAPI", "StoreIdentityAPI"]
