"""Configured-store identity facade."""

from __future__ import annotations

import abc

from typing import Protocol, runtime_checkable

from LiuXin_alpha.storage.api2.errors import StoreInvalidLocation, StoreUnsupportedOperation
from LiuXin_alpha.storage.api2.models import Digest, Location, StoreRef


@runtime_checkable
class StoreSpecAPI(Protocol):
    """Store-level view of configured identity and endpoint information.

    Manager-owned specifications may contain policy fields as well; the store
    deliberately depends only on this smaller structural view.

    Example:
        >>> def endpoint(spec: StoreSpecAPI) -> str:
        ...     return spec.store_root_uri
    """

    @property
    def store_id(self) -> int | None:
        """Return the optional durable database identifier.

        Example:
            >>> store_id = spec.store_id  # doctest: +SKIP
        """
        ...

    @property
    def store_name(self) -> str:
        """Return the configured human-readable name.

        Example:
            >>> name = spec.store_name  # doctest: +SKIP
        """
        ...

    @property
    def store_kind(self) -> str:
        """Return the driver or backend kind selected by configuration.

        Example:
            >>> kind = spec.store_kind  # doctest: +SKIP
        """
        ...

    @property
    def store_root_uri(self) -> str:
        """Return the configured root or endpoint URI.

        Example:
            >>> root_uri = spec.store_root_uri  # doctest: +SKIP
        """
        ...

    @property
    def read_only(self) -> bool:
        """Return whether configuration forbids all store mutations.

        Example:
            >>> read_only = spec.read_only  # doctest: +SKIP
        """
        ...


class StoreIdentityAPI(abc.ABC):
    """Identity and location ownership for exactly one configured store.

    The store specification is supplied by the manager-facing configuration
    layer.  Physical backend identity remains an implementation detail for the
    owned ``StoreDriverAPI``.

    Example:
        >>> def display_name(store: StoreIdentityAPI) -> str:
        ...     return store.spec.store_name
    """

    @property
    @abc.abstractmethod
    def spec(self) -> StoreSpecAPI:
        """Return the durable configuration represented by this store.

        Example:
            >>> configured_name = store.spec.store_name  # doctest: +SKIP
        """
        ...

    @property
    def store_ref(self) -> StoreRef:
        """Return the durable id, or the configured name before persistence.

        Example:
            >>> store_ref = store.store_ref  # doctest: +SKIP
        """
        if self.spec.store_id is not None:
            return self.spec.store_id
        return self.spec.store_name

    def owns_location(self, location: Location) -> bool:
        """Return whether a routed location belongs to this configured store.

        Example:
            >>> store.owns_location(Location(store.store_ref, "objects/42"))  # doctest: +SKIP
            True
        """
        return location.store_ref == self.store_ref

    def require_location(self, location: Location) -> Location:
        """Return an owned location or raise ``StoreInvalidLocation``.

        Store implementations should call this before passing ``location.key``
        to a low-level driver.

        Example:
            >>> owned = store.require_location(  # doctest: +SKIP
            ...     Location(store.store_ref, "objects/42"),
            ... )
        """
        if not self.owns_location(location):
            raise StoreInvalidLocation(
                f"location belongs to store {location.store_ref!r}, "
                f"not {self.store_ref!r}."
            )
        return location

    @abc.abstractmethod
    def location(self, *tokens: str) -> Location:
        """Build a location using the owned driver's key-joining semantics.

        Example:
            >>> location = store.location("authors", "book.epub")  # doctest: +SKIP
        """
        ...

    def locate(self, identifier: str | Location) -> Location:
        """Resolve a persisted key or validate an existing routed location.

        Example:
            >>> location = store.locate("authors/book.epub")  # doctest: +SKIP
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
    ) -> Location:
        """Allocate a driver-selected location when inherently supported.

        Writable store implementations override this method by delegating to
        ``StoreDriverAPI.allocate_key``.  This replaces unsafe legacy writes
        whose implicit destination was hidden inside ``write_bytes``.

        Example:
            >>> location = store.allocate_location(  # doctest: +SKIP
            ...     expected_size=4, name_hint="book.epub",
            ... )
        """
        raise StoreUnsupportedOperation(
            f"{type(self).__name__} does not support driver-selected locations."
        )


__all__ = ["StoreIdentityAPI", "StoreSpecAPI"]
