"""
Expose configured Store identity and keep backend addressing below Location routing.

The configuration protocol is the small view needed by a Store, not the whole
manager policy record. Ownership helpers compare durable Store UUIDs; backend
key syntax, canonical form, and physical existence remain separate checks.
Optional URI conversion and target allocation default to unsupported behavior.

Example:
    >>> location = store.locate("incoming/book.epub")  # doctest: +SKIP
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

    Manager-owned configurations may contain policy fields as well; the store deliberately depends
    only on this smaller structural view.

    Runtime protocol membership checks structural attributes, not the validity of a configuration or
    whether a concrete Store enforces its read-only policy.

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


        :return: Stable UUID used as the Store component of durable Locations.
        """
        ...

    @property
    def store_name(self) -> str:
        """
        Return the configured human-readable name.

        Example:
            >>> name = configuration.store_name  # doctest: +SKIP


        :return: Configured display name, rather than an identifier for Location routing.
        """
        ...

    @property
    def store_kind(self) -> str:
        """
        Return the driver or backend kind selected by configuration.

        Example:
            >>> kind = configuration.store_kind  # doctest: +SKIP


        :return: Configured backend/driver kind used when constructing the Store.
        """
        ...

    @property
    def store_root_uri(self) -> str:
        """
        Return the configured root or endpoint URI.

        Example:
            >>> root_uri = configuration.store_root_uri  # doctest: +SKIP


        :return: Configured endpoint or root URI; this protocol does not normalize or redact it.
        """
        ...

    @property
    def read_only(self) -> bool:
        """
        Return whether configuration forbids all store mutations.

        Example:
            >>> read_only = configuration.read_only  # doctest: +SKIP


        :return: Whether configuration prohibits mutations regardless of backend write support.
        """
        ...


class StoreIdentityAPI(abc.ABC):
    """
    Identity and location ownership for exactly one configured store.

    Store configuration is supplied by the manager-facing configuration layer.  Physical backend
    identity remains an implementation detail for the owned ``StorageDriverAPI``.

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


        :return: Manager-supplied configuration view for this one durable Store identity.
        """
        ...

    @property
    def store_ref(self) -> StoreUUID:
        """
        Return the configured store's durable UUID.

        Example:
            >>> store_ref = store.store_ref  # doctest: +SKIP


        :return: configuration.store_uuid as supplied, without generating a new identity.
        """
        return self.configuration.store_uuid

    def owns_location(self, location: Location) -> bool:
        """
        Return whether a routed location belongs to this configured store.

        This helper reads the two store_ref attributes directly; it does not perform a runtime
        Location type check or parse the key.

        Example:
            >>> store.owns_location(Location(store.store_ref, "objects/42"))  # doctest: +SKIP
            True


        :param location: Routed Location whose Store UUID is compared with this configuration.
        :return: Whether the UUIDs compare equal; key validity and physical existence are not inspected.
        """
        return location.store_ref == self.store_ref

    def require_location(self, location: Location) -> Location:
        """
        Return an owned location or raise ``StoreInvalidLocation``.

        Store implementations should call this before passing ``location.key`` to a low-level
        driver.

        The returned value is not copied or canonicalized. Configuration access and ownership-check
        failures propagate.

        Example:
            >>> owned = store.require_location(  # doctest: +SKIP
            ...     Location(store.store_ref, "objects/42"),
            ... )


        :param location: Location that must name this configured Store before backend dispatch.
        :return: The same Location object when owned; a foreign Store UUID raises StoreInvalidLocation.
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


        :param tokens: Backend-specific key components joined by the concrete Store.
        :return: Location scoped to this Store, without an implied existence check or byte publication.
        """
        ...

    def locate(self, identifier: str | Location) -> Location:
        """
        Resolve a persisted key or validate an existing routed location.

        Only actual Location instances take the ownership branch. The fallback performs no
        additional coercion or URI detection before invoking location.

        Example:
            >>> location = store.locate("authors/book.epub")  # doctest: +SKIP


        :param identifier: Existing Location to check or persisted key passed as one location token.
        :return: Owned existing Location or the concrete location builder result.
        """
        if isinstance(identifier, Location):
            return self.require_location(identifier)
        return self.location(identifier)

    def location_from_uri(self, uri: str) -> Location:
        """
        Resolve an external URI owned by this Store when supported.

        The Store validates that the URI belongs to its configured endpoint; callers must not strip
        roots or parse backend address syntax themselves.

        The default performs no parsing or endpoint access. Unsupported URI handling remains
        distinct from a missing object.

        Example:
            >>> location = store.location_from_uri(  # doctest: +SKIP
            ...     "s3://library/books/book.epub",
            ... )


        :param uri: External object URI whose endpoint ownership a supporting Store must validate.
        :return: Owned Location in an override; this default always raises StoreUnsupportedOperation.
        """

        _ = uri
        raise StoreUnsupportedOperation(
            f"{type(self).__name__} does not resolve external object URIs."
        )

    def location_uri(self, location: Location) -> str | None:
        """
        Return a credential-free external URI for an owned Location, if any.

        ``None`` means that the Store cannot safely or canonically render one. Generic code should
        preserve the opaque Location regardless.

        Example:
            >>> uri = store.location_uri(location)  # doctest: +SKIP


        :param location: Owned Location whose external representation is requested.
        :return: None after ownership validation in this default; supporting overrides may return a credential-free URI.
        """

        _ = self.require_location(location)
        return None

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
        ``ObjectAddressAllocatorStorageDriverAPI.allocate_object_address``. This replaces unsafe
        legacy writes whose implicit destination was hidden inside ``write_bytes``.
        ``placement_hints`` is advisory library metadata. Rich Stores may use it to choose a
        meaningful layout; ordinary Stores may ignore it.

        Allocation selects a target, not a completed write. Reservation or exclusivity requires a
        concrete implementation contract; this default performs no validation or allocation.

        Example:
            >>> location = store.allocate_location(  # doctest: +SKIP
            ...     expected_size=4, name_hint="book.epub",
            ...     placement_hints=ItemStorageHints(work_id=5),
            ... )


        :param expected_size: Optional expected object length in bytes supplied to allocation policy.
        :param expected_digest: Optional expected content digest supplied to allocation policy.
        :param name_hint: Optional suggested object name rather than an explicit destination key.
        :param placement_hints: Optional advisory library metadata; ordinary Stores may ignore it.
        :return: Driver-selected Location in a supporting override; this default raises StoreUnsupportedOperation.
        """
        raise StoreUnsupportedOperation(
            f"{type(self).__name__} does not support driver-selected locations."
        )


__all__ = ["StoreConfigurationAPI", "StoreIdentityAPI"]
