"""
Separate driver-relative address parsing, runtime ownership, and external URI handling.

``StorageDriverObjectAddressContract`` states the pure abstract contract.
``StorageDriverObjectAddressAPI`` preserves the historical public API and its shared
defaults, while ``StorageDriverObjectAddressBase`` remains the explicitly named base
used by composed drivers. External URI conversion remains distinct from persisted
opaque parsing.

Example:
    >>> checked = driver.require_canonical_object_address(address)  # doctest: +SKIP
"""

from __future__ import annotations

import abc

from typing import Generic

from LiuXin_alpha.storage.api.errors import (
    StorageIntegrityError,
    StorageUnsupportedOperation,
)
from LiuXin_alpha.storage.api.store_driver_api.models import (
    DriverObjectAddressCheckerAPI,
    DriverObjectAddressInput,
    DriverObjectAddressT,
)


class StorageDriverObjectAddressContract(Generic[DriverObjectAddressT], abc.ABC):
    """
    Parse opaque persisted addresses for one configured driver endpoint.

    Parsing a stored driver-relative value is deliberately separate from resolving an external URI.
    This prevents URI credentials, host syntax, or endpoint aliases from silently becoming persisted
    object addresses.

    Example:
        >>> address = driver.parse_object_address("incoming/book.epub")  # doctest: +SKIP
    """

    @property
    @abc.abstractmethod
    def object_address_checker(
        self,
    ) -> DriverObjectAddressCheckerAPI[DriverObjectAddressT]:
        """
        Return the injected runtime checker for this driver instance.

        Example:
            >>> checker = driver.object_address_checker  # doctest: +SKIP


        :return: Callable responsible for validating this driver concrete address type and endpoint scope.
        """
        ...

    @abc.abstractmethod
    def check_object_address(
        self,
        object_address: DriverObjectAddressT,
    ) -> DriverObjectAddressT:
        """
        Validate an address's concrete type and configured address space.

        Validation is delegated entirely to the injected callable, so the checker defines the actual
        runtime checks.

        Example:
            >>> checked = driver.check_object_address(address)  # doctest: +SKIP


        :param object_address: Typed object address expected to belong to this configured driver.
        :return: Checker result; this wrapper adds no parsing, canonicalization, or I/O.
        """
        ...

    @abc.abstractmethod
    def require_canonical_object_address(
        self,
        object_address: DriverObjectAddressT,
    ) -> DriverObjectAddressT:
        """
        Require stable scoped serialization for a produced address.

        Call the checker, stringify its result, and compare with parse_object_address.
        Parser/checker errors propagate; this does not probe object existence or inspect bytes.

        Example:
            >>> address = driver.require_canonical_object_address(address)  # doctest: +SKIP


        :param object_address: Typed object address expected to belong to this configured driver.
        :return: Original checked address when reparsing its string compares equal; otherwise raise StorageIntegrityError.
        """
        ...

    @property
    @abc.abstractmethod
    def driver_kind(self) -> str:
        """
        Return the concrete class name as the default driver kind.

        Example:
            >>> kind = driver.driver_kind  # doctest: +SKIP


        :return: Concrete Python class name, unless overridden by a driver.
        """
        ...

    @property
    @abc.abstractmethod
    def root_uri(self) -> str:
        """
        Return a credential-free URI identifying the configured endpoint.

        The value is suitable for logs and configuration displays. Drivers must remove passwords,
        access tokens, and equivalent secrets.

        Example:
            >>> root_uri = driver.root_uri  # doctest: +SKIP


        :return: Display-safe endpoint URI with credentials removed by the concrete implementation.
        """
        ...

    @abc.abstractmethod
    def suggest_endpoint_name(self) -> str:
        """
        Suggest a human-readable name for this configured endpoint.

        Example:
            >>> suggested = driver.suggest_endpoint_name()  # doctest: +SKIP


        :return: Current driver_kind value without further sanitization or uniqueness checks.
        """
        ...

    @abc.abstractmethod
    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[DriverObjectAddressT],
    ) -> DriverObjectAddressT:
        """
        Canonicalize a typed address or persisted driver-relative value.

        Existing typed values must pass ``check_object_address``. Strings are not URIs;
        implementations mint their concrete, scoped address type. Parsing is canonical and stable:
        ``parse_object_address(str(address)) == address`` for every address produced by this driver.

        Example:
            >>> address = driver.parse_object_address("objects/42")  # doctest: +SKIP


        :param identifier: Existing typed address or persisted driver-relative string, distinct from an external URI.
        :return: Canonical concrete address scoped to this driver; invalid input raises the appropriate typed error.
        """
        ...

    @abc.abstractmethod
    def object_address_from_uri(self, uri: str) -> DriverObjectAddressT:
        """
        Resolve an endpoint-owned external URI when explicitly supported.

        Implementations require ``capabilities.external_uri_parsing``. The result must be canonical,
        checked, scoped to this driver instance, and must reject URIs outside the configured
        endpoint.

        The default rejects every request without parsing the supplied URI.

        Example:
            >>> address = driver.object_address_from_uri(  # doctest: +SKIP
            ...     "ftp://example.test/incoming/book.epub",
            ... )


        :param uri: External URI a supporting implementation must validate against its configured endpoint.
        :return: No result from the default implementation; it raises StorageUnsupportedOperation.
        """
        ...

    @abc.abstractmethod
    def object_uri(self, object_address: DriverObjectAddressT) -> str | None:
        """
        Return a credential-free external URI, or ``None`` if unavailable.

        Non-``None`` results require ``capabilities.external_uri_rendering`` and must not expose
        credentials or other secrets.

        The default performs only check_object_address and returns None; it neither reparses nor
        formats an address.

        Example:
            >>> uri = driver.object_uri(address)  # doctest: +SKIP


        :param object_address: Typed object address expected to belong to this configured driver.
        :return: None by default after checking ownership; supporting overrides return a credential-free URI.
        """
        ...


class StorageDriverObjectAddressAPI(
    StorageDriverObjectAddressContract[DriverObjectAddressT],
    Generic[DriverObjectAddressT],
    abc.ABC,
):
    """
    Preserve the public address API with endpoint-neutral checks and conservative URI defaults.

    Historically, direct subclasses of ``StorageDriverObjectAddressAPI`` only had to provide their
    checker, root URI, and parser. Keeping the defaults on this public compatibility class avoids
    making those third-party subclasses abstract after the pure contract was separated. New code
    that needs a declaration-only interface can inherit ``StorageDriverObjectAddressContract``;
    composed drivers normally inherit ``StorageDriverObjectAddressBase``. This class performs no
    object I/O and owns no endpoint resources.

    Example:
        >>> checked = driver.require_canonical_object_address(address)  # doctest: +SKIP
    """

    def check_object_address(
        self,
        object_address: DriverObjectAddressT,
    ) -> DriverObjectAddressT:
        """
        Delegate concrete-type and address-space validation to the endpoint checker.

        Example:
            >>> checked = driver.check_object_address(address)  # doctest: +SKIP

        :param object_address: Typed address expected to belong to this configured endpoint.
        :return: The checker result without parsing or physical I/O.
        """
        return self.object_address_checker(object_address)

    def require_canonical_object_address(
        self,
        object_address: DriverObjectAddressT,
    ) -> DriverObjectAddressT:
        """
        Require an owned address to round-trip through persisted-string parsing.

        Example:
            >>> checked = driver.require_canonical_object_address(address)  # doctest: +SKIP

        :param object_address: Typed address checked for ownership and stable serialization.
        :return: Checked address when reparsing its string produces an equal value.
        """
        checked = self.check_object_address(object_address)
        reparsed = self.parse_object_address(str(checked))
        if reparsed != checked:
            raise StorageIntegrityError(
                "driver object address does not round-trip canonically."
            )
        return checked

    @property
    def driver_kind(self) -> str:
        """
        Return the concrete class name as the default driver-kind label.

        Example:
            >>> driver.driver_kind == type(driver).__name__  # doctest: +SKIP
            True

        :return: Concrete Python class name unless a subclass overrides the label.
        """
        return type(self).__name__

    def suggest_endpoint_name(self) -> str:
        """
        Use the driver-kind label as the default endpoint display name.

        Example:
            >>> driver.suggest_endpoint_name() == driver.driver_kind  # doctest: +SKIP
            True

        :return: Current driver-kind label without uniqueness or sanitization changes.
        """
        return self.driver_kind

    def object_address_from_uri(self, uri: str) -> DriverObjectAddressT:
        """
        Reject external URI resolution unless a concrete driver implements it.

        Example:
            >>> driver.object_address_from_uri("scheme://endpoint/object")  # doctest: +SKIP

        :param uri: External URI left uninterpreted by this conservative default.
        :return: No result; the default always raises StorageUnsupportedOperation.
        """
        _ = uri
        raise StorageUnsupportedOperation(
            f"{self.driver_kind} does not resolve external object URIs."
        )

    def object_uri(self, object_address: DriverObjectAddressT) -> str | None:
        """
        Check endpoint ownership and report that no external URI is available by default.

        Example:
            >>> driver.object_uri(address) is None  # doctest: +SKIP
            True

        :param object_address: Typed address checked against this configured endpoint.
        :return: None after successful ownership validation.
        """
        _ = self.check_object_address(object_address)
        return None


class StorageDriverObjectAddressBase(
    StorageDriverObjectAddressAPI[DriverObjectAddressT],
    Generic[DriverObjectAddressT],
    abc.ABC,
):
    """
    Name the reusable compatibility implementation used by complete composed drivers.

    The implementation is inherited unchanged from ``StorageDriverObjectAddressAPI``. This thin
    class keeps architectural intent visible in composed MROs while the older public API remains a
    safe direct-subclassing surface.

    Example:
        >>> issubclass(StorageDriverObjectAddressBase, StorageDriverObjectAddressAPI)
        True
    """


__all__ = [
    "StorageDriverObjectAddressContract",
    "StorageDriverObjectAddressAPI",
    "StorageDriverObjectAddressBase",
]
