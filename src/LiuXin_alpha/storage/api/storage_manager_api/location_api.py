"""
Bind a passive Location to a live router through a narrow structural contract.

Handles retain references and delegate each operation; they do not own routing
resources, cache metadata, interpret paths, or add publication guarantees.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import BinaryIO, Protocol

from LiuXin_alpha.storage.api.models import (
    Digest,
    FileInfo,
    Location,
    StoreUUID,
    WriteMode,
)


class _StorageRouterLike(Protocol):
    """
    Describe only the manager operations needed by BoundLocation.

    This structural typing seam avoids importing the full manager facade.
    It supplies signatures rather than routing, validation, or runtime protocol checks.

    Example:
        >>> router: _StorageRouterLike = manager  # doctest: +SKIP
    """

    def stat(self, location: Location) -> FileInfo:
        """
        Report current metadata for an addressed object, preserving access errors.

        Example:
            >>> info = router.stat(location)  # doctest: +SKIP


        :param location: Opaque address whose Store UUID selects the route and whose key is interpreted by that Store.
        :return: Current FileInfo supplied by the routed implementation.
        """
        ...

    def try_stat(self, location: Location) -> FileInfo | None:
        """
        Return current metadata or None for concrete object absence. Other route, availability,
        permission, and validation failures remain errors under this contract.

        Example:
            >>> info = router.try_stat(location)  # doctest: +SKIP


        :param location: Opaque address whose Store UUID selects the route and whose key is interpreted by that Store.
        :return: Current FileInfo, or None only when the addressed object is absent.
        """
        ...

    def exists(self, location: Location) -> bool:
        """
        Test concrete object existence without concealing other access failures.

        Example:
            >>> present = router.exists(location)  # doctest: +SKIP


        :param location: Opaque address whose Store UUID selects the route and whose key is interpreted by that Store.
        :return: True for an existing addressed object and False for concrete absence.
        """
        ...

    def get(
        self,
        location: Location,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Open a current routed binary reader, optionally guarded by a version. The caller owns the
        returned stream; the implementation supplies range and precondition checks.

        Example:
            >>> reader = router.get(location, offset=10, length=20)  # doctest: +SKIP


        :param location: Opaque address whose Store UUID selects the route and whose key is interpreted by that Store.
        :param offset: Requested starting byte offset, normally nonnegative; the routed implementation validates the range.
        :param length: Optional requested byte count; None reads to the end of the selected object.
        :param if_version: Optional opaque version precondition; None requests an unconditional read.
        :return: Caller-owned binary reader for the selected object or range.
        """
        ...

    def read_bytes(
        self,
        location: Location,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> bytes:
        """
        Materialize a routed object or range into memory. The implementation owns its temporary
        reader and preserves routing/read failures.

        Example:
            >>> payload = router.read_bytes(location, length=4)  # doctest: +SKIP


        :param location: Opaque address whose Store UUID selects the route and whose key is interpreted by that Store.
        :param offset: Requested starting byte offset, normally nonnegative; the routed implementation validates the range.
        :param length: Optional requested byte count; None reads to the end of the selected object.
        :param if_version: Optional opaque version precondition; None requests an unconditional read.
        :return: Bytes read from the selected object or range.
        """
        ...

    def put(
        self,
        location: Location,
        source: BinaryIO,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
    ) -> FileInfo:
        """
        Publish a streamed payload under the selected collision and validation requirements.
        Destination behavior owns staging and integrity checks; this protocol supplies no
        implementation.

        Example:
            >>> info = router.put(location, source, expected_size=4)  # doctest: +SKIP


        :param location: Opaque address whose Store UUID selects the route and whose key is interpreted by that Store.
        :param source: Borrowed binary input read from its current position; the caller retains ownership.
        :param mode: Destination collision policy, defaulting to CREATE_ONLY.
        :param expected_size: Optional exact expected logical byte count for publication validation.
        :param expected_digest: Optional expected digest for publication validation by the destination.
        :return: FileInfo for the successfully published destination.
        """
        ...

    def write_bytes(
        self,
        location: Location,
        data: bytes,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_digest: Digest | None = None,
    ) -> FileInfo:
        """
        Publish a complete in-memory payload with an exact size expectation. The routed
        implementation owns publication and digest checks.

        Example:
            >>> info = router.write_bytes(location, b"book")  # doctest: +SKIP


        :param location: Opaque address whose Store UUID selects the route and whose key is interpreted by that Store.
        :param data: Complete in-memory byte payload to publish.
        :param mode: Destination collision policy, defaulting to CREATE_ONLY.
        :param expected_digest: Optional expected digest for publication validation by the destination.
        :return: FileInfo for the successfully published destination.
        """
        ...

    def delete(
        self,
        location: Location,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Delete a routed object with explicit absence and version policy. The implementation must
        preserve unsupported-protection and precondition failures.

        Example:
            >>> router.delete(location, if_version="v3")  # doctest: +SKIP


        :param location: Opaque address whose Store UUID selects the route and whose key is interpreted by that Store.
        :param missing_ok: Whether absence at the addressed Store is permitted as a successful no-op.
        :param if_version: Optional opaque version required to match before deletion.
        :return: None after successful deletion or allowed absence.
        """
        ...


@dataclass(slots=True, frozen=True, eq=False)
class BoundLocation:
    """
    Pair a live router with an opaque Location for short-lived operational use.

    Frozen fields retain both references without validating their types; eq=False preserves object-identity equality
    rather than comparing addresses. The manager is omitted from the generated representation.

    The handle stores no size, digest, version, connection, or routing snapshot. Operations delegate
    to the current manager, including its errors and any partial effects. The handle neither
    implements filesystem path navigation nor owns the manager lifetime.

    Example:
        >>> bound = manager.bind(location)  # doctest: +SKIP
        >>> bound.location is location  # doctest: +SKIP
        True


    :ivar _manager: Live router reference used by every operation; not closed by this handle.
    :ivar location: Retained passive address, exposed unchanged to callers.
    """

    _manager: _StorageRouterLike = field(repr=False)
    location: Location

    @property
    def store_ref(self) -> StoreUUID:
        """
        Read the Store UUID from the retained Location without routing or probing it.

        Example:
            >>> bound.store_ref == bound.location.store_ref  # doctest: +SKIP
            True


        :return: Configured Store UUID from location.store_ref.
        """

        return self.location.store_ref

    @property
    def key(self) -> str:
        """
        Read the retained opaque key without parsing, joining, or normalizing it.

        Example:
            >>> bound.key == bound.location.key  # doctest: +SKIP
            True


        :return: Key value from location.key, unchanged.
        """

        return self.location.key

    def stat(self) -> FileInfo:
        """
        Delegate a fresh metadata request for this Location. The handle caches no result and does
        not translate manager errors.

        Example:
            >>> info = bound.stat()  # doctest: +SKIP


        :return: Current FileInfo returned by the manager.
        """

        return self._manager.stat(self.location)

    def try_stat(self) -> FileInfo | None:
        """
        Delegate optional metadata lookup for this Location. The manager owns absence handling; this
        wrapper adds no exception suppression or cache.

        Example:
            >>> info = bound.try_stat()  # doctest: +SKIP


        :return: Manager-provided FileInfo or None for reported concrete absence.
        """

        return self._manager.try_stat(self.location)

    def exists(self) -> bool:
        """
        Delegate an existence check for this Location without retaining the result. Non-absence
        errors remain governed by the manager contract.

        Example:
            >>> present = bound.exists()  # doctest: +SKIP


        :return: Boolean existence result returned by the manager.
        """

        return self._manager.exists(self.location)

    def open_read(
        self,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Return the manager reader for this Location with the requested range/version. A None version
        omits the if_version keyword entirely, allowing ordinary reads through older router
        signatures. No capability or range check occurs in this wrapper, and the caller must close
        the returned reader.

        Example:
            >>> with bound.open_read(offset=10, length=20) as reader:  # doctest: +SKIP
            ...     header = reader.read()


        :param offset: Requested starting byte offset, normally nonnegative; the routed implementation validates the range.
        :param length: Optional requested byte count; None reads to the end of the selected object.
        :param if_version: Optional opaque version precondition; None requests an unconditional read.
        :return: Caller-owned binary reader returned by manager.get.
        """

        if if_version is None:
            return self._manager.get(
                self.location, offset=offset, length=length
            )
        return self._manager.get(
            self.location,
            offset=offset,
            length=length,
            if_version=if_version,
        )

    def read_bytes(
        self,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> bytes:
        """
        Delegate complete materialization of the selected object or range. A None version omits the
        if_version keyword. The manager owns any reader lifetime and allocation; this wrapper adds
        no byte limit or cache.

        Example:
            >>> payload = bound.read_bytes(length=4)  # doctest: +SKIP


        :param offset: Requested starting byte offset, normally nonnegative; the routed implementation validates the range.
        :param length: Optional requested byte count; None reads to the end of the selected object.
        :param if_version: Optional opaque version precondition; None requests an unconditional read.
        :return: Bytes returned by the manager for the selected range.
        """

        if if_version is None:
            return self._manager.read_bytes(
                self.location, offset=offset, length=length
            )
        return self._manager.read_bytes(
            self.location,
            offset=offset,
            length=length,
            if_version=if_version,
        )

    def put(
        self,
        source: BinaryIO,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
    ) -> FileInfo:
        """
        Forward a borrowed stream and publication requirements to the current manager. The wrapper
        adds no staging, size/digest checks, or cleanup of the caller stream. Errors and any
        publication already performed remain visible through the manager behavior.

        Example:
            >>> info = bound.put(source, expected_size=4)  # doctest: +SKIP


        :param source: Borrowed binary input read from its current position; the caller retains ownership.
        :param mode: Destination collision policy, defaulting to CREATE_ONLY.
        :param expected_size: Optional exact expected logical byte count for publication validation.
        :param expected_digest: Optional expected digest for publication validation by the destination.
        :return: FileInfo returned after manager publication succeeds.
        """

        return self._manager.put(
            self.location,
            source,
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
        )

    def write_bytes(
        self,
        data: bytes,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_digest: Digest | None = None,
    ) -> FileInfo:
        """
        Forward an in-memory payload, collision policy, and optional digest requirement. The manager
        constructs the write stream and exact-size expectation; the handle does not duplicate that
        work.

        Example:
            >>> info = bound.write_bytes(b"book")  # doctest: +SKIP


        :param data: Complete in-memory byte payload to publish.
        :param mode: Destination collision policy, defaulting to CREATE_ONLY.
        :param expected_digest: Optional expected digest for publication validation by the destination.
        :return: FileInfo returned by the manager for the published destination.
        """

        return self._manager.write_bytes(
            self.location,
            data,
            mode=mode,
            expected_digest=expected_digest,
        )

    def delete(
        self,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Forward this Location and both deletion policies without interpreting the key. The manager
        owns absence, conditional protection, and any backend errors.

        Example:
            >>> bound.delete(if_version="v3")  # doctest: +SKIP


        :param missing_ok: Whether absence at the addressed Store is permitted as a successful no-op.
        :param if_version: Optional opaque version required to match before deletion.
        :return: None after the manager call succeeds.
        """

        self._manager.delete(
            self.location,
            missing_ok=missing_ok,
            if_version=if_version,
        )


__all__ = ["BoundLocation"]
