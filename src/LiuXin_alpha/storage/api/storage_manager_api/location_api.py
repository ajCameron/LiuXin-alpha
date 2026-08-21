"""
Short-lived operational facade for an opaque storage Location.
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
    Operations used by ``BoundLocation`` without importing the facade.

    Example:
        >>> def accepts_router(router: _StorageRouterLike) -> None:
        ...     pass
    """

    def stat(self, location: Location) -> FileInfo:
        """
        Describe one Location.

        Example:
            >>> info = router.stat(location)  # doctest: +SKIP


        :param location:
        :return:
        """
        ...

    def try_stat(self, location: Location) -> FileInfo | None:
        """
        Describe one Location or return ``None`` for absence.

        Example:
            >>> info = router.try_stat(location)  # doctest: +SKIP


        :param location:
        :return:
        """
        ...

    def exists(self, location: Location) -> bool:
        """
        Test concrete existence without suppressing other failures.

        Example:
            >>> present = router.exists(location)  # doctest: +SKIP


        :param location:
        :return:
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
        Open a routed binary stream.

        Example:
            >>> source = router.get(location)  # doctest: +SKIP


        :param location:
        :param offset:
        :param length:
        :param if_version:
        :return:
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
        Read a routed object into memory.

        Example:
            >>> payload = router.read_bytes(location)  # doctest: +SKIP


        :param location:
        :param offset:
        :param length:
        :param if_version:
        :return:
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
        Publish a streamed write.

        Example:
            >>> info = router.put(location, source)  # doctest: +SKIP


        :param location:
        :param source:
        :param mode:
        :param expected_size:
        :param expected_digest:
        :return:
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
        Publish an in-memory payload.

        Example:
            >>> info = router.write_bytes(location, b"book")  # doctest: +SKIP


        :param location:
        :param data:
        :param mode:
        :param expected_digest:
        :return:
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
        Delete one routed object.

        Example:
            >>> router.delete(location)  # doctest: +SKIP


        :param location:
        :param missing_ok:
        :param if_version:
        :return:
        """
        ...


@dataclass(slots=True, frozen=True, eq=False)
class BoundLocation:
    """
    Short-lived operational handle pairing a manager with a Location.

    The durable identity remains the immutable ``location`` value.  This facade
    contains no cached size, digest, version, status, connection, or path state;
    every operation delegates to the manager so current routing, policy, typed
    errors, and transactional semantics remain authoritative.

    ``BoundLocation`` is intentionally not path-like.  In particular, it does
    not join or parse keys, expose parents, or implement ``os.PathLike``.

    Example:
        >>> from uuid import UUID
        >>> location = Location(UUID(int=1), "objects/42")
        >>> bound = manager.bind(location)  # doctest: +SKIP
        >>> bound.location == location  # doctest: +SKIP
        True
    """

    _manager: _StorageRouterLike = field(repr=False)
    location: Location

    @property
    def store_ref(self) -> StoreUUID:
        """
        Return the configured Store UUID from the durable Location.

        Example:
            >>> bound.store_ref  # doctest: +SKIP
            UUID('00000000-0000-0000-0000-000000000001')


        :return:
        """

        return self.location.store_ref

    @property
    def key(self) -> str:
        """
        Return the opaque backend key without interpreting it.

        Example:
            >>> bound.key  # doctest: +SKIP
            'objects/42'


        :return:
        """

        return self.location.key

    def stat(self) -> FileInfo:
        """
        Fetch fresh information through the bound manager.

        No result is cached on the handle.

        Example:
            >>> info = bound.stat()  # doctest: +SKIP


        :return:
        """

        return self._manager.stat(self.location)

    def try_stat(self) -> FileInfo | None:
        """
        Return fresh information or ``None`` only for genuine absence.

        Availability, permission, and connection errors remain visible.

        Example:
            >>> info = bound.try_stat()  # doctest: +SKIP


        :return:
        """

        return self._manager.try_stat(self.location)

    def exists(self) -> bool:
        """
        Test current existence without concealing non-absence failures.

        Example:
            >>> present = bound.exists()  # doctest: +SKIP


        :return:
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
        Open a current binary read stream, optionally range-limited.

        Example:
            >>> with bound.open_read(offset=10, length=20) as source:  # doctest: +SKIP
            ...     header = source.read()


        :param offset:
        :param length:
        :param if_version:
        :return:
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
        Read the current object or range fully into memory.

        Example:
            >>> payload = bound.read_bytes(length=4)  # doctest: +SKIP


        :param offset:
        :param length:
        :param if_version:
        :return:
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
        Publish a streamed write through the manager's transactional route.

        ``CREATE_ONLY`` remains the safe default; replacement must be explicit.

        Example:
            >>> import io
            >>> info = bound.put(  # doctest: +SKIP
            ...     io.BytesIO(b"book"), expected_size=4,
            ... )


        :param source:
        :param mode:
        :param expected_size:
        :param expected_digest:
        :return:
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
        Publish a small in-memory payload through the manager.

        Example:
            >>> info = bound.write_bytes(b"book")  # doctest: +SKIP


        :param data:
        :param mode:
        :param expected_digest:
        :return:
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
        Delete through the manager with optional idempotence and protection.

        Example:
            >>> bound.delete(if_version="v3")  # doctest: +SKIP


        :param missing_ok:
        :param if_version:
        :return:
        """

        self._manager.delete(
            self.location,
            missing_ok=missing_ok,
            if_version=if_version,
        )


__all__ = ["BoundLocation"]
