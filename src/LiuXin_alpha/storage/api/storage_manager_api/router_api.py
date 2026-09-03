"""
Compact byte router above one or more transactional file stores.
"""

from __future__ import annotations

import abc
import io

from collections.abc import Iterator
from typing import BinaryIO

from LiuXin_alpha.storage.api.errors import (
    StoreNotFound,
    StoreUnsupportedOperation,
)
from LiuXin_alpha.storage.api.models import (
    Digest, FileInfo, Location, StoreCapabilities, StoreUUID, StoreStatus, WriteMode,
)
from LiuXin_alpha.storage.api.characteristics_api import StorageCharacteristics
from LiuXin_alpha.storage.api.storage_manager_api.location_api import BoundLocation


class StorageRouterAPI(abc.ABC):
    """
    Small public put/get/stat/delete/list surface over raw stores.

    The router chooses a configured backend from ``Location.store_ref`` while
    preserving the raw store's typed errors and transactional write semantics.

    Example:
        >>> def save(manager: StorageRouterAPI, payload: bytes) -> FileInfo:
        ...     location = Location(UUID(int=1), "objects/42")
        ...     return manager.write_bytes(location, payload)
    """

    @abc.abstractmethod
    def stat(self, location: Location) -> FileInfo:
        """
        Describe one routed object without suppressing backend errors.

        Example:
            >>> info = manager.stat(Location(UUID(int=1), "objects/42"))  # doctest: +SKIP


        :param location:
        :return:
        """
        ...

    @abc.abstractmethod
    def get(
        self, location: Location, *, offset: int = 0, length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Open a routed object as a binary, optionally ranged stream.

        Example:
            >>> stream = manager.get(  # doctest: +SKIP
            ...     Location(UUID(int=1), "objects/42"), offset=10, length=20,
            ... )


        :param location:
        :param offset:
        :param length:
        :param if_version:
        :return:
        """
        ...

    @abc.abstractmethod
    def put(
        self, location: Location, source: BinaryIO, *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
    ) -> FileInfo:
        """
        Stream a staged write to the backend selected by the location.

        Example:
            >>> import io
            >>> info = manager.put(  # doctest: +SKIP
            ...     Location(UUID(int=1), "objects/42"), io.BytesIO(b"book"),
            ...     expected_size=4,
            ... )


        :param location:
        :param source:
        :param mode:
        :param expected_size:
        :param expected_digest:
        :return:
        """
        ...

    @abc.abstractmethod
    def delete(
        self, location: Location, *, missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Delete a routed object with optional idempotence and precondition.

        Supplying ``if_version`` requests conditional deletion from the
        source Store. Unsupported protection raises
        ``StoreUnsupportedOperation`` and a stale version raises
        ``StorePreconditionFailed``.

        Example:
            >>> manager.delete(  # doctest: +SKIP
            ...     Location(UUID(int=1), "objects/42"), if_version="v3",
            ... )


        :param location:
        :param missing_ok:
        :param if_version:
        :return:
        """
        ...

    @abc.abstractmethod
    def iter_locations(
        self, *, store_ref: StoreUUID | None = None,
        prefix: Location | None = None,
    ) -> Iterator[Location]:
        """
        Enumerate concrete locations across one or all configured stores.

        Example:
            >>> locations = list(manager.iter_locations(  # doctest: +SKIP
            ...     store_ref=UUID(int=1),
            ...     prefix=Location(UUID(int=1), "objects/"),
            ... ))


        :param store_ref:
        :param prefix:
        :return:
        """
        ...

    @abc.abstractmethod
    def capabilities(self, store_ref: StoreUUID) -> StoreCapabilities:
        """
        Return the inherent capabilities of one configured store.

        Example:
            >>> capabilities = manager.capabilities(UUID(int=1))  # doctest: +SKIP


        :param store_ref:
        :return:
        """
        ...

    def characteristics(self, store_ref: StoreUUID) -> StorageCharacteristics:
        """
        Return structured constraints for a configured Store when known.

        Minimal routers may retain this unknown-safe default. Full managers
        override it and delegate to the selected Store's optional contract.

        Example:
            >>> manager.characteristics(UUID(int=1)).publication_model  # doctest: +SKIP
            <StoragePublicationModel.UNKNOWN: 'unknown'>


        :param store_ref: Configured Store UUID.
        :return: Structured characteristics or an explicitly unknown profile.
        """

        del store_ref
        return StorageCharacteristics()

    @abc.abstractmethod
    def status(self, store_ref: StoreUUID) -> StoreStatus:
        """
        Return the current operational status of one configured store.

        Example:
            >>> status = manager.status(UUID(int=1))  # doctest: +SKIP


        :param store_ref:
        :return:
        """
        ...

    def bind(self, location: Location) -> BoundLocation:
        """
        Return a short-lived operational facade for one durable Location.

        Binding performs no I/O and caches no backend state.  Routing and
        existence errors surface when an operation is invoked on the returned
        facade.

        Example:
            >>> bound = manager.bind(Location(UUID(int=1), "objects/42"))  # doctest: +SKIP
            >>> bound.location  # doctest: +SKIP
            Location(store_ref=UUID('00000000-0000-0000-0000-000000000001'), key='objects/42')


        :param location:
        :return:
        """

        return BoundLocation(self, location)

    def try_stat(self, location: Location) -> FileInfo | None:
        """
        Return ``None`` only when the routed store reports true absence.

        Example:
            >>> manager.try_stat(Location(UUID(int=1), "missing")) is None  # doctest: +SKIP
            True


        :param location:
        :return:
        """
        try:
            return self.stat(location)
        except StoreNotFound:
            return None

    def exists(self, location: Location) -> bool:
        """
        Test routed existence without masking availability or access errors.

        Example:
            >>> manager.exists(Location(UUID(int=1), "objects/42"))  # doctest: +SKIP
            True


        :param location:
        :return:
        """

        return self.try_stat(location) is not None

    def read_bytes(
        self, location: Location, *, offset: int = 0, length: int | None = None,
        if_version: str | None = None,
    ) -> bytes:
        """
        Read a routed object or range fully into memory.

        Example:
            >>> manager.read_bytes(  # doctest: +SKIP
            ...     Location(UUID(int=1), "objects/42"), length=4,
            ... )
            b'book'


        :param location:
        :param offset:
        :param length:
        :param if_version:
        :return:
        """

        reader = (
            self.get(location, offset=offset, length=length)
            if if_version is None
            else self.get(
                location,
                offset=offset,
                length=length,
                if_version=if_version,
            )
        )
        with reader as source:
            return source.read()

    def write_bytes(
        self, location: Location, data: bytes, *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_digest: Digest | None = None,
    ) -> FileInfo:
        """
        Write a small in-memory payload with an exact size expectation.

        Example:
            >>> info = manager.write_bytes(  # doctest: +SKIP
            ...     Location(UUID(int=1), "objects/42"), b"book",
            ... )


        :param location:
        :param data:
        :param mode:
        :param expected_digest:
        :return:
        """

        return self.put(
            location, io.BytesIO(data), mode=mode, expected_size=len(data),
            expected_digest=expected_digest,
        )

    def copy(
        self,
        source: Location,
        destination: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> FileInfo:
        """
        Copy between Locations using a verified streaming fallback.

        Concrete managers may override this method to select a host-local,
        server-side, or other native transfer after consulting Store topology.
        The public transfer boundary remains Location-based even when the
        selected execution path ultimately calls driver-local operations.

        Example:
            >>> info = manager.copy(  # doctest: +SKIP
            ...     source_location, destination_location,
            ... )


        :param source:
        :param destination:
        :param mode:
        :return:
        """

        source_info = self.stat(source)
        reader = (
            self.get(source, if_version=source_info.version)
            if self.capabilities(source.store_ref).conditional_read
            and source_info.version is not None
            else self.get(source)
        )
        with reader as source_stream:
            return self.put(
                destination,
                source_stream,
                mode=mode,
                expected_size=source_info.size,
                expected_digest=source_info.digest,
            )

    def move(
        self,
        source: Location,
        destination: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
    ) -> FileInfo:
        """
        Copy between Locations, then conditionally delete the source.

        Concrete managers may override this for a topology-aware native move.
        The generic path publishes and verifies the destination before asking
        the source Store to delete the version that was copied, using its
        version precondition. The fallback is unavailable when the source
        Store does not advertise conditional deletion or cannot supply a
        version token; this is checked before destination publication.

        Example:
            >>> info = manager.move(  # doctest: +SKIP
            ...     source_location, destination_location,
            ... )


        :param source:
        :param destination:
        :param mode:
        :return:
        """

        source_info = self.stat(source)
        if not self.capabilities(source.store_ref).conditional_delete:
            raise StoreUnsupportedOperation(
                "safe fallback move requires conditional deletion."
            )
        if source_info.version is None:
            raise StoreUnsupportedOperation(
                "safe fallback move requires a source version for "
                + "conditional deletion."
            )
        result = self.copy(source, destination, mode=mode)
        self.delete(source, if_version=source_info.version)
        return result

    def iter_file_infos(
        self, *, store_ref: StoreUUID | None = None,
        prefix: Location | None = None,
    ) -> Iterator[FileInfo]:
        """
        Enumerate locations and describe each one with ``stat``.

        Example:
            >>> infos = list(manager.iter_file_infos(store_ref=UUID(int=1)))  # doctest: +SKIP


        :param store_ref:
        :param prefix:
        :return:
        """

        for location in self.iter_locations(store_ref=store_ref, prefix=prefix):
            yield self.stat(location)


__all__ = ["StorageRouterAPI"]
