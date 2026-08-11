"""Compact byte router above one or more transactional file stores."""

from __future__ import annotations

import abc
import io

from collections.abc import Iterator
from typing import BinaryIO

from LiuXin_alpha.storage.api2.errors import StoreNotFound
from LiuXin_alpha.storage.api2.models import (
    Digest, FileInfo, Location, StoreCapabilities, StoreRef, StoreStatus, WriteMode,
)
from LiuXin_alpha.storage.api2.storage_manager_api.location_api import BoundLocation


class StorageRouterAPI(abc.ABC):
    """Small public put/get/stat/delete/list surface over raw stores.

    The router chooses a configured backend from ``Location.store_ref`` while
    preserving the raw store's typed errors and transactional write semantics.

    Example:
        >>> def save(manager: StorageRouterAPI, payload: bytes) -> FileInfo:
        ...     location = Location("primary", "objects/42")
        ...     return manager.write_bytes(location, payload)
    """

    @abc.abstractmethod
    def stat(self, location: Location) -> FileInfo:
        """Describe one routed object without suppressing backend errors.

        Example:
            >>> info = manager.stat(Location("primary", "objects/42"))  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def get(
        self, location: Location, *, offset: int = 0, length: int | None = None,
    ) -> BinaryIO:
        """Open a routed object as a binary, optionally ranged stream.

        Example:
            >>> stream = manager.get(  # doctest: +SKIP
            ...     Location("primary", "objects/42"), offset=10, length=20,
            ... )
        """
        ...

    @abc.abstractmethod
    def put(
        self, location: Location, source: BinaryIO, *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
    ) -> FileInfo:
        """Stream a staged write to the backend selected by the location.

        Example:
            >>> import io
            >>> info = manager.put(  # doctest: +SKIP
            ...     Location("primary", "objects/42"), io.BytesIO(b"book"),
            ...     expected_size=4,
            ... )
        """
        ...

    @abc.abstractmethod
    def delete(
        self, location: Location, *, missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """Delete a routed object with optional idempotence and precondition.

        Example:
            >>> manager.delete(  # doctest: +SKIP
            ...     Location("primary", "objects/42"), if_version="v3",
            ... )
        """
        ...

    @abc.abstractmethod
    def iter_locations(
        self, *, store_ref: StoreRef | None = None,
        prefix: Location | None = None,
    ) -> Iterator[Location]:
        """Enumerate concrete locations across one or all configured stores.

        Example:
            >>> locations = list(manager.iter_locations(  # doctest: +SKIP
            ...     store_ref="primary",
            ...     prefix=Location("primary", "objects/"),
            ... ))
        """
        ...

    @abc.abstractmethod
    def capabilities(self, store_ref: StoreRef) -> StoreCapabilities:
        """Return the inherent capabilities of one configured store.

        Example:
            >>> capabilities = manager.capabilities("primary")  # doctest: +SKIP
        """
        ...

    @abc.abstractmethod
    def status(self, store_ref: StoreRef) -> StoreStatus:
        """Return the current operational status of one configured store.

        Example:
            >>> status = manager.status("primary")  # doctest: +SKIP
        """
        ...

    def bind(self, location: Location) -> BoundLocation:
        """Return a short-lived operational facade for one durable Location.

        Binding performs no I/O and caches no backend state.  Routing and
        existence errors surface when an operation is invoked on the returned
        facade.

        Example:
            >>> bound = manager.bind(Location("primary", "objects/42"))  # doctest: +SKIP
            >>> bound.location  # doctest: +SKIP
            Location(store_ref='primary', key='objects/42')
        """

        return BoundLocation(self, location)

    def try_stat(self, location: Location) -> FileInfo | None:
        """Return ``None`` only when the routed store reports true absence.

        Example:
            >>> manager.try_stat(Location("primary", "missing")) is None  # doctest: +SKIP
            True
        """
        try:
            return self.stat(location)
        except StoreNotFound:
            return None

    def exists(self, location: Location) -> bool:
        """Test routed existence without masking availability or access errors.

        Example:
            >>> manager.exists(Location("primary", "objects/42"))  # doctest: +SKIP
            True
        """

        return self.try_stat(location) is not None

    def read_bytes(
        self, location: Location, *, offset: int = 0, length: int | None = None,
    ) -> bytes:
        """Read a routed object or range fully into memory.

        Example:
            >>> manager.read_bytes(  # doctest: +SKIP
            ...     Location("primary", "objects/42"), length=4,
            ... )
            b'book'
        """

        with self.get(location, offset=offset, length=length) as source:
            return source.read()

    def write_bytes(
        self, location: Location, data: bytes, *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_digest: Digest | None = None,
    ) -> FileInfo:
        """Write a small in-memory payload with an exact size expectation.

        Example:
            >>> info = manager.write_bytes(  # doctest: +SKIP
            ...     Location("primary", "objects/42"), b"book",
            ... )
        """

        return self.put(
            location, io.BytesIO(data), mode=mode, expected_size=len(data),
            expected_digest=expected_digest,
        )

    def iter_infos(
        self, *, store_ref: StoreRef | None = None,
        prefix: Location | None = None,
    ) -> Iterator[FileInfo]:
        """Enumerate locations and describe each one with ``stat``.

        Example:
            >>> infos = list(manager.iter_infos(store_ref="primary"))  # doctest: +SKIP
        """

        for location in self.iter_locations(store_ref=store_ref, prefix=prefix):
            yield self.stat(location)


__all__ = ["StorageRouterAPI"]
