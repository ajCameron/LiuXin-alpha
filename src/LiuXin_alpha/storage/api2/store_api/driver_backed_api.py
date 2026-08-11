"""Reusable configured-store bridge over a private ``StoreDriverAPI``."""

from __future__ import annotations

import abc
import dataclasses

from collections.abc import Iterator
from types import TracebackType
from typing import BinaryIO

from LiuXin_alpha.storage.api2.errors import StoreReadOnly
from LiuXin_alpha.storage.api2.models import (
    Digest,
    FileInfo,
    Location,
    StoreCapabilities,
    StoreStatus,
    WriteMode,
)
from LiuXin_alpha.storage.api2.store_api import StoreAPI, WriteSession
from LiuXin_alpha.storage.api2.store_driver_api import (
    DriverFileInfo,
    DriverKey,
    DriverWriteSession,
    StoreDriverAPI,
)


class _DriverWriteSessionAdapter:
    """Translate a driver write session into a routed store write session.

    Example:
        >>> adapter = _DriverWriteSessionAdapter(store, session)  # doctest: +SKIP
    """

    def __init__(
        self,
        store: DriverBackedStoreAPI,
        session: DriverWriteSession,
    ) -> None:
        """Bind one driver session to its configured store identity.

        Example:
            >>> adapter = _DriverWriteSessionAdapter(store, session)  # doctest: +SKIP
        """
        self._store = store
        self._session = session

    def write(self, data: bytes) -> int:
        """Forward staged bytes to the driver session.

        Example:
            >>> accepted = adapter.write(b"payload")  # doctest: +SKIP
        """
        return self._session.write(data)

    def commit(self) -> FileInfo:
        """Commit and translate driver-local metadata into routed metadata.

        Example:
            >>> info = adapter.commit()  # doctest: +SKIP
        """
        return self._store._file_info(self._session.commit())

    def abort(self) -> None:
        """Abort the underlying driver session idempotently.

        Example:
            >>> adapter.abort()  # doctest: +SKIP
        """
        self._session.abort()

    def __enter__(self) -> _DriverWriteSessionAdapter:
        """Enter the adapter lifetime and return this adapter.

        Example:
            >>> entered = adapter.__enter__()  # doctest: +SKIP
        """
        self._session.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Forward context exit so uncommitted driver state is aborted.

        Example:
            >>> adapter.__exit__(None, None, None)  # doctest: +SKIP
        """
        self._session.__exit__(exc_type, exc, traceback)


class DriverBackedStoreAPI(StoreAPI, abc.ABC):
    """Configured ``StoreAPI`` whose primitives delegate to one driver.

    Concrete stores supply only ``spec`` and the protected ``_driver`` binding;
    this bridge handles routed Location validation, key translation, lifecycle,
    status, metadata translation, enumeration, and staged-write adaptation.

    Example:
        >>> class ConcreteStore(DriverBackedStoreAPI):  # doctest: +SKIP
        ...     spec = configured_spec
        ...     _driver = concrete_driver
    """

    @property
    @abc.abstractmethod
    def _driver(self) -> StoreDriverAPI:
        """Return the privately owned driver used by this configured store.

        Manager and policy code should not access this implementation detail.

        Example:
            >>> driver = store._driver  # doctest: +SKIP
        """
        ...

    @property
    def capabilities(self) -> StoreCapabilities:
        """Expose driver capabilities constrained by store configuration.

        Example:
            >>> capabilities = store.capabilities  # doctest: +SKIP
        """
        capabilities = self._driver.capabilities
        if not self.spec.read_only:
            return capabilities
        return dataclasses.replace(
            capabilities,
            create=False,
            replace=False,
            delete=False,
        )

    def startup(self) -> StoreStatus:
        """Start the owned driver and return its status.

        Example:
            >>> status = store.startup()  # doctest: +SKIP
        """
        return self._effective_status(self._driver.startup())

    def probe(self) -> StoreStatus:
        """Actively probe the owned driver.

        Example:
            >>> status = store.probe()  # doctest: +SKIP
        """
        return self._effective_status(self._driver.probe())

    def status(self, *, refresh: bool = False) -> StoreStatus:
        """Return current driver status, probing first when requested.

        Example:
            >>> status = store.status(refresh=True)  # doctest: +SKIP
        """
        if refresh:
            return self.probe()
        return self._effective_status(self._driver.status())

    def close(self) -> None:
        """Close the owned driver.

        Example:
            >>> store.close()  # doctest: +SKIP
        """
        self._driver.close()

    def location(self, *tokens: str) -> Location:
        """Build a routed location using driver-specific key joining.

        Example:
            >>> location = store.location("authors", "book.epub")  # doctest: +SKIP
        """
        return self._location(self._driver.join_key(*tokens))

    def locate(self, identifier: str | Location) -> Location:
        """Resolve a persisted key or validate an existing routed location.

        Example:
            >>> location = store.locate("authors/book.epub")  # doctest: +SKIP
        """
        if isinstance(identifier, Location):
            return self.require_location(identifier)
        return self._location(self._driver.resolve_key(identifier))

    def allocate_location(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        name_hint: str | None = None,
    ) -> Location:
        """Allocate and route a safe driver-selected key.

        Example:
            >>> location = store.allocate_location(  # doctest: +SKIP
            ...     expected_size=4, name_hint="book.epub",
            ... )
        """
        key = self._driver.allocate_key(
            expected_size=expected_size,
            expected_digest=expected_digest,
            name_hint=name_hint,
        )
        return self._location(key)

    def stat(self, location: Location) -> FileInfo:
        """Describe one routed object through the owned driver.

        Example:
            >>> info = store.stat(location)  # doctest: +SKIP
        """
        return self._file_info(self._driver.stat(self._key(location)))

    def open_read(
        self,
        location: Location,
        *,
        offset: int = 0,
        length: int | None = None,
    ) -> BinaryIO:
        """Open a routed binary stream through the owned driver.

        Example:
            >>> source = store.open_read(location, length=20)  # doctest: +SKIP
        """
        return self._driver.open_read(
            self._key(location),
            offset=offset,
            length=length,
        )

    def begin_write(
        self,
        location: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
    ) -> WriteSession:
        """Begin a driver write and adapt its committed metadata to the store.

        Example:
            >>> session = store.begin_write(  # doctest: +SKIP
            ...     location, expected_size=4,
            ... )
        """
        self._require_writable()
        session = self._driver.begin_write(
            self._key(location),
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
        )
        return _DriverWriteSessionAdapter(self, session)

    def delete(
        self,
        location: Location,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """Delete a routed object through the owned driver.

        Example:
            >>> store.delete(location, if_version="v3")  # doctest: +SKIP
        """
        self._require_writable()
        self._driver.delete(
            self._key(location),
            missing_ok=missing_ok,
            if_version=if_version,
        )

    def iter_locations(
        self,
        *,
        prefix: Location | None = None,
    ) -> Iterator[Location]:
        """Translate driver inventory keys into routed locations.

        Example:
            >>> locations = list(store.iter_locations())  # doctest: +SKIP
        """
        driver_prefix = None if prefix is None else self._key(prefix)
        for key in self._driver.iter_keys(prefix=driver_prefix):
            yield self._location(key)

    def _key(self, location: Location) -> DriverKey:
        """Validate a routed location and resolve its driver-local key.

        Example:
            >>> key = store._key(location)  # doctest: +SKIP
        """
        owned = self.require_location(location)
        return self._driver.resolve_key(owned.key)

    def _location(self, key: DriverKey) -> Location:
        """Pair one driver key with this configured store's identity.

        Example:
            >>> location = store._location(DriverKey("objects/42"))  # doctest: +SKIP
        """
        return Location(self.store_ref, str(key))

    def _file_info(self, info: DriverFileInfo) -> FileInfo:
        """Translate driver-local metadata into routed store metadata.

        Example:
            >>> routed = store._file_info(driver_info)  # doctest: +SKIP
        """
        return FileInfo(
            location=self._location(info.key),
            size=info.size,
            modified_at=info.modified_at,
            digest=info.digest,
            version=info.version,
        )

    def _effective_status(self, status: StoreStatus) -> StoreStatus:
        """Apply configured read-only state to a driver status snapshot.

        Example:
            >>> status = store._effective_status(driver_status)  # doctest: +SKIP
        """
        if not self.spec.read_only or not status.writable:
            return status
        return dataclasses.replace(status, writable=False)

    def _require_writable(self) -> None:
        """Raise when configured store policy forbids mutation.

        Example:
            >>> store._require_writable()  # doctest: +SKIP
        """
        if self.spec.read_only:
            raise StoreReadOnly(f"configured store {self.store_ref!r} is read-only.")


__all__ = ["DriverBackedStoreAPI"]
