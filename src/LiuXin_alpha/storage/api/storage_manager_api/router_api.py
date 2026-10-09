"""
Define Store routing primitives and their small default conveniences.

Abstract operations own backend policy and evidence. Default transfers compose
those operations with explicit stream lifetimes and failure boundaries; they
do not create catalogue records or atomic transactions across Stores.
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
    Route byte operations across configured Stores using Location ownership. Seven abstract
    primitives cover stat, get, put, delete, enumeration, capabilities, and status. Implementations
    own routing, validation, and Store errors. This base adds small read/write, binding, and
    streaming transfer conveniences; catalogue registration and policy belong to the broader manager
    interfaces.

    Transfers are not transactions across Stores. A destination can remain published when a later
    close or source-deletion step fails, and the base adds no rollback.

    Example:
        >>> bound = manager.bind(location)  # doctest: +SKIP
        >>> info = bound.write_bytes(b"book")  # doctest: +SKIP
    """

    @abc.abstractmethod
    def stat(self, location: Location) -> FileInfo:
        """
        Report current metadata for an addressed object, preserving access errors.

        Example:
            >>> info = manager.stat(location)  # doctest: +SKIP


        :param location: Opaque address whose Store UUID selects the route and whose key is interpreted by that Store.
        :return: Current FileInfo supplied by the routed implementation.
        """
        ...

    @abc.abstractmethod
    def get(
        self, location: Location, *, offset: int = 0, length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Open a current routed binary reader, optionally guarded by a version. The caller owns the
        returned stream; the implementation supplies range and precondition checks.

        Example:
            >>> reader = manager.get(location, offset=10, length=20)  # doctest: +SKIP


        :param location: Opaque address whose Store UUID selects the route and whose key is interpreted by that Store.
        :param offset: Requested starting byte offset, normally nonnegative; the routed implementation validates the range.
        :param length: Optional requested byte count; None reads to the end of the selected object.
        :param if_version: Optional opaque version precondition; None requests an unconditional read.
        :return: Caller-owned binary reader for the selected object or range.
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
        Route a borrowed stream to the destination Store for staged publication. The implementation
        must preserve collision policy, expected byte count/digest checks, and Store failure
        categories. This abstract method supplies no stream loop or transaction machinery.

        Example:
            >>> info = manager.put(location, source, expected_size=4)  # doctest: +SKIP


        :param location: Opaque address whose Store UUID selects the route and whose key is interpreted by that Store.
        :param source: Borrowed binary input read from its current position; the caller retains ownership.
        :param mode: Destination collision policy, defaulting to CREATE_ONLY.
        :param expected_size: Optional exact expected logical byte count for publication validation.
        :param expected_digest: Optional expected digest for publication validation by the destination.
        :return: FileInfo for the successfully published destination.
        """
        ...

    @abc.abstractmethod
    def delete(
        self, location: Location, *, missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Delete a routed object with optional idempotence and version protection. Unsupported
        conditional deletion raises StoreUnsupportedOperation and a stale version raises
        StorePreconditionFailed under the Store contract. The implementation owns routing and any
        side effects.

        Example:
            >>> manager.delete(location, if_version="v3")  # doctest: +SKIP


        :param location: Opaque address whose Store UUID selects the route and whose key is interpreted by that Store.
        :param missing_ok: Whether absence at the addressed Store is permitted as a successful no-op.
        :param if_version: Optional opaque version required to match before deletion.
        :return: None after successful deletion or allowed absence.
        """
        ...

    @abc.abstractmethod
    def iter_locations(
        self, *, store_ref: StoreUUID | None = None,
        prefix: Location | None = None,
    ) -> Iterator[Location]:
        """
        Enumerate concrete object addresses in one configured Store or across the router. Ordering,
        prefix interpretation, completeness, and failures depend on the implementation and
        advertised Store capabilities; this abstract contract creates no inventory snapshot.

        Example:
            >>> locations = list(manager.iter_locations(store_ref=store_uuid))  # doctest: +SKIP


        :param store_ref: Optional configured Store UUID restricting enumeration; None requests all routes.
        :param prefix: Optional Store-owned prefix address interpreted by the implementing router/Store.
        :return: Iterator yielding matching Location values, possibly lazily.
        """
        ...

    @abc.abstractmethod
    def capabilities(self, store_ref: StoreUUID) -> StoreCapabilities:
        """
        Report the operation capabilities of a configured Store.

        Capability claims describe support
        rather than proving current availability or successful execution.

        Example:
            >>> capabilities = manager.capabilities(store_uuid)  # doctest: +SKIP


        :param store_ref: Configured Store UUID, rather than a database row ID or display name.
        :return: StoreCapabilities advertised for the selected configured Store.
        """
        ...

    def characteristics(self, store_ref: StoreUUID) -> StorageCharacteristics:
        """
        Return a new all-unknown characteristics profile without looking up the Store. This
        conservative default ignores the UUID and performs no validation or probe. Full managers may
        override it to expose the selected Store profile.

        Example:
            >>> profile = StorageRouterAPI.characteristics(manager, store_uuid)  # doctest: +SKIP


        :param store_ref: Configured Store UUID, rather than a database row ID or display name.
        :return: Fresh StorageCharacteristics with unknown categories and unspecified limits.
        """

        del store_ref
        return StorageCharacteristics()

    @abc.abstractmethod
    def status(self, store_ref: StoreUUID) -> StoreStatus:
        """
        Observe current availability, writability, and optional capacity/diagnostics for one Store.
        The concrete router owns probe behavior and unavailable/unknown-Store handling.

        Example:
            >>> status = manager.status(store_uuid)  # doctest: +SKIP


        :param store_ref: Configured Store UUID, rather than a database row ID or display name.
        :return: StoreStatus supplied by the concrete implementation.
        """
        ...

    def bind(self, location: Location) -> BoundLocation:
        """
        Create a fresh operational handle retaining this router and the exact Location. Construction
        performs no I/O, ownership/type validation, or existence check. Later operations use the
        live router rather than a captured backend.

        Example:
            >>> bound = manager.bind(location)  # doctest: +SKIP
            >>> bound.location is location  # doctest: +SKIP
            True


        :param location: Opaque address whose Store UUID selects the route and whose key is interpreted by that Store.
        :return: New BoundLocation referencing this router and the supplied address.
        """

        return BoundLocation(self, location)

    def try_stat(self, location: Location) -> FileInfo | None:
        """
        Call stat and suppress only StoreNotFound. Unknown configuration, unavailable Stores,
        permission failures, and all other exception categories propagate. Each invocation performs
        a fresh call.

        Example:
            >>> missing = manager.try_stat(location)  # doctest: +SKIP


        :param location: Opaque address whose Store UUID selects the route and whose key is interpreted by that Store.
        :return: FileInfo from stat, or None when stat raises StoreNotFound.
        """
        try:
            return self.stat(location)
        except StoreNotFound:
            return None

    def exists(self, location: Location) -> bool:
        """
        Classify the current try_stat result by comparison with None. This convenience adds no catch
        block; the implementation of try_stat controls which failures become absence.

        Example:
            >>> present = manager.exists(location)  # doctest: +SKIP


        :param location: Opaque address whose Store UUID selects the route and whose key is interpreted by that Store.
        :return: True when try_stat returns a value, otherwise False.
        """

        return self.try_stat(location) is not None

    def read_bytes(
        self, location: Location, *, offset: int = 0, length: int | None = None,
        if_version: str | None = None,
    ) -> bytes:
        """
        Open a reader through get, enter its context, and read the selected content into memory
        once. None if_version omits that keyword when calling get. Range/version checks remain with
        get; this wrapper has no independent size limit or payload-type check. Reader context exit
        handles closing and can propagate its own errors.

        Example:
            >>> payload = manager.read_bytes(location, offset=2, length=4)  # doctest: +SKIP


        :param location: Opaque address whose Store UUID selects the route and whose key is interpreted by that Store.
        :param offset: Requested starting byte offset, normally nonnegative; the routed implementation validates the range.
        :param length: Optional requested byte count; None reads to the end of the selected object.
        :param if_version: Optional opaque version precondition; None requests an unconditional read.
        :return: Bytes returned by the reader read call after successful context exit.
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
        self, location: Location,
        data: bytes,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_digest: Digest | None = None,
    ) -> FileInfo:
        """
        Wrap data in BytesIO and call put with len(data) as the exact expected size. Digest and
        collision arguments are forwarded unchanged. The temporary BytesIO is not explicitly closed
        by this wrapper, and no independent validation or rollback is performed.

        Example:
            >>> info = manager.write_bytes(location, b"book")  # doctest: +SKIP


        :param location: Opaque address whose Store UUID selects the route and whose key is interpreted by that Store.
        :param data: Complete in-memory byte payload to publish.
        :param mode: Destination collision policy, defaulting to CREATE_ONLY.
        :param expected_digest: Optional expected digest for publication validation by the destination.
        :return: FileInfo returned by put for the complete payload.
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
        Stat the source and stream it to destination put with observed size and digest expectations.
        The read includes the observed version only when the source advertises conditional_read and
        supplies a non-None version. Otherwise the read is unconditional. The destination
        implementation owns integrity validation and publication.

        The source reader is context-managed. A close error can propagate after destination
        publication, and no rollback or self-copy special case is added. Concrete managers may
        override transfer selection using Store topology.

        Example:
            >>> info = manager.copy(source_location, destination_location)  # doctest: +SKIP


        :param source: Source Location to stat and open through this router.
        :param destination: Destination Location whose Store receives the borrowed source stream.
        :param mode: Destination collision policy, defaulting to CREATE_ONLY.
        :return: Destination FileInfo from put after successful source-reader context exit.
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
        Require source conditional deletion and a non-None observed version, then copy and delete.
        Both requirements are checked before calling copy. The default copy implementation stats the
        source again, while deletion uses the version captured by this method first. Changing
        metadata can therefore cause deletion to fail after destination publication.

        Calls remain dynamically dispatched, and this base supplies no native transfer, cross-Store
        transaction, or destination rollback. Copy failures prevent deletion; deletion failures
        propagate with whatever destination state already exists.

        Example:
            >>> info = manager.move(source_location, destination_location)  # doctest: +SKIP


        :param source: Source Location whose first observed version guards later deletion.
        :param destination: Destination Location forwarded to copy.
        :param mode: Destination collision policy, defaulting to CREATE_ONLY.
        :return: FileInfo returned by copy, only after source deletion succeeds.
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
        Lazily stat each Location yielded by iter_locations with the same selection filters.

        There is no snapshot, extra deduplication, or suppression of changes/disappearance between
        enumeration and stat. Errors may follow already yielded records.

        Example:
            >>> infos = list(manager.iter_file_infos(store_ref=store_uuid))  # doctest: +SKIP

        :param store_ref: Optional configured Store UUID restricting enumeration; None requests all routes.
        :param prefix: Optional Store-owned prefix address interpreted by the implementing router/Store.

        :return: Iterator yielding one current FileInfo per successfully described enumerated Location.
        """

        for location in self.iter_locations(store_ref=store_ref, prefix=prefix):
            yield self.stat(location)


__all__ = ["StorageRouterAPI"]
