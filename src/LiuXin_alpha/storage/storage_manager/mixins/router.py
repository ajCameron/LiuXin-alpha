"""
Route Location operations to attached Stores without changing manager metadata.

The router resolves Store UUIDs at each call and delegates byte mechanics,
capabilities, and status. It adds a known-object-size preflight for publication
and Store selection for inventory. Asset identity, Replica observations, policy
enforcement, and reconciliation belong to the other manager components.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import BinaryIO, override

from LiuXin_alpha.storage.api import errors as storage_errors
from LiuXin_alpha.storage.api import models as storage_models
from LiuXin_alpha.storage.api import store_api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState


class StorageRouterMixin(_StorageManagerState):
    """
    Supply byte-routing primitives using the shared manager Store registry.

    Lookup distinguishes an unknown configuration from a configured Store without an attached
    facade; it does not itself establish availability. These methods neither register new Replica
    claims nor update existing observations after byte writes or deletion. Store implementations
    retain responsibility for validation, access restrictions, and publication safety. The shared
    abstract base requires the other manager components; use this mixin through a complete manager
    composition.

    Example:
        >>> info = manager.stat(location)  # doctest: +SKIP
    """

    @override
    def stat(self, location: storage_models.Location) -> storage_models.FileInfo:
        """
        Return the owning Store's metadata for one opaque Location.

        Resolve the Store using location.store_ref and forward the original Location unchanged.
        Lookup and Store errors propagate; this method adds no digest verification or catalogue
        observation update.

        Example:
            >>> info = manager.stat(location)  # doctest: +SKIP


        :param location: Location whose Store UUID selects the attached facade and whose key the Store interprets.
        :return: The FileInfo supplied by the Store, without an independent physical read.
        """

        return self.get_store(location.store_ref).stat(location)

    @override
    def get(
        self,
        location: storage_models.Location,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Open the owning Store's binary reader and transfer its lifetime to the caller.

        Forward the byte offset and optional length without validating them here. Omit the
        if_version keyword entirely for None, preserving unconditional reads through Stores with
        narrower signatures. An explicit version is forwarded and may be rejected by the provider.
        No reader context is entered and no exception is suppressed by the router.

        Example:
            >>> with manager.get(location, offset=4, length=8) as reader:  # doctest: +SKIP
            ...     chunk = reader.read()


        :param location: Location identifying the attached Store and object to read.
        :param offset: Zero-based starting byte position forwarded to the Store; defaults to zero.
        :param length: Maximum requested byte count, or None to read through the available end.
        :param if_version: Optional opaque version required by the provider before reading; None omits the keyword.
        :return: The Store-supplied binary reader, which the caller must close.
        """

        store = self.get_store(location.store_ref)
        if if_version is None:
            return store.open_read(location, offset=offset, length=length)
        return store.open_read(
            location, offset=offset, length=length, if_version=if_version
        )

    @override
    def put(
        self,
        location: storage_models.Location,
        source: BinaryIO,
        *,
        mode: storage_models.WriteMode = storage_models.WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: storage_models.Digest | None = None,
    ) -> storage_models.FileInfo:
        """
        Preflight a known object size, then delegate complete publication to its Store.

        The shared size helper rejects a negative known size and checks the Store's per-object
        characteristics when a size is supplied. None skips that preflight. The subsequent Store
        lookup is separate, so facade selection and publication are not one registry transaction.
        Forward collision mode and integrity expectations to Store.put; this router does not enforce
        manager placement policy, register a Replica, close the caller's source, or provide rollback
        beyond the Store's publication contract.

        Example:
            >>> from io import BytesIO
            >>> info = manager.put(location, BytesIO(b"book"), expected_size=4)  # doctest: +SKIP


        :param location: Destination Location selecting the Store and object key.
        :param source: Caller-owned binary input consumed by the destination Store from its current position.
        :param mode: Collision behavior forwarded unchanged; defaults to create-only publication.
        :param expected_size: Expected total bytes, used for the size preflight and Store integrity validation; None leaves it unspecified.
        :param expected_digest: Optional digest the Store must validate while publishing the supplied bytes.
        :return: The destination Store publication result; subsequent manager metadata is unchanged.
        """

        self._require_supported_object_size(location.store_ref, expected_size)
        return self.get_store(location.store_ref).put(
            location,
            source,
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
        )

    @override
    def delete(
        self,
        location: storage_models.Location,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Delegate physical deletion without removing or updating Replica claims.

        Always forward both missing_ok and if_version, including their default values. Unlike get,
        this path does not omit a None version keyword. Store lookup, unsupported operations, and
        failed preconditions propagate. Manager loss-policy checks are not performed by this
        low-level route.

        Example:
            >>> manager.delete(location, missing_ok=True)  # doctest: +SKIP


        :param location: Location of the physical object to delete through its attached Store.
        :param missing_ok: Whether the Store should accept an already absent object; it does not suppress manager lookup errors.
        :param if_version: Optional opaque object version forwarded as the deletion precondition, including None.
        :return: None after the Store deletion call returns successfully.
        """

        self.get_store(location.store_ref).delete(
            location,
            missing_ok=missing_ok,
            if_version=if_version,
        )

    @override
    def iter_locations(
        self,
        *,
        store_ref: storage_models.StoreUUID | None = None,
        prefix: storage_models.Location | None = None,
    ) -> Iterator[storage_models.Location]:
        """
        Lazily enumerate one Store or a snapshot of all attached Store facades.

        A prefix selects its own Store UUID. An explicitly different store_ref raises
        StoreInvalidLocation before any Store lookup. Validation and Store selection happen when
        iteration starts, not when the generator is created. With neither selector, the default
        registry implementation snapshots attached facades in UUID integer order without filtering
        availability.

        Each Store receives prefix unchanged and controls its own enumeration order and
        completeness. Earlier Locations can be yielded before a later Store fails; there is no
        aggregate snapshot of bytes, error suppression, or de-duplication. Detached configurations
        without facades are absent from the all-Store inventory.

        Example:
            >>> locations = tuple(manager.iter_locations(store_ref=store_uuid))  # doctest: +SKIP


        :param store_ref: Optional UUID selecting one attached Store; None permits prefix selection or all attached Stores.
        :param prefix: Optional Store-owned Location used to narrow inventory and select its Store.
        :return: A generator delegating each selected Store inventory in sequence.
        """

        if prefix is not None:
            if store_ref is not None and prefix.store_ref != store_ref:
                raise storage_errors.StoreInvalidLocation(
                    "prefix Location does not belong to the requested Store."
                )
            store_ref = prefix.store_ref
        stores = (
            (self.get_store(store_ref),)
            if store_ref is not None
            else tuple(self.iter_stores())
        )
        for store in stores:
            yield from store.iter_locations(prefix=prefix)

    @override
    def capabilities(
        self, store_ref: storage_models.StoreUUID
    ) -> storage_models.StoreCapabilities:
        """
        Return the attached Store's capability value without probing availability.

        Read the facade property directly. Lookup and property errors propagate; capability support
        alone does not prove that an operation is currently permitted or that enough destination
        capacity is available.

        Example:
            >>> supported = manager.capabilities(store_uuid)  # doctest: +SKIP


        :param store_ref: UUID of the attached Store whose inherent capabilities are requested.
        :return: The Store-provided capability value without copying or filtering.
        """

        return self.get_store(store_ref).capabilities

    @override
    def characteristics(
        self,
        store_ref: storage_models.StoreUUID,
    ) -> store_api.StorageCharacteristics:
        """
        Read the optional Store characteristics interface or return unknown constraints.

        Resolve the attached facade first, even when no characteristics interface is present. A
        Store satisfying StoreCharacteristicsAPI supplies its own property; otherwise construct a
        fresh StorageCharacteristics with unknown fields. Structural protocol membership does not
        validate the property's value, and provider errors propagate instead of becoming unknown
        evidence.

        Example:
            >>> limits = manager.characteristics(store_uuid)  # doctest: +SKIP


        :param store_ref: UUID of the attached Store whose structured constraints are requested.
        :return: The Store characteristics value, or a new unknown profile when the optional interface is absent.
        """

        store = self.get_store(store_ref)
        if isinstance(store, store_api.StoreCharacteristicsAPI):
            return store.characteristics
        return store_api.StorageCharacteristics()

    @override
    def status(self, store_ref: storage_models.StoreUUID) -> storage_models.StoreStatus:
        """
        Call the attached Store's status method without requesting a refresh.

        The Store API normally returns cached dynamic evidence by default. This router supplies no
        arguments, performs no separate probe, and leaves the actual status/error behavior to the
        facade implementation.

        Example:
            >>> current = manager.status(store_uuid)  # doctest: +SKIP


        :param store_ref: UUID of the attached Store whose current reported status is requested.
        :return: The Store-supplied status observation without manager-side aggregation.
        """

        return self.get_store(store_ref).status()


__all__ = ["StorageRouterMixin"]
