"""
Location-based byte routing for the storage manager.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import BinaryIO, override

import LiuXin_alpha.storage.api as api
from LiuXin_alpha.storage.storage_manager.mixins._state import _StorageManagerState


class StorageRouterMixin(_StorageManagerState):
    """
    Route opaque Locations to their owning Store plugins.

    These methods deliberately add no catalogue or placement policy: they
    resolve ``Location.store_ref`` and preserve the Store API's validation,
    conditional-write, and error semantics.  Higher-level Asset workflows live
    in ingest, retrieval, and Replica components.
    """

    @override
    def stat(self, location: api.Location) -> api.FileInfo:
        """
        Route ``stat`` by the Location's Store UUID.


        :param location:
        :return:
        """

        return self.get_store(location.store_ref).stat(location)

    @override
    def get(
        self,
        location: api.Location,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Route a binary read by the Location's Store UUID.


        :param location:
        :param offset:
        :param length:
        :param if_version:
        :return:
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
        location: api.Location,
        source: BinaryIO,
        *,
        mode: api.WriteMode = api.WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: api.Digest | None = None,
    ) -> api.FileInfo:
        """
        Route one transactional Store publication.


        :param location:
        :param source:
        :param mode:
        :param expected_size:
        :param expected_digest:
        :return:
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
        location: api.Location,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Route deletion while preserving Store errors and preconditions.


        :param location:
        :param missing_ok:
        :param if_version:
        :return:
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
        store_ref: api.StoreUUID | None = None,
        prefix: api.Location | None = None,
    ) -> Iterator[api.Location]:
        """
        Enumerate one Store or every live Store in stable UUID order.


        :param store_ref:
        :param prefix:
        :return:
        """

        if prefix is not None:
            if store_ref is not None and prefix.store_ref != store_ref:
                raise api.StoreInvalidLocation(
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
    def capabilities(self, store_ref: api.StoreUUID) -> api.StoreCapabilities:
        """
        Return one routed Store's inherent capabilities.


        :param store_ref:
        :return:
        """

        return self.get_store(store_ref).capabilities

    @override
    def characteristics(
        self,
        store_ref: api.StoreUUID,
    ) -> api.StorageCharacteristics:
        """
        Return structured constraints for one routed Store.


        :param store_ref:
        :return:
        """

        store = self.get_store(store_ref)
        if isinstance(store, api.StoreCharacteristicsAPI):
            return store.characteristics
        return api.StorageCharacteristics()

    @override
    def status(self, store_ref: api.StoreUUID) -> api.StoreStatus:
        """
        Return one routed Store's cached dynamic status.


        :param store_ref:
        :return:
        """

        return self.get_store(store_ref).status()


__all__ = ["StorageRouterMixin"]
