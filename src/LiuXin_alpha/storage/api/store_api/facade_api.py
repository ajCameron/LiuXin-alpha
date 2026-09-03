"""
Complete configured-Store facade.
"""

from __future__ import annotations

import abc

from types import TracebackType

from LiuXin_alpha.storage.api.store_api.convenience_api import (
    StoreConvenienceAPI,
)
from LiuXin_alpha.storage.api.store_api.file_api import StoreFileAPI
from LiuXin_alpha.storage.api.store_api.identity_api import StoreIdentityAPI
from LiuXin_alpha.storage.api.store_api.lifecycle_api import StoreLifecycleAPI


class StoreAPI(
    StoreConvenienceAPI,
    StoreIdentityAPI,
    StoreLifecycleAPI,
    StoreFileAPI,
    abc.ABC,
):
    """
    Complete facade for one configured store.

    Concrete stores enforce that every ``Location`` belongs to ``store_ref``
    and implement the small transactional primitives by delegating physical
    operations to a backend-specific ``StorageDriverAPI`` without exposing that
    driver to the manager.

    Example:
        >>> def read_object(store: StoreAPI, key: str) -> bytes:
        ...     return store.read_file(key)
    """

    def __enter__(self) -> StoreAPI:
        """
        Enter the configured-store lifetime and return this store.

        Example:
            >>> entered = store.__enter__()  # doctest: +SKIP


        :return:
        """
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Close the configured store when leaving its context.

        Example:
            >>> store.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type:
        :param exc:
        :param traceback:
        :return:
        """
        self.close()


__all__ = ["StoreAPI"]
