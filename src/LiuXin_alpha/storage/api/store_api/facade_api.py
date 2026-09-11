"""
Compose configured-Store identity, lifecycle, file primitives, and convenience methods.

StoreAPI remains abstract until its primitive contracts are implemented. Context
entry returns the same Store without starting or probing it; context exit closes
it. Low-level driver addresses are kept behind the routed Location boundary.

Example:
    >>> with store as active:  # doctest: +SKIP
    ...     payload = active.read_file("incoming/book.epub")
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

    Concrete stores enforce that every ``Location`` belongs to ``store_ref`` and implement the small
    transactional primitives by delegating physical operations to a backend-specific
    ``StorageDriverAPI`` without exposing that driver to the manager.

    Example:
        >>> def read_object(store: StoreAPI, key: str) -> bytes:
        ...     return store.read_file(key)
    """

    def __enter__(self) -> StoreAPI:
        """
        Enter the configured-store lifetime and return this store.

        The caller or concrete construction policy must establish readiness. This differs from the
        raw driver context, whose entry calls startup.

        Example:
            >>> entered = store.__enter__()  # doctest: +SKIP


        :return: This Store unchanged; startup, probing, and availability checks are not performed.
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

        Cleanup is attempted regardless of the exception arguments. A close exception propagates and
        can mask an exception from the body.

        Example:
            >>> store.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type: Exception class from the context body, or None; ignored by this method.
        :param exc: Exception instance from the body, or None; ignored by this method.
        :param traceback: Body exception traceback, or None; ignored by this method.
        :return: None after close returns, leaving any body exception unsuppressed.
        """
        self.close()


__all__ = ["StoreAPI"]
