"""Declare shared mixin state and guard access to a detached wrapper's driver."""

from __future__ import annotations

from abc import abstractmethod
from threading import RLock
from typing import TYPE_CHECKING

from LiuXin_alpha.databases.api.driver_wrapper_api import DatabaseDriverWrapperAPI

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.driver_api import DatabaseDriverAPI


class DriverWrapperBase(DatabaseDriverWrapperAPI):
    """Keep partial wrapper implementations abstract and cross-mixin calls typed."""

    _driver: DatabaseDriverAPI | None
    lock: RLock | None

    @property
    def driver(self) -> DatabaseDriverAPI:
        """Return the live driver or reject operations after wrapper shutdown."""
        if self._driver is None:
            raise RuntimeError("The driver wrapper is closed")
        return self._driver

    @driver.setter
    def driver(self, driver: DatabaseDriverAPI) -> None:
        self._driver = driver

    @abstractmethod
    def _clear_derived_schema_caches(self) -> None:
        """Discard wrapper descriptions after a schema refresh."""
