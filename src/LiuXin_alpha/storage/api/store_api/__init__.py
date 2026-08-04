"""Compatibility import surface for legacy store API contracts.

Examples:
    Prefer ``StorePluginAPI`` for new code; legacy annotations can still use::

        def inventory(store: StoreAPI):
            return list(store.iter())
"""

from __future__ import annotations

import abc
import pprint
from typing import Optional, Iterator, TYPE_CHECKING

from LiuXin_alpha.storage.api.store_api.storage_backend_api import StoreBackendAPI

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
    from LiuXin_alpha.storage.api import StoreStatus
    from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI


class StoreAPI(StoreBackendAPI):
    """Compatibility contract for one physical/logical store.

    Examples:
        Existing implementations expose locations through ``iter``::

            locations = list(store.iter())
    """

    _url: str
    _name: str
    _uuid: Optional[str]
    _db: "DatabaseAPI | None"

    def __init__(
        self,
        url: str,
        db: "DatabaseAPI | None" = None,
        name: Optional[str] = None,
        uuid: Optional[str] = None,
    ) -> None:
        """Initialise legacy store identity and optional database binding.

        Examples:
            Construct a concrete store around a local root::

                store = ConcreteStore("/srv/books", name="main")
        """
        self.set_url(url)
        self._name = name if name is not None else self.url_to_name(url)
        self._uuid = uuid
        self._db = db

    @abc.abstractmethod
    def url_to_name(self, url: str) -> str:
        """Derive a default display name from a root URL.

        Examples:
            Preview a derived name::

                name = store.url_to_name("/srv/books")
        """
        ...

    @abc.abstractmethod
    def startup(self) -> "StoreStatus":
        """Start the store and return its status.

        Examples:
            Initialise a lazily connected store::

                status = store.startup()
        """
        ...

    @abc.abstractmethod
    def self_test(self) -> "StoreStatus":
        """Run store health checks.

        Examples:
            Verify a store before writing::

                status = store.self_test()
        """
        ...

    @abc.abstractmethod
    def status(self) -> "StoreStatus":
        """Return current store status.

        Examples:
            Inspect online state::

                online = store.status().online
        """
        ...

    @property
    def url(self) -> str:
        """Return the store root URL.

        Examples:
            Include the endpoint in diagnostics::

                endpoint = store.url
        """
        return self._url

    @url.setter
    def url(self, url: str) -> None:
        """Reject direct URL assignment.

        Examples:
            Use the explicit mutator instead::

                store.set_url("/srv/new-books")
        """
        raise AttributeError("Cannot directly set the url of a store.")

    def set_url(self, new_url: str) -> None:
        """Replace the store root URL explicitly.

        Examples:
            Repoint an unregistered store::

                store.set_url("/srv/new-books")
        """
        self._url = new_url

    @property
    def name(self) -> str:
        """Return the store display name.

        Examples:
            Label an inventory entry::

                label = store.name
        """
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        """Reject direct name assignment.

        Examples:
            Supply a name during construction::

                store = ConcreteStore("/srv/books", name="main")
        """
        raise AttributeError("Cannot directly set the name of a store.")

    @property
    def uuid(self) -> Optional[str]:
        """Return the optional stable store UUID.

        Examples:
            Correlate the store with durable metadata::

                store_uuid = store.uuid
        """
        return self._uuid

    @uuid.setter
    def uuid(self, uuid: str) -> None:
        """Reject direct UUID assignment.

        Examples:
            Supply the UUID during construction::

                store = ConcreteStore("/srv/books", uuid="store-uuid")
        """
        raise AttributeError("Cannot directly set the uuid of a store.")

    @property
    def db(self) -> "DatabaseAPI | None":
        """Return the optional database bound to this legacy store.

        Examples:
            Detect standalone operation::

                standalone = store.db is None
        """
        return self._db

    @property
    def online(self) -> bool:
        """Return whether status reports the store online.

        Examples:
            Skip reads from an offline store::

                if store.online:
                    locations = list(store.iter())
        """
        try:
            return self.status().online
        except Exception:
            return False

    @property
    def checked(self) -> bool:
        """Return whether the current status was health-checked.

        Examples:
            Run a self-test when needed::

                if not store.checked:
                    store.self_test()
        """
        try:
            return bool(self.status().checked)
        except Exception:
            return False

    def status_str(self) -> str:
        """Pretty-format current store status.

        Examples:
            Emit readable diagnostic output::

                logger.info(store.status_str())
        """
        return pprint.pformat(self.status())

    def iter_replicas(self) -> Iterator["StoreLocationMixinAPI"]:
        """Iterate over concrete replica locations in the store.

        Examples:
            Inventory stored replicas::

                replicas = list(store.iter_replicas())
        """
        return self.true_files()

    def iter(self) -> Iterator["StoreLocationMixinAPI"]:
        """Compatibility alias for iterating true file locations.

        Examples:
            Consume every visible location::

                locations = list(store.iter())
        """
        return self.true_files()
