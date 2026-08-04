"""Raw storage plugin API.

A store plugin talks to one physical medium or remote endpoint. It should know
nothing about database rows, item graphs, replica policy, or orchestration. If
it starts learning those things, the boundary has gone bad.

Examples:
    Raw-backend tools may use a concrete plugin without a database::

        plugin = ConcreteStorePlugin(url="/srv/books", name="books")
        location = plugin.write_bytes(b"payload")
"""

from __future__ import annotations

import abc
import pprint
from collections.abc import Iterator
from typing import TYPE_CHECKING, Optional

from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI
from LiuXin_alpha.storage.single_file import SingleFileStatus

if TYPE_CHECKING:
    from LiuXin_alpha.storage.api.info_containers_api import StoreStatus


class StorePluginAPI(abc.ABC):
    """Contract for one raw storage plugin bound to one root location.

    This is the reusable bit that ingest, repair, or export code should be able
    to call without dragging in the whole managed-storage stack.

    Examples:
        Resolve and read a backend-local path::

            location = plugin.locate("authors/book.epub")
            payload = location.read_bytes()
    """

    _url: str
    _name: str
    _uuid: Optional[str]

    def __init__(
        self,
        *,
        url: str,
        name: Optional[str] = None,
        uuid: Optional[str] = None,
    ) -> None:
        """Initialise a plugin with one immutable logical identity.

        Examples:
            Let the plugin derive a display name from its URL::

                plugin = ConcreteStorePlugin(url="/srv/books")
        """
        self.set_url(url)
        self._name = name if name is not None else self.url_to_name(url)
        self._uuid = uuid

    @property
    def plugin_kind(self) -> str:
        """Return the concrete plugin class name as its default kind.

        Examples:
            Include the kind in a backend inventory::

                kind = plugin.plugin_kind
        """
        return type(self).__name__

    @abc.abstractmethod
    def url_to_name(self, url: str) -> str:
        """Derive a useful default store name from a root URL.

        Examples:
            Preview the default name before constructing a spec::

                name = plugin.url_to_name("/srv/books")
        """
        ...

    @property
    @abc.abstractmethod
    def root_path(self):
        """Return the plugin-specific root path or endpoint object.

        Examples:
            Display the backend root in diagnostics::

                root = plugin.root_path
        """
        ...

    @abc.abstractmethod
    def startup(self) -> "StoreStatus":
        """Start or connect to the backend and return status.

        Examples:
            Initialise a lazily connected remote plugin::

                status = plugin.startup()
        """
        ...

    @abc.abstractmethod
    def self_test(self) -> "StoreStatus":
        """Run backend-specific health checks.

        Examples:
            Use a self-test before admitting a store to write routing::

                status = plugin.self_test()
        """
        ...

    @abc.abstractmethod
    def status(self) -> "StoreStatus":
        """Return the plugin's current status record.

        Examples:
            Check whether the backend reports itself online::

                online = plugin.status().online
        """
        ...

    def close(self) -> None:
        """Release backend resources; the base implementation is a no-op.

        Examples:
            Close the plugin after a standalone operation::

                plugin.close()
        """
        return None

    @property
    def url(self) -> str:
        """Return the plugin root URL.

        Examples:
            Persist the configured endpoint in a store spec::

                root_url = plugin.url
        """
        return self._url

    @url.setter
    def url(self, url: str) -> None:
        """Reject direct URL assignment so changes remain explicit.

        Examples:
            Use ``set_url`` for a controlled reconfiguration::

                plugin.set_url("/srv/new-books")
        """
        raise AttributeError("Cannot directly set the url of a store plugin.")

    def set_url(self, new_url: str) -> None:
        """Replace the backend root URL explicitly.

        Examples:
            Repoint a not-yet-registered plugin::

                plugin.set_url("/srv/new-books")
        """
        self._url = str(new_url)

    @property
    def name(self) -> str:
        """Return the plugin's display name.

        Examples:
            Show a store label in diagnostics::

                label = plugin.name
        """
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        """Reject direct name assignment so plugin identity stays stable.

        Examples:
            Supply a name during construction instead::

                plugin = ConcreteStorePlugin(url="/srv/books", name="main")
        """
        raise AttributeError("Cannot directly set the name of a store plugin.")

    @property
    def uuid(self) -> Optional[str]:
        """Return the optional stable store UUID.

        Examples:
            Use the UUID when correlating an external inventory::

                store_uuid = plugin.uuid
        """
        return self._uuid

    @property
    def store_uuid(self) -> Optional[str]:
        """Return ``uuid`` under the store-oriented compatibility name.

        Examples:
            Read identity consistently with ``StoreContainerAPI``::

                store_uuid = plugin.store_uuid
        """
        return self._uuid

    @uuid.setter
    def uuid(self, uuid: str) -> None:
        """Reject direct UUID assignment so plugin identity stays stable.

        Examples:
            Supply a UUID during construction instead::

                plugin = ConcreteStorePlugin(url="/srv/books", uuid="store-uuid")
        """
        raise AttributeError("Cannot directly set the uuid of a store plugin.")

    @property
    def online(self) -> bool:
        """Return whether status reports the backend online.

        Examples:
            Skip a standalone read when its plugin is offline::

                if plugin.online:
                    location = plugin.locate(key)
        """
        try:
            return self.status().online
        except Exception:
            return False

    @property
    def checked(self) -> bool:
        """Return whether the current status has completed a health check.

        Examples:
            Trigger a self-test when no check has run::

                if not plugin.checked:
                    plugin.self_test()
        """
        try:
            return bool(self.status().checked)
        except Exception:
            return False

    def status_str(self) -> str:
        """Pretty-format the current status for diagnostics.

        Examples:
            Emit a readable health message::

                logger.info(plugin.status_str())
        """
        return pprint.pformat(self.status())

    @abc.abstractmethod
    def location(self, *tokens: str) -> StoreLocationMixinAPI:
        """Create a location relative to the backend root.

        Examples:
            Join portable storage-key components::

                location = plugin.location("authors", "book.epub")
        """
        ...

    @abc.abstractmethod
    def locate(self, file_identifier: str | StoreLocationMixinAPI) -> StoreLocationMixinAPI:
        """Resolve a storage key, URL, or existing location.

        Examples:
            Resolve a database storage key::

                location = plugin.locate("authors/book.epub")
        """
        ...

    @abc.abstractmethod
    def exists(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        """Return whether a selected file exists.

        Examples:
            Check a key before requesting its bytes::

                present = plugin.exists("authors/book.epub")
        """
        ...

    @abc.abstractmethod
    def file_size(self, file_identifier: str | StoreLocationMixinAPI) -> int | None:
        """Return the selected file's byte size when available.

        Examples:
            Compare a remote size with stored metadata::

                size = plugin.file_size(location)
        """
        ...

    @abc.abstractmethod
    def stat(self, file_identifier: str | StoreLocationMixinAPI) -> SingleFileStatus:
        """Return a fresh backend status record for one file.

        Examples:
            Refresh cached hash and size information::

                file_status = plugin.stat(location)
        """
        ...

    @abc.abstractmethod
    def iter_locations(self) -> Iterator[StoreLocationMixinAPI]:
        """Iterate over file locations visible below the backend root.

        Examples:
            Inventory a raw store::

                urls = [location.file_url for location in plugin.iter_locations()]
        """
        ...

    @abc.abstractmethod
    def write_bytes(
        self,
        file_bytes: bytes,
        *,
        metadata=None,
        location: str | None = None,
    ) -> StoreLocationMixinAPI:
        """Write bytes to an explicit or backend-selected location.

        Examples:
            Write to a human-readable relative path::

                location = plugin.write_bytes(
                    b"hello", location="notes/hello.txt"
                )
        """
        ...

    def copy_within_plugin(
        self,
        src_location: str | StoreLocationMixinAPI,
        dst_location: str | StoreLocationMixinAPI,
    ) -> StoreLocationMixinAPI:
        """Copy a file within this plugin when the backend supports it.

        Examples:
            Duplicate one backend-local object::

                copied = plugin.copy_within_plugin(source, "copies/book.epub")
        """
        raise PermissionError("This store plugin does not support in-plugin copies.")

    @abc.abstractmethod
    def delete(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        """Delete one file when the backend is writable.

        Examples:
            Delete a location returned by ``write_bytes``::

                removed = plugin.delete(location)
        """
        ...

    def update_bytes(
        self,
        file_identifier: str | StoreLocationMixinAPI,
        file_bytes: bytes,
        *,
        append: bool = False,
    ) -> bool:
        """Replace or append bytes when the backend supports mutation.

        Examples:
            Replace the contents of a small file::

                changed = plugin.update_bytes(location, b"revised")
        """
        raise PermissionError("This store plugin does not support in-place updates.")
