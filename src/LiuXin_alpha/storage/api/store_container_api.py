"""Managed store container API.

A store container represents exactly one configured store. It owns one raw
plugin plus the DB/spec/status state around that plugin, but it should not grow
into an orchestrator or into a second backend API.

Examples:
    Work through a manager-owned container when store configuration matters::

        container = manager.get_store_container("main")
        location = container.write_bytes(b"payload")
"""

from __future__ import annotations

import abc
from collections.abc import Iterator
from typing import TYPE_CHECKING, Optional

from LiuXin_alpha.storage.api.location_api import StoreLocationMixinAPI
from LiuXin_alpha.storage.single_file import SingleFileStatus

if TYPE_CHECKING:
    from LiuXin_alpha.databases.api.database_api.database_api import DatabaseAPI
    from LiuXin_alpha.storage.api.info_containers_api import StoreSpec, StoreStatus
    from LiuXin_alpha.storage.api.store_plugin_api import StorePluginAPI


class StoreContainerAPI(abc.ABC):
    """Managed wrapper around one and only one configured plugin.

    Examples:
        Inspect one store's durable identity and live status::

            print(container.store_id, container.store_name, container.status())
    """

    @property
    @abc.abstractmethod
    def plugin(self) -> "StorePluginAPI":
        """Return the raw backend plugin owned by this container.

        Examples:
            Inspect backend-specific capability data only when necessary::

                plugin = container.plugin
        """
        ...

    @property
    @abc.abstractmethod
    def spec(self) -> "StoreSpec":
        """Return the container's current store specification.

        Examples:
            Read configured tags without querying the database again::

                spec = container.spec
        """
        ...

    @property
    @abc.abstractmethod
    def db(self) -> "DatabaseAPI | None":
        """Return the optional database bound to this container.

        Examples:
            Detect a standalone, database-free container::

                standalone = container.db is None
        """
        ...

    @property
    def store_id(self) -> Optional[int]:
        """Return the durable database id from the store specification.

        Examples:
            Include the store id in an inventory record::

                store_id = container.store_id
        """
        return self.spec.store_id

    @property
    def store_uuid(self) -> Optional[str]:
        """Return the stable UUID from the store specification.

        Examples:
            Use the UUID as an external reference::

                external_ref = container.store_uuid
        """
        return self.spec.store_uuid

    @property
    def store_name(self) -> str:
        """Return the configured store name.

        Examples:
            Display the selected store::

                label = container.store_name
        """
        return self.spec.store_name

    @property
    def store_url(self) -> str:
        """Return the configured root URL.

        Examples:
            Show the physical endpoint in diagnostics::

                endpoint = container.store_url
        """
        return self.spec.store_url

    @abc.abstractmethod
    def startup(self) -> "StoreStatus":
        """Start the plugin and return its resulting status.

        Examples:
            Start a lazily loaded store before its first operation::

                status = container.startup()
        """
        ...

    @abc.abstractmethod
    def probe(self) -> "StoreStatus":
        """Actively test the configured store and return fresh status.

        Examples:
            Refresh a health dashboard::

                status = container.probe()
        """
        ...

    @abc.abstractmethod
    def status(self, *, refresh: bool = False) -> "StoreStatus":
        """Return cached status, optionally probing first.

        Examples:
            Force a live status check::

                status = container.status(refresh=True)
        """
        ...

    def close(self) -> None:
        """Close the owned plugin and release its resources.

        Examples:
            Close a temporary container in a ``finally`` block::

                container.close()
        """
        self.plugin.close()

    def location(self, *tokens: str) -> StoreLocationMixinAPI:
        """Create a location relative to this store's root.

        Examples:
            Address a nested logical path::

                location = container.location("authors", "book.epub")
        """
        return self.plugin.location(*tokens)

    def locate(self, file_identifier: str | StoreLocationMixinAPI) -> StoreLocationMixinAPI:
        """Resolve a storage key, URL, or existing location.

        Examples:
            Resolve a key returned by database metadata::

                location = container.locate("authors/book.epub")
        """
        return self.plugin.locate(file_identifier)

    def exists(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        """Return whether the selected file exists on this store.

        Examples:
            Avoid reading a missing location::

                if container.exists("authors/book.epub"):
                    location = container.locate("authors/book.epub")
        """
        return self.plugin.exists(file_identifier)

    def file_size(self, file_identifier: str | StoreLocationMixinAPI) -> int | None:
        """Return a file's byte size, or ``None`` when unavailable.

        Examples:
            Include a known size in an inventory::

                size = container.file_size(location)
        """
        return self.plugin.file_size(file_identifier)

    def stat(self, file_identifier: str | StoreLocationMixinAPI) -> SingleFileStatus:
        """Return the backend's current status record for one file.

        Examples:
            Refresh size and hash information::

                file_status = container.stat(location)
        """
        return self.plugin.stat(file_identifier)

    def iter_locations(self) -> Iterator[StoreLocationMixinAPI]:
        """Iterate over all file locations visible on this store.

        Examples:
            Build a per-store inventory::

                locations = list(container.iter_locations())
        """
        return self.plugin.iter_locations()

    def write_bytes(self, file_bytes: bytes, *, metadata=None, location: str | None = None) -> StoreLocationMixinAPI:
        """Write bytes to an explicit or plugin-selected location.

        Examples:
            Request an explicit relative destination::

                location = container.write_bytes(b"hello", location="notes/hello.txt")
        """
        return self.plugin.write_bytes(file_bytes=file_bytes, metadata=metadata, location=location)

    def copy_within_store(
        self,
        src_location: str | StoreLocationMixinAPI,
        dst_location: str | StoreLocationMixinAPI,
    ) -> StoreLocationMixinAPI:
        """Copy bytes between two locations on this same store.

        Examples:
            Duplicate an object without routing it through the manager::

                copied = container.copy_within_store(source, "copies/book.epub")
        """
        return self.plugin.copy_within_plugin(src_location, dst_location)

    def delete(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        """Delete one file from this store.

        Examples:
            Delete the exact location returned by a write::

                removed = container.delete(location)
        """
        return self.plugin.delete(file_identifier)

    def update_bytes(
        self,
        file_identifier: str | StoreLocationMixinAPI,
        file_bytes: bytes,
        *,
        append: bool = False,
    ) -> bool:
        """Replace or append bytes at an existing location.

        Examples:
            Replace a small text payload::

                changed = container.update_bytes(location, b"revised")
        """
        return self.plugin.update_bytes(file_identifier, file_bytes=file_bytes, append=append)

    @abc.abstractmethod
    def reload_spec_from_db(self) -> "StoreSpec":
        """Reload this container's specification from its bound database.

        Examples:
            Pick up an externally changed store name::

                spec = container.reload_spec_from_db()
        """
        ...

    @abc.abstractmethod
    def save_spec_to_db(self) -> "StoreSpec":
        """Persist the current store specification.

        Examples:
            Save configuration after a managed update::

                spec = container.save_spec_to_db()
        """
        ...

    @abc.abstractmethod
    def delete_from_db(self) -> bool:
        """Delete this store's durable configuration row.

        Examples:
            Remove configuration after unregistering a retired store::

                removed = container.delete_from_db()
        """
        ...
