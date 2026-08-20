"""Configured writable Store powered by an arbitrary rclone remote."""

from __future__ import annotations

import dataclasses

from typing import Optional
from uuid import UUID

from LiuXin_alpha.storage.api import (
    Digest,
    FileInfo,
    Location,
    StoragePlacementHints,
    StorageInvalidAddress,
    StoreCoreAPI,
    StoreConfiguration,
    StoreUnsupportedOperation,
    WriteMode,
)
from LiuXin_alpha.storage.drivers.rclone import (
    RcloneObjectAddress,
    WritableRcloneStorageDriver,
)
from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly.rclone_http_storage_backend import (
    RcloneBackendOptions,
    RcloneHttpReadOnlyStorageBackend,
    _normalize_rclone_fs_root,
)


class RcloneWritableStorageBackend(RcloneHttpReadOnlyStorageBackend):
    """Expose one rclone remote with staged create, replace, and deletion."""

    store_kind = "rclone_writable"

    def __init__(
        self,
        url: str,
        *,
        name: Optional[str] = None,
        uuid: str | UUID | None = None,
        options: RcloneBackendOptions | None = None,
        local_staging_directory: str | None = None,
        configuration: StoreConfiguration | None = None,
    ) -> None:
        normalized = _normalize_rclone_fs_root(url)
        if normalized.startswith(":http,"):
            raise StorageInvalidAddress(
                "rclone's config-less HTTP remote is read-only."
            )
        super().__init__(
            normalized,
            name=name,
            uuid=uuid,
            options=(
                options
                or RcloneBackendOptions(max_http_requests_per_hour=0.0)
            ),
            configuration=configuration,
        )
        self.__writable_driver = WritableRcloneStorageDriver(
            self.url,
            address_space_uuid=self.store_ref,
            json_runner=lambda arguments: self.run_rclone_json(
                arguments,
                check=True,
            ),
            command_runner=lambda arguments: self.run_rclone(
                arguments,
                check=True,
            ),
            process_spawner=self.spawn_rclone_process,
            probe=self._probe_rclone,
            local_staging_directory=local_staging_directory,
        )
        if configuration is None:
            backend_options = self._configuration.backend_options
            if local_staging_directory is not None:
                backend_options += (
                    ("local_staging_directory", str(local_staging_directory)),
                )
            self._configuration = dataclasses.replace(
                self._configuration,
                store_kind=self.store_kind,
                read_only=False,
                backend_options=backend_options,
            )

    @property
    def _driver(self) -> WritableRcloneStorageDriver:
        return self.__writable_driver

    @property
    def driver(self) -> WritableRcloneStorageDriver:
        return self.__writable_driver

    def locate(self, identifier: str | Location) -> Location:
        """Accept a relative key or this writable remote's full identifier."""

        if isinstance(identifier, Location):
            return self.require_location(identifier)
        text = str(identifier)
        prefix = (
            self.url
            if self.url.endswith(":")
            else self.url.rstrip("/") + "/"
        )
        if text.startswith(prefix):
            return self._location(
                self.__writable_driver.object_address_from_uri(text)
            )
        return self._location(
            self.__writable_driver.parse_object_address(text)
        )

    def can_import_from(self, source: StoreCoreAPI) -> bool:
        """Return whether rclone can address the configured source directly."""

        if not isinstance(source, RcloneHttpReadOnlyStorageBackend):
            return False
        if source.url.startswith(":"):
            return True
        source_remote = source.url.split(":", 1)[0]
        destination_remote = self.url.split(":", 1)[0]
        if source_remote == destination_remote:
            return True
        return (
            source.options.rclone_exe == self.options.rclone_exe
            and tuple(source.options.rclone_args)
            == tuple(self.options.rclone_args)
            and source.options.env == self.options.env
        )

    def import_from(
        self,
        source: StoreCoreAPI,
        source_location: Location,
        destination: Location,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int,
        expected_digest: Digest,
        placement_hints: StoragePlacementHints | None = None,
    ) -> FileInfo:
        """Transfer one rclone-addressable source through remote staging."""

        if not isinstance(source, RcloneHttpReadOnlyStorageBackend):
            raise StoreUnsupportedOperation(
                "rclone native import requires another rclone-backed Store."
            )
        _ = placement_hints  # Generic rclone Stores do not persist hints.
        source.require_location(source_location)
        target = self._object_address(destination)
        source_uri = source.location_uri(source_location)
        if source_uri is None:
            raise StoreUnsupportedOperation(
                "source Store does not expose a credential-free rclone identifier."
            )
        info = self.__writable_driver.import_from_uri(
            source_uri,
            target,
            mode=mode,
            expected_size=expected_size,
            expected_digest=expected_digest,
        )
        return self._file_info(info)


__all__ = ["RcloneWritableStorageBackend"]
