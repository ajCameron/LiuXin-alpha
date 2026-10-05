"""
Compose writable rclone staging and native URI import with configured Store behavior.

The shared adapter owns executable/environment/pacing policy. The writable driver
owns local and remote staging, while Store locations retain configured identity.
Native-import compatibility is a configuration heuristic; successful publication
and later metadata verification remain separate steps with observable partial effects.
"""

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
    """
    Expose staged create/replace/delete operations using the shared rclone invocation adapter.

    The constructor also creates the base read-only driver, then routes operations through a
    writable driver with local staging. Generic moves need not be atomic, and final verification can
    fail after publication. Supplied configuration is retained, including any read-only policy it
    declares.

    Example:
        >>> store = RcloneWritableStorageBackend("archive:", local_staging_directory="/tmp/rclone-stage")  # doctest: +SKIP
    """

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
        """
        Configure shared invocation policy and a writable raw driver, rejecting recognized
        config-less HTTP roots.

        Plain HTTP/HTTPS URLs normalize to the rejected :http, prefix. Other roots are not probed
        for writability. Omitted options disable default command-rate limits; supplied options
        retain their policy. Without supplied configuration, update the base snapshot to writable
        and include an explicit staging path. Supplied configuration is retained without
        reconciliation against runtime arguments. Writable-driver construction creates local staging
        but invokes no remote command.

        Example:
            >>> store = RcloneWritableStorageBackend("archive:", options=options)  # doctest: +SKIP


        :param url: Rclone root or HTTP URL passed through normalization; recognized config-less HTTP roots are read-only and rejected.
        :param name: Truthy display name used only when generating configuration.
        :param uuid: UUID or UUID text used only without supplied configuration; None creates a new identity.
        :param options: Mutable runtime settings; None creates defaults with max_http_requests_per_hour=0.
        :param local_staging_directory: Optional caller-managed staging directory; None lets the raw driver create an owned temporary directory.
        :param configuration: Optional retained StoreConfiguration whose UUID and Store policy take precedence over generated configuration.
        :return: None after configuring writable routing and local staging; local setup failures may propagate as storage errors.
        """
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
            max_inventory_entries=self.options.max_inventory_entries,
            max_json_token_chars=self.options.max_json_token_chars,
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
        """
        Supply the retained writable driver to inherited Store operations.

        Example:
            >>> store._driver is store.driver  # doctest: +SKIP
            True


        :return: Owned WritableRcloneStorageDriver created after base adapter initialization.
        """
        return self.__writable_driver

    @property
    def driver(self) -> WritableRcloneStorageDriver:
        """
        Expose the retained writable raw driver for transport-level reads, writes, and diagnostics.

        Example:
            >>> store.driver.capabilities.create  # doctest: +SKIP
            True


        :return: Shared WritableRcloneStorageDriver instance; property access performs no I/O.
        """
        return self.__writable_driver

    def locate(self, identifier: str | Location) -> Location:
        """
        Check an owned Location or resolve text through the writable driver's URI/key parsers.

        Matching root prefixes select URI parsing; other text is treated as a relative key. Parsing
        does not apply the driver's later reserved-staging operation guard or inspect object
        existence.

        Example:
            >>> store.locate("archive:books/book.epub").key  # doctest: +SKIP
            'books/book.epub'


        :param identifier: Owned Location, root-owned rclone identifier, or relative key text.
        :return: Opaque Location bound to this Store identity; later operations may reject reserved keys.
        """

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
        """
        Apply a configuration heuristic for whether another rclone Store is directly addressable.

        Reject other Store types. Accept colon-prefixed source roots or matching remote-name
        prefixes immediately; otherwise compare executable, argument tuple, and environment. This
        does not test access, digest availability, or whether matching names resolve to equivalent
        remote configuration.

        Example:
            >>> compatible = store.can_import_from(source)  # doctest: +SKIP


        :param source: Candidate source Store, including read-only or writable subclasses of the shared rclone adapter.
        :return: Whether the root/options heuristic accepts the source; this is not a remote connectivity or identity guarantee.
        """

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
        """
        Validate Store locations and delegate a native URI transfer with required reported identity.

        Require another rclone-backed Store, but do not call can_import_from or perform its option
        comparisons here. Placement hints are ignored. Source/destination ownership is checked
        before asking the raw driver to stage, publish, and compare remote size/digest evidence. A
        final failure may follow visible publication; this adapter does not independently stream
        source or destination bytes.

        Example:
            >>> info = store.import_from(source, source_location, destination, expected_size=4, expected_digest=digest)  # doctest: +SKIP


        :param source: Rclone-backed source Store providing a renderable object identifier.
        :param source_location: Location required to belong to the source Store.
        :param destination: Owned destination Location converted to a raw driver address.
        :param mode: Publication WriteMode forwarded unchanged to the raw native-transfer implementation.
        :param expected_size: Required byte count compared to remote staging and final metadata.
        :param expected_digest: Required algorithm/value compared to remote staging and final digest evidence.
        :param placement_hints: Accepted API-compatible hints deliberately ignored by this generic rclone adapter.
        :return: Store FileInfo translated from the verified reported metadata, without a cross-Store transaction or rollback guarantee.
        """

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
