"""Configured native S3-compatible Store."""

from __future__ import annotations

import dataclasses
import json

from collections.abc import Mapping
from typing import Any
from urllib.parse import unquote, urlsplit
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import (
    DriverBackedStoreAPI,
    DriverObjectHints,
    FileHints,
    IngestMetadataAvailability,
    IngestSourceCapabilities,
    StoragePlacementHints,
    StoreConfiguration,
    StorageUnavailable,
)
from LiuXin_alpha.storage.drivers.s3 import (
    DEFAULT_MAX_S3_INVENTORY_ENTRIES,
    DEFAULT_MAX_S3_INVENTORY_CURSOR_CHARS,
    DEFAULT_MAX_S3_INVENTORY_PAGE_ENTRIES,
    DEFAULT_MAX_S3_INVENTORY_PAGES,
    DEFAULT_MULTIPART_PART_SIZE,
    DEFAULT_MULTIPART_THRESHOLD,
    S3ClientAPI,
    S3ObjectAddress,
    S3StorageDriver,
)


@dataclasses.dataclass(slots=True, frozen=True)
class S3BackendOptions:
    """Non-secret runtime options for a native S3-compatible endpoint."""

    region_name: str | None = None
    endpoint_url: str | None = None
    profile_name: str | None = None
    multipart_threshold: int = DEFAULT_MULTIPART_THRESHOLD
    multipart_part_size: int = DEFAULT_MULTIPART_PART_SIZE
    local_staging_directory: str | None = None
    max_inventory_pages: int = DEFAULT_MAX_S3_INVENTORY_PAGES
    max_inventory_entries: int = DEFAULT_MAX_S3_INVENTORY_ENTRIES
    max_inventory_page_entries: int = DEFAULT_MAX_S3_INVENTORY_PAGE_ENTRIES
    max_inventory_cursor_chars: int = DEFAULT_MAX_S3_INVENTORY_CURSOR_CHARS


class S3Store(DriverBackedStoreAPI[S3ObjectAddress]):
    """One S3 bucket or bucket prefix with native object semantics."""

    store_kind = "s3"

    def __init__(
        self,
        url: str,
        *,
        name: str | None = None,
        uuid: str | UUID | None = None,
        client: S3ClientAPI | None = None,
        options: S3BackendOptions | None = None,
        configuration: StoreConfiguration | None = None,
    ) -> None:
        bucket, prefix = _parse_s3_root(url)
        selected_options = options or S3BackendOptions()
        store_uuid = (
            configuration.store_uuid
            if configuration is not None
            else (uuid4() if uuid is None else uuid if isinstance(uuid, UUID) else UUID(uuid))
        )
        self._configuration = configuration or StoreConfiguration(
            store_uuid=store_uuid,
            store_name=name or (f"{bucket}/{prefix}" if prefix else bucket),
            store_kind=self.store_kind,
            store_root_uri=_s3_uri(bucket, prefix),
            store_url=_s3_uri(bucket, prefix),
            store_access_protocol="s3",
            read_only=False,
            supports_folders=True,
            backend_options=tuple(
                (field.name, value)
                for field in dataclasses.fields(selected_options)
                if (value := getattr(selected_options, field.name)) is not None
            ),
        )
        self._options = selected_options
        owns_client = client is None
        selected_client = (
            _default_s3_client(selected_options)
            if client is None
            else client
        )
        self.__driver = S3StorageDriver(
            bucket,
            prefix=prefix,
            address_space_uuid=store_uuid,
            client=selected_client,
            multipart_threshold=selected_options.multipart_threshold,
            multipart_part_size=selected_options.multipart_part_size,
            local_staging_directory=selected_options.local_staging_directory,
            close_client=owns_client,
            max_inventory_pages=selected_options.max_inventory_pages,
            max_inventory_entries=selected_options.max_inventory_entries,
            max_inventory_page_entries=selected_options.max_inventory_page_entries,
            max_inventory_cursor_chars=selected_options.max_inventory_cursor_chars,
        )

    @property
    def configuration(self) -> StoreConfiguration:
        return self._configuration

    @property
    def options(self) -> S3BackendOptions:
        return self._options

    @property
    def _driver(self) -> S3StorageDriver:
        return self.__driver

    @property
    def driver(self) -> S3StorageDriver:
        return self.__driver

    @property
    def capabilities(self):
        return dataclasses.replace(
            super().capabilities,
            placement_hints=not self.configuration.read_only,
        )

    @property
    def ingest_capabilities(self) -> IngestSourceCapabilities:
        """Advertise S3 checksum, cursor, and inspection metadata support.

        Example:
            >>> profile = store.ingest_capabilities  # doctest: +SKIP
            >>> profile.authoritative_digest_algorithms  # doctest: +SKIP
            ('sha256',)

        :return: S3-specific source-ingest capability profile.
        """

        return dataclasses.replace(
            super().ingest_capabilities,
            authoritative_digest_algorithms=("sha256",),
            metadata_availability=IngestMetadataAvailability.INSPECTION,
        )

    def _native_write_metadata(
        self,
        placement_hints: StoragePlacementHints | None,
    ) -> tuple[tuple[str, str], ...]:
        if placement_hints is None:
            return ()
        hints = (
            placement_hints
            if isinstance(placement_hints, Mapping)
            else placement_hints.to_mapping()
        )
        allowed = (
            "title",
            "canonical_title",
            "sort_title",
            "subtitle",
            "media_type",
            "original_name",
            "role",
            "work_id",
            "work_type",
            "medium",
            "primary_agents",
            "series",
            "genres",
            "subjects",
            "languages",
            "labels",
            "manifestation_types",
            "file_formats",
            "preferred_folder_tokens",
            "preferred_filename_stem",
            "expression_id",
            "expression_type",
            "language_code",
            "edition_statement",
            "format_detail",
            "carrier_type",
            "publication_year",
            "item_id",
            "item_type",
            "item_location",
            "inventory_code",
            "lifecycle_status",
            "condition",
            "source",
            "source_name",
            "identifiers",
            "manifestation_id",
            "file_id",
        )
        return tuple(
            (f"liuxin-{key.replace('_', '-')}", json.dumps(hints[key], ensure_ascii=True))
            for key in allowed
            if key in hints and hints[key] is not None
        )

    def _file_hints(self, hints: DriverObjectHints) -> FileHints:
        """Decode LiuXin-owned S3 metadata into structured placement hints."""

        routed = super()._file_hints(hints)
        placement: dict[str, Any] = {}
        for name, encoded in hints.metadata:
            lowered = name.lower()
            if not lowered.startswith("liuxin-"):
                continue
            key = lowered.removeprefix("liuxin-").replace("-", "_")
            try:
                placement[key] = json.loads(encoded)
            except (TypeError, ValueError):
                # Malformed native metadata remains observable in ``metadata``
                # but is not promoted to the structured, trusted projection.
                continue
        return dataclasses.replace(
            routed,
            placement_hints=placement or None,
        )

    @classmethod
    def from_configuration(
        cls,
        configuration: StoreConfiguration,
        *,
        client: S3ClientAPI | None = None,
    ) -> S3Store:
        return cls(
            configuration.store_root_uri,
            configuration=configuration,
            client=client,
            options=S3BackendOptions(**dict(configuration.backend_options)),
        )


def _parse_s3_root(value: str) -> tuple[str, str]:
    parsed = urlsplit(str(value).strip())
    if parsed.scheme.lower() != "s3" or not parsed.netloc:
        raise ValueError("S3 Store root must be an s3://bucket[/prefix] URI.")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("S3 Store URIs must not embed credentials.")
    if parsed.query or parsed.fragment:
        raise ValueError("S3 Store URI must not contain query or fragment data.")
    prefix = unquote(parsed.path).strip("/")
    return parsed.netloc, prefix


def _s3_uri(bucket: str, prefix: str) -> str:
    return f"s3://{bucket}" + (f"/{prefix}" if prefix else "")


def _default_s3_client(options: S3BackendOptions) -> Any:
    try:
        import boto3
    except ImportError as error:
        raise StorageUnavailable(
            "native S3 storage requires the `s3` optional dependency (boto3)."
        ) from error
    session = boto3.Session(profile_name=options.profile_name)
    return session.client(
        "s3",
        region_name=options.region_name,
        endpoint_url=options.endpoint_url,
    )


__all__ = ["S3BackendOptions", "S3Store"]
