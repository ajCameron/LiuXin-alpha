"""
Configure native S3 Stores and map placement hints to object metadata.

The raw driver owns object transfer, staging, inventory bounds, and scoped
addresses. This facade owns Store configuration, client construction policy,
ingest declarations, and the JSON projection of liuxin-* metadata fields.
"""

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
    """
    Carry endpoint, multipart, staging, and inventory settings for an S3 driver.

    This frozen record does not validate or sanitize its field values. The driver validates transfer
    and inventory bounds; the SDK resolves credentials at runtime using the selected profile and its
    normal credential mechanisms.

    Example:
        >>> options = S3BackendOptions(region_name="eu-west-2")
        >>> options.profile_name is None
        True


    :ivar region_name: Optional SDK region override.
    :ivar endpoint_url: Optional S3-compatible endpoint URL passed to the SDK.
    :ivar profile_name: Optional SDK credential/configuration profile name.
    :ivar multipart_threshold: Object size in bytes at which multipart upload is selected.
    :ivar multipart_part_size: Requested multipart part size in bytes.
    :ivar local_staging_directory: Optional local directory for raw-driver staging.
    :ivar max_inventory_pages: Positive maximum pages consumed by inventory traversal.
    :ivar max_inventory_entries: Positive maximum object entries consumed by inventory traversal.
    :ivar max_inventory_page_entries: Positive bound on entries accepted from one inventory page.
    :ivar max_inventory_cursor_chars: Positive maximum inventory continuation-token length.
    """

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
    """
    Represent one S3 bucket or prefix with configured identity and native metadata hints.

    The facade owns an SDK client it creates, while an injected client remains borrowed. Constructor
    options configure the raw driver independently of an explicitly supplied StoreConfiguration;
    from_configuration reconstructs those options.

    Example:
        >>> store = S3Store("s3://books/primary", client=client)  # doctest: +SKIP
    """

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
        """
        Parse an S3 root and construct a raw driver with selected runtime options.

        Configuration is retained when supplied, without comparing its declared URI to url.
        Otherwise a configuration is built from the parsed bucket/prefix and all non-None option
        fields. No Store startup or probe is performed here, although SDK construction and
        raw-driver staging setup can have their own side effects.

        Example:
            >>> store = S3Store("s3://books/archive", client=client, options=options)  # doctest: +SKIP


        :param url: Credential-free s3://bucket[/prefix] URI selecting the physical endpoint namespace.
        :param name: Generated configuration name; None or empty derives bucket/prefix text.
        :param uuid: UUID or UUID text for generated configuration; None creates an identity.
        :param client: Borrowed S3 client, or None to create an owned boto3 client.
        :param options: Runtime driver/SDK options, or None for S3BackendOptions defaults.
        :param configuration: Existing configuration to retain; its UUID overrides uuid but its options are not automatically loaded here.
        :return: None after configuration, options, and the raw S3 driver are retained.
        """
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
        """
        Return the configuration retained for this bucket or prefix.

        Example:
            >>> store.configuration.store_kind  # doctest: +SKIP
            's3'


        :return: The configured Store identity and durable settings without a remote lookup.
        """
        return self._configuration

    @property
    def options(self) -> S3BackendOptions:
        """
        Return the selected runtime option record, without reconstructing it from configuration.

        Example:
            >>> store.options.multipart_part_size  # doctest: +SKIP


        :return: The S3BackendOptions instance used to build this Store driver.
        """
        return self._options

    @property
    def _driver(self) -> S3StorageDriver:
        """
        Supply the S3 driver to inherited Store routing and operation adapters.

        Example:
            >>> store._driver is store.driver  # doctest: +SKIP
            True


        :return: The existing raw driver scoped to this Store UUID.
        """
        return self.__driver

    @property
    def driver(self) -> S3StorageDriver:
        """
        Expose the shared raw S3 driver for native import and diagnostic workflows.

        Example:
            >>> driver = store.driver  # doctest: +SKIP


        :return: The owned driver, retaining its existing client-ownership policy.
        """
        return self.__driver

    @property
    def capabilities(self):
        """
        Advertise writable placement-hint support alongside inherited S3 capabilities.

        The flag reflects configured read-only policy. It does not probe credentials, endpoint
        health, or whether a particular metadata payload will be accepted.

        Example:
            >>> store.capabilities.placement_hints  # doctest: +SKIP
            True


        :return: Driver-backed capabilities with placement_hints enabled for writable configuration.
        """
        return dataclasses.replace(
            super().capabilities,
            placement_hints=not self.configuration.read_only,
        )

    @property
    def ingest_capabilities(self) -> IngestSourceCapabilities:
        """
        Declare inspected ingest metadata and authoritative SHA-256 support.

        Range, cursor, and version properties remain those of the inherited profile. Algorithm
        support does not promise that every remote object supplies a checksum.

        Example:
            >>> store.ingest_capabilities.authoritative_digest_algorithms  # doctest: +SKIP
            ('sha256',)


        :return: Inherited ingest profile with sha256 and INSPECTION metadata availability.
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
        """
        Encode allowed placement fields as JSON-valued liuxin-* S3 metadata entries.

        Entries follow the fixed allowlist order. Missing and None values are omitted; false, zero,
        and empty containers are retained. JSON uses ASCII escapes, and serialization errors
        propagate. This helper imposes no separate byte budget.

        Example:
            >>> store._native_write_metadata({"title": "Example"})  # doctest: +SKIP
            (('liuxin-title', '"Example"'),)


        :param placement_hints: Structured hints or supported mapping input, or None for no native metadata.
        :return: Tuple of prefixed, hyphenated field names and JSON text values.
        """
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
        """
        Translate raw hints and decode JSON from case-insensitive liuxin-* metadata names.

        Raw metadata remains visible. Invalid JSON is skipped only in the structured projection.
        Unlike writing, reading accepts every prefixed field name without an allowlist or
        value-schema check; later normalized duplicate names win.

        Example:
            >>> hints = store._file_hints(raw_hints)  # doctest: +SKIP
            >>> hints.placement_hints  # doctest: +SKIP


        :param hints: Raw driver hints whose filename/media type and metadata are routed by the base adapter.
        :return: FileHints retaining raw values and a decoded placement mapping, or None for that mapping when empty.
        """

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
                # but is not promoted to the structured placement projection.
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
        """
        Rebuild runtime options from a persisted configuration and construct an S3 Store.

        backend_options are passed as S3BackendOptions keyword arguments, so unknown names raise
        TypeError. An injected client is borrowed; otherwise the Store creates and owns its SDK
        client.

        Example:
            >>> store = S3Store.from_configuration(configuration, client=client)  # doctest: +SKIP


        :param configuration: Configuration supplying the URI, identity, policy, and serialized backend options.
        :param client: Borrowed client to inject, or None for the configured SDK client factory.
        :return: Unstarted S3Store retaining the supplied configuration.
        """
        return cls(
            configuration.store_root_uri,
            configuration=configuration,
            client=client,
            options=S3BackendOptions(**dict(configuration.backend_options)),
        )


def _parse_s3_root(value: str) -> tuple[str, str]:
    """
    Extract bucket authority and decoded prefix from an S3 URI.

    Reject missing buckets, other schemes, embedded credentials, queries, and fragments.
    Leading/trailing prefix slashes are removed after percent decoding. Detailed bucket and
    object-key validation belongs to the raw driver.

    Example:
        >>> _parse_s3_root("s3://books/my%20archive/")
        ('books', 'my archive')


    :param value: S3 root text; surrounding whitespace is stripped before URL parsing.
    :return: Bucket authority and prefix, with an empty prefix representing the bucket root.
    """
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
    """
    Format an S3 root from already selected bucket and prefix text without URL escaping.

    Example:
        >>> _s3_uri("books", "archive")
        's3://books/archive'
        >>> _s3_uri("books", "")
        's3://books'


    :param bucket: Bucket text to insert unchanged after s3://.
    :param prefix: Prefix text to append after a slash, or empty for the bucket root.
    :return: Interpolated URI text; this helper does not validate its components.
    """
    return f"s3://{bucket}" + (f"/{prefix}" if prefix else "")


def _default_s3_client(options: S3BackendOptions) -> Any:
    """
    Create a boto3 S3 client using the selected profile, region, and endpoint.

    The optional SDK is imported here. Missing boto3 becomes StorageUnavailable with installation
    context; other session and client-construction errors propagate.

    Example:
        >>> client = _default_s3_client(S3BackendOptions(profile_name="archive"))  # doctest: +SKIP


    :param options: Runtime settings whose profile, region, and endpoint are passed to boto3.
    :return: New SDK S3 client, whose lifecycle is owned by the constructing Store.
    """
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
