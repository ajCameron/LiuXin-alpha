"""Canonical registry of configured storage backend kinds."""

from __future__ import annotations

import dataclasses
import os

from collections.abc import Callable, Iterator
from typing import Any, Literal
from urllib.parse import unquote_to_bytes, urlparse
from uuid import UUID

from LiuXin_alpha.storage import api


BackendBuilder = Callable[
    [api.StoreConfiguration, "StoreConstructionContext"],
    api.StoreAPI,
]


@dataclasses.dataclass(slots=True, frozen=True)
class StoreConstructionContext:
    """Runtime-only dependencies that must never be persisted as Store options."""

    s3_client: Any | None = None
    store_resolver: Callable[[api.StoreUUID], api.StoreAPI] | None = None
    encryption_key_provider: Any | None = None
    backing_path_resolver: Callable[[api.StoreConfiguration], str] | None = None


@dataclasses.dataclass(slots=True, frozen=True)
class StorageBackendDescriptor:
    """Construction and presentation metadata for one canonical backend kind."""

    kind: str
    label: str
    builder: BackendBuilder
    aliases: tuple[str, ...] = ()
    access_protocol: str = "file"
    access_protocol_aliases: tuple[str, ...] = ()
    read_only_default: bool = False
    location_type: Literal["dir", "file", "remote"] = "remote"
    supports_folders: bool = True
    supports_hierarchical_list: bool = True
    supports_random_read: bool = True
    supports_random_write: bool = False
    supports_delete: bool = False
    supports_checksums: bool = False
    supports_immutable_objects: bool = False
    user_selectable: bool = True
    presentation_order: int = 100
    policy_section: str | None = None
    characteristics: api.StorageCharacteristics = dataclasses.field(
        default_factory=api.StorageCharacteristics
    )


class StorageBackendRegistry:
    """Resolve aliases and construct Stores from one authoritative catalogue."""

    def __init__(
        self,
        descriptors: tuple[StorageBackendDescriptor, ...] = (),
    ) -> None:
        self._descriptors: dict[str, StorageBackendDescriptor] = {}
        self._aliases: dict[str, str] = {}
        for descriptor in descriptors:
            self.register(descriptor)

    def register(self, descriptor: StorageBackendDescriptor) -> None:
        canonical = normalize_backend_kind(descriptor.kind)
        if canonical != descriptor.kind:
            raise ValueError(
                f"backend kind must already be canonical: {descriptor.kind!r}."
            )
        names = (canonical, *descriptor.aliases)
        normalized_names = tuple(normalize_backend_kind(name) for name in names)
        collisions = [name for name in normalized_names if name in self._aliases]
        if collisions:
            raise ValueError(f"backend kind or alias is already registered: {collisions[0]!r}.")
        self._descriptors[canonical] = descriptor
        for name in normalized_names:
            self._aliases[name] = canonical

    def descriptor(self, kind: str) -> StorageBackendDescriptor:
        normalized = normalize_backend_kind(kind)
        try:
            return self._descriptors[self._aliases[normalized]]
        except KeyError as error:
            raise api.StoreUnsupportedOperation(
                f"no Store factory is registered for kind {kind!r}."
            ) from error

    def canonical_kind(self, kind: str) -> str:
        return self.descriptor(kind).kind

    def build(
        self,
        configuration: api.StoreConfiguration,
        *,
        context: StoreConstructionContext | None = None,
    ) -> api.StoreAPI:
        descriptor = self.descriptor(configuration.store_kind)
        construction_context = context or StoreConstructionContext()
        if configuration.backing is not None:
            if not configuration.read_only:
                raise api.StoreUnsupportedOperation(
                    "an Asset-backed Store must be read-only."
                )
            if not descriptor.read_only_default or descriptor.location_type != "file":
                raise api.StoreUnsupportedOperation(
                    f"backend {descriptor.kind!r} cannot expose a read-only "
                    "Store backed by a Digital Asset."
                )
            if construction_context.backing_path_resolver is None:
                raise api.StoreUnsupportedOperation(
                    "Asset-backed storage requires a runtime backing-path resolver."
                )
        return descriptor.builder(configuration, construction_context)

    def iter_descriptors(
        self,
        *,
        user_selectable_only: bool = False,
    ) -> Iterator[StorageBackendDescriptor]:
        for descriptor in sorted(
            self._descriptors.values(),
            key=lambda item: (item.presentation_order, item.kind),
        ):
            if not user_selectable_only or descriptor.user_selectable:
                yield descriptor

    def __iter__(self) -> Iterator[StorageBackendDescriptor]:
        return self.iter_descriptors()


def normalize_backend_kind(kind: str) -> str:
    """
    Normalize a storage backend identifier for registry lookup.


    :param kind:
    :return:
    """
    normalized = str(kind).strip().lower().replace("-", "_")
    if not normalized:
        raise ValueError("backend kind must not be empty.")
    return normalized


def _common(configuration: api.StoreConfiguration) -> dict[str, object]:
    return {
        "name": configuration.store_name,
        "uuid": configuration.store_uuid,
    }


def _options(configuration: api.StoreConfiguration) -> dict[str, object]:
    return dict(configuration.backend_options)


def _container_path(
    configuration: api.StoreConfiguration,
    context: StoreConstructionContext,
) -> str:
    """Resolve an archive image from its direct URI or backing Asset."""

    if configuration.backing is None:
        return _local_path(configuration.store_root_uri)
    resolver = context.backing_path_resolver
    if resolver is None:
        raise api.StoreUnsupportedOperation(
            "Asset-backed storage requires a runtime backing-path resolver."
        )
    return resolver(configuration)


def _build_filesystem(configuration, _context):
    from LiuXin_alpha.storage.stores import FilesystemStore

    return FilesystemStore.from_configuration(configuration)


def _build_managed(configuration, _context):
    from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive import (
        OnDiskExistingManagedStorageBackend,
    )

    return OnDiskExistingManagedStorageBackend(
        configuration.store_root_uri,
        **_common(configuration),
    )


def _build_unmanaged(configuration, _context):
    from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive import (
        OnDiskUnmanagedStorageBackend,
    )

    return OnDiskUnmanagedStorageBackend(
        configuration.store_root_uri,
        **_common(configuration),
    )


def _build_flat(configuration, _context):
    from LiuXin_alpha.storage.store_backend_plugins.on_disk_flat import (
        OnDiskFlatStorageBackend,
    )

    return OnDiskFlatStorageBackend(configuration.store_root_uri, **_common(configuration))


def _build_calibre_like(configuration, _context):
    from LiuXin_alpha.storage.store_backend_plugins.on_disk_calibre_like import (
        OnDiskCalibreLikeStorageBackend,
    )

    return OnDiskCalibreLikeStorageBackend(
        configuration.store_root_uri,
        **_common(configuration),
    )


def _build_sqlite(configuration, _context):
    from LiuXin_alpha.storage.store_backend_plugins.single_file_sqlite import (
        SingleFileSqliteStorageBackend,
    )

    return SingleFileSqliteStorageBackend(
        configuration.store_root_uri,
        **_common(configuration),
    )


def _build_http(configuration, _context):
    from LiuXin_alpha.storage.stores import HttpReadOnlyStore

    options = _options(configuration)
    return HttpReadOnlyStore(
        configuration.store_root_uri,
        store_kind="http_readonly",
        timeout_s=options.get("timeout_s", 30.0),
        max_requests_per_hour=options.get("max_requests_per_hour"),
        max_inventory_entries=options.get("max_inventory_entries", 100_000),
        **_common(configuration),
    )


def _build_native_html(configuration, _context):
    from LiuXin_alpha.storage.store_backend_plugins.native_html_readonly import (
        NativeHtmlBackendOptions,
        NativeHtmlReadOnlyStorageBackend,
    )

    return NativeHtmlReadOnlyStorageBackend(
        configuration.store_root_uri,
        options=NativeHtmlBackendOptions(**_options(configuration)),
        **_common(configuration),
    )


def _build_wget_html(configuration, _context):
    from LiuXin_alpha.storage.store_backend_plugins.wget_html_readonly import (
        WgetBackendOptions,
        WgetHtmlReadOnlyStorageBackend,
    )

    return WgetHtmlReadOnlyStorageBackend(
        configuration.store_root_uri,
        options=WgetBackendOptions(**_options(configuration)),
        **_common(configuration),
    )


def _build_ftp(configuration, _context):
    from LiuXin_alpha.storage.store_backend_plugins.ftp_readonly import (
        FtpBackendOptions,
        FtpReadOnlyStorageBackend,
    )

    return FtpReadOnlyStorageBackend(
        configuration.store_root_uri,
        options=FtpBackendOptions(**_options(configuration)),
        **_common(configuration),
    )


def _build_rclone_readonly(configuration, _context):
    from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import (
        RcloneBackendOptions,
        RcloneHttpReadOnlyStorageBackend,
    )

    return RcloneHttpReadOnlyStorageBackend(
        configuration.store_root_uri,
        options=RcloneBackendOptions(**_options(configuration)),
        configuration=configuration,
    )


def _build_rclone_writable(configuration, _context):
    from LiuXin_alpha.storage.store_backend_plugins.rclone_http_readonly import (
        RcloneBackendOptions,
    )
    from LiuXin_alpha.storage.store_backend_plugins.rclone_writable import (
        RcloneWritableStorageBackend,
    )

    options = _options(configuration)
    staging = options.pop("local_staging_directory", None)
    return RcloneWritableStorageBackend(
        configuration.store_root_uri,
        options=RcloneBackendOptions(**options),
        local_staging_directory=(None if staging is None else str(staging)),
        configuration=configuration,
    )


def _build_s3(configuration, context):
    from LiuXin_alpha.storage.stores import S3Store

    return S3Store.from_configuration(configuration, client=context.s3_client)


def _build_squashfs_readonly(configuration, context):
    from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly import (
        SquashfsReadOnlyStorageBackend,
    )

    return SquashfsReadOnlyStorageBackend(
        _container_path(configuration, context),
        configuration=configuration,
        **_options(configuration),
    )


def _build_iso_readonly(configuration, context):
    from LiuXin_alpha.storage.store_backend_plugins.iso_readonly import (
        IsoReadOnlyStorageBackend,
    )

    return IsoReadOnlyStorageBackend(
        _container_path(configuration, context),
        configuration=configuration,
        **_options(configuration),
    )


def _build_iso_writable(configuration, _context):
    """
    Construct one writable ISO Store from durable configuration.

    Example:
        >>> store = _build_iso_writable(configuration, context)  # doctest: +SKIP


    :param configuration:
    :param _context:
    :return:
    """

    from LiuXin_alpha.storage.store_backend_plugins.iso_writable import (
        IsoWritableStorageBackend,
    )

    return IsoWritableStorageBackend(
        _local_path(configuration.store_root_uri),
        configuration=configuration,
        **_options(configuration),
    )


def _build_zip_readonly(configuration, context):
    from LiuXin_alpha.storage.store_backend_plugins.zip_readonly import (
        ZipReadOnlyStorageBackend,
    )

    return ZipReadOnlyStorageBackend(
        _container_path(configuration, context),
        configuration=configuration,
        **_options(configuration),
    )


def _build_zip_writable(configuration, _context):
    from LiuXin_alpha.storage.store_backend_plugins.zip_writable import (
        ZipWritableStorageBackend,
    )

    return ZipWritableStorageBackend(
        _local_path(configuration.store_root_uri),
        configuration=configuration,
        **_options(configuration),
    )


def _build_tar_readonly(configuration, context):
    from LiuXin_alpha.storage.store_backend_plugins.tar_readonly import (
        TarReadOnlyStorageBackend,
    )

    return TarReadOnlyStorageBackend(
        _container_path(configuration, context),
        configuration=configuration,
        **_options(configuration),
    )


def _build_tar_writable(configuration, _context):
    from LiuXin_alpha.storage.store_backend_plugins.tar_writable import (
        TarWritableStorageBackend,
    )

    return TarWritableStorageBackend(
        _local_path(configuration.store_root_uri),
        configuration=configuration,
        **_options(configuration),
    )


def _build_rar_readonly(configuration, context):
    from LiuXin_alpha.storage.store_backend_plugins.rar_readonly import (
        RarReadOnlyStorageBackend,
    )

    return RarReadOnlyStorageBackend(
        _container_path(configuration, context),
        configuration=configuration,
        **_options(configuration),
    )


def _build_sevenzip_readonly(configuration, context):
    """
    Construct one optional-dependency read-only 7z Store.

    Example:
        >>> store = _build_sevenzip_readonly(configuration, context)  # doctest: +SKIP


    :param configuration:
    :param _context:
    :return:
    """

    from LiuXin_alpha.storage.store_backend_plugins.sevenzip_readonly import (
        SevenZipReadOnlyStorageBackend,
    )

    return SevenZipReadOnlyStorageBackend(
        _container_path(configuration, context),
        configuration=configuration,
        **_options(configuration),
    )


def _build_rar_build(configuration, _context):
    """
    Recreate one durable build-once RAR staging Store.

    Example:
        >>> store = _build_rar_build(configuration, context)  # doctest: +SKIP


    :param configuration:
    :param _context:
    :return:
    """

    from LiuXin_alpha.storage.store_backend_plugins.rar_build import (
        RarBuildStorageBackend,
    )

    return RarBuildStorageBackend(
        _local_path(configuration.store_root_uri),
        configuration=configuration,
        **_options(configuration),
    )


def _build_squashfs_build(configuration, _context):
    from LiuXin_alpha.storage.store_backend_plugins.squashfs_build import (
        SquashfsBuildStorageBackend,
    )

    return SquashfsBuildStorageBackend(
        _local_path(configuration.store_root_uri),
        configuration=configuration,
        **_options(configuration),
    )


def _build_encrypted(configuration, context):
    from LiuXin_alpha.storage.stores import EncryptedStore

    if context.store_resolver is None:
        raise api.StoreUnsupportedOperation(
            "encrypted storage requires a runtime inner-Store resolver."
        )
    if context.encryption_key_provider is None:
        raise api.StoreUnsupportedOperation(
            "encrypted storage requires a runtime encryption key provider."
        )
    options = _options(configuration)
    raw_inner_ref = options.pop("inner_store_uuid", None)
    if raw_inner_ref is None:
        raw_inner_ref = _encrypted_inner_ref(configuration.store_root_uri)
    try:
        inner_ref = UUID(str(raw_inner_ref))
    except (TypeError, ValueError) as error:
        raise ValueError(
            "encrypted Store configuration requires an inner_store_uuid option."
        ) from error
    options.pop("key_id", None)
    allowed_options = {
        "chunk_size",
        "forward_placement_hints",
        "inner_prefix",
        "local_staging_directory",
    }
    unknown_options = sorted(set(options) - allowed_options)
    if unknown_options:
        raise ValueError(
            "unsupported encrypted Store option: "
            + ", ".join(unknown_options)
        )
    return EncryptedStore(
        context.store_resolver(inner_ref),
        key_provider=context.encryption_key_provider,
        configuration=configuration,
        **options,
    )


def _encrypted_inner_ref(root_uri: str) -> str | None:
    parsed = urlparse(root_uri)
    if parsed.scheme != "encrypted":
        return None
    return parsed.netloc or parsed.path.strip("/") or None


def _local_path(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme == "file":
        if parsed.netloc not in {"", "localhost"}:
            raise ValueError("file Store URI must refer to the local host.")
        # File URIs quote the original filesystem bytes.  Decode them through
        # the platform codec so POSIX surrogate-escaped names survive DB
        # configuration -> backend reconstruction without U+FFFD replacement.
        return os.fsdecode(unquote_to_bytes(parsed.path))
    return value


def _descriptor(
    kind: str,
    label: str,
    builder: BackendBuilder,
    *,
    aliases: tuple[str, ...] = (),
    access_protocol: str,
    read_only: bool,
    location_type: Literal["dir", "file", "remote"],
    folders: bool = True,
    hierarchical: bool = True,
    random_write: bool = False,
    delete: bool = False,
    checksums: bool = False,
    immutable: bool = False,
    order: int = 100,
    policy_section: str | None = None,
    access_protocol_aliases: tuple[str, ...] = (),
    user_selectable: bool = True,
    characteristics: api.StorageCharacteristics | None = None,
) -> StorageBackendDescriptor:
    return StorageBackendDescriptor(
        kind=kind,
        label=label,
        builder=builder,
        aliases=aliases,
        access_protocol=access_protocol,
        access_protocol_aliases=access_protocol_aliases,
        read_only_default=read_only,
        location_type=location_type,
        supports_folders=folders,
        supports_hierarchical_list=hierarchical,
        supports_random_write=random_write,
        supports_delete=delete,
        supports_checksums=checksums,
        supports_immutable_objects=immutable,
        presentation_order=order,
        policy_section=policy_section,
        user_selectable=user_selectable,
        characteristics=(
            api.StorageCharacteristics()
            if characteristics is None
            else characteristics
        ),
    )


def _per_object_characteristics(
    *limitations: api.StorageLimitation,
) -> api.StorageCharacteristics:
    """Return the common staged per-object backend profile."""

    return api.StorageCharacteristics(
        publication_model=api.StoragePublicationModel.PER_OBJECT,
        temporary_space=api.StorageTemporarySpaceRequirement.OBJECT_STAGE,
        recommended_write_usage=api.StorageWriteUsage.GENERAL,
        preserves_unmodelled_entries=True,
        rewrites_container_format=False,
        limitations=limitations,
    )


def _read_only_characteristics(
    *limitations: api.StorageLimitation,
) -> api.StorageCharacteristics:
    """Return the common read-only backend profile."""

    return api.StorageCharacteristics(
        publication_model=api.StoragePublicationModel.READ_ONLY,
        temporary_space=api.StorageTemporarySpaceRequirement.NONE,
        recommended_write_usage=api.StorageWriteUsage.NOT_APPLICABLE,
        limitations=limitations,
    )


DEFAULT_BACKEND_REGISTRY = StorageBackendRegistry(
    (
        _descriptor(
            "filesystem", "Local folder (read/write)", _build_filesystem,
            aliases=("file", "on_disk"), access_protocol="file", read_only=False,
            location_type="dir", random_write=True, delete=True, checksums=True,
            characteristics=_per_object_characteristics(),
        ),
        _descriptor(
            "on_disk_existing_managed_drive", "Managed local folder (read/write)",
            _build_managed, aliases=("on_disk_existing_managed", "managed_drive"),
            access_protocol="file", read_only=False, location_type="dir",
            random_write=True, delete=True, checksums=True, order=10,
            characteristics=_per_object_characteristics(),
        ),
        _descriptor(
            "on_disk_existing_unmanaged_drive", "Existing unmanaged local folder (read-only)",
            _build_unmanaged,
            aliases=("on_disk_existing_unmanaged", "on_disk_unmanaged", "unmanaged_drive"),
            access_protocol="file", read_only=True, location_type="dir", checksums=True,
            order=20,
            characteristics=_read_only_characteristics(),
        ),
        _descriptor(
            "on_disk_flat", "Flat content-addressed local folder", _build_flat,
            aliases=("flat", "flat_store"), access_protocol="file", read_only=False,
            location_type="dir", folders=False, hierarchical=False,
            random_write=True, delete=True, checksums=True, immutable=True,
            characteristics=_per_object_characteristics(),
        ),
        _descriptor(
            "on_disk_calibre_like", "Calibre-like rich local folder", _build_calibre_like,
            aliases=("calibre_like",), access_protocol="file", read_only=False,
            location_type="dir", random_write=True, delete=True, checksums=True,
            order=30,
            characteristics=_per_object_characteristics(),
        ),
        _descriptor(
            "single_file_sqlite", "Single-file SQLite blob store", _build_sqlite,
            aliases=("sqlite", "sqlite_blob", "sqlite_store"), access_protocol="sqlite",
            read_only=False, location_type="file", folders=False, hierarchical=False,
            random_write=True, delete=True, checksums=True, order=40,
            characteristics=_per_object_characteristics(),
        ),
        _descriptor(
            "http_readonly", "Direct HTTP root (read-only)", _build_http,
            aliases=("http",), access_protocol="http",
            access_protocol_aliases=("https",), read_only=True,
            location_type="remote", hierarchical=False, policy_section="http",
            characteristics=_read_only_characteristics(),
        ),
        _descriptor(
            "native_html_readonly", "Native HTML crawler remote (read-only)",
            _build_native_html, aliases=("native_html",), access_protocol="native_html",
            read_only=True, location_type="remote", order=80,
            policy_section="native_html",
            characteristics=_read_only_characteristics(),
        ),
        _descriptor(
            "wget_html_readonly", "Wget HTML spider remote (read-only)", _build_wget_html,
            aliases=("wget_html",), access_protocol="wget", read_only=True,
            location_type="remote", order=70, policy_section="wget",
            characteristics=_read_only_characteristics(),
        ),
        _descriptor(
            "ftp_readonly", "FTP/FTPS remote (read-only)", _build_ftp,
            aliases=("ftp", "ftps", "ftps_readonly"), access_protocol="ftp",
            access_protocol_aliases=("ftps",),
            read_only=True, location_type="remote", checksums=True,
            policy_section="ftp",
            characteristics=_read_only_characteristics(),
        ),
        _descriptor(
            "rclone_http_readonly", "Rclone remote (read-only)", _build_rclone_readonly,
            aliases=("rclone", "rclone_readonly"), access_protocol="rclone",
            read_only=True, location_type="remote", checksums=True, order=60,
            policy_section="rclone",
            characteristics=_read_only_characteristics(),
        ),
        _descriptor(
            "rclone_writable", "Rclone remote (read/write)", _build_rclone_writable,
            aliases=("rclone_readwrite",), access_protocol="rclone", read_only=False,
            location_type="remote", random_write=True, delete=True, checksums=True,
            policy_section="rclone",
            characteristics=_per_object_characteristics(
                api.StorageLimitation(
                    "rclone_backend_dependent_limits",
                    "Object limits and publication atomicity depend on the selected rclone backend.",
                ),
            ),
        ),
        _descriptor(
            "s3", "Native S3-compatible bucket", _build_s3,
            aliases=("s3_compatible",), access_protocol="s3", read_only=False,
            location_type="remote", random_write=True, delete=True, checksums=True,
            policy_section="s3",
            characteristics=_per_object_characteristics(
                api.StorageLimitation(
                    "s3_service_limits_apply",
                    "Object and multipart limits are imposed by the configured S3-compatible service.",
                ),
            ),
        ),
        _descriptor(
            "squashfs_build", "Buildable SquashFS archive", _build_squashfs_build,
            aliases=("squashfs_backup", "open_squashfs_store"),
            access_protocol="squashfs-build", read_only=False, location_type="file",
            random_write=True, delete=True, checksums=True,
            policy_section="squashfs_build",
            characteristics=api.StorageCharacteristics(
                publication_model=api.StoragePublicationModel.STAGING_THEN_SEAL,
                temporary_space=api.StorageTemporarySpaceRequirement.STORE_COPY,
                recommended_write_usage=api.StorageWriteUsage.ARCHIVAL_SNAPSHOT,
                max_object_bytes=4 * 1024 * 1024 * 1024,
                max_component_bytes=65_535,
                max_path_depth=256,
                preserves_unmodelled_entries=True,
                rewrites_container_format=True,
                limitations=(
                    api.StorageLimitation(
                        "explicit_seal_required",
                        "Staged objects enter the SquashFS archive only after seal().",
                    ),
                    api.StorageLimitation(
                        "sealed_store_read_only",
                        "A successfully sealed staging Store refuses further mutation.",
                    ),
                    api.StorageLimitation(
                        "external_mksquashfs_required",
                        "Sealing requires a compatible mksquashfs executable.",
                    ),
                    api.StorageLimitation(
                        "validated_bounded_seal",
                        "Sealing preflights the staging tree and verifies candidate inventory and bytes within configured expansion limits.",
                    ),
                    api.StorageLimitation(
                        "nested_expansion_budget_external",
                        "Recursive ingest must impose its own cumulative cross-container budget.",
                    ),
                ),
            ),
        ),
        _descriptor(
            "squashfs_readonly", "Read-only SquashFS archive", _build_squashfs_readonly,
            aliases=("squashfs", "sealed_squashfs"), access_protocol="squashfs",
            read_only=True, location_type="file", checksums=True, immutable=True,
            order=50, policy_section="squashfs",
            characteristics=api.StorageCharacteristics(
                publication_model=api.StoragePublicationModel.READ_ONLY,
                temporary_space=api.StorageTemporarySpaceRequirement.OBJECT_STAGE,
                recommended_write_usage=api.StorageWriteUsage.NOT_APPLICABLE,
                max_object_bytes=4 * 1024 * 1024 * 1024,
                max_component_bytes=65_535,
                max_path_depth=256,
                limitations=(
                    api.StorageLimitation(
                        "unsafe_members_rejected",
                        "Non-regular, ambiguous, escaping, or conflicting members reject the archive.",
                    ),
                    api.StorageLimitation(
                        "regular_files_only",
                        "The exposed projection contains regular files only; other member types reject the archive.",
                    ),
                    api.StorageLimitation(
                        "external_unsquashfs_required",
                        "Reads and inventory require a compatible unsquashfs executable.",
                    ),
                    api.StorageLimitation(
                        "squashfs_member_reads_spooled",
                        "Members are size-verified in bounded temporary storage before ranges are returned.",
                    ),
                    api.StorageLimitation(
                        "bounded_squashfs_expansion",
                        "Inventory header, member size, total expansion, compression ratio, path depth, and entry count are bounded.",
                    ),
                    api.StorageLimitation(
                        "nested_expansion_budget_external",
                        "Recursive ingest must impose its own cumulative cross-container budget.",
                    ),
                ),
            ),
        ),
        _descriptor(
            "iso_writable", "ISO image (read/write)", _build_iso_writable,
            aliases=("iso_readwrite", "iso_rw", "iso_build"),
            access_protocol="iso-write",
            access_protocol_aliases=("iso-rw",),
            read_only=False, location_type="file",
            random_write=False, delete=True, checksums=True,
            order=51, policy_section="iso_writable",
            characteristics=api.StorageCharacteristics(
                publication_model=api.StoragePublicationModel.WHOLE_STORE_REBUILD,
                temporary_space=api.StorageTemporarySpaceRequirement.STORE_COPY,
                recommended_write_usage=api.StorageWriteUsage.ARCHIVAL_SNAPSHOT,
                max_object_bytes=(1 << 32) - 1,
                max_component_bytes=255,
                max_path_depth=256,
                preserves_unmodelled_entries=False,
                rewrites_container_format=True,
                limitations=(
                    api.StorageLimitation(
                        "whole_store_rebuild",
                        "Each mutation atomically rebuilds the complete ISO image.",
                    ),
                    api.StorageLimitation(
                        "regular_files_only",
                        "Rebuilds retain only regular-file keys and bytes.",
                    ),
                    api.StorageLimitation(
                        "bounded_iso_logical_expansion",
                        "Member size, total logical bytes, path size, parser metadata, and all-entry count are bounded before rebuild publication.",
                    ),
                    api.StorageLimitation(
                        "nested_expansion_budget_external",
                        "Recursive ingest must impose its own cumulative cross-container budget.",
                    ),
                ),
            ),
        ),
        _descriptor(
            "iso_readonly", "Read-only ISO image", _build_iso_readonly,
            aliases=("iso", "iso9660", "joliet", "rock_ridge", "udf"),
            access_protocol="iso",
            access_protocol_aliases=("iso9660", "joliet", "rock-ridge", "udf"),
            read_only=True, location_type="file",
            checksums=True, immutable=True, order=52, policy_section="iso",
            characteristics=api.StorageCharacteristics(
                publication_model=api.StoragePublicationModel.READ_ONLY,
                temporary_space=api.StorageTemporarySpaceRequirement.OBJECT_STAGE,
                recommended_write_usage=api.StorageWriteUsage.NOT_APPLICABLE,
                max_object_bytes=8 * 1024 * 1024 * 1024,
                max_component_bytes=65_535,
                max_path_depth=256,
                limitations=(
                    api.StorageLimitation(
                        "unsafe_members_rejected",
                        "Non-regular, ambiguous, escaping, or conflicting members reject the selected namespace.",
                    ),
                    api.StorageLimitation(
                        "optional_pycdlib_required_for_udf",
                        "UDF namespace inventory and reads require the optional pycdlib dependency.",
                    ),
                    api.StorageLimitation(
                        "udf_member_reads_spooled",
                        "UDF members are staged in private temporary storage before ranges are returned.",
                    ),
                    api.StorageLimitation(
                        "udf_only_images_unsupported",
                        "The optional UDF reader requires an ISO/UDF bridge image; UDF-only images remain unsupported.",
                    ),
                    api.StorageLimitation(
                        "zisofs_unsupported",
                        "zisofs-compressed members are unsupported.",
                    ),
                    api.StorageLimitation(
                        "bounded_iso_logical_expansion",
                        "Member size, total logical bytes, image expansion ratio, path size, parser metadata, and all-entry count are bounded.",
                    ),
                    api.StorageLimitation(
                        "nested_expansion_budget_external",
                        "Recursive ingest must impose its own cumulative cross-container budget.",
                    ),
                ),
            ),
        ),
        _descriptor(
            "zip_writable", "ZIP archive (read/write)", _build_zip_writable,
            aliases=("zip_readwrite", "zip_rw", "zip_build"),
            access_protocol="zip-write",
            access_protocol_aliases=("zip-rw",),
            read_only=False, location_type="file",
            random_write=False, delete=True, checksums=True,
            order=53, policy_section="zip_writable",
            characteristics=api.StorageCharacteristics(
                publication_model=api.StoragePublicationModel.WHOLE_STORE_REBUILD,
                temporary_space=api.StorageTemporarySpaceRequirement.STORE_COPY,
                recommended_write_usage=api.StorageWriteUsage.ARCHIVAL_SNAPSHOT,
                max_object_bytes=4 * 1024 * 1024 * 1024,
                max_component_bytes=65_535,
                max_path_depth=256,
                preserves_unmodelled_entries=False,
                rewrites_container_format=True,
                limitations=(
                    api.StorageLimitation(
                        "whole_store_rebuild",
                        "Each mutation atomically rebuilds the complete ZIP archive.",
                    ),
                    api.StorageLimitation(
                        "unsafe_members_rejected",
                        "Non-regular, ambiguous, escaping, or conflicting members reject the archive.",
                    ),
                    api.StorageLimitation(
                        "encrypted_members_unsupported",
                        "Password-encrypted and multi-disk ZIP members are unsupported.",
                    ),
                    api.StorageLimitation(
                        "metadata_normalized_on_rebuild",
                        "ZIP container and member metadata are normalized on rebuild.",
                    ),
                    api.StorageLimitation(
                        "bounded_zip_expansion",
                        "Entry count, central-directory size, member size, total expanded size, "
                        "and per-member compression ratio are bounded before reads or rebuilds.",
                    ),
                    api.StorageLimitation(
                        "nested_expansion_budget_external",
                        "Recursive ingest must impose its own cumulative cross-container budget.",
                    ),
                ),
            ),
        ),
        _descriptor(
            "zip_readonly", "Read-only ZIP archive", _build_zip_readonly,
            aliases=("zip",), access_protocol="zip",
            read_only=True, location_type="file", checksums=True,
            immutable=True, order=54, policy_section="zip_readonly",
            characteristics=api.StorageCharacteristics(
                publication_model=api.StoragePublicationModel.READ_ONLY,
                temporary_space=api.StorageTemporarySpaceRequirement.NONE,
                recommended_write_usage=api.StorageWriteUsage.NOT_APPLICABLE,
                max_object_bytes=4 * 1024 * 1024 * 1024,
                max_component_bytes=65_535,
                max_path_depth=256,
                limitations=(
                    api.StorageLimitation(
                        "unsafe_members_rejected",
                        "Non-regular, ambiguous, escaping, or conflicting members reject the archive.",
                    ),
                    api.StorageLimitation(
                        "encrypted_members_unsupported",
                        "Password-encrypted and multi-disk ZIP members are unsupported.",
                    ),
                    api.StorageLimitation(
                        "archive_wide_version",
                        "Any archive replacement changes every member version token.",
                    ),
                    api.StorageLimitation(
                        "bounded_zip_expansion",
                        "Entry count, central-directory size, member size, total expanded size, "
                        "and per-member compression ratio are bounded before reads.",
                    ),
                    api.StorageLimitation(
                        "nested_expansion_budget_external",
                        "Recursive ingest must impose its own cumulative cross-container budget.",
                    ),
                ),
            ),
        ),
        _descriptor(
            "tar_writable", "TAR archive (read/write)", _build_tar_writable,
            aliases=("tar_readwrite", "tar_rw", "tar_build"),
            access_protocol="tar-write",
            access_protocol_aliases=("tar-rw",),
            read_only=False, location_type="file",
            random_write=False, delete=True, checksums=True,
            order=55, policy_section="tar_writable",
            characteristics=api.StorageCharacteristics(
                publication_model=api.StoragePublicationModel.WHOLE_STORE_REBUILD,
                temporary_space=api.StorageTemporarySpaceRequirement.STORE_COPY,
                recommended_write_usage=api.StorageWriteUsage.ARCHIVAL_SNAPSHOT,
                max_object_bytes=4 * 1024 * 1024 * 1024,
                max_path_depth=256,
                preserves_unmodelled_entries=False,
                rewrites_container_format=True,
                limitations=(
                    api.StorageLimitation(
                        "whole_store_rebuild",
                        "Each mutation atomically rebuilds the complete TAR archive.",
                    ),
                    api.StorageLimitation(
                        "unsafe_members_rejected",
                        "Non-regular, ambiguous, escaping, or conflicting members reject the archive.",
                    ),
                    api.StorageLimitation(
                        "metadata_normalized_on_rebuild",
                        "TAR headers, ownership, permissions, and extended metadata are normalized on rebuild.",
                    ),
                    api.StorageLimitation(
                        "compressed_tar_rebuild_cost",
                        "Compressed TAR mutation recompresses every retained member.",
                    ),
                    api.StorageLimitation(
                        "bounded_tar_expansion",
                        "Member size, aggregate expansion, compression ratio, parser metadata, and entry count are bounded.",
                    ),
                    api.StorageLimitation(
                        "nested_expansion_budget_external",
                        "Recursive ingest must impose its own cumulative cross-container budget.",
                    ),
                ),
            ),
        ),
        _descriptor(
            "tar_readonly", "Read-only TAR archive", _build_tar_readonly,
            aliases=("tar", "tgz", "tar_gz"), access_protocol="tar",
            read_only=True, location_type="file", checksums=True,
            immutable=True, order=56, policy_section="tar_readonly",
            characteristics=api.StorageCharacteristics(
                publication_model=api.StoragePublicationModel.READ_ONLY,
                temporary_space=api.StorageTemporarySpaceRequirement.NONE,
                recommended_write_usage=api.StorageWriteUsage.NOT_APPLICABLE,
                max_object_bytes=4 * 1024 * 1024 * 1024,
                max_path_depth=256,
                limitations=(
                    api.StorageLimitation(
                        "unsafe_members_rejected",
                        "Non-regular, ambiguous, escaping, or conflicting members reject the archive.",
                    ),
                    api.StorageLimitation(
                        "archive_wide_version",
                        "Any archive replacement changes every member version token.",
                    ),
                    api.StorageLimitation(
                        "compressed_tar_range_cost",
                        "Ranges in compressed TAR archives may require decompression from an earlier stream position.",
                    ),
                    api.StorageLimitation(
                        "bounded_tar_expansion",
                        "Member size, aggregate expansion, compression ratio, parser metadata, and entry count are bounded.",
                    ),
                    api.StorageLimitation(
                        "nested_expansion_budget_external",
                        "Recursive ingest must impose its own cumulative cross-container budget.",
                    ),
                ),
            ),
        ),
        _descriptor(
            "rar_build", "Build-once RAR archive", _build_rar_build,
            aliases=("rar_backup", "rar_seal"),
            access_protocol="rar-build",
            read_only=False, location_type="file",
            random_write=True, delete=True, checksums=True,
            order=57, policy_section="rar_build",
            characteristics=api.StorageCharacteristics(
                publication_model=api.StoragePublicationModel.STAGING_THEN_SEAL,
                temporary_space=api.StorageTemporarySpaceRequirement.STORE_COPY,
                recommended_write_usage=api.StorageWriteUsage.ARCHIVAL_SNAPSHOT,
                max_object_bytes=4 * 1024 * 1024 * 1024,
                max_component_bytes=65_535,
                max_path_depth=256,
                preserves_unmodelled_entries=True,
                rewrites_container_format=True,
                limitations=(
                    api.StorageLimitation(
                        "explicit_seal_required",
                        "Staged objects enter the RAR archive only after seal().",
                    ),
                    api.StorageLimitation(
                        "sealed_store_read_only",
                        "A successfully sealed RAR builder permanently refuses mutation.",
                    ),
                    api.StorageLimitation(
                        "create_only_archive_publication",
                        "Sealing never replaces an existing output archive.",
                    ),
                    api.StorageLimitation(
                        "external_rar_creator_required",
                        "Sealing requires an operator-supplied licensed rar executable.",
                    ),
                    api.StorageLimitation(
                        "rar4_non_solid_output",
                        "The builder emits reader-compatible, non-solid RAR 4 archives.",
                    ),
                    api.StorageLimitation(
                        "rar_creation_license_operator_managed",
                        "Installation and licensing of the proprietary RAR creator are operator responsibilities.",
                    ),
                    api.StorageLimitation(
                        "validated_bounded_seal",
                        "Sealing preflights every staged entry and validates candidate expansion limits before create-only publication.",
                    ),
                    api.StorageLimitation(
                        "nested_expansion_budget_external",
                        "Recursive ingest must impose its own cumulative cross-container budget.",
                    ),
                ),
            ),
        ),
        _descriptor(
            "rar_readonly", "Read-only RAR archive", _build_rar_readonly,
            aliases=("rar",), access_protocol="rar",
            read_only=True, location_type="file", checksums=True,
            immutable=True, order=58, policy_section="rar_readonly",
            characteristics=api.StorageCharacteristics(
                publication_model=api.StoragePublicationModel.READ_ONLY,
                temporary_space=api.StorageTemporarySpaceRequirement.OBJECT_STAGE,
                recommended_write_usage=api.StorageWriteUsage.NOT_APPLICABLE,
                max_object_bytes=4 * 1024 * 1024 * 1024,
                max_component_bytes=65_535,
                max_path_depth=256,
                limitations=(
                    api.StorageLimitation(
                        "unsafe_members_rejected",
                        "Non-regular, ambiguous, escaping, or conflicting members reject the archive.",
                    ),
                    api.StorageLimitation(
                        "rar_compressed_members_require_extractor",
                        "Compressed RAR members require a compatible unrar or rar executable.",
                    ),
                    api.StorageLimitation(
                        "modern_rarfile_required_for_rar5",
                        "RAR 5 inventory and reads require the optional maintained rarfile dependency.",
                    ),
                    api.StorageLimitation(
                        "rar_member_reads_spooled",
                        "RAR members are verified into temporary local storage before ranges are returned.",
                    ),
                    api.StorageLimitation(
                        "multi_volume_unsupported",
                        "Multi-volume RAR archives are unsupported.",
                    ),
                    api.StorageLimitation(
                        "bounded_rar_expansion",
                        "Member size, total expansion, compression ratio, path size, and all-entry count are bounded before reads.",
                    ),
                    api.StorageLimitation(
                        "nested_expansion_budget_external",
                        "Recursive ingest must impose its own cumulative cross-container budget.",
                    ),
                ),
            ),
        ),
        _descriptor(
            "sevenzip_readonly", "Read-only 7z archive", _build_sevenzip_readonly,
            aliases=("7z", "sevenzip"), access_protocol="7z",
            read_only=True, location_type="file", checksums=True,
            immutable=True, order=59, policy_section="sevenzip_readonly",
            characteristics=api.StorageCharacteristics(
                publication_model=api.StoragePublicationModel.READ_ONLY,
                temporary_space=api.StorageTemporarySpaceRequirement.OBJECT_STAGE,
                recommended_write_usage=api.StorageWriteUsage.NOT_APPLICABLE,
                max_object_bytes=4 * 1024 * 1024 * 1024,
                max_component_bytes=65_535,
                max_path_depth=256,
                limitations=(
                    api.StorageLimitation(
                        "unsafe_members_rejected",
                        "Non-regular, ambiguous, escaping, or conflicting members reject the archive.",
                    ),
                    api.StorageLimitation(
                        "py7zr_dependency_required",
                        "7z inventory and reads require the optional py7zr dependency set.",
                    ),
                    api.StorageLimitation(
                        "sevenzip_member_reads_spooled",
                        "Each requested 7z member is verified in private temporary storage before ranges are returned.",
                    ),
                    api.StorageLimitation(
                        "solid_archive_read_amplification",
                        "Reading one member from a solid 7z block may decompress preceding block data.",
                    ),
                    api.StorageLimitation(
                        "encrypted_archives_unsupported",
                        "Password-encrypted 7z archives are unsupported.",
                    ),
                    api.StorageLimitation(
                        "multi_volume_unsupported",
                        "Multi-volume 7z archives are unsupported.",
                    ),
                    api.StorageLimitation(
                        "bounded_sevenzip_expansion",
                        "Header size, member size, total expansion, compression ratio, path size, and all-entry count are bounded before reads.",
                    ),
                    api.StorageLimitation(
                        "nested_expansion_budget_external",
                        "Recursive ingest must impose its own cumulative cross-container budget.",
                    ),
                ),
            ),
        ),
        _descriptor(
            "encrypted", "Authenticated encrypted Store wrapper", _build_encrypted,
            aliases=("encrypted_store", "aes_gcm"), access_protocol="encrypted",
            read_only=False, location_type="remote", random_write=True,
            delete=True, checksums=True, policy_section="encrypted",
            user_selectable=False,
            characteristics=api.StorageCharacteristics(
                temporary_space=api.StorageTemporarySpaceRequirement.OBJECT_STAGE,
                limitations=(
                    api.StorageLimitation(
                        "inner_store_dependent",
                        "Publication and size constraints depend on the configured inner Store.",
                    ),
                    api.StorageLimitation(
                        "encrypted_ciphertext_overhead",
                        "Ciphertext adds a header and one authentication tag per chunk.",
                    ),
                ),
            ),
        ),
    )
)


__all__ = [
    "DEFAULT_BACKEND_REGISTRY",
    "StorageBackendDescriptor",
    "StorageBackendRegistry",
    "StoreConstructionContext",
    "normalize_backend_kind",
]
