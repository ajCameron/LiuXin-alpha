"""Canonical registry of configured storage backend kinds."""

from __future__ import annotations

import dataclasses

from collections.abc import Callable, Iterator
from typing import Any, Literal
from urllib.parse import unquote, urlparse
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
        return descriptor.builder(configuration, context or StoreConstructionContext())

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


def _build_squashfs_readonly(configuration, _context):
    from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly import (
        SquashfsReadOnlyStorageBackend,
    )

    return SquashfsReadOnlyStorageBackend(
        _local_path(configuration.store_root_uri),
        **_options(configuration),
        **_common(configuration),
    )


def _build_squashfs_build(configuration, _context):
    from LiuXin_alpha.storage.store_backend_plugins.squashfs_build import (
        SquashfsBuildStorageBackend,
    )

    return SquashfsBuildStorageBackend(
        _local_path(configuration.store_root_uri),
        **_options(configuration),
        **_common(configuration),
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
        return unquote(parsed.path)
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
    )


DEFAULT_BACKEND_REGISTRY = StorageBackendRegistry(
    (
        _descriptor(
            "filesystem", "Local folder (read/write)", _build_filesystem,
            aliases=("file", "on_disk"), access_protocol="file", read_only=False,
            location_type="dir", random_write=True, delete=True, checksums=True,
        ),
        _descriptor(
            "on_disk_existing_managed_drive", "Managed local folder (read/write)",
            _build_managed, aliases=("on_disk_existing_managed", "managed_drive"),
            access_protocol="file", read_only=False, location_type="dir",
            random_write=True, delete=True, checksums=True, order=10,
        ),
        _descriptor(
            "on_disk_existing_unmanaged_drive", "Existing unmanaged local folder (read-only)",
            _build_unmanaged,
            aliases=("on_disk_existing_unmanaged", "on_disk_unmanaged", "unmanaged_drive"),
            access_protocol="file", read_only=True, location_type="dir", checksums=True,
            order=20,
        ),
        _descriptor(
            "on_disk_flat", "Flat content-addressed local folder", _build_flat,
            aliases=("flat", "flat_store"), access_protocol="file", read_only=False,
            location_type="dir", folders=False, hierarchical=False,
            random_write=True, delete=True, checksums=True, immutable=True,
        ),
        _descriptor(
            "on_disk_calibre_like", "Calibre-like rich local folder", _build_calibre_like,
            aliases=("calibre_like",), access_protocol="file", read_only=False,
            location_type="dir", random_write=True, delete=True, checksums=True,
            order=30,
        ),
        _descriptor(
            "single_file_sqlite", "Single-file SQLite blob store", _build_sqlite,
            aliases=("sqlite", "sqlite_blob", "sqlite_store"), access_protocol="sqlite",
            read_only=False, location_type="file", folders=False, hierarchical=False,
            random_write=True, delete=True, checksums=True, order=40,
        ),
        _descriptor(
            "http_readonly", "Direct HTTP root (read-only)", _build_http,
            aliases=("http",), access_protocol="http",
            access_protocol_aliases=("https",), read_only=True,
            location_type="remote", hierarchical=False, policy_section="http",
        ),
        _descriptor(
            "native_html_readonly", "Native HTML crawler remote (read-only)",
            _build_native_html, aliases=("native_html",), access_protocol="native_html",
            read_only=True, location_type="remote", order=80,
            policy_section="native_html",
        ),
        _descriptor(
            "wget_html_readonly", "Wget HTML spider remote (read-only)", _build_wget_html,
            aliases=("wget_html",), access_protocol="wget", read_only=True,
            location_type="remote", order=70, policy_section="wget",
        ),
        _descriptor(
            "ftp_readonly", "FTP/FTPS remote (read-only)", _build_ftp,
            aliases=("ftp", "ftps", "ftps_readonly"), access_protocol="ftp",
            access_protocol_aliases=("ftps",),
            read_only=True, location_type="remote", checksums=True,
            policy_section="ftp",
        ),
        _descriptor(
            "rclone_http_readonly", "Rclone remote (read-only)", _build_rclone_readonly,
            aliases=("rclone", "rclone_readonly"), access_protocol="rclone",
            read_only=True, location_type="remote", checksums=True, order=60,
            policy_section="rclone",
        ),
        _descriptor(
            "rclone_writable", "Rclone remote (read/write)", _build_rclone_writable,
            aliases=("rclone_readwrite",), access_protocol="rclone", read_only=False,
            location_type="remote", random_write=True, delete=True, checksums=True,
            policy_section="rclone",
        ),
        _descriptor(
            "s3", "Native S3-compatible bucket", _build_s3,
            aliases=("s3_compatible",), access_protocol="s3", read_only=False,
            location_type="remote", random_write=True, delete=True, checksums=True,
            policy_section="s3",
        ),
        _descriptor(
            "squashfs_build", "Buildable SquashFS archive", _build_squashfs_build,
            aliases=("squashfs_backup", "open_squashfs_store"),
            access_protocol="squashfs-build", read_only=False, location_type="file",
            random_write=True, delete=True, checksums=True,
            policy_section="squashfs_build",
        ),
        _descriptor(
            "squashfs_readonly", "Read-only SquashFS archive", _build_squashfs_readonly,
            aliases=("squashfs", "sealed_squashfs"), access_protocol="squashfs",
            read_only=True, location_type="file", checksums=True, immutable=True,
            order=50, policy_section="squashfs",
        ),
        _descriptor(
            "encrypted", "Authenticated encrypted Store wrapper", _build_encrypted,
            aliases=("encrypted_store", "aes_gcm"), access_protocol="encrypted",
            read_only=False, location_type="remote", random_write=True,
            delete=True, checksums=True, policy_section="encrypted",
            user_selectable=False,
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
