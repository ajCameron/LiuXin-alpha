"""
Register backend families, construction adapters, and declared storage profiles.

The shared default registry is assembled at import time from passive descriptors;
concrete backend imports are deferred to builder calls. Kind aliases determine
lookup, while protocol labels and characteristics support configuration and
presentation. Advertised profiles are not live capability or availability probes.

Builders retain full configuration only where explicitly forwarded. Compatibility
builders otherwise project selected fields, and each backend owns option/path
validation and resource effects. Runtime clients, keys, Store resolvers, and
Asset-image materialization callbacks travel through StoreConstructionContext.
The registry adds no lifecycle ownership, persistence, general rollback, or
post-construction result validation.
"""

from __future__ import annotations

import dataclasses
import os
from collections.abc import Callable, Iterator, Mapping
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
    """
    Carry borrowed runtime dependencies separately from durable Store options.

    This frozen dataclass retains references without validating client interfaces, copying clients,
    resolving Stores, or acquiring ownership. Builders consume only the dependencies they need and
    validate them at their own boundaries. Freezing fields does not make a client, provider, or
    resolver immutable or safe for concurrent use. No dependency serialization is performed here.

    Example:
        >>> StoreConstructionContext().s3_client is None
        True


    :ivar backend_clients: Borrowed clients keyed by canonical backend kind; a matching entry takes precedence over legacy backend-specific fields.
    :ivar s3_client: Legacy optional S3 client shortcut; use backend_clients for new integrations.
    :ivar store_resolver: Optional callback receiving an inner Store UUID for encrypted construction; its returned Store is validated by the wrapper.
    :ivar encryption_key_provider: Optional runtime provider of active/historical encryption keys, passed unchanged to EncryptedStore.
    :ivar backing_path_resolver: Optional callback receiving the complete Asset-backed StoreConfiguration and returning an accessible local container path.
    """

    backend_clients: Mapping[str, Any] = dataclasses.field(default_factory=dict)
    s3_client: Any | None = None
    store_resolver: Callable[[api.StoreUUID], api.StoreAPI] | None = None
    encryption_key_provider: Any | None = None
    backing_path_resolver: Callable[[api.StoreConfiguration], str] | None = None

    def client_for(self, backend_kind: str) -> Any | None:
        """
        Return an injected client for a canonical backend kind.

        A value in ``backend_clients`` wins even when it is explicitly None. The legacy
        ``s3_client`` field remains the fallback for S3 callers created before the generic mapping
        was added. No alias resolution or client-interface validation occurs here.

        Example:
            >>> client = object()
            >>> StoreConstructionContext(backend_clients={"s3": client}).client_for("s3") is client
            True


        :param backend_kind: Exact canonical backend kind used as the mapping key.
        :return: The borrowed mapped client, the legacy S3 client, or None when no client was supplied.
        """
        if backend_kind in self.backend_clients:
            return self.backend_clients[backend_kind]
        if backend_kind == "s3":
            return self.s3_client
        return None


@dataclasses.dataclass(slots=True, frozen=True)
class StorageBackendDescriptor:
    """
    Describe construction, presentation, and advertised properties of one backend family. The frozen
    dataclass performs no normalization or interface validation; registry registration separately
    checks canonical kind spelling and alias collisions. Capability flags and characteristics
    describe the family rather than probing a configured instance. They do not impose general
    runtime capability checks on builders. Referenced values are retained, not deep-copied.

    Example:
        >>> DEFAULT_BACKEND_REGISTRY.descriptor("file").kind
        'filesystem'


    :ivar kind: Canonical registry key, required to match normalize_backend_kind(kind) when registered.
    :ivar label: Human-readable backend label for presentation.
    :ivar builder: Callback invoked with a StoreConfiguration and StoreConstructionContext; callability/result type are not checked by this dataclass.
    :ivar aliases: Additional kind lookup names normalized during registration; distinct from access-protocol labels.
    :ivar access_protocol: Preferred protocol label for configuration and presentation, not automatically a lookup alias.
    :ivar access_protocol_aliases: Additional protocol labels for consumers; registration does not add them as kind aliases.
    :ivar read_only_default: Family's default read-only designation; also consulted when permitting an Asset-backed file view.
    :ivar location_type: Declared dir/file/remote location category; Asset-backed registry construction requires file.
    :ivar supports_folders: Advertised ability to expose folder-like locations, defaulting to True.
    :ivar supports_hierarchical_list: Advertised hierarchical enumeration, defaulting to True.
    :ivar supports_random_read: Advertised random reads, defaulting to True independently of other flags.
    :ivar supports_random_write: Advertised random writes, defaulting to False.
    :ivar supports_delete: Advertised deletion support, defaulting to False.
    :ivar supports_checksums: Advertised checksum support, defaulting to False.
    :ivar supports_immutable_objects: Advertised immutable-object behavior, defaulting to False.
    :ivar user_selectable: Whether filtered descriptor enumeration includes this backend for user selection.
    :ivar presentation_order: Primary ascending enumeration key, followed by kind for ties.
    :ivar policy_section: Optional backend-specific JSON policy section used by row translation.
    :ivar characteristics: Declared publication, temporary-space, limit, and usage profile; each omitted value gets a fresh default profile.
    """

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
    """
    Maintain canonical descriptors and normalized aliases for configured Store construction.
    Registration is additive and rejects collisions with prior registrations. Lookups return
    retained descriptors, and iteration orders them for presentation. The registry has no
    persistence, removal API, locking, or ownership of constructed Stores. Construction adds
    selected Asset-backed restrictions, then trusts the builder's result and propagates its errors
    and side effects. The shared default instance can be mutated by callers.

    Example:
        >>> registry = StorageBackendRegistry()
        >>> registry.register(DEFAULT_BACKEND_REGISTRY.descriptor("filesystem"))
        >>> registry.canonical_kind("FILE")
        'filesystem'
    """

    def __init__(
        self,
        descriptors: tuple[StorageBackendDescriptor, ...] = (),
    ) -> None:
        """
        Create empty descriptor/alias dictionaries and register the supplied descriptors in order. A
        later registration failure leaves earlier entries on the partially initialized instance; no
        backend builder is invoked.

        Example:
            >>> tuple(StorageBackendRegistry())
            ()
            >>> seeded = StorageBackendRegistry((DEFAULT_BACKEND_REGISTRY.descriptor("file"),))
            >>> seeded.canonical_kind("FILE")
            'filesystem'

        :param descriptors: Initial descriptor sequence, consumed in order with the same validation as register.
        :return: None after all descriptors are registered; normalization or collision errors propagate.
        """
        self._descriptors: dict[str, StorageBackendDescriptor] = {}
        self._aliases: dict[str, str] = {}
        for descriptor in descriptors:
            self.register(descriptor)

    def register(self, descriptor: StorageBackendDescriptor) -> None:
        """
        Add a descriptor after validating its canonical kind and collisions with existing names.
        Normalize kind and require it already equals that spelling. Normalize the kind and aliases,
        then reject any name present in the current alias map before updating either dictionary.
        Duplicate normalized aliases within this one descriptor are accepted and point to the same
        kind. Protocol labels are not registered as aliases. Other descriptor fields and builder
        callability are not validated, and there is no concurrent registration guard.

        Example:
            >>> registry = StorageBackendRegistry()
            >>> registry.register(DEFAULT_BACKEND_REGISTRY.descriptor("s3"))
            >>> registry.canonical_kind("s3-compatible")
            's3'


        :param descriptor: Descriptor retained by canonical kind, with its normalized kind/alias names added to lookup.
        :return: None after insertion; noncanonical kinds and prior-name collisions raise ValueError before ordinary dictionary updates.
        """
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
        """
        Normalize the supplied kind/alias and return its registered descriptor. Lookup KeyError
        becomes StoreUnsupportedOperation with the original requested value; normalization errors
        propagate separately. Protocol labels resolve only if explicitly registered as kind aliases.

        Example:
            >>> DEFAULT_BACKEND_REGISTRY.descriptor("ISO9660").kind
            'iso_readonly'


        :param kind: Backend kind or alias, stringified/stripped/lowercased with hyphens replaced by underscores.
        :return: The retained StorageBackendDescriptor; empty names raise ValueError and unknown names raise StoreUnsupportedOperation.
        """
        normalized = normalize_backend_kind(kind)
        try:
            return self._descriptors[self._aliases[normalized]]
        except KeyError as error:
            raise api.StoreUnsupportedOperation(
                f"no Store factory is registered for kind {kind!r}."
            ) from error

    def canonical_kind(self, kind: str) -> str:
        """
        Resolve a kind or alias through descriptor lookup and return the stored canonical spelling.
        This does not rewrite configuration or infer a backend from a URI.

        Example:
            >>> DEFAULT_BACKEND_REGISTRY.canonical_kind("managed-drive")
            'on_disk_existing_managed_drive'


        :param kind: Backend kind or alias accepted by descriptor.
        :return: The registered descriptor.kind string; lookup/normalization errors propagate.
        """
        return self.descriptor(kind).kind

    def build(
        self,
        configuration: api.StoreConfiguration,
        *,
        context: StoreConstructionContext | None = None,
    ) -> api.StoreAPI:
        """
        Resolve the configured kind, apply selected backed-view checks, and call its builder. Use
        the supplied truthy context or a new empty context. If backing is non-None, require truthy
        configuration.read_only, a descriptor with truthy read_only_default and location_type equal
        to file, and a non-None backing_path_resolver. These checks do not call the resolver or
        inspect the referenced Asset. Unbacked configurations bypass them even if their read-only
        setting differs from the descriptor default.

        Pass the original configuration and selected context positionally to the builder and return
        its result without a StoreAPI/type or identity check. No startup/probe, transaction,
        resource cleanup, or error translation is added; imports, constructor effects, and failures
        belong to the builder.

        Example:
            >>> store = DEFAULT_BACKEND_REGISTRY.build(configuration, context=context)  # doctest: +SKIP


        :param configuration: Store intent whose kind selects the builder and whose backing/read_only fields govern backed-view checks; retained unchanged.
        :param context: Optional runtime context; falsey input creates a fresh StoreConstructionContext.
        :return: The builder's result; lookup/backing restrictions raise typed errors and builder failures propagate.
        """
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
        """
        Yield retained descriptors ordered by presentation_order and then kind. The generator
        snapshots and sorts current dictionary values on first iteration, not when the generator is
        created. Later additions are absent from that snapshot. Optionally skip descriptors with
        falsey user_selectable; no Store construction or endpoint probe occurs, and concurrent
        mutation is not synchronized.

        Example:
            >>> tuple(StorageBackendRegistry().iter_descriptors(user_selectable_only=True))
            ()


        :param user_selectable_only: Whether to exclude descriptors not marked user-selectable.
        :return: A lazy iterator over the sorted descriptor snapshot, filtered when yielded.
        """
        for descriptor in sorted(
            self._descriptors.values(),
            key=lambda item: (item.presentation_order, item.kind),
        ):
            if not user_selectable_only or descriptor.user_selectable:
                yield descriptor

    def __iter__(self) -> Iterator[StorageBackendDescriptor]:
        """
        Return unfiltered descriptor iteration in presentation order. Snapshot creation remains
        deferred until the returned generator is first advanced; merely requesting an iterator
        performs no lookup or build.

        Example:
            >>> list(StorageBackendRegistry())
            []


        :return: The iterator returned by iter_descriptors with its default filter.
        """
        return self.iter_descriptors()


def normalize_backend_kind(kind: str) -> str:
    """
    Stringify a backend name, strip surrounding whitespace, lowercase it, and replace every hyphen
    with an underscore. Reject only an empty result; embedded whitespace, punctuation, and names
    absent from any registry can remain.

    Example:
        >>> normalize_backend_kind(" S3-compatible ")
        's3_compatible'


    :param kind: Kind-like value to stringify and normalize; no registry lookup is performed.
    :return: Nonempty normalized text, or ValueError for an empty result; string conversion errors propagate.
    """
    normalized = str(kind).strip().lower().replace("-", "_")
    if not normalized:
        raise ValueError("backend kind must not be empty.")
    return normalized


def _common(configuration: api.StoreConfiguration) -> dict[str, object]:
    """
    Project the configured name and UUID into constructor keyword names used by compatibility
    backends. Other configuration fields are not forwarded and no value is normalized or validated.

    Example:
        >>> values = _common(configuration)  # doctest: +SKIP


    :param configuration: Configuration supplying store_name and store_uuid.
    :return: A fresh dictionary with name and uuid pointing to the supplied values.
    """
    return {
        "name": configuration.store_name,
        "uuid": configuration.store_uuid,
    }


def _options(configuration: api.StoreConfiguration) -> dict[str, object]:
    """
    Shallow-copy configured backend option pairs into a dictionary. Values are retained, duplicate
    keys would take the last value, and neither names nor values are filtered here. Pair-shape
    errors propagate.

    Example:
        >>> options = _options(configuration)  # doctest: +SKIP


    :param configuration: Configuration whose backend_options iterable is passed to dict.
    :return: A new dictionary of backend option values, suitable for local popping without mutating configuration.
    """
    return dict(configuration.backend_options)


def _optional_float_option(
    options: Mapping[str, object],
    name: str,
    default: float | None,
) -> float | None:
    """
    Read an optional finite-or-infinite numeric backend option as a float.

    Missing names use the supplied default and explicit None remains None. Integers and floats are
    accepted, but bool is rejected despite being an int subtype. Range and finiteness constraints
    remain the receiving backend's responsibility.

    Example:
        >>> _optional_float_option({"timeout_s": 4}, "timeout_s", 30.0)
        4.0


    :param options: Backend option mapping to inspect without mutation.
    :param name: Option name used for lookup and error reporting.
    :param default: Value returned when the option is absent.
    :return: None or a float representation of the supplied numeric value.
    """
    value = options.get(name, default)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"backend option {name!r} must be numeric or None.")
    return float(value)


def _optional_int_option(
    options: Mapping[str, object],
    name: str,
    default: int | None,
) -> int | None:
    """
    Read an optional exact-integer backend option.

    Missing names use the supplied default and explicit None remains None. Bool and float values are
    rejected so limits cannot be silently truncated; positivity remains backend validation.

    Example:
        >>> _optional_int_option({}, "limit", 100)
        100


    :param options: Backend option mapping to inspect without mutation.
    :param name: Option name used for lookup and error reporting.
    :param default: Value returned when the option is absent.
    :return: None or the exact supplied integer.
    """
    value = options.get(name, default)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"backend option {name!r} must be an integer or None.")
    return value


def _container_path(
    configuration: api.StoreConfiguration,
    context: StoreConstructionContext,
) -> str:
    """
    Select a direct container pathname or ask the runtime resolver for an Asset-backed path.
    Unbacked intent sends store_root_uri through _local_path. Backed intent requires a non-None
    resolver and calls it once with the whole configuration. Resolver results are returned
    unchanged, without additional URI decoding, path containment, existence, or type checks. A
    noncallable resolver and its raised errors propagate; resolving/materializing catalogue bytes
    belongs to the callback.

    Example:
        >>> path = _container_path(configuration, context)  # doctest: +SKIP


    :param configuration: Archive Store intent selecting a direct root or backing relationship.
    :param context: Runtime context supplying backing_path_resolver when configuration.backing is non-None.
    :return: The decoded direct pathname or resolver result; missing backing resolver raises StoreUnsupportedOperation.
    """

    if configuration.backing is None:
        return _local_path(configuration.store_root_uri)
    resolver = context.backing_path_resolver
    if resolver is None:
        raise api.StoreUnsupportedOperation(
            "Asset-backed storage requires a runtime backing-path resolver."
        )
    return resolver(configuration)


def _build_filesystem(configuration, _context):
    """
    Construct FilesystemStore through from_configuration, retaining the supplied configuration. That
    factory creates a missing root only for writable intent and uses defaults for other driver
    options; backend_options and this runtime context are not reconstructed here.

    Example:
        >>> store = _build_filesystem(configuration, context)  # doctest: +SKIP


    :param configuration: Filesystem intent supplying root URI, identity, and read-only policy.
    :param _context: Unused runtime context accepted for the common builder signature.
    :return: An unstarted FilesystemStore; local path and constructor errors propagate.
    """
    from LiuXin_alpha.storage.stores import FilesystemStore

    return FilesystemStore.from_configuration(configuration)


def _build_memory(configuration, _context):
    """Construct an empty process-local cache Store from portable configuration.

    ``max_bytes`` is the only backend option. Rebuilding from the same
    configuration restores identity and limits, never previously stored bytes.

    :param configuration: Memory Store identity, root URI, policy, and options.
    :param _context: Unused runtime context accepted for the common builder signature.
    :return: An unstarted, empty MemoryStore retaining the supplied configuration.
    """
    from LiuXin_alpha.storage.stores import MemoryStore

    return MemoryStore.from_configuration(configuration)


def _build_managed(configuration, _context):
    """
    Construct an existing managed local folder using the root, name, and UUID only. Lazily import
    OnDiskExistingManagedStorageBackend and pass store_root_uri unchanged with _common identity
    fields. Full configuration and backend_options are not forwarded, so the backend derives
    remaining policy from its own constructor defaults. Path interpretation and resource effects
    belong to that constructor; this adapter adds no startup or database persistence.

    Example:
        >>> store = _build_managed(configuration, context)  # doctest: +SKIP


    :param configuration: Intent supplying store_root_uri, store_name, and store_uuid; other fields are unused by this adapter.
    :param _context: Unused runtime context accepted for the common builder signature.
    :return: The constructed OnDiskExistingManagedStorageBackend; import, path, and constructor failures propagate.
    """
    from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive import (
        OnDiskExistingManagedStorageBackend,
    )

    return OnDiskExistingManagedStorageBackend(
        configuration.store_root_uri,
        **_common(configuration),
    )


def _build_unmanaged(configuration, _context):
    """
    Construct an existing unmanaged read-only folder using the root, name, and UUID only. Lazily
    import OnDiskUnmanagedStorageBackend and pass store_root_uri unchanged with _common identity
    fields. Full configuration and backend_options are not forwarded, so the backend derives
    remaining policy from its own constructor defaults. Path interpretation and resource effects
    belong to that constructor; this adapter adds no startup or database persistence.

    Example:
        >>> store = _build_unmanaged(configuration, context)  # doctest: +SKIP


    :param configuration: Intent supplying store_root_uri, store_name, and store_uuid; other fields are unused by this adapter.
    :param _context: Unused runtime context accepted for the common builder signature.
    :return: The constructed OnDiskUnmanagedStorageBackend; import, path, and constructor failures propagate.
    """
    from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_unmanaged_drive import (
        OnDiskUnmanagedStorageBackend,
    )

    return OnDiskUnmanagedStorageBackend(
        configuration.store_root_uri,
        **_common(configuration),
    )


def _build_flat(configuration, _context):
    """
    Construct a flat content-addressed folder using the root, name, and UUID only. Lazily import
    OnDiskFlatStorageBackend and pass store_root_uri unchanged with _common identity fields. Full
    configuration and backend_options are not forwarded, so the backend derives remaining policy
    from its own constructor defaults. Path interpretation and resource effects belong to that
    constructor; this adapter adds no startup or database persistence.

    Example:
        >>> store = _build_flat(configuration, context)  # doctest: +SKIP


    :param configuration: Intent supplying store_root_uri, store_name, and store_uuid; other fields are unused by this adapter.
    :param _context: Unused runtime context accepted for the common builder signature.
    :return: The constructed OnDiskFlatStorageBackend; import, path, and constructor failures propagate.
    """
    from LiuXin_alpha.storage.store_backend_plugins.on_disk_flat import (
        OnDiskFlatStorageBackend,
    )

    return OnDiskFlatStorageBackend(configuration.store_root_uri, **_common(configuration))


def _build_calibre_like(configuration, _context):
    """
    Construct a Calibre-like rich folder using the root, name, and UUID only. Lazily import
    OnDiskCalibreLikeStorageBackend and pass store_root_uri unchanged with _common identity fields.
    Full configuration and backend_options are not forwarded, so the backend derives remaining
    policy from its own constructor defaults. Path interpretation and resource effects belong to
    that constructor; this adapter adds no startup or database persistence.

    Example:
        >>> store = _build_calibre_like(configuration, context)  # doctest: +SKIP


    :param configuration: Intent supplying store_root_uri, store_name, and store_uuid; other fields are unused by this adapter.
    :param _context: Unused runtime context accepted for the common builder signature.
    :return: The constructed OnDiskCalibreLikeStorageBackend; import, path, and constructor failures propagate.
    """
    from LiuXin_alpha.storage.store_backend_plugins.on_disk_calibre_like import (
        OnDiskCalibreLikeStorageBackend,
    )

    return OnDiskCalibreLikeStorageBackend(
        configuration.store_root_uri,
        **_common(configuration),
    )


def _build_sqlite(configuration, _context):
    """
    Construct a single-file SQLite blob Store using the root, name, and UUID only. Lazily import
    SQLiteStore and pass store_root_uri unchanged with _common identity fields.
    Full configuration and backend_options are not forwarded, so the backend derives remaining
    policy from its own constructor defaults. Path interpretation and resource effects belong to
    that constructor; this adapter adds no startup or database persistence.

    Example:
        >>> store = _build_sqlite(configuration, context)  # doctest: +SKIP


    :param configuration: Intent supplying store_root_uri, store_name, and store_uuid; other fields are unused by this adapter.
    :param _context: Unused runtime context accepted for the common builder signature.
    :return: The constructed SQLiteStore; import, path, and constructor failures propagate.
    """
    from LiuXin_alpha.storage.stores.sqlite import SQLiteStore

    return SQLiteStore(
        configuration.store_root_uri,
        **_common(configuration),
    )


def _build_http(configuration, _context):
    """
    Construct a direct HTTP Store from root/name/UUID and three selected options. Force the concrete
    kind http_readonly and forward timeout_s (default 30), max_requests_per_hour (default None), and
    max_inventory_entries (default 100000). Defaults apply only to missing keys, so explicit None is
    forwarded. Other options are ignored and the full configuration object is not passed; the
    constructor owns option validation and derives its configuration.

    Example:
        >>> store = _build_http(configuration, context)  # doctest: +SKIP


    :param configuration: Intent supplying the HTTP root, name, UUID, and selected request/inventory settings.
    :param _context: Unused runtime context; this builder does not inject an HTTP client.
    :return: The constructed HttpReadOnlyStore; lazy import and constructor errors propagate.
    """
    from LiuXin_alpha.storage.stores import HttpReadOnlyStore

    options = _options(configuration)
    return HttpReadOnlyStore(
        configuration.store_root_uri,
        store_kind="http_readonly",
        timeout_s=_optional_float_option(options, "timeout_s", 30.0),
        max_requests_per_hour=_optional_float_option(
            options,
            "max_requests_per_hour",
            None,
        ),
        max_inventory_entries=_optional_int_option(
            options,
            "max_inventory_entries",
            100_000,
        ),
        **_common(configuration),
    )


def _build_native_html(configuration, _context):
    """
    Construct a read-only Store for native HTML discovery from root/name/UUID and options. Pass
    every backend option as a NativeHtmlBackendOptions keyword, then pass the options object to
    NativeHtmlReadOnlyStorageBackend. Unknown names fail in option construction. The full
    configuration is not retained through this call; remaining fields follow backend defaults.
    Imports and validation are delegated without explicitly starting a crawler, transfer, or
    inventory operation.

    Example:
        >>> store = _build_native_html(configuration, context)  # doctest: +SKIP


    :param configuration: Intent supplying the root/name/UUID and keywords for NativeHtmlBackendOptions.
    :param _context: Unused runtime context; this adapter does not inject a transport client.
    :return: The constructed NativeHtmlReadOnlyStorageBackend; option/import/constructor errors propagate.
    """
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
    """
    Construct a read-only Store for wget HTML discovery from root/name/UUID and options. Pass every
    backend option as a WgetBackendOptions keyword, then pass the options object to
    WgetHtmlReadOnlyStorageBackend. Unknown names fail in option construction. The full
    configuration is not retained through this call; remaining fields follow backend defaults.
    Imports and validation are delegated without explicitly starting a crawler, transfer, or
    inventory operation.

    Example:
        >>> store = _build_wget_html(configuration, context)  # doctest: +SKIP


    :param configuration: Intent supplying the root/name/UUID and keywords for WgetBackendOptions.
    :param _context: Unused runtime context; this adapter does not inject a transport client.
    :return: The constructed WgetHtmlReadOnlyStorageBackend; option/import/constructor errors propagate.
    """
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
    """
    Construct a read-only Store for FTP/FTPS access from root/name/UUID and options. Pass every
    backend option as a FtpDriverOptions keyword, then pass the options object to
    FtpReadOnlyStorageBackend. Unknown names fail in option construction. The full configuration is
    not retained through this call; remaining fields follow backend defaults. Imports and validation
    are delegated without explicitly starting a crawler, transfer, or inventory operation.

    Example:
        >>> store = _build_ftp(configuration, context)  # doctest: +SKIP


    :param configuration: Intent supplying the root/name/UUID and keywords for FtpDriverOptions.
    :param _context: Unused runtime context; this adapter does not inject a transport client.
    :return: The constructed FtpReadOnlyStorageBackend; option/import/constructor errors propagate.
    """
    from LiuXin_alpha.storage.drivers.ftp import FtpDriverOptions
    from LiuXin_alpha.storage.store_backend_plugins.ftp_readonly import (
        FtpReadOnlyStorageBackend,
    )

    return FtpReadOnlyStorageBackend(
        configuration.store_root_uri,
        options=FtpDriverOptions(**_options(configuration)),
        **_common(configuration),
    )



def _build_rclone_readonly(configuration, _context):
    """
    Construct a read-only rclone Store with the original configuration and RcloneBackendOptions
    built from every option pair. Unknown option keywords fail in option construction; runtime
    context is unused and no rclone operation is explicitly invoked by this adapter.

    Example:
        >>> store = _build_rclone_readonly(configuration, context)  # doctest: +SKIP


    :param configuration: Intent supplying the rclone root, complete backend options, and retained Store configuration.
    :param _context: Unused runtime context; process configuration comes from backend options.
    :return: The constructed RcloneHttpReadOnlyStorageBackend; option, import, and constructor errors propagate.
    """
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
    """
    Construct a writable rclone Store with retained configuration and local staging policy. Copy
    options and remove local_staging_directory before creating RcloneBackendOptions from the
    remainder. Pass None for missing/null staging, otherwise str(value), without expanding or
    resolving it here. The backend owns staging-directory preparation and option validation; unknown
    remaining option names fail. The source configuration and its option pairs are unchanged.

    Example:
        >>> store = _build_rclone_writable(configuration, context)  # doctest: +SKIP


    :param configuration: Rclone intent with process options and optional local_staging_directory.
    :param _context: Unused runtime context; no client/resolver is consumed.
    :return: The constructed RcloneWritableStorageBackend; failures may follow backend resource preparation.
    """
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
    """
    Construct S3Store through from_configuration with the context's S3 client. The Store retains
    configuration, reconstructs S3BackendOptions, and borrows an injected client or uses its SDK
    client-construction path when None. This adapter adds no startup or request.

    Example:
        >>> store = _build_s3(configuration, StoreConstructionContext(s3_client=client))  # doctest: +SKIP


    :param configuration: S3 intent supplying URI, identity, policy, and backend options.
    :param context: Runtime context whose generic or legacy S3 client is passed to the Store factory.
    :return: An unstarted S3Store; option, optional SDK, and client-construction errors propagate.
    """
    from LiuXin_alpha.storage.stores import S3Store

    return S3Store.from_configuration(configuration, client=context.client_for("s3"))


def _build_squashfs_readonly(configuration, context):
    """
    Construct a read-only SquashFS Store over a direct or Asset-resolved image. Lazily import
    SquashfsReadOnlyStorageBackend, obtain the path through _container_path, and forward the
    original configuration plus every backend option as constructor keywords. The resolver can
    materialize bytes before option/constructor failure; this adapter adds no cleanup. It does not
    filter unsupported or colliding keywords, inspect archive contents, or explicitly call startup.
    Parser/tool requirements and image validation belong to backend operations.

    Example:
        >>> store = _build_squashfs_readonly(configuration, context)  # doctest: +SKIP


    :param configuration: SquashFS Store intent supplying root/backing, retained identity/policy, and constructor options.
    :param context: Runtime context supplying a backing-path resolver for Asset-backed intent; unused for direct paths.
    :return: The constructed SquashfsReadOnlyStorageBackend; resolver, option, import, and constructor failures propagate.
    """
    from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly.squashfs_readonly_storage_backend import (
        SquashfsReadOnlyStorageBackend,
    )

    return SquashfsReadOnlyStorageBackend(
        _container_path(configuration, context),
        configuration=configuration,
        **_options(configuration),
    )


def _build_iso_readonly(configuration, context):
    """
    Construct a read-only ISO Store over a direct or Asset-resolved image. Lazily import
    IsoReadOnlyStorageBackend, obtain the path through _container_path, and forward the original
    configuration plus every backend option as constructor keywords. The resolver can materialize
    bytes before option/constructor failure; this adapter adds no cleanup. It does not filter
    unsupported or colliding keywords, inspect archive contents, or explicitly call startup.
    Parser/tool requirements and image validation belong to backend operations.

    Example:
        >>> store = _build_iso_readonly(configuration, context)  # doctest: +SKIP


    :param configuration: ISO Store intent supplying root/backing, retained identity/policy, and constructor options.
    :param context: Runtime context supplying a backing-path resolver for Asset-backed intent; unused for direct paths.
    :return: The constructed IsoReadOnlyStorageBackend; resolver, option, import, and constructor failures propagate.
    """
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
    Construct a writable ISO image from a direct local-path projection. Decode store_root_uri
    through _local_path and pass the original configuration plus every backend option to
    IsoWritableStorageBackend. Unlike read-only image builders, this adapter does not call a
    backing-path resolver; registry.build owns the Asset-backed restriction when used as the entry
    point. The constructor can prepare paths, allocate staging, or create an empty image as
    appropriate, then fail after those effects. No separate mutation, sealing, startup, or tool call
    is added beyond construction, and option/constructor failures are not translated or rolled back.

    Example:
        >>> store = _build_iso_writable(configuration, context)  # doctest: +SKIP


    :param configuration: Intent supplying the direct image root, retained configuration, and backend constructor keywords.
    :param _context: Unused runtime context; this builder does not resolve catalogue-backed images.
    :return: The constructed IsoWritableStorageBackend; path, option, import, and constructor failures propagate.
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
    """
    Construct a read-only ZIP Store over a direct or Asset-resolved image. Lazily import
    ZipReadOnlyStorageBackend, obtain the path through _container_path, and forward the original
    configuration plus every backend option as constructor keywords. The resolver can materialize
    bytes before option/constructor failure; this adapter adds no cleanup. It does not filter
    unsupported or colliding keywords, inspect archive contents, or explicitly call startup.
    Parser/tool requirements and image validation belong to backend operations.

    Example:
        >>> store = _build_zip_readonly(configuration, context)  # doctest: +SKIP


    :param configuration: ZIP Store intent supplying root/backing, retained identity/policy, and constructor options.
    :param context: Runtime context supplying a backing-path resolver for Asset-backed intent; unused for direct paths.
    :return: The constructed ZipReadOnlyStorageBackend; resolver, option, import, and constructor failures propagate.
    """
    from LiuXin_alpha.storage.store_backend_plugins.zip_readonly import (
        ZipReadOnlyStorageBackend,
    )

    return ZipReadOnlyStorageBackend(
        _container_path(configuration, context),
        configuration=configuration,
        **_options(configuration),
    )


def _build_zip_writable(configuration, _context):
    """
    Construct a writable ZIP archive from a direct local-path projection. Decode store_root_uri
    through _local_path and pass the original configuration plus every backend option to
    ZipWritableStorageBackend. Unlike read-only image builders, this adapter does not call a
    backing-path resolver; registry.build owns the Asset-backed restriction when used as the entry
    point. The constructor can prepare paths, allocate staging, or create an empty image as
    appropriate, then fail after those effects. No separate mutation, sealing, startup, or tool call
    is added beyond construction, and option/constructor failures are not translated or rolled back.

    Example:
        >>> store = _build_zip_writable(configuration, context)  # doctest: +SKIP


    :param configuration: Intent supplying the direct image root, retained configuration, and backend constructor keywords.
    :param _context: Unused runtime context; this builder does not resolve catalogue-backed images.
    :return: The constructed ZipWritableStorageBackend; path, option, import, and constructor failures propagate.
    """
    from LiuXin_alpha.storage.store_backend_plugins.zip_writable import (
        ZipWritableStorageBackend,
    )

    return ZipWritableStorageBackend(
        _local_path(configuration.store_root_uri),
        configuration=configuration,
        **_options(configuration),
    )


def _build_tar_readonly(configuration, context):
    """
    Construct a read-only TAR Store over a direct or Asset-resolved image. Lazily import
    TarReadOnlyStorageBackend, obtain the path through _container_path, and forward the original
    configuration plus every backend option as constructor keywords. The resolver can materialize
    bytes before option/constructor failure; this adapter adds no cleanup. It does not filter
    unsupported or colliding keywords, inspect archive contents, or explicitly call startup.
    Parser/tool requirements and image validation belong to backend operations.

    Example:
        >>> store = _build_tar_readonly(configuration, context)  # doctest: +SKIP


    :param configuration: TAR Store intent supplying root/backing, retained identity/policy, and constructor options.
    :param context: Runtime context supplying a backing-path resolver for Asset-backed intent; unused for direct paths.
    :return: The constructed TarReadOnlyStorageBackend; resolver, option, import, and constructor failures propagate.
    """
    from LiuXin_alpha.storage.store_backend_plugins.tar_readonly import (
        TarReadOnlyStorageBackend,
    )

    return TarReadOnlyStorageBackend(
        _container_path(configuration, context),
        configuration=configuration,
        **_options(configuration),
    )


def _build_tar_writable(configuration, _context):
    """
    Construct a writable TAR archive from a direct local-path projection. Decode store_root_uri
    through _local_path and pass the original configuration plus every backend option to
    TarWritableStorageBackend. Unlike read-only image builders, this adapter does not call a
    backing-path resolver; registry.build owns the Asset-backed restriction when used as the entry
    point. The constructor can prepare paths, allocate staging, or create an empty image as
    appropriate, then fail after those effects. No separate mutation, sealing, startup, or tool call
    is added beyond construction, and option/constructor failures are not translated or rolled back.

    Example:
        >>> store = _build_tar_writable(configuration, context)  # doctest: +SKIP


    :param configuration: Intent supplying the direct image root, retained configuration, and backend constructor keywords.
    :param _context: Unused runtime context; this builder does not resolve catalogue-backed images.
    :return: The constructed TarWritableStorageBackend; path, option, import, and constructor failures propagate.
    """
    from LiuXin_alpha.storage.store_backend_plugins.tar_writable import (
        TarWritableStorageBackend,
    )

    return TarWritableStorageBackend(
        _local_path(configuration.store_root_uri),
        configuration=configuration,
        **_options(configuration),
    )


def _build_rar_readonly(configuration, context):
    """
    Construct a read-only RAR Store over a direct or Asset-resolved image. Lazily import
    RarReadOnlyStorageBackend, obtain the path through _container_path, and forward the original
    configuration plus every backend option as constructor keywords. The resolver can materialize
    bytes before option/constructor failure; this adapter adds no cleanup. It does not filter
    unsupported or colliding keywords, inspect archive contents, or explicitly call startup.
    Parser/tool requirements and image validation belong to backend operations.

    Example:
        >>> store = _build_rar_readonly(configuration, context)  # doctest: +SKIP


    :param configuration: RAR Store intent supplying root/backing, retained identity/policy, and constructor options.
    :param context: Runtime context supplying a backing-path resolver for Asset-backed intent; unused for direct paths.
    :return: The constructed RarReadOnlyStorageBackend; resolver, option, import, and constructor failures propagate.
    """
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
    Construct a read-only 7z Store over a direct or Asset-resolved image. Lazily import
    SevenZipReadOnlyStorageBackend, obtain the path through _container_path, and forward the
    original configuration plus every backend option as constructor keywords. The resolver can
    materialize bytes before option/constructor failure; this adapter adds no cleanup. It does not
    filter unsupported or colliding keywords, inspect archive contents, or explicitly call startup.
    Parser/tool requirements and image validation belong to backend operations.

    Example:
        >>> store = _build_sevenzip_readonly(configuration, context)  # doctest: +SKIP


    :param configuration: 7z Store intent supplying root/backing, retained identity/policy, and constructor options.
    :param context: Runtime context supplying a backing-path resolver for Asset-backed intent; unused for direct paths.
    :return: The constructed SevenZipReadOnlyStorageBackend; resolver, option, import, and constructor failures propagate.
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
    Construct a build-once RAR staging Store from a direct local-path projection. Decode
    store_root_uri through _local_path and pass the original configuration plus every backend option
    to RarBuildStorageBackend. Unlike read-only image builders, this adapter does not call a
    backing-path resolver; registry.build owns the Asset-backed restriction when used as the entry
    point. The constructor can prepare paths, allocate staging, or create an empty image as
    appropriate, then fail after those effects. No separate mutation, sealing, startup, or tool call
    is added beyond construction, and option/constructor failures are not translated or rolled back.

    Example:
        >>> store = _build_rar_build(configuration, context)  # doctest: +SKIP


    :param configuration: Intent supplying the direct image root, retained configuration, and backend constructor keywords.
    :param _context: Unused runtime context; this builder does not resolve catalogue-backed images.
    :return: The constructed RarBuildStorageBackend; path, option, import, and constructor failures propagate.
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
    """
    Construct a buildable SquashFS staging Store from a direct local-path projection. Decode
    store_root_uri through _local_path and pass the original configuration plus every backend option
    to SquashfsBuildStorageBackend. Unlike read-only image builders, this adapter does not call a
    backing-path resolver; registry.build owns the Asset-backed restriction when used as the entry
    point. The constructor can prepare paths, allocate staging, or create an empty image as
    appropriate, then fail after those effects. No separate mutation, sealing, startup, or tool call
    is added beyond construction, and option/constructor failures are not translated or rolled back.

    Example:
        >>> store = _build_squashfs_build(configuration, context)  # doctest: +SKIP


    :param configuration: Intent supplying the direct image root, retained configuration, and backend constructor keywords.
    :param _context: Unused runtime context; this builder does not resolve catalogue-backed images.
    :return: The constructed SquashfsBuildStorageBackend; path, option, import, and constructor failures propagate.
    """
    from LiuXin_alpha.storage.store_backend_plugins.squashfs_build import (
        SquashfsBuildStorageBackend,
    )

    return SquashfsBuildStorageBackend(
        _local_path(configuration.store_root_uri),
        configuration=configuration,
        **_options(configuration),
    )


def _build_encrypted(configuration, context):
    """
    Resolve an inner Store and construct an encrypted wrapper using runtime key material. Require
    non-None store_resolver and encryption_key_provider before parsing options. Prefer a non-None
    inner_store_uuid option; otherwise extract it from an encrypted URI. Parse it as a UUID,
    translating TypeError/ValueError to the existing missing-inner-option diagnostic. Empty/invalid
    explicit values do not fall back to the URI.

    Remove key_id without selecting or checking a provider key. Allow only chunk_size,
    forward_placement_hints, inner_prefix, and local_staging_directory among remaining options,
    rejecting unknown names before calling the resolver. Resolve the inner Store once and pass it,
    the provider, retained configuration, and allowed options to EncryptedStore. That constructor
    validates Store/provider interfaces, current active key, and wrapper settings, and prepares
    staging. Its default leaves the inner Store borrowed; persisted key_id may differ from the
    active runtime key. No local rollback or resolver cleanup is added.

    Example:
        >>> store = _build_encrypted(configuration, context)  # doctest: +SKIP


    :param configuration: Wrapper intent retaining the configured UUID and backend option metadata.
    :param context: Runtime inner-Store resolver and key provider; non-None presence is checked before use.
    :return: The constructed EncryptedStore; dependency, identity, unknown-option, resolver, or constructor failures propagate.
    """
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
    """
    Extract the authority, or slash-stripped path, from a URI with scheme encrypted. Other schemes
    and empty extracted text return None. Do not percent-decode, strip internal whitespace, or
    validate a UUID; query/fragment components are ignored.

    Example:
        >>> _encrypted_inner_ref("encrypted:///inner-id?ignored#fragment")
        'inner-id'


    :param root_uri: URI text parsed by urlparse; malformed URI parsing can raise.
    :return: The selected authority/path text or None, without resolving a Store.
    """
    parsed = urlparse(root_uri)
    if parsed.scheme != "encrypted":
        return None
    return parsed.netloc or parsed.path.strip("/") or None


def _local_path(value: str) -> str:
    """
    Decode local file-URI path bytes while retaining other input strings unchanged. A file URI
    accepts only an empty authority or literal localhost, then percent-decodes the path to bytes and
    uses os.fsdecode so POSIX surrogate escapes survive. Query and fragment are ignored. Other
    schemes, relative paths, and plain strings pass through without rejection, expansion,
    resolution, containment, or existence checks. Even an empty file-URI path is returned for the
    eventual constructor to interpret.

    Example:
        >>> _local_path("file://localhost/tmp/a%20b?ignored#fragment")
        '/tmp/a b'


    :param value: Local path or URI text; only the parsed file scheme triggers decoding and authority validation.
    :return: Decoded filesystem text for a local file URI, otherwise the original value; nonlocal file authorities raise ValueError.
    """
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
    """
    Assemble one default-registry descriptor from concise declaration fields. Map supplied flags
    directly into the passive dataclass, retaining its supports_random_read default of True. A None
    characteristics value creates a fresh default profile; other values are retained unchanged. This
    helper neither registers the descriptor nor validates names, builders, advertised capabilities,
    or configuration compatibility.

    Example:
        >>> descriptor = _descriptor(
        ...     "demo", "Demo", _build_filesystem, access_protocol="file",
        ...     read_only=False, location_type="dir",
        ... )
        >>> descriptor.supports_random_read
        True


    :param kind: Canonical kind intended for later registry registration.
    :param label: Human-readable backend label.
    :param builder: Construction callback retained without invocation.
    :param aliases: Kind lookup aliases to normalize at registration.
    :param access_protocol: Preferred protocol label, separate from kind alias registration.
    :param read_only: Declared default read-only flag.
    :param location_type: Declared dir/file/remote category used by presentation and backed-view checks.
    :param folders: Advertised folder support.
    :param hierarchical: Advertised hierarchical enumeration.
    :param random_write: Advertised random-write support.
    :param delete: Advertised deletion support.
    :param checksums: Advertised checksum support.
    :param immutable: Advertised immutable-object support.
    :param order: Primary ascending presentation sort key.
    :param policy_section: Optional JSON policy section consumed by row translation.
    :param access_protocol_aliases: Additional protocol labels; not automatically registered as kind aliases.
    :param user_selectable: Whether filtered descriptor enumeration includes this backend.
    :param characteristics: Optional profile retained unchanged; None constructs a fresh default StorageCharacteristics.
    :return: A new unregistered StorageBackendDescriptor; supplied-value failures arise only through dataclass construction.
    """
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
    """
    Create the common per-object publication profile with object staging and general write usage.
    Mark unmodelled entries as preserved and container-format rewriting as false; pass limitation
    objects in caller order. This is declared family metadata, not a backend probe or limit
    measurement.

    Example:
        >>> _per_object_characteristics().publication_model is api.StoragePublicationModel.PER_OBJECT
        True


    :param limitations: Zero or more StorageLimitation values retained in their supplied order.
    :return: A new StorageCharacteristics value with the common per-object settings and supplied limitations.
    """

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
    """
    Create the common read-only profile with no declared temporary-space requirement and
    nonapplicable write usage. Other characteristics use their dataclass defaults; specific readers
    needing spooling use explicit profiles instead. No endpoint is inspected.

    Example:
        >>> _read_only_characteristics().temporary_space is api.StorageTemporarySpaceRequirement.NONE
        True


    :param limitations: Zero or more StorageLimitation values retained in their supplied order.
    :return: A new StorageCharacteristics value with read-only defaults and the supplied limitations.
    """

    return api.StorageCharacteristics(
        publication_model=api.StoragePublicationModel.READ_ONLY,
        temporary_space=api.StorageTemporarySpaceRequirement.NONE,
        recommended_write_usage=api.StorageWriteUsage.NOT_APPLICABLE,
        limitations=limitations,
    )


DEFAULT_BACKEND_REGISTRY = StorageBackendRegistry(
    (
        _descriptor(
            "memory", "Process-local memory cache", _build_memory,
            aliases=("ram", "in_memory"), access_protocol="memory",
            read_only=False, location_type="remote", random_write=True,
            delete=True, checksums=True, order=5,
            characteristics=_per_object_characteristics(
                api.StorageLimitation(
                    "process_local_non_durable",
                    "All objects are lost when the Store instance or process ends.",
                ),
            ),
        ),
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
