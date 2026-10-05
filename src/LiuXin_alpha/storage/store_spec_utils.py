
# Todo: There should be no legacy schemas. Some of these may still make sense
"""
Translate durable Store rows and configurations across legacy and current schemas.

Readers support scalar columns and versioned policy extensions while preserving
UUID routing identity. Writers project supported columns without owning database
writes or merging old policy JSON. Compatibility coercions, null omission, and
option-name filtering are deliberate implementation boundaries, not proof of
schema completeness, secret-free values, or an exact round trip for every input.
"""

from __future__ import annotations

import json

from collections.abc import Iterable, Mapping
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from LiuXin_alpha.storage.api import (
    BackupPolicyID,
    DigitalAssetID,
    ReplicaMode,
    ReplicaID,
    ReplicationPolicyID,
    StoreBackingReference,
    StoreConfiguration,
    StoreUnsupportedOperation,
)
from LiuXin_alpha.storage.backend_registry import DEFAULT_BACKEND_REGISTRY
from LiuXin_alpha.utils.adaptors import _optional_text, _to_int, _boolish, _parse_tags, _optional_uuid

_SENSITIVE_OPTION_MARKERS = (
    "access_key",
    "credential",
    "password",
    "private_key",
    "api_key",
    "secret",
    "token",
)
_CONFIGURATION_EXTENSION_KEY = "_liuxin_storage"
_EXTENDED_REPLICA_MODES = frozenset(
    {ReplicaMode.CACHE, ReplicaMode.TRANSIENT, ReplicaMode.UNMANAGED}
)


def store_configuration_from_row(
    row: Any,
    *,
    fallback_store_id: int | None = None,
) -> StoreConfiguration:
    """
    Reconstruct a configured Store from current or compatibility row fields.

    Read mapping keys or row attributes through _row_get, strip optional text, and require a
    nonblank kind and root URI (falling back to store_url). Missing names use the parsed/fallback
    row ID. Missing UUIDs derive from that row ID, or from the root when no ID is available;
    existing UUIDs are validated rather than replaced. This neither writes the generated UUID back
    nor builds a Store.

    Missing legacy mode flags enable active, backup, and archive modes; a version-1 policy extension
    can replace that whole set and supply a backing reference. Malformed outer policy JSON is
    ignored, but malformed recognized extensions raise. Backend options use registry-selected
    sections and name filtering, which does not inspect scalar values for credentials. The final
    StoreConfiguration constructor applies its own selected validation.

    Example:
        >>> config = store_configuration_from_row({
        ...     "store_id": 7, "store_kind": "filesystem", "store_url": "file:///srv/books",
        ... })
        >>> config.store_name, config.store_root_uri
        ('store-7', 'file:///srv/books')


    :param row: Mapping or row-like object containing Store columns; unavailable fields use the documented fallbacks.
    :param fallback_store_id: ID used for default naming and deterministic UUID derivation when store_id cannot be int-converted; not independently converted here.
    :return: A new StoreConfiguration; malformed required text, UUIDs, extension fields, or constructor invariants can raise.
    """

    store_id = _to_int(_row_get(row, "store_id"))
    if store_id is None:
        store_id = fallback_store_id
    root_uri = _optional_text(_row_get(row, "store_root_uri")) or _optional_text(
        _row_get(row, "store_url")
    )
    if root_uri is None:
        raise ValueError("Store row must provide store_root_uri.")
    name = _optional_text(_row_get(row, "store_name")) or f"store-{store_id or 'unknown'}"
    kind = _optional_text(_row_get(row, "store_kind"))
    if kind is None:
        raise ValueError("Store row must provide store_kind.")
    store_uuid = _store_uuid(
        _row_get(row, "store_uuid"),
        store_id=store_id,
        root_uri=root_uri,
    )
    supported_modes: set[ReplicaMode] = set()
    if _boolish(_row_get(row, "store_supports_active_replica_mode"), default=True):
        supported_modes.add(ReplicaMode.ACTIVE)
    if _boolish(_row_get(row, "store_supports_backup_replica_mode"), default=True):
        supported_modes.add(ReplicaMode.BACKUP)
    if _boolish(_row_get(row, "store_supports_archive_replica_mode"), default=True):
        supported_modes.add(ReplicaMode.ARCHIVE)
    extension = _parse_configuration_extension(
        _row_get(row, "store_policy_json")
    )
    extended_modes = extension.get("replica_modes")
    if extended_modes is not None:
        if not isinstance(extended_modes, list) or not all(
            isinstance(value, str) for value in extended_modes
        ):
            raise ValueError(
                "Store policy _liuxin_storage.replica_modes must be a string list."
            )
        try:
            supported_modes = {ReplicaMode(value) for value in extended_modes}
        except ValueError as error:
            raise ValueError(
                "Store policy contains an unknown Replica mode."
            ) from error

    return StoreConfiguration(
        store_uuid=store_uuid,
        store_name=name,
        store_kind=kind,
        store_root_uri=root_uri,
        store_url=_optional_text(_row_get(row, "store_url")),
        store_access_protocol=_optional_text(
            _row_get(row, "store_access_protocol")
        ),
        store_failure_domain=_optional_text(
            _row_get(row, "store_failure_domain")
        ),
        store_region=_optional_text(_row_get(row, "store_region")),
        store_host_uuid=_optional_uuid(_row_get(row, "store_host_uuid")),
        store_device_uuid=_optional_uuid(_row_get(row, "store_device_uuid")),
        store_tags=_parse_tags(
            _row_get(row, "store_tags_json")
            or _row_get(row, "store_tags")
        ),
        store_default_replication_policy_id=_policy_id(
            _row_get(row, "store_default_replication_policy_id"),
            ReplicationPolicyID,
        ),
        store_default_backup_policy_id=_policy_id(
            _row_get(row, "store_default_backup_policy_id"),
            BackupPolicyID,
        ),
        supported_replica_modes=frozenset(supported_modes),
        operational_role=_optional_text(
            _row_get(row, "store_operational_role")
        ),
        read_only=_boolish(
            _row_get(row, "store_is_read_only"),
            default=False,
        ),
        supports_folders=_boolish(
            _row_get(row, "store_supports_folders"),
            default=True,
        ),
        backend_options=_parse_backend_options(
            kind,
            _row_get(row, "store_policy_json"),
        ),
        backing=_parse_backing_reference(extension.get("backing")),
    )


def store_configuration_to_row_dict(
    configuration: StoreConfiguration,
    *,
    allowed_columns: Iterable[str] | None = None,
    include_nulls: bool = False,
) -> dict[str, Any]:
    """
    Project configuration into selected Store columns without writing a row.

    Encode UUIDs as text, legacy mode/boolean flags as integers, tags as JSON, and backend/manager
    extensions as policy JSON. No store_id or legacy store_url column is emitted. A missing or empty
    allowed-column set allows every projected column; a nonempty set filters keys only after all
    values have been computed, so excluded fields can still fail serialization.

    None values are omitted by default. Updating a row with this default result therefore cannot
    clear old nullable fields or an old policy value. Policy serialization builds a new supported
    payload rather than merging unknown sections. Option-name filtering does not establish that
    arbitrary values, URLs, or other configuration text are free of secrets.

    Example:
        >>> config = StoreConfiguration(UUID(int=1), "books", "filesystem", "file:///srv")
        >>> store_configuration_to_row_dict(config, allowed_columns={"store_name"})
        {'store_name': 'books'}


    :param configuration: Configuration whose values are read and converted without mutating it.
    :param allowed_columns: Optional iterable of permitted column names; None and an empty resulting set both disable filtering.
    :param include_nulls: Whether to retain None values so callers may explicitly clear nullable columns.
    :return: A fresh dictionary of projected columns; conversion or JSON errors propagate even for subsequently filtered fields.
    """

    allowed = set(allowed_columns or ())

    def keep(key: str) -> bool:
        """
        Accept any column when the captured allowlist is empty, otherwise require membership. This
        closure only selects keys; it does not validate values or schema compatibility.

        Example:
            >>> keep("store_name")  # doctest: +SKIP


        :param key: Projected column name to compare with the enclosing call's captured set.
        :return: True for an unrestricted or explicitly allowed key, otherwise False.
        """
        return not allowed or key in allowed

    modes = configuration.supported_replica_modes
    values: dict[str, Any] = {
        "store_uuid": str(configuration.store_uuid),
        "store_name": configuration.store_name,
        "store_kind": configuration.store_kind,
        "store_access_protocol": configuration.store_access_protocol,
        "store_root_uri": configuration.store_root_uri,
        "store_failure_domain": configuration.store_failure_domain,
        "store_region": configuration.store_region,
        "store_host_uuid": (
            str(configuration.store_host_uuid)
            if configuration.store_host_uuid is not None
            else None
        ),
        "store_device_uuid": (
            str(configuration.store_device_uuid)
            if configuration.store_device_uuid is not None
            else None
        ),
        "store_tags_json": (
            json.dumps(list(configuration.store_tags))
            if configuration.store_tags
            else None
        ),
        "store_default_replication_policy_id": (
            int(configuration.store_default_replication_policy_id)
            if configuration.store_default_replication_policy_id is not None
            else None
        ),
        "store_default_backup_policy_id": (
            int(configuration.store_default_backup_policy_id)
            if configuration.store_default_backup_policy_id is not None
            else None
        ),
        "store_supports_active_replica_mode": int(ReplicaMode.ACTIVE in modes),
        "store_supports_backup_replica_mode": int(ReplicaMode.BACKUP in modes),
        "store_supports_archive_replica_mode": int(ReplicaMode.ARCHIVE in modes),
        "store_operational_role": configuration.operational_role,
        "store_is_read_only": int(configuration.read_only),
        "store_supports_folders": int(configuration.supports_folders),
        "store_policy_json": _backend_policy_json(configuration),
    }
    return {
        key: value
        for key, value in values.items()
        if keep(key) and (include_nulls or value is not None)
    }


def _row_get(row: Any, key: str, default: Any = None) -> Any:
    """
    Read a compatibility row field using mapping, declared-column, and attribute conventions.

    None returns the default. Mappings use get directly. Other objects with an allowed_columns value
    reject undeclared keys before attempting access. For allowed/unrestricted objects, any Exception
    from subscription triggers an attribute lookup, potentially masking a read failure. Mapping,
    column-list, and final attribute-access errors are not caught here.

    Example:
        >>> _row_get({"store_name": "books"}, "missing", "fallback")
        'fallback'


    :param row: Mapping, row-like object, or None.
    :param key: Column or fallback attribute name.
    :param default: Value used when the field is unavailable; explicit stored None is retained.
    :return: The retrieved value or default, without copying or coercion.
    """
    if row is None:
        return default
    if isinstance(row, Mapping):
        return row.get(key, default)
    allowed_columns = getattr(row, "allowed_columns", None)
    if allowed_columns is not None and key not in allowed_columns:
        return default
    try:
        return row[key]
    except Exception:
        return getattr(row, key, default)


def _store_uuid(value: Any, *, store_id: int | None, root_uri: str) -> UUID:
    """
    Retain/parse an explicit UUID or derive a deterministic legacy Store identity. UUID objects are
    returned unchanged. Other nonempty values must parse as UUID text; whitespace-only input is
    invalid. For None or empty text, UUID5 in the URL namespace hashes liuxin-store-row:<id> when an
    ID exists, otherwise liuxin-store-root:<root>. Row-ID derivation ignores the root and database
    identity, so equal IDs in different catalogues derive equal UUIDs.

    Example:
        >>> _store_uuid(None, store_id=7, root_uri="a") == _store_uuid(None, store_id=7, root_uri="b")
        True


    :param value: Optional persisted UUID object or text.
    :param store_id: Legacy ID used as the stable key when not None, without positivity validation.
    :param root_uri: Fallback identity text used only when both explicit UUID and row ID are absent.
    :return: An explicit or derived UUID; malformed explicit UUID text raises ValueError.
    """
    if isinstance(value, UUID):
        return value
    if value is not None and value != "":
        try:
            return UUID(str(value))
        except ValueError as error:
            raise ValueError("store_uuid must be a UUID.") from error
    stable_key = f"liuxin-store-row:{store_id}" if store_id is not None else f"liuxin-store-root:{root_uri}"
    return uuid5(NAMESPACE_URL, stable_key)


def _policy_id(value: Any, constructor):
    """
    Convert a legacy policy value with _to_int and wrap any resulting integer with the supplied
    constructor. Invalid optional text becomes None; constructor validation and other conversion
    errors propagate.

    Example:
        >>> _policy_id("12", int), _policy_id("bad", int)
        (12, None)


    :param value: Stored optional policy identifier.
    :param constructor: Callable receiving the parsed integer, commonly a nominal ID constructor.
    :return: The constructed identifier, or None when _to_int treats the input as absent.
    """
    parsed = _to_int(value)
    return None if parsed is None else constructor(parsed)


def _policy_section(store_kind: str) -> str | None:
    """
    Resolve the backend policy section through the shared default registry. Invalid/unknown kinds
    return None when lookup raises ValueError or StoreUnsupportedOperation; other errors propagate.
    A known descriptor can also declare no section.

    Example:
        >>> _policy_section("S3-compatible")
        's3'


    :param store_kind: Backend kind or alias accepted by the default registry.
    :return: The descriptor's policy section name, or None.
    """
    try:
        return DEFAULT_BACKEND_REGISTRY.descriptor(store_kind).policy_section
    except (ValueError, StoreUnsupportedOperation):
        return None


def _safe_option_name(key: str) -> bool:
    """
    Apply the legacy option-name exclusion heuristic using stripped lowercase text. Reject empty
    names, env, and names containing access_key, credential, password, private_key, api_key, secret,
    or token anywhere. This neither validates a backend-specific option schema nor examines values;
    innocuous names can still carry sensitive values and substring matches can exclude harmless
    names.

    Example:
        >>> _safe_option_name("timeout_s"), _safe_option_name("SESSION_TOKEN")
        (True, False)


    :param key: Option-name string; non-string inputs can fail at strip/lower.
    :return: True when the name passes this heuristic, without a secrecy guarantee.
    """
    lowered = key.strip().lower()
    return (
        bool(lowered)
        and lowered != "env"
        and not any(marker in lowered for marker in _SENSITIVE_OPTION_MARKERS)
    )


def _parse_backend_options(
    store_kind: str,
    policy_json: Any,
) -> tuple[tuple[str, object], ...]:
    """
    Extract sorted scalar/string-tuple options from the backend's policy section. Resolve the
    section through the default registry and JSON-decode str(policy_json). Missing sections,
    malformed outer JSON, or non-object sections produce no options. Strip keys, discard excluded
    names and unsupported value shapes, and turn JSON arrays consisting entirely of strings into
    tuples. Scalar values, including None and nonfinite floats accepted by json, are retained.

    The policy's backend label is ignored in favor of store_kind. Keys are not case-normalized or
    deduplicated after stripping; collisions can later fail sorting or StoreConfiguration
    validation. Values are not scanned for secrets.

    Example:
        >>> _parse_backend_options("s3", '{"s3":{"timeout_s":4,"secret":"x","labels":["a"]}}')
        (('labels', ('a',)), ('timeout_s', 4))


    :param store_kind: Kind or alias selecting a policy section from the default registry.
    :param policy_json: Optional JSON source; arbitrary mapping objects are stringified, not read directly.
    :return: Sorted (name, value) pairs for retained options, or an empty tuple for handled missing/malformed containers.
    """
    section_name = _policy_section(store_kind)
    if section_name is None or policy_json is None or policy_json == "":
        return ()
    try:
        payload = json.loads(str(policy_json))
    except (TypeError, ValueError, json.JSONDecodeError):
        return ()
    if not isinstance(payload, Mapping):
        return ()
    section = payload.get(section_name)
    if not isinstance(section, Mapping):
        return ()
    options: list[tuple[str, object]] = []
    for raw_key, raw_value in section.items():
        key = str(raw_key).strip()
        if not _safe_option_name(key):
            continue
        value: object = raw_value
        if isinstance(raw_value, list) and all(
            isinstance(item, str) for item in raw_value
        ):
            value = tuple(raw_value)
        if value is None or isinstance(value, (str, int, float, bool, tuple)):
            options.append((key, value))
    return tuple(sorted(options))


def _parse_configuration_extension(policy_json: Any) -> Mapping[str, Any]:
    """
    Decode the manager-owned _liuxin_storage section from backend policy JSON. Missing/invalid outer
    JSON, a non-object outer value, or an absent/null extension returns an empty mapping. A present
    extension must be an object with version equal to 1, defaulting to 1 when omitted. Equality
    admits True and 1.0; unknown fields are retained and mode/backing validation occurs later.

    Example:
        >>> _parse_configuration_extension('{"_liuxin_storage":{"version":1}}')
        {'version': 1}


    :param policy_json: Optional policy source, passed to JSON decoding after str conversion.
    :return: The decoded extension mapping or a fresh empty dictionary; invalid recognized shape/version raises ValueError.
    """

    if policy_json is None or policy_json == "":
        return {}
    try:
        payload = json.loads(str(policy_json))
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    if not isinstance(payload, Mapping):
        return {}
    extension = payload.get(_CONFIGURATION_EXTENSION_KEY)
    if extension is None:
        return {}
    if not isinstance(extension, Mapping):
        raise ValueError(
            "Store policy _liuxin_storage extension must be an object."
        )
    version = extension.get("version", 1)
    if version != 1:
        raise ValueError(
            f"Unsupported Store policy _liuxin_storage version: {version!r}."
        )
    return extension


def _parse_backing_reference(value: Any) -> StoreBackingReference | None:
    """
    Reconstruct an optional Asset-backed Store reference from a policy mapping. None means no
    backing. Require a mapping and an int-convertible Asset ID; invalid optional Replica IDs become
    absent through _to_int. Nonempty materialization UUID text must parse. Integer coercion precedes
    the StoreBackingReference positive-ID checks, so True and positive truncatable floats can become
    accepted integer identities. No referenced record is resolved.

    Example:
        >>> int(_parse_backing_reference({"digital_asset_id": "7"}).digital_asset_id)
        7


    :param value: Decoded backing object with digital_asset_id and optional Replica/materialization fields, or None.
    :return: A validated StoreBackingReference or None; malformed required fields, nonpositive IDs, and invalid UUIDs raise.
    """

    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise ValueError("Store policy backing reference must be an object.")
    asset_id = _to_int(value.get("digital_asset_id"))
    if asset_id is None:
        raise ValueError(
            "Store policy backing reference requires digital_asset_id."
        )
    replica_id = _to_int(value.get("preferred_replica_id"))
    raw_materialization_ref = value.get("materialization_store_uuid")
    try:
        materialization_ref = (
            None
            if raw_materialization_ref is None
            or raw_materialization_ref == ""
            else UUID(str(raw_materialization_ref))
        )
    except ValueError as error:
        raise ValueError(
            "Store policy materialization_store_uuid must be a UUID."
        ) from error
    return StoreBackingReference(
        DigitalAssetID(asset_id),
        preferred_replica_id=(
            None if replica_id is None else ReplicaID(replica_id)
        ),
        materialization_store_ref=materialization_ref,
    )


def _backend_policy_json(configuration: StoreConfiguration) -> str | None:
    """
    Build a fresh policy payload for supported backend options and manager metadata. Resolve the
    backend section, discard excluded option names, and encode string tuples as JSON arrays. Emit
    the backend label only when retained options exist. Emit a version-1 manager extension for
    backing or any cache/transient/ unmanaged mode; when extended modes are present, serialize the
    complete mode set in ReplicaMode declaration order. Legacy-only modes use scalar row flags.

    No prior policy sections are merged. An unknown backend can still carry a manager extension.
    Name filtering does not inspect values, and JSON encoding may fail or emit nonfinite numeric
    literals under the default json settings.

    Example:
        >>> config = StoreConfiguration(UUID(int=1), "books", "filesystem", "file:///srv")
        >>> _backend_policy_json(config) is None
        True


    :param configuration: Configuration supplying backend options, mode set, and optional backing relationship.
    :return: Sorted-key JSON text, or None when there are no supported options or manager extension fields.
    """
    section_name = _policy_section(configuration.store_kind)
    payload: dict[str, object] = {}
    if section_name is not None and configuration.backend_options:
        options = {
            key: list(value) if isinstance(value, tuple) else value
            for key, value in configuration.backend_options
            if _safe_option_name(key)
        }
        if options:
            payload.update(
                {
                    "backend": configuration.store_kind,
                    section_name: options,
                }
            )

    extension: dict[str, object] = {"version": 1}
    modes = configuration.supported_replica_modes
    if modes & _EXTENDED_REPLICA_MODES:
        extension["replica_modes"] = [
            mode.value for mode in ReplicaMode if mode in modes
        ]
    if configuration.backing is not None:
        backing = configuration.backing
        extension["backing"] = {
            "digital_asset_id": int(backing.digital_asset_id),
            "preferred_replica_id": (
                None
                if backing.preferred_replica_id is None
                else int(backing.preferred_replica_id)
            ),
            "materialization_store_uuid": (
                None
                if backing.materialization_store_ref is None
                else str(backing.materialization_store_ref)
            ),
        }
    if len(extension) > 1:
        payload[_CONFIGURATION_EXTENSION_KEY] = extension
    if not payload:
        return None
    return json.dumps(payload, sort_keys=True)


__all__ = [
    "store_configuration_from_row",
    "store_configuration_to_row_dict",
]
