"""Database-row translation for second-generation Store configurations."""

from __future__ import annotations

import json

from collections.abc import Iterable, Mapping
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from LiuXin_alpha.storage.api import (
    BackupPolicyID,
    ReplicaMode,
    ReplicationPolicyID,
    StoreConfiguration,
    StoreUnsupportedOperation,
)
from LiuXin_alpha.storage.backend_registry import DEFAULT_BACKEND_REGISTRY


_SENSITIVE_OPTION_MARKERS = (
    "access_key",
    "credential",
    "password",
    "private_key",
    "api_key",
    "secret",
    "token",
)


def store_configuration_from_row(
    row: Any,
    *,
    fallback_store_id: int | None = None,
) -> StoreConfiguration:
    """Build a durable configuration without leaking row IDs into Locations."""

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
    )


def store_configuration_to_row_dict(
    configuration: StoreConfiguration,
    *,
    allowed_columns: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Serialize only schema-supported, non-secret configuration fields."""

    allowed = set(allowed_columns or ())

    def keep(key: str) -> bool:
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
        if keep(key) and value is not None
    }


def _row_get(row: Any, key: str, default: Any = None) -> Any:
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


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _boolish(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"1", "true", "yes", "y", "on"}:
            return True
        if lowered in {"0", "false", "no", "n", "off", ""}:
            return False
    return default


def _parse_tags(value: Any) -> tuple[str, ...]:
    if value is None or value == "" or value == ():
        return ()
    if isinstance(value, str):
        try:
            return _parse_tags(json.loads(value))
        except (TypeError, ValueError, json.JSONDecodeError):
            return (value.strip(),) if value.strip() else ()
    if isinstance(value, (list, tuple, set, frozenset)):
        return tuple(
            text for item in value if (text := _optional_text(item)) is not None
        )
    text = _optional_text(value)
    return () if text is None else (text,)


def _store_uuid(value: Any, *, store_id: int | None, root_uri: str) -> UUID:
    if isinstance(value, UUID):
        return value
    if value is not None and value != "":
        try:
            return UUID(str(value))
        except ValueError as error:
            raise ValueError("store_uuid must be a UUID.") from error
    stable_key = f"liuxin-store-row:{store_id}" if store_id is not None else f"liuxin-store-root:{root_uri}"
    return uuid5(NAMESPACE_URL, stable_key)


def _optional_uuid(value: Any) -> UUID | None:
    text = _optional_text(value)
    return None if text is None else UUID(text)


def _policy_id(value: Any, constructor):
    parsed = _to_int(value)
    return None if parsed is None else constructor(parsed)


def _policy_section(store_kind: str) -> str | None:
    try:
        return DEFAULT_BACKEND_REGISTRY.descriptor(store_kind).policy_section
    except (ValueError, StoreUnsupportedOperation):
        return None


def _safe_option_name(key: str) -> bool:
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


def _backend_policy_json(configuration: StoreConfiguration) -> str | None:
    section_name = _policy_section(configuration.store_kind)
    if section_name is None or not configuration.backend_options:
        return None
    options = {
        key: list(value) if isinstance(value, tuple) else value
        for key, value in configuration.backend_options
        if _safe_option_name(key)
    }
    if not options:
        return None
    return json.dumps(
        {
            "backend": configuration.store_kind,
            section_name: options,
        },
        sort_keys=True,
    )


__all__ = [
    "store_configuration_from_row",
    "store_configuration_to_row_dict",
]
