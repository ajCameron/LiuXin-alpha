"""Helpers for converting between store rows and `StoreSpec` objects.

This keeps store-row parsing and serialization in one place so the storage
manager, store containers, and DB-facing helpers do not quietly drift apart.
"""

from __future__ import annotations

import json

from collections.abc import Mapping
from typing import Any, Iterable

from LiuXin_alpha.storage.api.info_containers_api import StoreSpec



def _row_get(row: Any, key: str, default: Any = None) -> Any:
    if row is None:
        return default
    if isinstance(row, Mapping):
        return row.get(key, default)
    try:
        return row[key]
    except Exception:
        return getattr(row, key, default)



def _coerce_optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None



def _to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None



def _to_boolish(value: Any, *, default: bool = False) -> bool:
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
    if value in (None, "", ()):
        return ()
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except Exception:
            return (value,)
        return _parse_tags(decoded)
    if isinstance(value, (list, tuple, set, frozenset)):
        out: list[str] = []
        for item in value:
            text = _coerce_optional_str(item)
            if text is not None:
                out.append(text)
        return tuple(out)
    text = _coerce_optional_str(value)
    return (text,) if text is not None else ()



def store_spec_from_row(row: Any, *, fallback_store_id: int | None = None) -> StoreSpec:
    root_uri = _coerce_optional_str(_row_get(row, "store_root_uri"))
    store_id = _to_int(_row_get(row, "store_id"))
    if store_id is None:
        store_id = fallback_store_id
    store_name = _coerce_optional_str(_row_get(row, "store_name")) or str(root_uri or store_id or "store")
    store_kind = _coerce_optional_str(_row_get(row, "store_kind")) or "unknown"

    raw_tags = _row_get(row, "store_tags_json")
    if raw_tags in (None, ""):
        raw_tags = _row_get(row, "store_tags")

    return StoreSpec(
        store_id=store_id,
        store_uuid=_coerce_optional_str(_row_get(row, "store_uuid")) or (
            f"store-{store_id}" if store_id is not None else None
        ),
        store_name=store_name,
        store_kind=store_kind,
        store_url=str(root_uri or ""),
        store_access_protocol=_coerce_optional_str(_row_get(row, "store_access_protocol")),
        store_root_uri=root_uri,
        store_failure_domain=_coerce_optional_str(_row_get(row, "store_failure_domain")),
        store_region=_coerce_optional_str(_row_get(row, "store_region")),
        store_tags=_parse_tags(raw_tags),
        store_default_replication_policy_id=_to_int(_row_get(row, "store_default_replication_policy_id")),
        store_default_backup_policy_id=_to_int(_row_get(row, "store_default_backup_policy_id")),
        store_supports_active_replica_mode=_to_boolish(_row_get(row, "store_supports_active_replica_mode"), default=True),
        store_supports_backup_replica_mode=_to_boolish(_row_get(row, "store_supports_backup_replica_mode"), default=True),
        store_supports_archive_replica_mode=_to_boolish(_row_get(row, "store_supports_archive_replica_mode"), default=True),
        store_operational_role=_coerce_optional_str(_row_get(row, "store_operational_role")),
        store_is_read_only=_to_boolish(_row_get(row, "store_is_read_only"), default=False),
        store_supports_folders=_to_boolish(_row_get(row, "store_supports_folders"), default=True),
        store_policy_json=_coerce_optional_str(_row_get(row, "store_policy_json")),
        store_scratch=_coerce_optional_str(_row_get(row, "store_scratch")),
    )



def store_spec_to_row_dict(
    spec: StoreSpec,
    *,
    allowed_columns: Iterable[str] | None = None,
) -> dict[str, Any]:
    allowed = set(allowed_columns or ())

    def keep(key: str) -> bool:
        return (not allowed) or (key in allowed)

    row_dict: dict[str, Any] = {}

    root_uri = spec.store_root_uri or spec.store_url
    values: dict[str, Any] = {
        "store_name": spec.store_name,
        "store_kind": spec.store_kind,
        "store_access_protocol": spec.store_access_protocol,
        "store_root_uri": root_uri,
        "store_failure_domain": spec.store_failure_domain,
        "store_region": spec.store_region,
        "store_tags_json": json.dumps(list(spec.store_tags)) if spec.store_tags else None,
        "store_default_replication_policy_id": spec.store_default_replication_policy_id,
        "store_default_backup_policy_id": spec.store_default_backup_policy_id,
        "store_supports_active_replica_mode": 1 if spec.store_supports_active_replica_mode else 0,
        "store_supports_backup_replica_mode": 1 if spec.store_supports_backup_replica_mode else 0,
        "store_supports_archive_replica_mode": 1 if spec.store_supports_archive_replica_mode else 0,
        "store_operational_role": spec.store_operational_role,
        "store_is_read_only": 1 if spec.store_is_read_only else 0,
        "store_supports_folders": 1 if spec.store_supports_folders else 0,
        "store_policy_json": spec.store_policy_json,
        "store_scratch": spec.store_scratch,
    }
    for key, value in values.items():
        if keep(key) and value is not None:
            row_dict[key] = value
    return row_dict


__all__ = ["store_spec_from_row", "store_spec_to_row_dict"]
