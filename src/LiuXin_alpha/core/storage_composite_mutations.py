"""Translate complete Composite declarations and partial patches for storage.

Storage owns membership validation, referenced Assets, persistence and revisions.
Core accepts domain values rather than writes to SQL projections or envelopes.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Mapping
from typing import Any

from LiuXin_alpha.storage.api.storage_manager_api import (
    CompositeDigitalAssetDeclaration,
    CompositeDigitalAssetID,
    CompositeDigitalAssetMembership,
    CompositeDigitalAssetRecord,
    StorageManagerAPI,
)

_MEMBER_FIELDS = frozenset(
    field.name for field in dataclasses.fields(CompositeDigitalAssetMembership)
)


def _members(value: Any) -> tuple[CompositeDigitalAssetMembership, ...]:
    if not isinstance(value, (list, tuple)):
        raise ValueError("Composite members must be an array of objects")
    members = []
    for position, raw in enumerate(value):
        if not isinstance(raw, Mapping):
            raise ValueError("Each Composite member must be an object")
        if any(field not in _MEMBER_FIELDS for field in raw):
            raise ValueError("Unsupported Composite member field")
        if "digital_asset_id" not in raw:
            raise ValueError("Composite members require digital_asset_id")
        fields = dict(raw)
        fields.setdefault("sequence_number", position)
        members.append(CompositeDigitalAssetMembership(**fields))
    return tuple(members)


def _attributes(value: Any) -> tuple[tuple[str, str], ...]:
    pairs = list(value.items()) if isinstance(value, Mapping) else value
    if not isinstance(pairs, (list, tuple)):
        raise ValueError(
            "Composite attributes must be an object or array of string pairs"
        )
    result = []
    for pair in pairs:
        if (
            not isinstance(pair, (list, tuple))
            or len(pair) != 2
            or not all(isinstance(part, str) for part in pair)
        ):
            raise ValueError("Composite attributes must be string pairs")
        result.append((pair[0], pair[1]))
    return tuple(result)


def _declaration(
    values: Mapping[str, Any], current: CompositeDigitalAssetRecord | None = None
) -> CompositeDigitalAssetDeclaration:
    fields: dict[str, Any] = {}
    for raw_name, value in values.items():
        if not isinstance(raw_name, str):
            raise ValueError("Composite field names must be strings")
        name = raw_name.strip().removeprefix("composite_digital_asset_")
        if name not in {"name", "members", "attributes"}:
            raise ValueError(f"Unsupported Composite field: {raw_name!r}")
        if name in fields:
            raise ValueError(f"Duplicate Composite field: {name!r}")
        if name == "members":
            value = _members(value)
        elif name == "attributes":
            value = _attributes(value)
        fields[name] = value
    if current is not None:
        fields.setdefault("members", current.members)
        fields.setdefault("name", current.name)
        fields.setdefault("attributes", current.attributes)
    if "members" not in fields:
        raise ValueError("Composite creation requires complete members")
    return CompositeDigitalAssetDeclaration(**fields)


def _revision(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Composite if_revision must be a nonblank string or null")
    return value


def create_composite(
    manager: StorageManagerAPI, values: Mapping[str, Any]
) -> CompositeDigitalAssetRecord:
    """Declare a nonempty membership graph in one owned transaction."""
    return manager.declare_composite_digital_asset(_declaration(values))


def update_composite(
    manager: StorageManagerAPI,
    identifier: int,
    values: Mapping[str, Any],
    *,
    if_revision: object = None,
) -> CompositeDigitalAssetRecord:
    """Preserve omitted values and fence replacement against the source revision."""
    expected = _revision(if_revision)
    current = manager.get_composite_digital_asset_record(
        CompositeDigitalAssetID(identifier)
    )
    declaration = _declaration(values, current)
    return manager.replace_composite_digital_asset(
        CompositeDigitalAssetID(identifier),
        declaration,
        if_revision=current.revision if expected is None else expected,
    )


def delete_composite(
    manager: StorageManagerAPI, identifier: int, *, if_revision: object = None
) -> bool:
    """Forget only unreferenced metadata; retain member Assets and Store bytes."""
    return manager.forget_composite_digital_asset(
        CompositeDigitalAssetID(identifier),
        require_unlinked=True,
        if_revision=_revision(if_revision),
    )
