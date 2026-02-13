"""Stage B: Calibre schema/version handling.

This module is intentionally conservative.

It records observed ``PRAGMA application_id`` and ``PRAGMA user_version`` values
and, when possible, compares them to the Calibre SQL snapshot vendored with
LiuXin.

It does **not** attempt schema upgrades/downgrades.

Default policy:
- Older-than-snapshot schemas are treated as readable best-effort.
- Newer-than-snapshot schemas produce a warning (schema drift is likely).
- Mismatched ``application_id`` produces a warning.
"""

from __future__ import annotations

from typing import Optional, Tuple

from .types import CalibreVersionPlan


def _try_load_snapshot_versions() -> Tuple[Optional[int], Optional[int]]:
    """Return (expected_application_id, latest_supported_user_version) if available."""
    try:
        from LiuXin_alpha.databases.database_driver_plugins.SQL.calibre_database_generator.database_generator import (
            calibre_metadata_application_id,
            calibre_metadata_user_version,
        )

        return int(calibre_metadata_application_id()), int(calibre_metadata_user_version())
    except Exception:
        return None, None


def resolve_version_plan(
    *,
    application_id: int,
    user_version: int,
    target_user_version: str | int = "latest_supported",
    expected_application_id: Optional[int] = None,
    latest_supported_user_version: Optional[int] = None,
    known_user_version_min: int = 0,
) -> CalibreVersionPlan:
    """Build a CalibreVersionPlan for the observed DB pragmas."""
    exp_app = expected_application_id
    latest = latest_supported_user_version

    if exp_app is None or latest is None:
        snap_app, snap_latest = _try_load_snapshot_versions()
        if exp_app is None:
            exp_app = snap_app
        if latest is None:
            latest = snap_latest

    # Default known range: [0, latest] (if latest is known).
    known_max = latest

    warnings: list[str] = []
    status = "ok"

    if exp_app is not None and int(application_id) != int(exp_app):
        status = "application_id_mismatch"
        warnings.append(f"application_id_mismatch:{application_id}!={exp_app}")

    if known_max is not None and int(user_version) > int(known_max):
        if status == "ok":
            status = "newer_than_supported"
        warnings.append(f"schema_newer_than_supported:{user_version}>{known_max}")

    if known_max is not None and int(user_version) < int(known_max) and status == "ok":
        status = "older_than_latest"

    # Resolve target.
    resolved_target: Optional[int]
    if isinstance(target_user_version, int):
        resolved_target = int(target_user_version)
    else:
        if target_user_version == "latest_supported":
            resolved_target = int(known_max) if known_max is not None else None
        else:
            resolved_target = None

    return CalibreVersionPlan(
        application_id=int(application_id),
        user_version=int(user_version),
        target_user_version=resolved_target,
        expected_application_id=None if exp_app is None else int(exp_app),
        latest_supported_user_version=None if latest is None else int(latest),
        known_user_version_min=int(known_user_version_min),
        known_user_version_max=None if known_max is None else int(known_max),
        status=str(status),
        warnings=tuple(warnings),
    )
