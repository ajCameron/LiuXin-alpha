"""
Database-driven SquashFS archival workflow.

Workflow:
1. Create/reuse an "open" SquashFS store row.
2. Designate source files via `file_store_links` (type: squashfs_designation).
3. Build the SquashFS archive, lock the store, verify hashes from the archive.
4. Duplicate verified file rows into the locked archive store.
"""

from __future__ import annotations

import dataclasses
import hashlib
import json
import pathlib
import re
import tempfile
import time

from contextlib import contextmanager
from collections.abc import Iterable, Mapping, Sequence
from typing import Optional

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.storage.reconcile.models import (
    SquashfsArchivePublishReport,
    SquashfsDesignationReport,
)
from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly import (
    SquashfsReadOnlyStorageBackend,
    build_squashfs_from_manifest,
)
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


OPEN_SQUASHFS_STORE_KIND = "open_squashfs_store"
OPEN_SQUASHFS_STORE_KIND_COMPAT = "open_swuashfs_store"  # historical typo compatibility
LOCKED_SQUASHFS_STORE_KIND = "squashfs_readonly"
SQUASHFS_DESIGNATION_LINK_TYPE = "squashfs_designation"
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

STORE_STATE_OPEN = "open"
STORE_STATE_BUILDING = "building"
STORE_STATE_LOCKED = "locked"
STORE_STATE_FAILED = "failed"
STORE_STATES = {
    STORE_STATE_OPEN,
    STORE_STATE_BUILDING,
    STORE_STATE_LOCKED,
    STORE_STATE_FAILED,
}
STORE_STATE_TRANSITIONS: dict[str, set[str]] = {
    STORE_STATE_OPEN: {STORE_STATE_BUILDING, STORE_STATE_FAILED, STORE_STATE_OPEN},
    STORE_STATE_BUILDING: {STORE_STATE_BUILDING, STORE_STATE_LOCKED, STORE_STATE_FAILED},
    STORE_STATE_LOCKED: {STORE_STATE_LOCKED},
    STORE_STATE_FAILED: {STORE_STATE_FAILED, STORE_STATE_OPEN, STORE_STATE_BUILDING},
}

LINK_STATE_DESIGNATED = "designated"
LINK_STATE_BUILDING = "building"
LINK_STATE_VERIFIED = "verified"
LINK_STATE_HASH_MISMATCH = "hash_mismatch"
LINK_STATE_MISSING = "missing"
LINK_STATE_FAILED = "failed"
LINK_STATES = {
    LINK_STATE_DESIGNATED,
    LINK_STATE_BUILDING,
    LINK_STATE_VERIFIED,
    LINK_STATE_HASH_MISMATCH,
    LINK_STATE_MISSING,
    LINK_STATE_FAILED,
}
LINK_STATE_TRANSITIONS: dict[str, set[str]] = {
    LINK_STATE_DESIGNATED: {
        LINK_STATE_DESIGNATED,
        LINK_STATE_BUILDING,
        LINK_STATE_FAILED,
    },
    LINK_STATE_BUILDING: {
        LINK_STATE_BUILDING,
        LINK_STATE_VERIFIED,
        LINK_STATE_HASH_MISMATCH,
        LINK_STATE_MISSING,
        LINK_STATE_FAILED,
    },
    LINK_STATE_VERIFIED: {
        LINK_STATE_VERIFIED,
        LINK_STATE_BUILDING,
        LINK_STATE_FAILED,
        LINK_STATE_DESIGNATED,
    },
    LINK_STATE_HASH_MISMATCH: {
        LINK_STATE_HASH_MISMATCH,
        LINK_STATE_BUILDING,
        LINK_STATE_FAILED,
        LINK_STATE_DESIGNATED,
    },
    LINK_STATE_MISSING: {
        LINK_STATE_MISSING,
        LINK_STATE_BUILDING,
        LINK_STATE_FAILED,
        LINK_STATE_DESIGNATED,
    },
    LINK_STATE_FAILED: {
        LINK_STATE_FAILED,
        LINK_STATE_BUILDING,
        LINK_STATE_DESIGNATED,
    },
}


@dataclasses.dataclass
class _SquashfsDesignation:
    """Resolved designation entry with source snapshot and link row context."""
    file_id: int
    archive_path: str
    source_row: Row
    source_path: pathlib.Path
    snapshot_sha256: str
    snapshot_size_bytes: int
    snapshot_mtime_ns: Optional[int]
    current_sha256: Optional[str]
    current_size_bytes: Optional[int]
    link_row: Row


def _now_ep_ms() -> int:
    return int(time.time() * 1000)


def _table_columns(db, table_name: str) -> set[str]:
    return set(db.get_column_headings(table_name))


def _coerce_int(value) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_text(value) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_archive_path(raw: str) -> str:
    text = str(raw).strip().replace("\\", "/")
    if not text:
        raise InputIntegrityError("archive_path cannot be empty.")
    if text.startswith("/"):
        raise InputIntegrityError("archive_path must be relative: {!r}".format(raw))

    parts: list[str] = []
    for part in text.split("/"):
        if part in {"", "."}:
            continue
        if part == "..":
            raise InputIntegrityError("archive_path cannot contain '..': {!r}".format(raw))
        parts.append(part)

    if not parts:
        raise InputIntegrityError("archive_path resolves to empty path: {!r}".format(raw))
    return "/".join(parts)


def _sha256_file(path: pathlib.Path, *, chunk_size: int = 1024 * 1024) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _normalize_sha256(candidate: Optional[str]) -> Optional[str]:
    if candidate is None:
        return None
    text = str(candidate).strip().lower()
    if not text:
        return None
    if _SHA256_RE.match(text) is None:
        return None
    return text


def _parse_policy_json(value: Optional[str]) -> dict[str, object]:
    if value is None:
        return {}
    text = str(value).strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _dump_policy_json(payload: dict[str, object]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _is_open_store_kind(kind: Optional[str]) -> bool:
    if kind is None:
        return False
    normalized = str(kind).strip().lower()
    return normalized in {OPEN_SQUASHFS_STORE_KIND, OPEN_SQUASHFS_STORE_KIND_COMPAT}


def _infer_store_state_from_kind(kind: Optional[str]) -> str:
    text = _coerce_text(kind)
    if text is None:
        return STORE_STATE_OPEN
    if text.lower() == LOCKED_SQUASHFS_STORE_KIND:
        return STORE_STATE_LOCKED
    if _is_open_store_kind(text):
        return STORE_STATE_OPEN
    return STORE_STATE_OPEN


def _parse_json_object(value: Optional[str]) -> dict[str, object]:
    if value is None:
        return {}
    text = str(value).strip()
    if not text:
        return {}
    try:
        payload = json.loads(text)
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    return payload


def _encode_json_object(payload: Mapping[str, object]) -> str:
    return json.dumps(dict(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _history_with_transition(
    history: object,
    *,
    to_state: str,
    now_epk: int,
    detail: Optional[str] = None,
) -> list[dict[str, object]]:
    out: list[dict[str, object]] = []
    if isinstance(history, list):
        for item in history:
            if isinstance(item, dict):
                state = _coerce_text(item.get("state"))
                ts = _coerce_int(item.get("timestamp_ep_k"))
                if state is not None:
                    row: dict[str, object] = {"state": state}
                    if ts is not None:
                        row["timestamp_ep_k"] = ts
                    if "detail" in item and item.get("detail") is not None:
                        row["detail"] = str(item.get("detail"))
                    out.append(row)
    row: dict[str, object] = {"state": to_state, "timestamp_ep_k": int(now_epk)}
    if detail is not None:
        row["detail"] = str(detail)
    if not out or out[-1].get("state") != to_state:
        out.append(row)
    else:
        out[-1]["timestamp_ep_k"] = int(now_epk)
        if detail is not None:
            out[-1]["detail"] = str(detail)
    return out


def _validate_transition(*, current_state: str, next_state: str, transitions: Mapping[str, set[str]], kind: str) -> None:
    allowed = transitions.get(current_state)
    if allowed is None:
        raise InputIntegrityError("Unknown {} state {!r}.".format(kind, current_state))
    if next_state not in allowed:
        raise InputIntegrityError(
            "Invalid {} state transition {} -> {}.".format(kind, current_state, next_state)
        )


def _store_scratch_with_state(
    existing_store_scratch: Optional[str],
    *,
    next_state: str,
    now_epk: int,
    detail: Optional[str] = None,
) -> str:
    if next_state not in STORE_STATES:
        raise InputIntegrityError("Unknown store state: {!r}".format(next_state))

    scratch = _parse_json_object(existing_store_scratch)
    current_state = _coerce_text(scratch.get("squashfs_state"))
    if current_state is None:
        current_state = STORE_STATE_OPEN
    _validate_transition(
        current_state=current_state,
        next_state=next_state,
        transitions=STORE_STATE_TRANSITIONS,
        kind="store",
    )

    scratch["squashfs_state"] = next_state
    scratch["squashfs_state_updated_timestamp_ep_k"] = int(now_epk)
    scratch["squashfs_state_history"] = _history_with_transition(
        scratch.get("squashfs_state_history"),
        to_state=next_state,
        now_epk=now_epk,
        detail=detail,
    )
    if detail is not None:
        scratch["squashfs_state_detail"] = str(detail)
    return _encode_json_object(scratch)


def _policy_with_state(
    existing_policy_json: Optional[str],
    *,
    next_state: str,
    now_epk: int,
    detail: Optional[str] = None,
) -> dict[str, object]:
    if next_state not in LINK_STATES:
        raise InputIntegrityError("Unknown designation link state: {!r}".format(next_state))

    policy = _parse_policy_json(existing_policy_json)
    current_state = _coerce_text(policy.get("state"))
    if current_state is None:
        current_state = LINK_STATE_DESIGNATED
    _validate_transition(
        current_state=current_state,
        next_state=next_state,
        transitions=LINK_STATE_TRANSITIONS,
        kind="designation link",
    )
    policy["state"] = next_state
    policy["state_updated_timestamp_ep_k"] = int(now_epk)
    policy["state_history"] = _history_with_transition(
        policy.get("state_history"),
        to_state=next_state,
        now_epk=now_epk,
        detail=detail,
    )
    if detail is not None:
        policy["detail"] = str(detail)
    return policy


@contextmanager
def _db_transaction(db):
    conn = db.driver.get_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _update_row_in_tx(
    conn,
    *,
    table: str,
    id_column: str,
    row_id: int,
    updates: Mapping[str, object],
) -> None:
    if not updates:
        return
    assignments = ", ".join("`{}` = ?".format(col) for col in updates.keys())
    values = list(updates.values()) + [int(row_id)]
    stmt = "UPDATE `{}` SET {} WHERE `{}` = ?".format(table, assignments, id_column)
    conn.execute(stmt, values)


def _insert_row_in_tx(conn, *, table: str, payload: Mapping[str, object]) -> int:
    columns = list(payload.keys())
    placeholders = ", ".join("?" for _ in columns)
    col_sql = ", ".join("`{}`".format(col) for col in columns)
    stmt = "INSERT INTO `{}` ({}) VALUES ({})".format(table, col_sql, placeholders)
    cur = conn.execute(stmt, [payload[col] for col in columns])
    return int(cur.lastrowid)


def _ensure_schema_support(db) -> tuple[set[str], set[str], set[str], set[str], set[str]]:
    tables = set(db.get_tables())
    required_tables = {"stores", "files", "file_store_links"}
    missing_tables = sorted(required_tables - tables)
    if missing_tables:
        raise InputIntegrityError(
            "Database schema missing required tables for SquashFS workflow: {}".format(", ".join(missing_tables))
        )

    store_columns = _table_columns(db, "stores")
    file_columns = _table_columns(db, "files")
    link_columns = _table_columns(db, "file_store_links")
    derivation_columns = _table_columns(db, "file_derivations") if "file_derivations" in tables else set()

    required_store_cols = {"store_root_uri", "store_kind"}
    required_file_cols = {"file_store_id", "file_storage_key"}
    required_link_cols = {"file_store_link_file_id", "file_store_link_store_id", "file_store_link_type"}

    missing_store_cols = sorted(required_store_cols - store_columns)
    missing_file_cols = sorted(required_file_cols - file_columns)
    missing_link_cols = sorted(required_link_cols - link_columns)
    if missing_store_cols or missing_file_cols or missing_link_cols:
        chunks: list[str] = []
        if missing_store_cols:
            chunks.append("stores missing columns: {}".format(", ".join(missing_store_cols)))
        if missing_file_cols:
            chunks.append("files missing columns: {}".format(", ".join(missing_file_cols)))
        if missing_link_cols:
            chunks.append("file_store_links missing columns: {}".format(", ".join(missing_link_cols)))
        raise InputIntegrityError("; ".join(chunks))

    return tables, store_columns, file_columns, link_columns, derivation_columns


def _store_row_id(store_row: Row) -> int:
    if store_row.row_id is not None:
        return int(store_row.row_id)
    return int(store_row["store_id"])


def _designation_link_rows_for_store(db, *, store_id: int) -> list[Row]:
    rows = db.search("file_store_links", "file_store_link_store_id", int(store_id))
    out: list[Row] = []
    for row in rows:
        if _coerce_text(row["file_store_link_type"]) != SQUASHFS_DESIGNATION_LINK_TYPE:
            continue
        out.append(row)
    return out


def _get_store_row(db, *, store_id: int) -> Row:
    row = db.get_row_from_id("stores", int(store_id))
    if row is None:
        raise InputIntegrityError("Store row not found: store_id={}".format(store_id))
    return row


def _resolve_source_file_path(db, *, file_row: Row, store_cache: dict[int, Row]) -> pathlib.Path:
    file_store_id = _coerce_int(file_row["file_store_id"])
    if file_store_id is None:
        raise InputIntegrityError("File {} has no file_store_id.".format(file_row.row_id))

    source_store = store_cache.get(file_store_id)
    if source_store is None:
        source_store = db.get_row_from_id("stores", file_store_id)
        if source_store is None:
            raise InputIntegrityError(
                "File {} references missing source store {}.".format(file_row.row_id, file_store_id)
            )
        store_cache[file_store_id] = source_store

    root_uri = _coerce_text(source_store["store_root_uri"])
    storage_key = _coerce_text(file_row["file_storage_key"])
    if root_uri is None or storage_key is None:
        raise InputIntegrityError(
            "Cannot resolve source path for file {} (root_uri={}, storage_key={}).".format(
                file_row.row_id, root_uri, storage_key
            )
        )

    key_path = pathlib.Path(storage_key).expanduser()
    if key_path.is_absolute():
        target = key_path.resolve()
    else:
        target = pathlib.Path(root_uri).expanduser().joinpath(*storage_key.split("/")).resolve()

    if not target.exists() or not target.is_file():
        raise FileNotFoundError(
            "Designated source file is missing on disk: file_id={}, path={!r}".format(file_row.row_id, str(target))
        )
    return target


def _coerce_designation_item(item) -> tuple[int, Optional[str]]:
    if isinstance(item, Row):
        file_id = _coerce_int(item.row_id if item.row_id is not None else item["file_id"])
        return int(file_id), _coerce_text(item["file_storage_key"])

    if isinstance(item, Mapping):
        file_id = _coerce_int(item.get("file_id", item.get("id", item.get("file"))))
        archive_path = _coerce_text(
            item.get(
                "archive_path",
                item.get("internal_path", item.get("target", item.get("dest"))),
            )
        )
        if file_id is None:
            raise InputIntegrityError("Designation mapping is missing file_id: {!r}".format(dict(item)))
        return int(file_id), archive_path

    if isinstance(item, Sequence) and not isinstance(item, (str, bytes, bytearray)):
        if len(item) != 2:
            raise InputIntegrityError("Designation sequence must be (file_id, archive_path): {!r}".format(item))
        file_id = _coerce_int(item[0])
        if file_id is None:
            raise InputIntegrityError("Designation has invalid file_id: {!r}".format(item[0]))
        return int(file_id), _coerce_text(item[1])

    file_id = _coerce_int(item)
    if file_id is None:
        raise InputIntegrityError(
            "Unsupported designation entry {!r}; expected file_id, (file_id, archive_path), Row, or mapping.".format(
                item
            )
        )
    return int(file_id), None


def _set_store_row_values(store_row: Row, *, updates: Mapping[str, object]) -> None:
    changed = False
    for key, value in updates.items():
        if key not in store_row.allowed_columns:
            continue
        if store_row[key] != value:
            store_row[key] = value
            changed = True
    if changed:
        store_row.sync()


def ensure_open_squashfs_store(
    db,
    *,
    archive_path: str | pathlib.Path,
    store_name: Optional[str] = None,
) -> Row:
    """
    Create or refresh a store row representing an open (not yet locked) SquashFS archive target.
    """
    _, store_columns, _, _, _ = _ensure_schema_support(db)

    archive = pathlib.Path(archive_path).expanduser().resolve()
    if archive.exists() and archive.is_dir():
        raise IsADirectoryError("archive_path points to a directory, expected a file path: {!r}".format(str(archive)))

    existing_rows = db.search("stores", "store_root_uri", str(archive))
    for existing in existing_rows:
        kind = _coerce_text(existing["store_kind"])
        if kind and kind.lower() == LOCKED_SQUASHFS_STORE_KIND:
            raise InputIntegrityError(
                "Store row {} already represents a locked SquashFS archive. "
                "Use a new archive path or reopen it explicitly.".format(_store_row_id(existing))
            )

        if _is_open_store_kind(kind):
            now_epk = _now_ep_ms()
            scratch = _store_scratch_with_state(
                _coerce_text(existing["store_scratch"]),
                next_state=STORE_STATE_OPEN,
                now_epk=now_epk,
                detail="reopened",
            )
            updates = {
                "store_name": store_name or existing["store_name"] or safe_path_to_name(str(archive)),
                "store_kind": OPEN_SQUASHFS_STORE_KIND,
                "store_access_protocol": "squashfs",
                "store_root_uri": str(archive),
        "store_operational_role": "backup",
                "store_operational_role": "backup",
                "store_is_read_only": 0,
                "store_online_status": "offline",
                "store_supports_folders": 0,
                "store_supports_hierarchical_list": 0,
                "store_supports_random_read": 0,
                "store_supports_random_write": 1,
                "store_supports_delete": 1,
                "store_modified_timestamp_ep_k": now_epk,
                "store_scratch": scratch,
            }
            _set_store_row_values(existing, updates=updates)
            return existing

    now_epk = _now_ep_ms()
    scratch = _store_scratch_with_state(
        None,
        next_state=STORE_STATE_OPEN,
        now_epk=now_epk,
        detail="created",
    )
    payload = {
        "store_name": store_name or safe_path_to_name(str(archive)),
        "store_kind": OPEN_SQUASHFS_STORE_KIND,
        "store_access_protocol": "squashfs",
        "store_root_uri": str(archive),
        "store_is_read_only": 0,
        "store_online_status": "offline",
        "store_supports_folders": 0,
        "store_supports_hierarchical_list": 0,
        "store_supports_random_read": 0,
        "store_supports_random_write": 1,
        "store_supports_delete": 1,
        "store_created_timestamp_ep_k": now_epk,
        "store_modified_timestamp_ep_k": now_epk,
        "store_scratch": scratch,
    }
    row_dict = {key: value for key, value in payload.items() if key in store_columns}
    return Row.from_idless_row_dict(db, row_dict=row_dict, table="stores")


def designate_files_for_squashfs_store(
    db,
    *,
    store_id: int,
    designations: Iterable[object],
    replace_existing: bool = False,
) -> SquashfsDesignationReport:
    """
    Designate source files for inclusion in an open SquashFS store.

    `designations` accepts:
    - file_id ints
    - (file_id, archive_path) tuples
    - mappings with `file_id` and optional `archive_path`
    - file `Row` objects
    """
    _, _, _, link_columns, _ = _ensure_schema_support(db)
    store_row = _get_store_row(db, store_id=int(store_id))

    if not _is_open_store_kind(_coerce_text(store_row["store_kind"])):
        raise InputIntegrityError(
            "Store {} is not an open SquashFS store (kind={!r}).".format(store_id, store_row["store_kind"])
        )

    report = SquashfsDesignationReport(
        store_row_id=int(store_id),
        store_root_uri=str(store_row["store_root_uri"]),
        store_name=str(store_row["store_name"] or ""),
    )

    existing_links = _designation_link_rows_for_store(db, store_id=int(store_id))
    existing_by_file_id: dict[int, Row] = {}
    existing_by_archive_path: dict[str, int] = {}
    source_store_cache: dict[int, Row] = {}
    for link_row in existing_links:
        link_file_id = _coerce_int(link_row["file_store_link_file_id"])
        if link_file_id is None:
            continue
        existing_by_file_id[link_file_id] = link_row
        policy = _parse_policy_json(_coerce_text(link_row["file_store_link_policy"]))
        archive_path = _coerce_text(policy.get("archive_path"))
        if archive_path:
            try:
                normalized = _normalize_archive_path(archive_path)
                existing_by_archive_path[normalized] = link_file_id
            except Exception:
                pass

    request_target_map: dict[str, int] = {}
    link_priority = int(store_id)

    for item in designations:
        file_id, archive_path = _coerce_designation_item(item)
        source_file = db.get_row_from_id("files", file_id)
        if source_file is None:
            raise InputIntegrityError("Cannot designate missing file row: file_id={}".format(file_id))
        source_path = _resolve_source_file_path(db, file_row=source_file, store_cache=source_store_cache)
        source_stat = source_path.stat()
        source_size = int(source_stat.st_size)
        source_mtime_ns = _coerce_int(getattr(source_stat, "st_mtime_ns", None))
        source_hash = _normalize_sha256(_coerce_text(source_file["file_hash_sha256"]))
        if source_hash is None:
            source_hash = _sha256_file(source_path)

        if archive_path is None:
            archive_path = _coerce_text(source_file["file_storage_key"]) or _coerce_text(source_file["file_name"])
        if archive_path is None:
            raise InputIntegrityError(
                "Designation for file {} needs archive_path; source row has no storage key/name.".format(file_id)
            )

        normalized_target = _normalize_archive_path(archive_path)
        report.requested_files += 1

        prior_file = request_target_map.get(normalized_target)
        if prior_file is not None and prior_file != file_id:
            raise InputIntegrityError(
                "Duplicate archive_path in designation request: {!r} used by file_ids {} and {}.".format(
                    normalized_target, prior_file, file_id
                )
            )
        request_target_map[normalized_target] = file_id

        already_targeted_file = existing_by_archive_path.get(normalized_target)
        if already_targeted_file is not None and already_targeted_file != file_id:
            raise InputIntegrityError(
                "archive_path {!r} is already designated to file_id {} in store {}.".format(
                    normalized_target, already_targeted_file, store_id
                )
            )

        policy = {
            "archive_path": normalized_target,
            "source_snapshot": {
                "hash_sha256": source_hash,
                "size_bytes": source_size,
                "mtime_ns": source_mtime_ns,
                "path": str(source_path),
                "taken_timestamp_ep_k": _now_ep_ms(),
            },
            "source_hash_sha256": source_hash,
            "source_size_bytes": source_size,
            "source_mtime_ns": source_mtime_ns,
        }
        existing_link = existing_by_file_id.get(file_id)
        if existing_link is None:
            now_epk = _now_ep_ms()
            policy = _policy_with_state(
                None,
                next_state=LINK_STATE_DESIGNATED,
                now_epk=now_epk,
                detail="designated",
            ) | policy
            payload = {
                "file_store_link_file_id": file_id,
                "file_store_link_store_id": int(store_id),
                "file_store_link_priority": link_priority,
                "file_store_link_type": SQUASHFS_DESIGNATION_LINK_TYPE,
                "file_store_link_policy": _dump_policy_json(policy),
            }
            row_dict = {k: v for k, v in payload.items() if k in link_columns and v is not None}
            Row.from_idless_row_dict(db, row_dict=row_dict, table="file_store_links")
            existing_by_archive_path[normalized_target] = file_id
            report.created_links += 1
            continue

        old_policy = _parse_policy_json(_coerce_text(existing_link["file_store_link_policy"]))
        old_target = _coerce_text(old_policy.get("archive_path"))
        if old_target:
            old_target = _normalize_archive_path(old_target)
        if old_target == normalized_target and not replace_existing:
            report.unchanged_links += 1
            continue
        if old_target != normalized_target and not replace_existing:
            raise InputIntegrityError(
                "File {} is already designated to archive_path {!r}. "
                "Pass replace_existing=True to retarget it.".format(file_id, old_target)
            )

        updates = {
            "file_store_link_priority": link_priority,
            "file_store_link_type": SQUASHFS_DESIGNATION_LINK_TYPE,
            "file_store_link_policy": _dump_policy_json(
                _policy_with_state(
                    _coerce_text(existing_link["file_store_link_policy"]),
                    next_state=LINK_STATE_DESIGNATED,
                    now_epk=_now_ep_ms(),
                    detail="retargeted" if old_target != normalized_target else "redesignated",
                )
                | policy
            ),
        }
        _set_store_row_values(existing_link, updates=updates)
        existing_by_archive_path.pop(old_target or "", None)
        existing_by_archive_path[normalized_target] = file_id
        report.updated_links += 1

    report.finished_timestamp_ep_k = _now_ep_ms()
    return report


def _collect_designations(db, *, store_id: int) -> list[_SquashfsDesignation]:
    link_rows = _designation_link_rows_for_store(db, store_id=store_id)
    if not link_rows:
        raise InputIntegrityError(
            "No designated files found for store_id {} (link type {!r}).".format(
                store_id, SQUASHFS_DESIGNATION_LINK_TYPE
            )
        )

    store_cache: dict[int, Row] = {}
    out: list[_SquashfsDesignation] = []
    used_targets: set[str] = set()
    for link_row in link_rows:
        file_id = _coerce_int(link_row["file_store_link_file_id"])
        if file_id is None:
            raise InputIntegrityError("Designation link {} has no file id.".format(link_row.row_id))

        source_row = db.get_row_from_id("files", file_id)
        if source_row is None:
            raise InputIntegrityError("Designation references missing file row: file_id={}".format(file_id))

        policy = _parse_policy_json(_coerce_text(link_row["file_store_link_policy"]))
        archive_path = _coerce_text(policy.get("archive_path"))
        if archive_path is None:
            fallback = _coerce_text(source_row["file_storage_key"]) or _coerce_text(source_row["file_name"])
            archive_path = fallback
        if archive_path is None:
            raise InputIntegrityError(
                "Designation for file {} is missing archive_path and source fallback key.".format(file_id)
            )
        archive_path = _normalize_archive_path(archive_path)
        if archive_path in used_targets:
            raise InputIntegrityError(
                "Store {} has duplicate designated archive_path {!r}.".format(store_id, archive_path)
            )
        used_targets.add(archive_path)

        source_path = _resolve_source_file_path(db, file_row=source_row, store_cache=store_cache)
        snapshot = policy.get("source_snapshot")
        snapshot_hash = None
        snapshot_size = None
        snapshot_mtime_ns = None
        if isinstance(snapshot, dict):
            snapshot_hash = _normalize_sha256(_coerce_text(snapshot.get("hash_sha256")))
            snapshot_size = _coerce_int(snapshot.get("size_bytes"))
            snapshot_mtime_ns = _coerce_int(snapshot.get("mtime_ns"))

        if snapshot_hash is None:
            snapshot_hash = _normalize_sha256(_coerce_text(policy.get("source_hash_sha256")))
        if snapshot_size is None:
            snapshot_size = _coerce_int(policy.get("source_size_bytes"))
        if snapshot_mtime_ns is None:
            snapshot_mtime_ns = _coerce_int(policy.get("source_mtime_ns"))

        if snapshot_hash is None:
            snapshot_hash = _normalize_sha256(_coerce_text(source_row["file_hash_sha256"]))
        if snapshot_hash is None:
            snapshot_hash = _sha256_file(source_path)

        if snapshot_size is None:
            snapshot_size = int(source_path.stat().st_size)
        if snapshot_mtime_ns is None:
            snapshot_mtime_ns = _coerce_int(getattr(source_path.stat(), "st_mtime_ns", None))

        out.append(
            _SquashfsDesignation(
                file_id=file_id,
                archive_path=archive_path,
                source_row=source_row,
                source_path=source_path,
                snapshot_sha256=snapshot_hash,
                snapshot_size_bytes=int(snapshot_size),
                snapshot_mtime_ns=snapshot_mtime_ns,
                current_sha256=None,
                current_size_bytes=None,
                link_row=link_row,
            )
        )

    out.sort(key=lambda item: item.archive_path)
    return out


def _validate_snapshot_consistency(designations: list[_SquashfsDesignation]) -> list[str]:
    errors: list[str] = []
    for item in designations:
        live_stat = item.source_path.stat()
        live_size = int(live_stat.st_size)
        live_hash = _sha256_file(item.source_path)
        item.current_size_bytes = live_size
        item.current_sha256 = live_hash

        if live_size != int(item.snapshot_size_bytes):
            errors.append(
                "file_id={} archive_path={!r} changed size (designated={} live={})".format(
                    item.file_id, item.archive_path, item.snapshot_size_bytes, live_size
                )
            )
            continue

        if live_hash.lower() != item.snapshot_sha256.lower():
            errors.append(
                "file_id={} archive_path={!r} changed hash (designated={} live={})".format(
                    item.file_id, item.archive_path, item.snapshot_sha256, live_hash
                )
            )

    return errors


def _lock_store_row_for_squashfs(store_row: Row, *, archive_path: pathlib.Path) -> None:
    now_epk = _now_ep_ms()
    scratch = _store_scratch_with_state(
        _coerce_text(store_row["store_scratch"]),
        next_state=STORE_STATE_LOCKED,
        now_epk=now_epk,
        detail="publish_complete",
    )
    updates = {
        "store_kind": LOCKED_SQUASHFS_STORE_KIND,
        "store_access_protocol": "squashfs",
        "store_root_uri": str(archive_path),
        "store_operational_role": "archive",
        "store_is_read_only": 1,
        "store_online_status": "online",
        "store_supports_folders": 1,
        "store_supports_hierarchical_list": 1,
        "store_supports_random_read": 1,
        "store_supports_random_write": 0,
        "store_supports_delete": 0,
        "store_supports_immutable_objects": 1,
        "store_modified_timestamp_ep_k": now_epk,
        "store_last_seen_online_timestamp_ep_k": now_epk,
        "store_last_healthcheck_ok_timestamp_ep_k": now_epk,
        "store_scratch": scratch,
    }
    _set_store_row_values(store_row, updates=updates)


def _upsert_designation_state(
    designation_link_row: Row,
    *,
    state: str,
    archive_path: str,
    source_hash: str,
    archive_hash: Optional[str] = None,
    detail: Optional[str] = None,
) -> None:
    now_epk = _now_ep_ms()
    policy = _policy_with_state(
        _coerce_text(designation_link_row["file_store_link_policy"]),
        next_state=str(state),
        now_epk=now_epk,
        detail=detail,
    )
    policy["archive_path"] = archive_path
    policy["source_hash_sha256"] = str(source_hash)
    if archive_hash is not None:
        policy["archive_hash_sha256"] = str(archive_hash)
    policy["updated_timestamp_ep_k"] = now_epk

    updates = {"file_store_link_policy": _dump_policy_json(policy)}
    _set_store_row_values(designation_link_row, updates=updates)


def _ensure_primary_link_for_file(db, *, file_id: int, store_id: int, link_columns: set[str]) -> None:
    for link_row in db.search("file_store_links", "file_store_link_file_id", int(file_id)):
        if _coerce_int(link_row["file_store_link_store_id"]) != int(store_id):
            continue
        if _coerce_text(link_row["file_store_link_type"]) != "primary":
            continue
        return

    payload = {
        "file_store_link_file_id": int(file_id),
        "file_store_link_store_id": int(store_id),
        "file_store_link_priority": 0,
        "file_store_link_type": "primary",
    }
    row_dict = {k: v for k, v in payload.items() if k in link_columns and v is not None}
    Row.from_idless_row_dict(db, row_dict=row_dict, table="file_store_links")


def _ensure_primary_link_for_file_tx(tx_conn, *, file_id: int, store_id: int, link_columns: set[str]) -> None:
    rows = tx_conn.execute(
        """
        SELECT file_store_link_id
        FROM file_store_links
        WHERE file_store_link_file_id = ?
          AND file_store_link_store_id = ?
          AND file_store_link_type = 'primary'
        LIMIT 1
        """,
        (int(file_id), int(store_id)),
    ).fetchall()
    if rows:
        return

    payload = {
        "file_store_link_file_id": int(file_id),
        "file_store_link_store_id": int(store_id),
        "file_store_link_priority": 0,
        "file_store_link_type": "primary",
    }
    row_dict = {k: v for k, v in payload.items() if k in link_columns and v is not None}
    _insert_row_in_tx(tx_conn, table="file_store_links", payload=row_dict)


def _duplicate_verified_file_row(
    tx_conn,
    *,
    source_row: Row,
    source_path: pathlib.Path,
    locked_store_id: int,
    archive_path: str,
    archive_hash: str,
    archive_size: int,
    file_columns: set[str],
    link_columns: set[str],
    existing_rows_by_key: dict[str, object],
) -> tuple[bool, bool, Optional[int]]:
    existing = existing_rows_by_key.get(archive_path)
    if existing is not None:
        if isinstance(existing, Mapping):
            existing_hash = _coerce_text(existing.get("file_hash_sha256"))
            existing_row_id = _coerce_int(existing.get("row_id"))
        else:
            existing_hash = _coerce_text(existing["file_hash_sha256"])
            existing_row_id = _coerce_int(getattr(existing, "row_id", None))
        if existing_hash and existing_hash.lower() == archive_hash.lower():
            return False, True, existing_row_id
        raise InputIntegrityError(
            "Existing file row in target store conflicts with archive entry {!r} (file_id={}).".format(
                archive_path, existing_row_id
            )
        )

    file_name = pathlib.PurePosixPath(archive_path).name
    ext = pathlib.PurePosixPath(file_name).suffix.lower().lstrip(".")
    now_epk = _now_ep_ms()

    payload: dict[str, object] = {}
    for col in file_columns:
        if col == "file_id":
            continue
        if col not in source_row.allowed_columns:
            continue
        payload[col] = source_row[col]

    payload.update(
        {
            "file_store_id": int(locked_store_id),
            "file_storage_key": archive_path,
            "file_name": file_name,
            "file_base_name": pathlib.PurePosixPath(file_name).stem,
            "file_extension": ext,
            "file_size_bytes": int(archive_size),
            "file_hash_sha256": archive_hash.lower(),
            "file_integrity_status": "ok",
            "file_last_seen_timestamp_ep_k": now_epk,
            "file_last_integrity_check_timestamp_ep_k": now_epk,
            "file_acquired_timestamp_ep_k": now_epk,
            "file_modified_timestamp_ep_k": now_epk,
            "file_source": "squashfs_archive_duplicate",
            "file_original_name": _coerce_text(source_row["file_name"]) or file_name,
            "file_original_path": str(source_path),
            "file_folder_id": None,
        }
    )

    row_dict = {k: v for k, v in payload.items() if k in file_columns and v is not None}
    inserted_id = _insert_row_in_tx(tx_conn, table="files", payload=row_dict)
    existing_rows_by_key[archive_path] = {
        "row_id": int(inserted_id),
        "file_hash_sha256": archive_hash.lower(),
    }

    if inserted_id is not None:
        _ensure_primary_link_for_file_tx(
            tx_conn,
            file_id=int(inserted_id),
            store_id=int(locked_store_id),
            link_columns=link_columns,
        )
    return True, False, int(inserted_id)


def _current_store_state(store_row: Row) -> str:
    scratch = _parse_json_object(_coerce_text(store_row["store_scratch"]))
    state = _coerce_text(scratch.get("squashfs_state"))
    if state in STORE_STATES:
        return str(state)
    return _infer_store_state_from_kind(_coerce_text(store_row["store_kind"]))


def _best_effort_mark_store_failed(db, *, store_row: Row, detail: str) -> None:
    now_epk = _now_ep_ms()
    try:
        scratch = _store_scratch_with_state(
            _coerce_text(store_row["store_scratch"]),
            next_state=STORE_STATE_FAILED,
            now_epk=now_epk,
            detail=detail,
        )
        updates = {
            "store_kind": OPEN_SQUASHFS_STORE_KIND,
            "store_access_protocol": "squashfs",
            "store_is_read_only": 0,
            "store_online_status": "offline",
            "store_supports_random_read": 0,
            "store_supports_random_write": 1,
            "store_supports_delete": 1,
            "store_modified_timestamp_ep_k": now_epk,
            "store_scratch": scratch,
        }
        _set_store_row_values(store_row, updates=updates)
    except Exception:
        return


def _supports_file_derivations(*, tables: set[str], derivation_columns: set[str]) -> bool:
    if "file_derivations" not in tables:
        return False
    required = {
        "file_derivation_parent_file_id",
        "file_derivation_child_file_id",
    }
    return required.issubset(derivation_columns)


def _insert_file_derivation_tx(
    tx_conn,
    *,
    parent_file_id: int,
    child_file_id: int,
    derivation_columns: set[str],
    derivation_kind: str = "repacked",
    derivation_note: str = "published_to_squashfs_store",
) -> None:
    if int(parent_file_id) == int(child_file_id):
        return

    existing = tx_conn.execute(
        """
        SELECT file_derivation_id
        FROM file_derivations
        WHERE file_derivation_parent_file_id = ?
          AND file_derivation_child_file_id = ?
        LIMIT 1
        """,
        (int(parent_file_id), int(child_file_id)),
    ).fetchone()
    if existing is not None:
        return

    now_epk = _now_ep_ms()
    payload = {
        "file_derivation_parent_file_id": int(parent_file_id),
        "file_derivation_child_file_id": int(child_file_id),
        "file_derivation_kind": str(derivation_kind),
        "file_derivation_note": str(derivation_note),
        "file_derivation_started_timestamp_ep_k": now_epk,
        "file_derivation_finished_timestamp_ep_k": now_epk,
        "file_derivation_created_timestamp_ep_k": now_epk,
        "file_derivation_modified_timestamp_ep_k": now_epk,
    }
    row_dict = {k: v for k, v in payload.items() if k in derivation_columns and v is not None}
    _insert_row_in_tx(tx_conn, table="file_derivations", payload=row_dict)


def _add_reproducibility_metadata_to_scratch(
    scratch_json: str,
    *,
    build_report: Optional[dict[str, object]],
    now_epk: int,
    published_state: str,
) -> str:
    scratch_payload = _parse_json_object(scratch_json)
    meta: dict[str, object] = {
        "published_state": str(published_state),
        "published_timestamp_ep_k": int(now_epk),
    }
    if isinstance(build_report, dict):
        for key in (
            "manifest_path",
            "output_archive",
            "manifest_sha256",
            "output_sha256",
            "compression",
            "deterministic",
            "file_count",
            "total_input_bytes",
            "output_bytes",
            "mksquashfs_executable",
            "mksquashfs_version",
            "build_flags",
        ):
            if key in build_report:
                meta[key] = build_report[key]
    scratch_payload["squashfs_last_build"] = meta
    return _encode_json_object(scratch_payload)


def publish_open_squashfs_store(
    db,
    *,
    store_id: int,
    output_archive: Optional[str | pathlib.Path] = None,
    compression: str = "zstd",
    deterministic: bool = False,
    force: bool = False,
    duplicate_verified_files: bool = True,
    strict: bool = False,
    refresh_storage_manager: bool = True,
) -> SquashfsArchivePublishReport:
    """
    Build and lock an open SquashFS store, then duplicate verified file rows into it.

    If `strict=True`, any verification or persistence error raises and aborts publication.
    """
    tables, _, file_columns, link_columns, derivation_columns = _ensure_schema_support(db)

    store_row = _get_store_row(db, store_id=int(store_id))
    if not _is_open_store_kind(_coerce_text(store_row["store_kind"])):
        raise InputIntegrityError(
            "Store {} is not an open SquashFS store (kind={!r}).".format(store_id, store_row["store_kind"])
        )
    current_store_state = _current_store_state(store_row)
    if current_store_state not in {STORE_STATE_OPEN, STORE_STATE_FAILED}:
        raise InputIntegrityError(
            "Store {} is in state {!r}; expected one of: {}.".format(
                store_id, current_store_state, ", ".join(sorted({STORE_STATE_OPEN, STORE_STATE_FAILED}))
            )
        )

    if output_archive is None:
        root_uri = _coerce_text(store_row["store_root_uri"])
        if root_uri is None:
            raise InputIntegrityError("Store {} has no store_root_uri.".format(store_id))
        archive_path = pathlib.Path(root_uri).expanduser().resolve()
    else:
        archive_path = pathlib.Path(output_archive).expanduser().resolve()

    designations = _collect_designations(db, store_id=int(store_id))
    report = SquashfsArchivePublishReport(
        store_row_id=int(store_id),
        store_root_uri=str(archive_path),
        store_name=str(store_row["store_name"] or ""),
        designated_files=len(designations),
    )

    # Guard against source drift before we build the archive.
    snapshot_errors = _validate_snapshot_consistency(designations)
    if snapshot_errors:
        report.errors.extend(snapshot_errors)
        report.finished_timestamp_ep_k = _now_ep_ms()
        _best_effort_mark_store_failed(db, store_row=store_row, detail="snapshot_consistency_failed")
        if strict:
            raise InputIntegrityError(
                "Snapshot consistency failed for {} designated file(s).".format(len(snapshot_errors))
            )
        return report

    manifest_path: Optional[pathlib.Path] = None
    try:
        manifest_payload = [{"source": str(item.source_path), "archive_path": item.archive_path} for item in designations]
        with tempfile.NamedTemporaryFile(
            mode="w",
            prefix="liuxin-squashfs-manifest-",
            suffix=".json",
            encoding="utf-8",
            delete=False,
        ) as handle:
            json.dump({"files": manifest_payload}, handle, ensure_ascii=False, indent=2, sort_keys=True)
            manifest_path = pathlib.Path(handle.name)

        build_report = build_squashfs_from_manifest(
            manifest_path=manifest_path,
            output_archive=archive_path,
            compression=compression,
            deterministic=deterministic,
            force=force,
        )
        report.build_report = dataclasses.asdict(build_report)
        report.packed_files = int(build_report.file_count)
        report.reproducibility_metadata = {
            "manifest_sha256": report.build_report.get("manifest_sha256"),
            "output_sha256": report.build_report.get("output_sha256"),
            "mksquashfs_version": report.build_report.get("mksquashfs_version"),
            "mksquashfs_executable": report.build_report.get("mksquashfs_executable"),
            "build_flags": report.build_report.get("build_flags"),
            "compression": report.build_report.get("compression"),
            "deterministic": report.build_report.get("deterministic"),
        }
        report.store_root_uri = str(archive_path)

        archive_backend = SquashfsReadOnlyStorageBackend(url=str(archive_path), name=_coerce_text(store_row["store_name"]))
        existing_target_rows = db.search("files", "file_store_id", int(store_id))
        existing_rows_by_key: dict[str, object] = {}
        for existing in existing_target_rows:
            key = _coerce_text(existing["file_storage_key"])
            if key is None:
                continue
            existing_rows_by_key[key] = existing

        designation_results: list[dict[str, object]] = []
        for item in designations:
            if not archive_backend.exists(item.archive_path):
                detail = "missing_in_archive"
                report.errors.append(
                    "file_id={} archive_path={!r} :: {}".format(item.file_id, item.archive_path, detail)
                )
                designation_results.append(
                    {
                        "designation": item,
                        "state": LINK_STATE_MISSING,
                        "detail": detail,
                        "archive_hash": None,
                        "archive_size": None,
                        "should_duplicate": False,
                    }
                )
                continue

            status = archive_backend.stat(item.archive_path)
            status.recheck_self(all=True)
            archive_hash = _normalize_sha256(str(status.hash or ""))
            archive_size = int(status.size or 0)
            if not archive_hash:
                payload = archive_backend.read_file_bytes(item.archive_path)
                archive_hash = hashlib.sha256(payload).hexdigest()
                archive_size = len(payload)

            expected_hash = item.current_sha256 or item.snapshot_sha256
            if archive_hash.lower() != expected_hash.lower():
                detail = "hash_mismatch"
                report.hash_mismatches.append(
                    "file_id={} archive_path={!r} expected={} got={}".format(
                        item.file_id,
                        item.archive_path,
                        expected_hash,
                        archive_hash,
                    )
                )
                designation_results.append(
                    {
                        "designation": item,
                        "state": LINK_STATE_HASH_MISMATCH,
                        "detail": detail,
                        "archive_hash": archive_hash,
                        "archive_size": archive_size,
                        "should_duplicate": False,
                    }
                )
                continue

            report.verified_files += 1
            designation_results.append(
                {
                    "designation": item,
                    "state": LINK_STATE_VERIFIED,
                    "detail": "verified",
                    "archive_hash": archive_hash,
                    "archive_size": archive_size,
                    "should_duplicate": bool(duplicate_verified_files),
                }
            )

        final_store_state = STORE_STATE_LOCKED
        if report.errors or report.hash_mismatches:
            final_store_state = STORE_STATE_FAILED

        if strict and (report.errors or report.hash_mismatches):
            raise InputIntegrityError(
                "Publication verification failed (errors={}, hash_mismatches={}).".format(
                    len(report.errors), len(report.hash_mismatches)
                )
            )

        derivations_enabled = _supports_file_derivations(
            tables=tables,
            derivation_columns=derivation_columns,
        )

        with _db_transaction(db) as tx_conn:
            now_epk = _now_ep_ms()
            store_id_column = db.driver_wrapper.get_id_column("stores")
            link_id_column = db.driver_wrapper.get_id_column("file_store_links")

            # Store state: open/failed -> building -> locked/failed
            building_scratch = _store_scratch_with_state(
                _coerce_text(store_row["store_scratch"]),
                next_state=STORE_STATE_BUILDING,
                now_epk=now_epk,
                detail="publish_started",
            )
            _update_row_in_tx(
                tx_conn,
                table="stores",
                id_column=store_id_column,
                row_id=int(store_id),
                updates={
                    "store_kind": OPEN_SQUASHFS_STORE_KIND,
                    "store_access_protocol": "squashfs",
                    "store_root_uri": str(archive_path),
                    "store_operational_role": "backup",
                    "store_operational_role": "archive",
                    "store_operational_role": "backup",
                    "store_is_read_only": 0,
                    "store_online_status": "offline",
                    "store_supports_random_read": 0,
                    "store_supports_random_write": 1,
                    "store_supports_delete": 1,
                    "store_modified_timestamp_ep_k": now_epk,
                    "store_scratch": building_scratch,
                },
            )

            for outcome in designation_results:
                item = outcome["designation"]
                final_link_state = str(outcome["state"])
                detail = _coerce_text(outcome["detail"])
                archive_hash = _coerce_text(outcome["archive_hash"])
                archive_size = _coerce_int(outcome["archive_size"])

                base_policy = _policy_with_state(
                    _coerce_text(item.link_row["file_store_link_policy"]),
                    next_state=LINK_STATE_BUILDING,
                    now_epk=now_epk,
                    detail="publish_started",
                )
                base_policy = _policy_with_state(
                    _dump_policy_json(base_policy),
                    next_state=final_link_state,
                    now_epk=now_epk,
                    detail=detail,
                )
                base_policy["archive_path"] = item.archive_path
                base_policy["source_hash_sha256"] = item.snapshot_sha256
                base_policy["source_size_bytes"] = int(item.snapshot_size_bytes)
                base_policy["source_mtime_ns"] = item.snapshot_mtime_ns
                base_policy["source_snapshot"] = {
                    "hash_sha256": item.snapshot_sha256,
                    "size_bytes": int(item.snapshot_size_bytes),
                    "mtime_ns": item.snapshot_mtime_ns,
                    "path": str(item.source_path),
                    "taken_timestamp_ep_k": _coerce_int(
                        _parse_policy_json(_coerce_text(item.link_row["file_store_link_policy"]))
                        .get("source_snapshot", {})
                        .get("taken_timestamp_ep_k")
                    )
                    or now_epk,
                }
                if item.current_sha256 is not None:
                    base_policy["live_source_hash_sha256"] = item.current_sha256
                if item.current_size_bytes is not None:
                    base_policy["live_source_size_bytes"] = int(item.current_size_bytes)
                if archive_hash is not None:
                    base_policy["archive_hash_sha256"] = archive_hash
                if archive_size is not None:
                    base_policy["archive_size_bytes"] = int(archive_size)
                base_policy["updated_timestamp_ep_k"] = now_epk

                _update_row_in_tx(
                    tx_conn,
                    table="file_store_links",
                    id_column=link_id_column,
                    row_id=int(item.link_row.row_id or item.link_row["file_store_link_id"]),
                    updates={"file_store_link_policy": _dump_policy_json(base_policy)},
                )

                if not bool(outcome["should_duplicate"]):
                    continue

                inserted, skipped, child_file_id = _duplicate_verified_file_row(
                    tx_conn,
                    source_row=item.source_row,
                    source_path=item.source_path,
                    locked_store_id=int(store_id),
                    archive_path=item.archive_path,
                    archive_hash=str(archive_hash),
                    archive_size=int(archive_size),
                    file_columns=file_columns,
                    link_columns=link_columns,
                    existing_rows_by_key=existing_rows_by_key,
                )
                if inserted:
                    report.duplicated_files += 1
                if skipped:
                    report.skipped_existing_duplicates += 1
                if inserted and derivations_enabled and child_file_id is not None:
                    source_file_id = _coerce_int(
                        item.source_row.row_id if item.source_row.row_id is not None else item.source_row["file_id"]
                    )
                    if source_file_id is not None:
                        _insert_file_derivation_tx(
                            tx_conn,
                            parent_file_id=int(source_file_id),
                            child_file_id=int(child_file_id),
                            derivation_columns=derivation_columns,
                            derivation_kind="repacked",
                            derivation_note="published_to_squashfs_store",
                        )
                        report.provenance_links_created += 1

            if final_store_state == STORE_STATE_LOCKED:
                final_scratch = _store_scratch_with_state(
                    building_scratch,
                    next_state=STORE_STATE_LOCKED,
                    now_epk=now_epk,
                    detail="publish_complete",
                )
                final_scratch = _add_reproducibility_metadata_to_scratch(
                    final_scratch,
                    build_report=report.build_report,
                    now_epk=now_epk,
                    published_state=STORE_STATE_LOCKED,
                )
                store_updates = {
                    "store_kind": LOCKED_SQUASHFS_STORE_KIND,
                    "store_access_protocol": "squashfs",
                    "store_root_uri": str(archive_path),
                    "store_is_read_only": 1,
                    "store_online_status": "online",
                    "store_supports_folders": 1,
                    "store_supports_hierarchical_list": 1,
                    "store_supports_random_read": 1,
                    "store_supports_random_write": 0,
                    "store_supports_delete": 0,
                    "store_supports_immutable_objects": 1,
                    "store_modified_timestamp_ep_k": now_epk,
                    "store_last_seen_online_timestamp_ep_k": now_epk,
                    "store_last_healthcheck_ok_timestamp_ep_k": now_epk,
                    "store_scratch": final_scratch,
                }
            else:
                final_scratch = _store_scratch_with_state(
                    building_scratch,
                    next_state=STORE_STATE_FAILED,
                    now_epk=now_epk,
                    detail="publish_verification_failed",
                )
                final_scratch = _add_reproducibility_metadata_to_scratch(
                    final_scratch,
                    build_report=report.build_report,
                    now_epk=now_epk,
                    published_state=STORE_STATE_FAILED,
                )
                store_updates = {
                    "store_kind": OPEN_SQUASHFS_STORE_KIND,
                    "store_access_protocol": "squashfs",
                    "store_root_uri": str(archive_path),
                    "store_is_read_only": 0,
                    "store_online_status": "offline",
                    "store_supports_random_read": 0,
                    "store_supports_random_write": 1,
                    "store_supports_delete": 1,
                    "store_modified_timestamp_ep_k": now_epk,
                    "store_scratch": final_scratch,
                }

            _update_row_in_tx(
                tx_conn,
                table="stores",
                id_column=store_id_column,
                row_id=int(store_id),
                updates=store_updates,
            )
    except Exception as exc:
        report.errors.append("publish_failed :: {!r}".format(exc))
        report.finished_timestamp_ep_k = _now_ep_ms()
        _best_effort_mark_store_failed(db, store_row=store_row, detail="publish_failed")
        if strict:
            raise

    finally:
        if manifest_path is not None:
            try:
                manifest_path.unlink(missing_ok=True)
            except Exception:
                pass

    if refresh_storage_manager and hasattr(db, "bootstrap_storage_manager"):
        try:
            db.bootstrap_storage_manager(clear_existing=True)
        except Exception as exc:
            report.errors.append("storage_manager_bootstrap_failed :: {!r}".format(exc))

    report.finished_timestamp_ep_k = _now_ep_ms()
    if strict and report.errors:
        raise InputIntegrityError(
            "SquashFS publish reported errors ({}). First error: {}".format(
                len(report.errors), report.errors[0]
            )
        )
    return report


def publish_squashfs_archive_from_file_ids(
    db,
    *,
    file_ids: Iterable[int],
    archive_path: str | pathlib.Path,
    store_name: Optional[str] = None,
    compression: str = "zstd",
    deterministic: bool = False,
    force: bool = False,
    strict: bool = False,
    refresh_storage_manager: bool = True,
) -> SquashfsArchivePublishReport:
    """
    Convenience helper: create an open store, designate file ids, then publish.
    """
    store_row = ensure_open_squashfs_store(db, archive_path=archive_path, store_name=store_name)
    store_id = _store_row_id(store_row)
    designate_files_for_squashfs_store(
        db,
        store_id=store_id,
        designations=[int(fid) for fid in file_ids],
        replace_existing=False,
    )
    return publish_open_squashfs_store(
        db,
        store_id=store_id,
        output_archive=archive_path,
        compression=compression,
        deterministic=deterministic,
        force=force,
        duplicate_verified_files=True,
        strict=strict,
        refresh_storage_manager=refresh_storage_manager,
    )


__all__ = [
    "OPEN_SQUASHFS_STORE_KIND",
    "LOCKED_SQUASHFS_STORE_KIND",
    "SQUASHFS_DESIGNATION_LINK_TYPE",
    "ensure_open_squashfs_store",
    "designate_files_for_squashfs_store",
    "publish_open_squashfs_store",
    "publish_squashfs_archive_from_file_ids",
    "SquashfsDesignationReport",
    "SquashfsArchivePublishReport",
]
