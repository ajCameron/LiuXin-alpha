"""
Designate local catalogue files and publish a verified SquashFS archive view.

Open Store declarations and designation snapshots are written incrementally.
Publication compares current source bytes with those snapshots, builds an archive,
then checks members before a legacy-row transaction records state, duplicates, and
links. Optional StorageManager bootstrap and Asset/Replica adoption form a later
phase; unchanged archived bytes remain replicas of the source Asset, not derivations.

No transaction covers filesystem publication and every database phase together.
Strict mode controls failure reporting rather than whole-operation rollback.
Reports can retain counters from work preceding a rollback, and temporary-manifest
cleanup does not remove a built archive. Source paths follow local filesystem
resolution and are not restricted to their declared Store root by these helpers.

Example:
    >>> report = publish_squashfs_archive_from_file_ids(db, file_ids=[7], archive_path=archive_path)  # doctest: +SKIP
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
from uuid import UUID, uuid4

from LiuXin_alpha.databases.row import Row
from LiuXin_alpha.errors import InputIntegrityError
from LiuXin_alpha.storage.api import (
    DigitalAssetMetadata,
    Location,
    ReplicaMode,
)
from LiuXin_alpha.storage.reconcile.models import (
    SquashfsArchivePublishReport,
    SquashfsDesignationReport,
)
from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly.squashfs_readonly_storage_backend import (
    SquashfsReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.squashfs_readonly.squashfs_manifest_builder import (
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
    """
    Carry a resolved designation, its recorded snapshot, and later live observations.

    This mutable record retains live source/link Row objects. Snapshot claims can come from legacy
    metadata; construction does not establish that the source is unchanged. Publication fills
    current hash/size during its separate pre-build check.

    Example:
        >>> item.current_sha256  # doctest: +SKIP


    :ivar file_id: Legacy source file identity.
    :ivar archive_path: Normalized relative member target.
    :ivar source_row: Borrowed source Row supplying metadata and Store identity.
    :ivar source_path: Resolved existing local source file path.
    :ivar snapshot_sha256: Selected normalized digest claim or fallback calculated digest.
    :ivar snapshot_size_bytes: Recorded size or current-stat fallback in bytes.
    :ivar snapshot_mtime_ns: Recorded/fallback modification time in nanoseconds, or None.
    :ivar current_sha256: Live pre-build digest, initially None.
    :ivar current_size_bytes: Live pre-build stat size, initially None.
    :ivar link_row: Borrowed designation Row used for policy updates.
    """
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
    """
    Read wall-clock Unix-epoch milliseconds, truncating fractional milliseconds.

    Example:
        >>> isinstance(_now_ep_ms(), int)
        True


    :return: Current epoch milliseconds, without a monotonicity guarantee.
    """
    return int(time.time() * 1000)


def _table_columns(db, table_name: str) -> set[str]:
    """
    Collect database-advertised column headings into a set.

    Example:
        >>> columns = _table_columns(db, "stores")  # doctest: +SKIP


    :param db: Database facade supplying column headings.
    :param table_name: Table requested without independent validation.
    :return: Unique advertised column names.
    """
    return set(db.get_column_headings(table_name))


def _coerce_int(value) -> Optional[int]:
    """
    Convert a value with int, returning None for absence, TypeError, or ValueError.

    Other failures, including OverflowError, propagate; successful conversion can truncate a float.

    Example:
        >>> _coerce_int('7')
        7


    :param value: Candidate integer-like value.
    :return: Converted integer, or None for the handled invalid cases.
    """
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_text(value) -> Optional[str]:
    """
    Stringify and strip a non-None value, treating blank text as absent.

    Example:
        >>> _coerce_text('  books  ')
        'books'


    :param value: Value converted through str, or None.
    :return: Nonblank stripped text or None; stringification errors propagate.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_archive_path(raw: str) -> str:
    """
    Normalize slash-separated member text while rejecting empty, leading-slash, and parent-traversal
    paths.

    Trim outer whitespace, replace backslashes, and remove empty/dot components. This lexical helper
    does not enforce every backend path rule, such as control-character, drive-prefix, or length
    policy.

    Example:
        >>> _normalize_archive_path(' ./books//one.epub ')
        'books/one.epub'


    :param raw: Member target stringified before lexical normalization.
    :return: Nonempty relative-looking POSIX key; rejected forms raise InputIntegrityError.
    """
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
    """
    Hash bytes from a binary file stream in chunks.

    No stable-file snapshot or chunk-size validation is added. A zero chunk size reads no bytes; a
    negative size uses the underlying read-all behavior.

    Example:
        >>> digest = _sha256_file(path)  # doctest: +SKIP


    :param path: Path opened for binary reading.
    :param chunk_size: Maximum bytes requested per read, normally a positive integer.
    :return: Lowercase SHA-256 hex digest of bytes read; I/O failures propagate.
    """
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
    return hasher.hexdigest()


def _normalize_sha256(candidate: Optional[str]) -> Optional[str]:
    """
    Accept only stripped, lowercased 64-digit hexadecimal SHA-256 text.

    This validates the claim shape without hashing bytes or proving correspondence to a file.

    Example:
        >>> _normalize_sha256('A' * 64) == 'a' * 64
        True


    :param candidate: Digest-like value, or None.
    :return: Normalized digest text, or None for absence/invalid shape.
    """
    if candidate is None:
        return None
    text = str(candidate).strip().lower()
    if not text:
        return None
    if _SHA256_RE.match(text) is None:
        return None
    return text


def _parse_policy_json(value: Optional[str]) -> dict[str, object]:
    """
    Parse designation policy as a JSON object, falling back to an empty mapping.

    None, blank text, ordinary JSON-decoding failures, and non-object JSON become an empty dict.
    Stringification before parsing can still raise. Object contents are not schema-validated.

    Example:
        >>> _parse_policy_json('[]')
        {}


    :param value: Optional JSON-like value stringified and stripped before parsing.
    :return: Decoded object dict or a fresh empty dict.
    """
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
    """
    Serialize a policy mapping with sorted keys, compact separators, and unescaped Unicode.

    Use json.dumps defaults for other values; this is stable formatting, not validation or secret
    redaction.

    Example:
        >>> _dump_policy_json({'state': 'open'})
        '{"state":"open"}'


    :param payload: JSON-serializable policy dict.
    :return: Compact JSON text; serialization failures propagate.
    """
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _is_open_store_kind(kind: Optional[str]) -> bool:
    """
    Recognize the canonical open Store kind and its retained historical spelling.

    Comparison strips text and ignores case; None and other labels return False.

    Example:
        >>> _is_open_store_kind(OPEN_SQUASHFS_STORE_KIND_COMPAT.upper())
        True


    :param kind: Optional Store-kind label.
    :return: Whether the label denotes an open SquashFS Store kind.
    """
    if kind is None:
        return False
    normalized = str(kind).strip().lower()
    return normalized in {OPEN_SQUASHFS_STORE_KIND, OPEN_SQUASHFS_STORE_KIND_COMPAT}


def _infer_store_state_from_kind(kind: Optional[str]) -> str:
    """
    Infer locked only from the locked kind, defaulting every other label to open.

    This fallback does not prove that an unknown Store kind supports SquashFS.

    Example:
        >>> _infer_store_state_from_kind('other')
        'open'


    :param kind: Optional kind normalized through the text helper.
    :return: locked for squashfs_readonly, otherwise open.
    """
    text = _coerce_text(kind)
    if text is None:
        return STORE_STATE_OPEN
    if text.lower() == LOCKED_SQUASHFS_STORE_KIND:
        return STORE_STATE_LOCKED
    if _is_open_store_kind(text):
        return STORE_STATE_OPEN
    return STORE_STATE_OPEN


def _parse_json_object(value: Optional[str]) -> dict[str, object]:
    """
    Parse scratch metadata as a JSON object, falling back to an empty mapping.

    None, blank text, ordinary JSON-decoding failures, and non-object JSON become an empty dict.
    Stringification before parsing can still raise. Object contents are not schema-validated.

    Example:
        >>> _parse_json_object('[]')
        {}


    :param value: Optional JSON-like value stringified and stripped before parsing.
    :return: Decoded object dict or a fresh empty dict.
    """
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
    """
    Shallow-copy a mapping and encode compact sorted JSON without ASCII escaping.

    Example:
        >>> _encode_json_object({'state': 'open'})
        '{"state":"open"}'


    :param payload: Mapping whose values must be JSON-serializable under json.dumps defaults.
    :return: JSON object text, without writing it to a Row.
    """
    return json.dumps(dict(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _history_with_transition(
    history: object,
    *,
    to_state: str,
    now_epk: int,
    detail: Optional[str] = None,
) -> list[dict[str, object]]:
    """
    Copy usable history rows and append or refresh the final state observation.

    Retain only dict entries with nonblank states, coercible timestamps, and optional stringified
    detail. State names are not validated here. Repeating the last state updates its timestamp and
    preserves old detail when none is supplied.

    Example:
        >>> _history_with_transition([], to_state='open', now_epk=1)
        [{'state': 'open', 'timestamp_ep_k': 1}]


    :param history: Prior list-like history; only an actual list is traversed.
    :param to_state: New state label retained verbatim.
    :param now_epk: Epoch milliseconds int-converted for the appended/refreshed row.
    :param detail: Optional explanatory value stringified when supplied.
    :return: Fresh sanitized history list; input dictionaries are not mutated.
    """
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
    """
    Require a known current state and an allowed outgoing edge in the supplied graph.

    Example:
        >>> _validate_transition(current_state="open", next_state="building", transitions=STORE_STATE_TRANSITIONS, kind="store")


    :param current_state: Exact source state key.
    :param next_state: Requested destination label.
    :param transitions: Mapping from state to accepted destination labels.
    :param kind: Object label included in diagnostic errors.
    :return: None for a permitted edge; otherwise raise InputIntegrityError.
    """
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
    """
    Validate a Store transition and retain unrelated decoded metadata.

    Malformed/non-object JSON falls back to defaults; missing current state defaults to the initial
    state. Preserve old detail when detail is None, and update repeated-state history timestamps.
    Unknown states or forbidden transitions raise InputIntegrityError.

    Example:
        >>> value = _store_scratch_with_state(None, next_state=STORE_STATE_OPEN, now_epk=1)


    :param existing_store_scratch: Prior serialized metadata, or None for initial state.
    :param next_state: Requested exact state from the corresponding state vocabulary.
    :param now_epk: Epoch milliseconds for current-state and history timestamps.
    :param detail: Optional detail replacing the retained detail when supplied.
    :return: Compact JSON scratch object with state/history updates.
    """
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
    """
    Validate a designation link transition and retain unrelated decoded metadata.

    Malformed/non-object JSON falls back to defaults; missing current state defaults to the initial
    state. Preserve old detail when detail is None, and update repeated-state history timestamps.
    Unknown states or forbidden transitions raise InputIntegrityError.

    Example:
        >>> value = _policy_with_state(None, next_state=LINK_STATE_DESIGNATED, now_epk=1)


    :param existing_policy_json: Prior serialized metadata, or None for initial state.
    :param next_state: Requested exact state from the corresponding state vocabulary.
    :param now_epk: Epoch milliseconds for current-state and history timestamps.
    :param detail: Optional detail replacing the retained detail when supplied.
    :return: Fresh decoded policy dict with validated state/history updates.
    """
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
    """
    Yield a driver connection after BEGIN IMMEDIATE and commit on normal context exit.

    For ordinary Exceptions from begin/body/commit, attempt rollback and re-raise. Ignore ordinary
    rollback/close failures, always attempting close after acquisition. BaseException subclasses
    bypass the explicit rollback handler. This context owns this connection, not external file
    publication or another manager transaction.

    Example:
        >>> with _db_transaction(db) as connection:  # doctest: +SKIP
        ...     connection.execute(statement, values)


    :param db: Database whose driver supplies a fresh transaction connection.
    :return: Context manager yielding the connection; it commits or attempts rollback/close on exit.
    """
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
    """
    Execute a parameterized row update on the supplied connection, skipping empty updates.

    Values are bound, but table/column identifiers are interpolated with backticks and must be
    trusted. This does not check affected-row count, commit, or update cached Row objects.

    Example:
        >>> _update_row_in_tx(connection, table="stores", id_column="store_id", row_id=1, updates={"store_name": "Archive"})  # doctest: +SKIP


    :param conn: Borrowed active transaction connection.
    :param table: Trusted SQL table identifier.
    :param id_column: Trusted primary-key column identifier.
    :param row_id: Identity int-converted for the WHERE value.
    :param updates: Ordered trusted column/value mapping.
    :return: None after execution, or immediately for an empty mapping.
    """
    if not updates:
        return
    assignments = ", ".join("`{}` = ?".format(col) for col in updates.keys())
    values = list(updates.values()) + [int(row_id)]
    stmt = "UPDATE `{}` SET {} WHERE `{}` = ?".format(table, assignments, id_column)
    conn.execute(stmt, values)


def _insert_row_in_tx(conn, *, table: str, payload: Mapping[str, object]) -> int:
    """
    Insert mapped values through a borrowed transaction and return the cursor lastrowid.

    Identifiers are trusted interpolated text; values are bound. No empty-payload special case,
    commit, or uniqueness handling is added.

    Example:
        >>> identity = _insert_row_in_tx(connection, table="files", payload=payload)  # doctest: +SKIP


    :param conn: Borrowed active transaction connection.
    :param table: Trusted SQL table identifier.
    :param payload: Ordered trusted column/value mapping.
    :return: lastrowid converted to int; SQL/conversion failures propagate.
    """
    columns = list(payload.keys())
    placeholders = ", ".join("?" for _ in columns)
    col_sql = ", ".join("`{}`".format(col) for col in columns)
    stmt = "INSERT INTO `{}` ({}) VALUES ({})".format(table, col_sql, placeholders)
    cur = conn.execute(stmt, [payload[col] for col in columns])
    return int(cur.lastrowid)


def _ensure_schema_support(db) -> tuple[set[str], set[str], set[str], set[str]]:
    """
    Require the legacy Store/file/link tables and minimal SquashFS address/link columns.

    Return all advertised columns without validating every later field or requiring a derivation
    table. Missing required tables/columns raise InputIntegrityError.

    Example:
        >>> tables, stores, files, links = _ensure_schema_support(db)  # doctest: +SKIP


    :param db: Database facade supplying table and column inventories.
    :return: Table names and the Store, file, link column sets.
    """
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

    return tables, store_columns, file_columns, link_columns


def _store_row_id(store_row: Row) -> int:
    """
    Prefer Row.row_id and otherwise read store_id, then convert to int.

    Example:
        >>> identity = _store_row_id(row)  # doctest: +SKIP


    :param store_row: Store Row with either identity representation.
    :return: Integer Store row identity; missing/invalid values propagate errors.
    """
    if store_row.row_id is not None:
        return int(store_row.row_id)
    return int(store_row["store_id"])


def _designation_link_rows_for_store(db, *, store_id: int) -> list[Row]:
    """
    Select a Store's links whose stripped type exactly matches squashfs_designation.

    Retain database order and duplicates; other link types are ignored.

    Example:
        >>> links = _designation_link_rows_for_store(db, store_id=1)  # doctest: +SKIP


    :param db: Database searched for legacy links.
    :param store_id: Store identity int-converted for the search.
    :return: List of matching live Row objects.
    """
    rows = db.search("file_store_links", "file_store_link_store_id", int(store_id))
    out: list[Row] = []
    for row in rows:
        if _coerce_text(row["file_store_link_type"]) != SQUASHFS_DESIGNATION_LINK_TYPE:
            continue
        out.append(row)
    return out


def _get_store_row(db, *, store_id: int) -> Row:
    """
    Fetch a Store Row by integer identity and reject a missing result.

    Example:
        >>> row = _get_store_row(db, store_id=1)  # doctest: +SKIP


    :param db: Database facade providing row lookup.
    :param store_id: Requested Store row identity.
    :return: Found Row; absence raises InputIntegrityError.
    """
    row = db.get_row_from_id("stores", int(store_id))
    if row is None:
        raise InputIntegrityError("Store row not found: store_id={}".format(store_id))
    return row


def _resolve_source_file_path(db, *, file_row: Row, store_cache: dict[int, Row]) -> pathlib.Path:
    """
    Resolve a source file's local Store root/key and require an existing regular file.

    Cache Store Rows by integer ID. Absolute keys override the root, while relative keys are joined
    and resolved, following normal filesystem symlinks. No root-containment or Store-kind check is
    imposed, and file URIs are not decoded. Later reads can observe different bytes.

    Example:
        >>> path = _resolve_source_file_path(db, file_row=row, store_cache={})  # doctest: +SKIP


    :param db: Database used for cache-miss Store lookup.
    :param file_row: Source Row supplying Store ID and storage key.
    :param store_cache: Mutable Store-ID/Row cache populated before later path checks.
    :return: Resolved local Path; invalid metadata or missing files raise.
    """
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
    """
    Extract file identity and optional member target from supported designation shapes.

    Accept a Row, integer-like scalar, two-element nontext Sequence, or Mapping. Mapping aliases are
    file_id/id/file and archive_path/internal_path/target/dest; a present None value does not fall
    through to later aliases. IDs are int-coerced without positivity checks, and target
    normalization occurs later.

    Example:
        >>> _coerce_designation_item({'id': 7, 'target': ' book.epub '})
        (7, 'book.epub')


    :param item: Caller-supplied designation value.
    :return: Integer file ID and stripped optional target; most invalid shapes raise InputIntegrityError.
    """
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
    """
    Assign supported unequal values to any Row and sync only when changed.

    Despite its name this also updates link Rows. None values can clear fields; an error can follow
    earlier in-memory assignments.

    Example:
        >>> _set_store_row_values(row, updates={"store_name": "Archive"})  # doctest: +SKIP


    :param store_row: Borrowed Row whose allowed_columns filters updates.
    :param updates: Candidate field/value mapping.
    :return: None after any needed sync; failures propagate.
    """
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
    Create or refresh an open SquashFS target declaration at a resolved local archive path.

    Reject an existing directory. Inspect matching Store rows in database order: encountering a
    locked row raises, while the first open-kind row is refreshed and returned without checking
    later duplicates. Other kinds are skipped. Reuse or generate its UUID, validate the transition
    to open, and write supported fields. No archive is built or removed.

    Example:
        >>> row = ensure_open_squashfs_store(db, archive_path=archive_path)  # doctest: +SKIP


    :param db: Database receiving the Store declaration.
    :param archive_path: Target path expanded and resolved; need not exist.
    :param store_name: Truthy name override, otherwise existing name or sanitized target path.
    :return: Created/refreshed Store Row; path/schema/state/write failures propagate.
    """
    _, store_columns, _, _ = _ensure_schema_support(db)

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
                "store_uuid": (
                    existing["store_uuid"]
                    if "store_uuid" in existing.allowed_columns
                    and existing["store_uuid"] not in (None, "")
                    else str(uuid4())
                ),
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
        "store_uuid": str(uuid4()),
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
    Write source snapshots and relative member targets into an open Store's designation links.

    Resolve each source and stat it; trust a syntactically valid stored SHA-256 or hash the bytes
    when absent/invalid. Retain existing snapshots when the target matches and replacement is
    disabled. Replacement refreshes the snapshot and can retarget after collision checks.

    Writes are incremental, so later input failures leave prior links. The initial file-ID map is
    not updated after new inserts; repeated new file IDs in one iterable are not promised
    deduplication. Target collisions across different file IDs raise. Iteration failures escape
    without a finished report.

    Example:
        >>> report = designate_files_for_squashfs_store(db, store_id=1, designations=[(7, "books/a.epub")])  # doctest: +SKIP


    :param db: Database used for source/Store lookup and link writes.
    :param store_id: Identity of a declaration whose kind must be open SquashFS.
    :param designations: Iterable of supported ID, Row, pair, or mapping values.
    :param replace_existing: Whether an existing file designation may refresh its snapshot or change target.
    :return: Finished designation counter report after normal completion; failures propagate.
    """
    _, _, _, link_columns = _ensure_schema_support(db)
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
    """
    Resolve and sort designation entries, filling missing legacy snapshot fields from fallback
    metadata.

    Require at least one matching link and unique normalized targets. Prefer nested snapshot fields,
    then flat policy fields; hashes further fall back to the source Row and finally current bytes.
    Missing sizes/mtimes fall back to current stat. Those fallbacks do not establish a historical
    snapshot, and sources can change after resolution.

    Example:
        >>> items = _collect_designations(db, store_id=1)  # doctest: +SKIP


    :param db: Database supplying designation, source, and Store Rows.
    :param store_id: Target Store identity.
    :return: Mutable designation records sorted by archive path, with current hash/size initially None.
    """
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
    """
    Stat and hash each current source, recording observations and reporting snapshot size/hash
    drift.

    Populate current fields before comparing. A size mismatch suppresses the additional
    hash-mismatch message for that item; mtime is not compared. Stat and hash are separate
    observations with no file lock. I/O failures propagate after earlier records may already be
    updated.

    Example:
        >>> errors = _validate_snapshot_consistency(items)  # doctest: +SKIP


    :param designations: Mutable records carrying expected snapshot hash/size and source paths.
    :return: Drift diagnostic strings, possibly empty; successful checking does not freeze later source bytes.
    """
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
    """
    Validate transition to locked and sync supported archive capability fields on a Store Row.

    This helper performs no archive build, probe, or member verification before marking metadata
    online/read-only. It relies on a caller to have established those facts.

    Example:
        >>> _lock_store_row_for_squashfs(row, archive_path=path)  # doctest: +SKIP


    :param store_row: Mutable Store Row with state scratch JSON.
    :param archive_path: Archive path stringified into the declaration.
    :return: None after the supported Row updates are synced.
    """
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
    """
    Validate a link-state transition and sync target/digest metadata into its policy.

    Digest strings are claims, not verified here. An absent archive_hash retains any prior claim, as
    does absent detail. Unrelated policy keys survive decoding.

    Example:
        >>> _upsert_designation_state(link, state="building", archive_path="a.epub", source_hash=digest)  # doctest: +SKIP


    :param designation_link_row: Borrowed designation link Row to mutate.
    :param state: Requested exact link state after string conversion.
    :param archive_path: Member target retained without normalization here.
    :param source_hash: Digest claim stringified into the policy.
    :param archive_hash: Optional archive digest claim replacing the old value when supplied.
    :param detail: Optional state explanation.
    :return: None after policy sync; validation/write failures propagate.
    """
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
    """
    Ensure one primary file/Store association through the Row facade.

    Return on any exact existing association, otherwise insert supported link fields with priority
    zero. Do not repair other fields or remove duplicates; concurrent uniqueness depends on the
    database.

    Example:
        >>> _ensure_primary_link_for_file(db, file_id=1, store_id=2, link_columns=columns)  # doctest: +SKIP


    :param db: Database/connection used for the matching-link query and insertion.
    :param file_id: Legacy file identity int-converted for lookup/writes.
    :param store_id: Legacy Store identity int-converted for lookup/writes.
    :param link_columns: Supported columns used to filter the inserted mapping.
    :return: None after finding or inserting the association; errors propagate.
    """
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
    """
    Ensure one primary file/Store association through the borrowed transaction.

    Return on any exact existing association, otherwise insert supported link fields with priority
    zero. Do not repair other fields or remove duplicates; concurrent uniqueness depends on the
    database.

    Example:
        >>> _ensure_primary_link_for_file_tx(tx_conn, file_id=1, store_id=2, link_columns=columns)  # doctest: +SKIP


    :param tx_conn: Database/connection used for the matching-link query and insertion.
    :param file_id: Legacy file identity int-converted for lookup/writes.
    :param store_id: Legacy Store identity int-converted for lookup/writes.
    :param link_columns: Supported columns used to filter the inserted mapping.
    :return: None after finding or inserting the association; errors propagate.
    """
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
    """
    Reuse a matching target digest or insert copied source metadata for a verified archive member.

    Existing rows match by case-insensitive digest text only, not size or refreshed verification. A
    match skips insertion and primary-link repair. New rows copy supported source columns, override
    archive fields, omit None values, and create a primary link in the borrowed transaction. The
    in-memory key cache is updated before link insertion and is not rolled back here.

    Example:
        >>> inserted, skipped, file_id = _duplicate_verified_file_row(connection, source_row=row, source_path=path, locked_store_id=1, archive_path="a.epub", archive_hash=digest, archive_size=3, file_columns=columns, link_columns=links, existing_rows_by_key=known)  # doctest: +SKIP


    :param tx_conn: Borrowed transaction connection for new file/link rows.
    :param source_row: Legacy source Row whose supported metadata is copied.
    :param source_path: Original local path retained as provenance.
    :param locked_store_id: Target Store ID; the helper does not check its current state.
    :param archive_path: Target member key and filename source.
    :param archive_hash: Caller-verified digest, compared/stored case-insensitively.
    :param archive_size: Caller-supplied member size in bytes.
    :param file_columns: Supported destination file columns.
    :param link_columns: Supported primary-link columns.
    :param existing_rows_by_key: Mutable key-to-Row/identity-mapping cache.
    :return: Inserted flag, matching-existing flag, and resulting optional file ID; conflicts raise InputIntegrityError.
    """
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
    """
    Prefer a recognized scratch state, otherwise infer state from the Store kind.

    A valid scratch state wins even when inconsistent with kind. Unknown/malformed scratch metadata
    falls back to locked-kind-or-open inference.

    Example:
        >>> state = _current_store_state(row)  # doctest: +SKIP


    :param store_row: Store Row supplying scratch JSON and kind.
    :return: Recognized scratch state or inferred open/locked label.
    """
    scratch = _parse_json_object(_coerce_text(store_row["store_scratch"]))
    state = _coerce_text(scratch.get("squashfs_state"))
    if state in STORE_STATES:
        return str(state)
    return _infer_store_state_from_kind(_coerce_text(store_row["store_kind"]))


def _best_effort_mark_store_failed(db, *, store_row: Row, detail: str) -> None:
    """
    Attempt to mark an open-kind Store offline/failed and suppress ordinary update failures.

    Transition validation can refuse the change, and partial Row mutations may survive errors. This
    does not remove an archive or undo committed records. The initial clock call is outside the
    suppression guard.

    Example:
        >>> _best_effort_mark_store_failed(db, store_row=row, detail="publish_failed")  # doctest: +SKIP


    :param db: Retained compatibility argument; this implementation updates through store_row.
    :param store_row: Borrowed Row used for state validation and supported writes.
    :param detail: Failure explanation retained in scratch state/history.
    :return: None after the attempted update or a suppressed ordinary failure.
    """
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


def _register_verified_digital_asset_replicas(
    db,
    *,
    locked_store_id: int,
    outcomes: Sequence[Mapping[str, object]],
) -> tuple[int, int]:
    """
    Adopt verified source/archive locations as two Replicas of one Digital Asset within a database
    macro transaction.

    Process only outcomes marked should_duplicate; require designation context, source/target Store
    UUIDs, and a database StorageManager. Adopt the source as ACTIVE and the same Asset in the
    archive as ARCHIVE, both with verification enabled. No derivation edges are created. Manager
    maps may mutate before rollback, so the publication caller attempts a reload after failure.

    Example:
        >>> assets, replicas = _register_verified_digital_asset_replicas(db, locked_store_id=1, outcomes=outcomes)  # doctest: +SKIP


    :param db: Database supplying storage manager, row lookup, and macros.transaction.
    :param locked_store_id: Archive Store row ID used to resolve its durable UUID.
    :param outcomes: Verification mappings; falsey should_duplicate entries are skipped.
    :return: New source Asset count and new source/archive Replica count after transaction completion.
    """

    manager = getattr(db, "storage", None)
    if manager is None:
        raise InputIntegrityError(
            "SquashFS publication cannot register Digital Asset replicas: "
            "the database has no StorageManager."
        )
    locked_store_row = db.get_row_from_id("stores", int(locked_store_id))
    locked_store_uuid = (
        None
        if locked_store_row is None
        else _coerce_text(locked_store_row["store_uuid"])
    )
    if locked_store_uuid is None:
        raise InputIntegrityError(
            "Locked SquashFS Store {} has no durable store_uuid.".format(
                locked_store_id
            )
        )

    asset_count = 0
    replica_count = 0
    with db.macros.transaction():
        for outcome in outcomes:
            if not bool(outcome.get("should_duplicate")):
                continue
            item = outcome["designation"]
            if not isinstance(item, _SquashfsDesignation):
                raise InputIntegrityError(
                    "SquashFS verification outcome has no designation context."
                )
            source_store_id = _coerce_int(item.source_row["file_store_id"])
            source_storage_key = _coerce_text(item.source_row["file_storage_key"])
            if source_store_id is None or source_storage_key is None:
                raise InputIntegrityError(
                    "Source file {} has no resolvable Store location.".format(
                        item.file_id
                    )
                )
            source_store_row = db.get_row_from_id("stores", source_store_id)
            source_store_uuid = (
                None
                if source_store_row is None
                else _coerce_text(source_store_row["store_uuid"])
            )
            if source_store_uuid is None:
                raise InputIntegrityError(
                    "Source Store {} has no durable store_uuid.".format(
                        source_store_id
                    )
                )

            metadata = DigitalAssetMetadata(
                original_name=(
                    _coerce_text(item.source_row["file_name"])
                    or pathlib.PurePosixPath(source_storage_key).name
                ),
            )
            source_result = manager.adopt_location(
                Location(UUID(source_store_uuid), source_storage_key),
                metadata=metadata,
                replica_mode=ReplicaMode.ACTIVE,
                verify=True,
            )
            archive_result = manager.adopt_location(
                Location(UUID(locked_store_uuid), item.archive_path),
                digital_asset_id=source_result.asset_record.digital_asset_id,
                replica_mode=ReplicaMode.ARCHIVE,
                verify=True,
            )
            asset_count += int(source_result.asset_created)
            replica_count += int(source_result.replica_created)
            replica_count += int(archive_result.replica_created)
    return asset_count, replica_count


def _add_reproducibility_metadata_to_scratch(
    scratch_json: str,
    *,
    build_report: Optional[dict[str, object]],
    now_epk: int,
    published_state: str,
) -> str:
    """
    Replace squashfs_last_build with selected builder observations and publication state/time.

    Copy only the declared build keys and retain other scratch metadata. Values are recorded claims;
    output bytes and hashes are not rechecked here.

    Example:
        >>> json.loads(_add_reproducibility_metadata_to_scratch('{}', build_report=None, now_epk=1, published_state='locked'))['squashfs_last_build']['published_state']
        'locked'


    :param scratch_json: Prior scratch JSON, with malformed/non-object input treated as empty.
    :param build_report: Optional dict of builder observations; unselected keys are discarded.
    :param now_epk: Publication epoch milliseconds int-converted for storage.
    :param published_state: State label stringified without state-graph validation.
    :return: Compact sorted JSON containing the replaced build-metadata object.
    """
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
    Build a designated SquashFS archive, verify members, and persist publication records in distinct
    phases.

    Collect sources and check live size/SHA-256 against snapshots before building. Setup and read
    failures before the main guard propagate even when strict is false; detected drift records
    failure and returns or raises by policy. The builder writes the archive before the legacy-row
    transaction. Verify an exact sha256 stat claim when available, otherwise read member bytes and
    hash them.

    Persist Store/link state transitions, verified duplicate rows, and primary links in one BEGIN
    IMMEDIATE transaction. Non-strict missing/mismatched members can produce a failed Store
    alongside duplicates of verified members. Report counters and the built archive are not undone
    by a later row rollback. Only the temporary manifest receives best-effort cleanup here.

    After that phase, optional manager bootstrap precedes a separate macro transaction for eligible
    Asset/Replica adoption. Failure can leave the earlier archive and legacy rows committed; reload
    manager views after adoption rollback is best-effort. Strict mode raises recorded failures but
    does not merge these phases into an all-run transaction.

    Example:
        >>> report = publish_open_squashfs_store(db, store_id=1, deterministic=True)  # doctest: +SKIP


    :param db: Borrowed database providing legacy rows, transaction connections, and optional manager bootstrap.
    :param store_id: Open-kind target Store row in open or failed scratch state.
    :param output_archive: Optional target override, otherwise the Store root; expanded and resolved.
    :param compression: Compression selector forwarded to the archive builder.
    :param deterministic: Whether to request the builder deterministic configuration.
    :param force: Whether to allow the builder to replace an existing output archive.
    :param duplicate_verified_files: Whether verified outcomes create legacy duplicates and participate in later Asset/Replica adoption.
    :param strict: Whether detected/reportable publication errors raise; this does not make all phases atomic.
    :param refresh_storage_manager: Whether available strict manager bootstrap and subsequent eligible replica registration are attempted.
    :return: Finished publication report on a non-raising path, possibly with errors or hash mismatches.
    """
    _, _, file_columns, link_columns = _ensure_schema_support(db)

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
    designation_results: list[dict[str, object]] = []

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

        for item in designations:
            archive_location = archive_backend.locate(item.archive_path)
            if not archive_backend.exists(archive_location):
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

            status = archive_backend.stat(archive_location)
            archive_hash = (
                status.digest.value
                if status.digest is not None
                and status.digest.algorithm == "sha256"
                else None
            )
            archive_size = status.size
            if not archive_hash:
                payload = archive_backend.read_bytes(archive_location)
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

                inserted, skipped, _child_file_id = _duplicate_verified_file_row(
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
            db.bootstrap_storage_manager(clear_existing=True, strict=True)
        except Exception as exc:
            report.errors.append(
                "storage_manager_bootstrap_failed :: {!r}".format(exc)
            )
        else:
            try:
                if not report.errors and not report.hash_mismatches:
                    assets, replicas = _register_verified_digital_asset_replicas(
                        db,
                        locked_store_id=int(store_id),
                        outcomes=designation_results,
                    )
                    report.digital_assets_registered += assets
                    report.replicas_registered += replicas
            except Exception as exc:
                report.errors.append(
                    "storage_replica_registration_failed :: {!r}".format(exc)
                )
                # Registration is database-atomic, but manager maps are
                # mutated as calls proceed. Reload after rollback to discard
                # those views.
                try:
                    db.bootstrap_storage_manager(
                        clear_existing=True,
                        strict=True,
                    )
                except Exception:
                    pass

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
    Ensure an open target, designate supplied file IDs, and publish with verified duplication
    enabled.

    Materialize/int-convert IDs after Store setup, preserve existing matching designations, and
    forward build/strict/bootstrap policy. Earlier Store/link effects survive later conversion,
    designation, or publication failure; no outer transaction is added.

    Example:
        >>> report = publish_squashfs_archive_from_file_ids(db, file_ids=[7], archive_path=archive_path)  # doctest: +SKIP


    :param db: Borrowed database providing legacy rows, transaction connections, and optional manager bootstrap.
    :param file_ids: Iterable materialized as integer file IDs for designation.
    :param archive_path: Target archive path used for Store setup and publication.
    :param store_name: Optional truthy target Store name override.
    :param compression: Compression selector forwarded to the archive builder.
    :param deterministic: Whether to request the builder deterministic configuration.
    :param force: Whether to allow the builder to replace an existing output archive.
    :param strict: Whether detected/reportable publication errors raise; this does not make all phases atomic.
    :param refresh_storage_manager: Whether available strict manager bootstrap and subsequent eligible replica registration are attempted.
    :return: Delegated publication report; earlier setup/designation reports are not returned.
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
