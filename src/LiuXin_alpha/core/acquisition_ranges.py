"""Bound one acquisition read and retain version evidence across client chunks."""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlparse

from LiuXin_alpha.core.errors import CoreDispatchError
from LiuXin_alpha.storage.api import Location
from LiuXin_alpha.storage.utils.store_configuration import store_configuration_from_row

CHUNK_BYTES = 1024 * 1024


def read_small_location(manager: Any, location: Location) -> bytes:
    """Read one complete small managed resource without an unbounded byte helper."""
    before = manager.stat(location)
    if before.size > CHUNK_BYTES:
        raise CoreDispatchError(
            "Asset exceeds one MiB; use acquisition.read-range",
            code="content_too_large",
        )
    with manager.get(
        location, length=CHUNK_BYTES + 1, if_version=before.version
    ) as source:
        content = source.read(CHUNK_BYTES + 1)
    after = manager.stat(location)
    if (before.size, before.version, before.modified_at) != (
        after.size,
        after.version,
        after.modified_at,
    ) or len(content) != before.size:
        raise CoreDispatchError(
            "Asset changed while reading", code="acquisition_changed"
        )
    if not isinstance(content, bytes):
        raise TypeError("Asset reader returned non-bytes")
    return content


def _version(stat: os.stat_result) -> str:
    facts = [stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns, stat.st_ctime_ns]
    return hashlib.sha256(json.dumps(facts).encode("ascii")).hexdigest()


def read_range(
    runtime: Any,
    *,
    kind: str,
    row: Mapping[str, Any],
    store: Mapping[str, Any] | None,
    offset: int = 0,
    length: int = CHUNK_BYTES,
    if_version: str | None = None,
) -> dict[str, Any]:
    """Read one bounded chunk, normalizing missing/offline filesystem resources."""
    try:
        return _read_range(
            runtime,
            kind=kind,
            row=row,
            store=store,
            offset=offset,
            length=length,
            if_version=if_version,
        )
    except OSError as error:
        raise CoreDispatchError(
            "Acquisition resource is unavailable", code="acquisition_unavailable"
        ) from error


def _read_range(
    runtime: Any,
    *,
    kind: str,
    row: Mapping[str, Any],
    store: Mapping[str, Any] | None,
    offset: int = 0,
    length: int = CHUNK_BYTES,
    if_version: str | None = None,
) -> dict[str, Any]:
    """Return at most one MiB, refusing stale versions and unsafe Store-key fallback."""
    if type(offset) is not int or not 0 <= offset <= 2**63 - 1:
        raise CoreDispatchError(
            "offset must be a nonnegative signed 64-bit integer", code="invalid_payload"
        )
    if type(length) is not int or not 1 <= length <= CHUNK_BYTES:
        raise CoreDispatchError(
            "length must be between 1 and 1048576 bytes", code="invalid_payload"
        )
    if if_version is not None and (not isinstance(if_version, str) or not if_version):
        raise CoreDispatchError(
            "if_version must be a nonempty string", code="invalid_payload"
        )
    prefix = (
        "file"
        if kind == "legacy-file"
        else "image"
        if kind == "image"
        else "asset_replica"
    )
    paths = (
        (row.get(prefix + "_original_path"), row.get(prefix + "_path"))
        if prefix != "asset_replica"
        else ()
    )
    path = next(
        (
            Path(value)
            for value in paths
            if isinstance(value, str) and value and Path(value).is_file()
        ),
        None,
    )
    if path is None and store is not None:
        key = row.get(prefix + "_storage_key")
        if not isinstance(key, str) or not key:
            raise CoreDispatchError(
                "Resource has no Store location", code="acquisition_unavailable"
            )
        manager = runtime.services.library.storage
        configuration = store_configuration_from_row(
            store, fallback_store_id=int(row[prefix + "_store_id"])
        )
        location = Location(configuration.store_uuid, key)
        if callable(getattr(manager, "get", None)):
            info = manager.stat(location)
            if if_version is not None and info.version != if_version:
                raise CoreDispatchError(
                    "Acquisition version changed", code="acquisition_changed"
                )
            if offset > info.size:
                raise CoreDispatchError(
                    "Acquisition range starts beyond EOF", code="invalid_range"
                )
            if (offset or info.size > length) and info.version is None:
                raise CoreDispatchError(
                    "Store cannot pin ranged reads to a version",
                    code="capability_unavailable",
                )
            with manager.get(
                location,
                offset=offset,
                length=min(length, info.size - offset),
                if_version=info.version,
            ) as source:
                content = source.read(length)
                if len(content) > length:
                    raise CoreDispatchError(
                        "Store exceeded its requested read bound",
                        code="acquisition_unavailable",
                    )
            after = manager.stat(location)
            if (info.size, info.version, info.modified_at) != (
                after.size,
                after.version,
                after.modified_at,
            ):
                raise CoreDispatchError(
                    "Acquisition changed while reading", code="acquisition_changed"
                )
            return _receipt(content, offset, info.size, info.version)
        root = str(store.get("store_root_uri") or "")
        parsed = urlparse(root)
        if (
            not root
            or parsed.scheme not in ("", "file")
            or parsed.netloc not in ("", "localhost")
        ):
            raise CoreDispatchError(
                "Store has no bounded reader", code="acquisition_unavailable"
            )
        base = Path(unquote(parsed.path) if parsed.scheme else root).resolve(
            strict=True
        )
        path = (base / key).resolve(strict=True)
        if not path.is_relative_to(base):
            raise CoreDispatchError(
                "Acquisition key escapes its Store root", code="acquisition_unavailable"
            )
    if path is None:
        raise CoreDispatchError(
            "Resource is not readable", code="acquisition_unavailable"
        )
    with path.open("rb") as source:
        before = os.fstat(source.fileno())
        version = _version(before)
        if if_version is not None and if_version != version:
            raise CoreDispatchError(
                "Acquisition version changed", code="acquisition_changed"
            )
        if offset > before.st_size:
            raise CoreDispatchError(
                "Acquisition range starts beyond EOF", code="invalid_range"
            )
        source.seek(offset)
        content = source.read(length)
        if _version(os.fstat(source.fileno())) != version:
            raise CoreDispatchError(
                "Acquisition changed while reading", code="acquisition_changed"
            )
        return _receipt(content, offset, before.st_size, version)


def _receipt(
    content: bytes, offset: int, total: int, version: str | None
) -> dict[str, Any]:
    if not isinstance(content, bytes):
        raise TypeError("Acquisition reader returned non-bytes")
    if offset + len(content) > total:
        raise CoreDispatchError(
            "Acquisition reader exceeded its declared size", code="acquisition_changed"
        )
    if not content and offset < total:
        raise CoreDispatchError(
            "Acquisition reader ended before EOF", code="acquisition_changed"
        )
    return {
        "content": content,
        "offset": offset,
        "next_offset": offset + len(content),
        "total_bytes": total,
        "version": version,
        "complete": offset + len(content) == total,
    }
