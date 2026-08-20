"""Calibre-like rich-placement Store on the new transactional API."""

from __future__ import annotations

import dataclasses
import hashlib
import os
import time

from collections.abc import Mapping
from pathlib import Path
from typing import Any
from uuid import UUID

from LiuXin_alpha.storage.api import (
    Digest,
    FileInfo,
    StoragePlacementHints,
    derive_storage_hints,
)
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive import (
    OnDiskExistingManagedStorageBackend,
)


class OnDiskCalibreLikeStorageBackend(OnDiskExistingManagedStorageBackend):
    """Place files in human-readable author/title folders when hinted."""

    store_kind = "on_disk_calibre_like"

    def __init__(
        self,
        url: str | os.PathLike[str],
        name: str | None = None,
        uuid: str | UUID | None = None,
        *,
        database: Any = None,
        store_id: int | None = None,
    ) -> None:
        super().__init__(url, name=name, uuid=uuid)
        self._database = database
        self._store_id = store_id

    @property
    def database(self) -> Any:
        return self._database

    def set_database(self, database: Any) -> None:
        self._database = database

    @property
    def store_id(self) -> int | None:
        return self._store_id

    def set_store_id(self, store_id: int | None) -> None:
        self._store_id = store_id

    @property
    def capabilities(self):
        return dataclasses.replace(super().capabilities, placement_hints=True)

    def allocate_location(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        name_hint: str | None = None,
        placement_hints: StoragePlacementHints | None = None,
    ):
        hints = _hint_mapping(placement_hints)
        preferred_key = _text(hints.get("preferred_storage_key"))
        if preferred_key:
            return self.locate(preferred_key)
        if not hints or not _text(hints.get("title")):
            if expected_digest is not None:
                return self.locate(
                    ".liuxin/managed_drive/"
                    f"{expected_digest.value[:5]}/{expected_digest.value}"
                )
            return super().allocate_location(
                expected_size=expected_size,
                expected_digest=expected_digest,
                name_hint=name_hint,
            )

        title = _component(_text(hints.get("title")) or "Untitled", "Untitled")
        authors = _authors(hints)
        author_text = _component(" & ".join(authors), "Unknown")
        identifier = _identifier(hints)
        folder_title = title if identifier is None else f"{title} ({identifier})"
        stem = _component(
            _text(hints.get("preferred_filename_stem"))
            or f"{title} - {author_text}",
            title,
        )
        extension = _extension(hints, name_hint)
        return self.location(
            author_text,
            _component(folder_title, title),
            f"{stem}.{extension}",
        )

    def store_bytes(
        self,
        data: bytes,
        *,
        expected_digest: Digest | None = None,
        **kwargs,
    ) -> FileInfo:
        digest = expected_digest or Digest(
            "sha256",
            hashlib.sha256(data).hexdigest(),
        )
        return super().store_bytes(data, expected_digest=digest, **kwargs)

    def store_stream(self, source, *, metadata=None, **kwargs) -> FileInfo:
        info = super().store_stream(source, metadata=metadata, **kwargs)
        hints = derive_storage_hints(metadata) if metadata is not None else None
        self._update_database(info, hints)
        return info

    def _update_database(
        self,
        info: FileInfo,
        placement_hints: StoragePlacementHints | None,
    ) -> None:
        if self._database is None:
            return
        hints = _hint_mapping(placement_hints)
        extra = hints.get("extra")
        file_id = hints.get("file_id")
        if file_id is None and isinstance(extra, Mapping):
            file_id = extra.get("file_id")
        if isinstance(file_id, bool) or not isinstance(file_id, int):
            return
        getter = getattr(self._database, "get_row_from_id", None)
        if not callable(getter):
            return
        row = getter("files", file_id)
        if row is None:
            return
        absolute = self.root_path.joinpath(*info.location.key.split("/"))
        values = {
            "file_storage_key": info.location.key,
            "file_url": str(absolute),
            "file_name": absolute.name,
            "file_store_id": self._store_id,
            "file_modified_timestamp_ep_k": int(time.time()),
        }
        for key, value in values.items():
            try:
                row[key] = value
            except (KeyError, TypeError, AttributeError):
                if hasattr(row, key):
                    setattr(row, key, value)
        sync = getattr(row, "sync", None)
        if callable(sync):
            sync()


def _hint_mapping(hints: StoragePlacementHints | None) -> Mapping[str, Any]:
    if hints is None:
        return {}
    if isinstance(hints, Mapping):
        return hints
    return hints.to_mapping()


def _authors(hints: Mapping[str, Any]) -> tuple[str, ...]:
    raw = hints.get("primary_agents", hints.get("authors", ()))
    if isinstance(raw, str):
        return (raw,)
    if isinstance(raw, (list, tuple)):
        values = tuple(_text(value) for value in raw)
        return tuple(value for value in values if value) or ("Unknown",)
    return ("Unknown",)


def _identifier(hints: Mapping[str, Any]) -> int | None:
    for key in ("work_id", "book_id", "item_id", "manifestation_id", "file_id"):
        value = hints.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    extra = hints.get("extra")
    if isinstance(extra, Mapping):
        for key in ("work_id", "book_id", "item_id", "file_id"):
            value = extra.get(key)
            if isinstance(value, int) and not isinstance(value, bool):
                return value
    return None


def _extension(hints: Mapping[str, Any], name_hint: str | None) -> str:
    candidates = (
        hints.get("file_formats"),
        hints.get("format_detail"),
        hints.get("format"),
        hints.get("file_extension"),
    )
    for candidate in candidates:
        if isinstance(candidate, (list, tuple)):
            candidate = candidate[0] if candidate else None
        value = _text(candidate)
        if value:
            return _component(value.removeprefix(".").lower(), "bin")
    if name_hint and Path(name_hint).suffix:
        return _component(Path(name_hint).suffix[1:].lower(), "bin")
    return "bin"


def _text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _component(value: str, fallback: str) -> str:
    forbidden = '<>:"/\\|?*\x00'
    cleaned = "".join(
        "_" if character in forbidden else character for character in value
    )
    cleaned = " ".join(cleaned.split()).strip(" .")
    return cleaned or fallback


__all__ = ["OnDiskCalibreLikeStorageBackend"]
