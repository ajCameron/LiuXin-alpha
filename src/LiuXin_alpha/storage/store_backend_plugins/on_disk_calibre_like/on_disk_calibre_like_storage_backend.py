"""
On-disk calibre-like store backend.

File placement follows a calibre-style hierarchy:
- top-level folder by author (or author combo)
- second-level folder by title with an optional id suffix
- files inside named "<Title> - <Author>.<ext>"
"""

from __future__ import annotations

import dataclasses
import mimetypes
import os
import pathlib
import re
import time
import uuid

from collections.abc import Iterable, Mapping
from typing import Any, Optional, Type

from LiuXin_alpha.storage.api.file_api import SingleFileAPI
from LiuXin_alpha.storage.api.storage_api import StoreStatus
from LiuXin_alpha.storage.store_backend_plugins.on_disk_calibre_like.on_disk_calibre_like_location import (
    OnDiskCalibreLikeStoreLocation,
)
from LiuXin_alpha.storage.store_backend_plugins.on_disk_calibre_like.on_disk_calibre_like_single_file import (
    OnDiskCalibreLikeSingleFile,
)
from LiuXin_alpha.storage.store_backend_plugins.on_disk_existing_managed_drive.on_disk_existing_managed_drive_storage_backend import (
    OnDiskExistingManagedStorageBackend,
)


@dataclasses.dataclass(slots=True)
class _PlacementMetadata:
    title_component: str
    author_component: str
    book_id_component: Optional[str]
    extension: str
    file_id: Optional[int]
    store_id: Optional[int]

    @property
    def book_folder_name(self) -> str:
        if self.book_id_component is None:
            return self.title_component
        return "{} ({})".format(self.title_component, self.book_id_component)

    @property
    def file_base_name(self) -> str:
        return "{} - {}".format(self.title_component, self.author_component)


class OnDiskCalibreLikeStorageBackend(OnDiskExistingManagedStorageBackend):
    """
    Managed local store with calibre-like folder/file layout semantics.
    """

    location_cls: Type[OnDiskCalibreLikeStoreLocation] = OnDiskCalibreLikeStoreLocation
    single_file_cls: Type[OnDiskCalibreLikeSingleFile] = OnDiskCalibreLikeSingleFile

    _sanitize_re = re.compile(r'[<>:"/\\|?*]+')
    _control_char_re = re.compile(r"[\x00-\x1f]+")
    _ext_re = re.compile(r"[^a-z0-9]+")

    def __init__(
        self,
        url: str,
        name: Optional[str] = None,
        uuid: Optional[str] = None,
        *,
        database: Any = None,
        store_id: Optional[int] = None,
        default_extension: str = "bin",
    ) -> None:
        super().__init__(url=url, name=name, uuid=uuid)
        self._database = database
        self._store_id = store_id
        self._default_extension = self._normalize_extension(default_extension)

    @property
    def database(self) -> Any:
        return self._database

    def set_database(self, database: Any) -> None:
        self._database = database

    @property
    def store_id(self) -> Optional[int]:
        return self._store_id

    def set_store_id(self, store_id: Optional[int]) -> None:
        self._store_id = store_id

    def self_test(self) -> StoreStatus:
        status = super().self_test()
        status.details["layout"] = "calibre_like"
        status.details["database_bound"] = self._database is not None
        if self._store_id is not None:
            status.details["store_id"] = self._store_id
        return status

    def add_file(self, file_bytes: bytes, *, metadata=None) -> SingleFileAPI:
        placement = self._extract_placement_metadata(metadata)

        relative_dir = pathlib.Path(placement.author_component) / placement.book_folder_name
        target_dir = self._root_path / relative_dir
        target_dir.mkdir(parents=True, exist_ok=True)

        filename = "{}.{}".format(placement.file_base_name, placement.extension)
        candidate = target_dir / filename
        target = self._resolve_collision_target(candidate=candidate, file_bytes=file_bytes)

        if not target.exists():
            tmp = target.with_suffix(target.suffix + ".{}.tmp".format(uuid.uuid4().hex))
            tmp.write_bytes(file_bytes)
            os.replace(tmp, target)

        relative_key = target.relative_to(self._root_path).as_posix()
        self._maybe_update_database(
            metadata=metadata,
            placement=placement,
            relative_key=relative_key,
            target=target,
            file_size=len(file_bytes),
        )

        return self.get_file(str(target))

    def _extract_placement_metadata(self, metadata: Any) -> _PlacementMetadata:
        hints = self._extract_hints(metadata)

        title_raw = self._extract_value(metadata, hints, ("title", "work_title", "book_title", "name"))
        if title_raw is None:
            title_raw = "Untitled"
        title_component = self._sanitize_component(str(title_raw), fallback="Untitled")

        authors_raw = self._extract_value(
            metadata,
            hints,
            ("authors", "author", "creators", "agents", "primary_agents"),
        )
        author_names = self._coerce_authors(authors_raw)
        if not author_names:
            author_names = ("Unknown",)
        author_component = self._sanitize_component(" & ".join(author_names), fallback="Unknown")

        file_id = self._to_int(self._extract_value(metadata, hints, ("file_id", "target_file_id")))
        book_id_component = self._extract_book_id_component(
            metadata=metadata,
            hints=hints,
            fallback_file_id=file_id,
        )

        extension_raw = self._extract_value(
            metadata,
            hints,
            ("extension", "file_extension", "format", "file_format", "mime_type", "file_mime_type"),
        )
        if extension_raw is None:
            extension_raw = self._extract_value(metadata, hints, ("file_formats", "formats"))
        extension_raw = self._pick_first_extension_candidate(extension_raw)
        extension = self._normalize_extension(extension_raw)

        store_id = self._store_id
        if store_id is None:
            store_id = self._to_int(self._extract_value(metadata, hints, ("file_store_id", "store_id")))

        return _PlacementMetadata(
            title_component=title_component,
            author_component=author_component,
            book_id_component=book_id_component,
            extension=extension,
            file_id=file_id,
            store_id=store_id,
        )

    def _extract_book_id_component(
        self,
        *,
        metadata: Any,
        hints: Any,
        fallback_file_id: Optional[int],
    ) -> Optional[str]:
        for key in ("work_id", "book_id", "title_id", "item_id"):
            value = self._extract_value(metadata, hints, (key,))
            if value is None:
                continue
            return self._sanitize_component(str(value), fallback="0")
        if fallback_file_id is not None:
            return self._sanitize_component(str(fallback_file_id), fallback="0")
        return None

    @staticmethod
    def _extract_hints(metadata: Any) -> Any:
        if metadata is None:
            return None
        hints_fn = getattr(metadata, "storage_hints", None)
        if not callable(hints_fn):
            return None
        try:
            return hints_fn()
        except Exception:
            return None

    def _extract_value(self, metadata: Any, hints: Any, keys: tuple[str, ...]) -> Any:
        for source in (metadata, hints):
            if source is None:
                continue
            for key in keys:
                value = self._extract_from_source(source, key)
                if value is not None:
                    return value

        if hints is not None:
            extra = self._extract_from_source(hints, "extra")
            if isinstance(extra, Mapping):
                for key in keys:
                    value = extra.get(key)
                    if value is not None:
                        return value
        return None

    @staticmethod
    def _extract_from_source(source: Any, key: str) -> Any:
        if isinstance(source, Mapping):
            return source.get(key)
        return getattr(source, key, None)

    def _coerce_authors(self, raw: Any) -> tuple[str, ...]:
        if raw is None:
            return ()

        if isinstance(raw, str):
            cleaned = raw.strip()
            return (cleaned,) if cleaned else ()

        if isinstance(raw, Mapping):
            names: list[str] = []
            for value in raw.values():
                names.extend(self._coerce_authors(value))
            return tuple(names)

        if isinstance(raw, Iterable):
            names: list[str] = []
            for item in raw:
                name = self._display_value(item)
                if not name:
                    continue
                names.append(name)
            return tuple(names)

        single = self._display_value(raw)
        return (single,) if single else ()

    @staticmethod
    def _display_value(value: Any) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value.strip()
        if isinstance(value, Mapping):
            for key in ("name", "title", "agent_name", "work_title", "value"):
                maybe = value.get(key)
                if maybe:
                    return str(maybe).strip()
            return ""
        for key in ("name", "title", "agent_name", "work_title", "value"):
            maybe = getattr(value, key, None)
            if maybe:
                return str(maybe).strip()
        return str(value).strip()

    def _normalize_extension(self, ext: Any) -> str:
        if ext is None:
            if hasattr(self, "_default_extension"):
                return self._default_extension
            return "bin"

        raw = str(ext).strip()
        if not raw:
            if hasattr(self, "_default_extension"):
                return self._default_extension
            return "bin"

        if "/" in raw and "." not in raw:
            guessed = mimetypes.guess_extension(raw, strict=False)
            if guessed:
                raw = guessed

        if raw.startswith("."):
            raw = raw[1:]

        raw = self._ext_re.sub("", raw.lower())
        if not raw:
            if hasattr(self, "_default_extension"):
                return self._default_extension
            return "bin"
        return raw[:16]

    @staticmethod
    def _pick_first_extension_candidate(value: Any) -> Any:
        if value is None:
            return None
        if isinstance(value, str):
            return value
        if isinstance(value, Mapping):
            for candidate in value.values():
                picked = OnDiskCalibreLikeStorageBackend._pick_first_extension_candidate(candidate)
                if not picked:
                    continue
                return picked
            return None
        if isinstance(value, Iterable):
            for candidate in value:
                picked = OnDiskCalibreLikeStorageBackend._pick_first_extension_candidate(candidate)
                if not picked:
                    continue
                return picked
            return None
        return value

    def _sanitize_component(self, value: str, fallback: str) -> str:
        value = value.replace("\x00", "").strip()
        value = self._sanitize_re.sub("_", value)
        value = self._control_char_re.sub("_", value)
        value = value.strip(" .")
        if not value:
            value = fallback
        return value[:120]

    @staticmethod
    def _to_int(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _resolve_collision_target(candidate: pathlib.Path, file_bytes: bytes) -> pathlib.Path:
        if not candidate.exists():
            return candidate

        try:
            if candidate.read_bytes() == file_bytes:
                return candidate
        except Exception:
            pass

        parent = candidate.parent
        stem = candidate.stem
        suffix = candidate.suffix
        idx = 2

        while True:
            option = parent / "{} ({}){}".format(stem, idx, suffix)
            if not option.exists():
                return option
            try:
                if option.read_bytes() == file_bytes:
                    return option
            except Exception:
                pass
            idx += 1

    def _maybe_update_database(
        self,
        *,
        metadata: Any,
        placement: _PlacementMetadata,
        relative_key: str,
        target: pathlib.Path,
        file_size: int,
    ) -> None:
        database = self._database
        if database is None:
            return

        file_row = self._resolve_file_row(database=database, file_id=placement.file_id, metadata=metadata)
        if file_row is None:
            return

        filename = target.name
        now_ep_k = int(time.time() * 1000)

        updates: list[tuple[str, Any]] = [
            ("file_storage_key", relative_key),
            ("file_url", str(target)),
            ("file_path", str(target)),
            ("file_name", filename),
            ("file_base_name", target.stem),
            ("file_extension", placement.extension),
            ("file_size_bytes", int(file_size)),
            ("file_size", int(file_size)),
            ("file_modified_timestamp_ep_k", now_ep_k),
        ]
        if placement.store_id is not None:
            updates.append(("file_store_id", placement.store_id))

        changed = 0
        for column, value in updates:
            if not self._try_set_row_value(file_row, column, value):
                continue
            changed += 1

        if changed == 0:
            return

        sync_fn = getattr(file_row, "sync", None)
        if callable(sync_fn):
            sync_fn()

    def _resolve_file_row(self, *, database: Any, file_id: Optional[int], metadata: Any) -> Any:
        row = self._extract_from_source(metadata, "file_row")
        if row is None and self._looks_like_file_row(metadata):
            row = metadata
        if row is not None:
            return row

        if file_id is None:
            return None

        get_row_from_id = getattr(database, "get_row_from_id", None)
        if not callable(get_row_from_id):
            return None
        return get_row_from_id("files", file_id)

    @staticmethod
    def _looks_like_file_row(obj: Any) -> bool:
        if obj is None:
            return False
        table = getattr(obj, "table", None)
        if isinstance(table, str) and table.lower() == "files":
            return True
        return False

    @staticmethod
    def _try_set_row_value(row: Any, column: str, value: Any) -> bool:
        try:
            row[column] = value
            return True
        except Exception:
            return False


__all__ = ["OnDiskCalibreLikeStorageBackend"]
