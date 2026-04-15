"""SQLite-backed content-addressed store plugin.

This plugin stores every payload inside one SQLite database file. It is a good
stress test for the new storage split because it is writable, but it is *not*
a normal directory tree: locations are canonical SHA256 content keys rather
than arbitrary user-chosen paths.
"""

from __future__ import annotations

import hashlib
import pathlib
import re
import sqlite3
import time

from typing import Iterator, Optional, Type

from LiuXin_alpha.storage.api import StorePluginAPI, StoreCheckStatus, StoreStatus, StoreLocationMixinAPI
from LiuXin_alpha.storage.single_file import SingleFileStatus
from LiuXin_alpha.storage.store_backend_plugins.single_file_sqlite.single_file_sqlite_location import (
    SingleFileSqliteStoreLocation,
)
from LiuXin_alpha.utils.logging.event_logs import DefaultEventLog
from LiuXin_alpha.utils.storage.local.local_store_properties import get_free_bytes
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


class SingleFileSqliteStorageBackend(StorePluginAPI):
    """Writable plugin that stores blobs by SHA256 inside one SQLite file.

    The plugin is content-addressed. Callers may *locate* a blob by its hash or
    canonical URL, but they may not choose an arbitrary path for stored bytes.
    """

    location_cls: Type[SingleFileSqliteStoreLocation] = SingleFileSqliteStoreLocation
    _hash_re = re.compile(r"^[0-9a-f]{64}$")

    def __init__(self, url: str, name: Optional[str] = None, uuid: Optional[str] = None) -> None:
        super().__init__(url=url, name=name, uuid=uuid)
        self._db_path = pathlib.Path(self.url).expanduser().resolve()
        if self._db_path.exists() and self._db_path.is_dir():
            raise ValueError(f"SingleFileSqliteStorageBackend requires a file path, got directory: {url!r}")
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self.set_url(str(self._db_path))
        self._event_log = DefaultEventLog()
        self._cached_status: Optional[StoreStatus] = None
        self._ensure_schema()

    @property
    def db_path(self) -> pathlib.Path:
        return self._db_path

    @property
    def root_path(self) -> pathlib.Path:
        return self._db_path

    def location(self, *tokens: str) -> SingleFileSqliteStoreLocation:
        return self.location_cls(*tokens, store=self)

    def _location_from_identifier(self, file_identifier: str | StoreLocationMixinAPI) -> SingleFileSqliteStoreLocation:
        if isinstance(file_identifier, StoreLocationMixinAPI):
            if file_identifier.store is self:
                return file_identifier
            file_identifier = file_identifier.file_url
        file_hash = self._extract_hash(str(file_identifier))
        if file_hash is None:
            raise ValueError(f"Malformed file identifier for single-file SQLite store: {file_identifier!r}")
        return self.location(file_hash)

    def url_to_name(self, url: str) -> str:
        return safe_path_to_name(url)

    def startup(self) -> StoreStatus:
        self._cached_status = self.self_test()
        return self._cached_status

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self._db_path), timeout=30.0)
        conn.row_factory = sqlite3.Row
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS files (
                    file_hash TEXT PRIMARY KEY,
                    file_size INTEGER NOT NULL,
                    file_bytes BLOB NOT NULL,
                    created_ts INTEGER NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_files_created_ts ON files(created_ts)")

    def _extract_hash(self, file_identifier: str | None) -> Optional[str]:
        if file_identifier is None:
            return None
        text = str(file_identifier).strip().lower()
        prefix = self.url.rstrip("/") + "/"
        if text.startswith(prefix):
            text = text[len(prefix):]
        if "/" in text:
            return None
        if self._hash_re.fullmatch(text) is None:
            return None
        return text

    def _hash_file_url(self, file_hash: str) -> str:
        return f"{self.url.rstrip('/')}/{file_hash}"

    def _blob_exists(self, file_hash: str) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM files WHERE file_hash = ? LIMIT 1", (file_hash,)).fetchone()
        return row is not None

    def _blob_size(self, file_hash: str) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT file_size FROM files WHERE file_hash = ?", (file_hash,)).fetchone()
        return 0 if row is None else int(row["file_size"])

    def _blob_bytes(self, file_hash: str) -> bytes:
        with self._connect() as conn:
            row = conn.execute("SELECT file_bytes FROM files WHERE file_hash = ?", (file_hash,)).fetchone()
        if row is None:
            raise FileNotFoundError(f"Unknown file hash in single-file store: {file_hash!r}")
        return bytes(row["file_bytes"])

    def self_test(self) -> StoreStatus:
        marker_ok = self._db_path.exists() and self._db_path.is_file()
        read_ok = False
        write_ok = False
        sundry_ok = False
        file_count: Optional[int] = None
        try:
            with self._connect() as conn:
                row = conn.execute("SELECT COUNT(*) AS c FROM files").fetchone()
                file_count = int(row["c"]) if row is not None else 0
            read_ok = True
            sundry_ok = True
        except Exception as exc:
            self._event_log.put(f"self_test read probe failed: {exc!r}")
        if read_ok:
            probe_hash = "__selftest__"
            try:
                with self._connect() as conn:
                    conn.execute(
                        "INSERT OR REPLACE INTO files(file_hash, file_size, file_bytes, created_ts) VALUES (?, ?, ?, ?)",
                        (probe_hash, 0, b"", int(time.time())),
                    )
                    conn.execute("DELETE FROM files WHERE file_hash = ?", (probe_hash,))
                write_ok = True
            except Exception as exc:
                self._event_log.put(f"self_test write probe failed: {exc!r}")
        try:
            free_space = int(get_free_bytes(str(self._db_path.parent)))
        except Exception:
            free_space = None
        check_status = StoreCheckStatus(store_marker_file=marker_ok, read=read_ok, write=write_ok, sundry=sundry_ok)
        status = StoreStatus(
            name=self.name,
            uuid=self.uuid or self.name,
            url=self.url,
            file_count=file_count,
            store_free_space=free_space,
            check_status=check_status,
            checked=check_status.all_ok,
            good=check_status.all_ok,
            event_log=self._event_log,
            details={"mode": "read_write", "container": "sqlite_single_file", "plugin_layer": "raw_storage"},
        )
        self._cached_status = status
        return status

    def status(self) -> StoreStatus:
        return self.self_test() if self._cached_status is None else self._cached_status

    def locate(self, file_identifier: str | StoreLocationMixinAPI) -> SingleFileSqliteStoreLocation:
        return self._location_from_identifier(file_identifier)

    def exists(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        try:
            location = self._location_from_identifier(file_identifier)
        except ValueError:
            return False
        file_hash = location.parts[0]
        return self._blob_exists(file_hash)

    def file_size(self, file_identifier: str | StoreLocationMixinAPI) -> Optional[int]:
        try:
            location = self._location_from_identifier(file_identifier)
        except ValueError:
            return None
        file_hash = location.parts[0]
        if not self._blob_exists(file_hash):
            return None
        return self._blob_size(file_hash)

    def stat(self, file_identifier: str | StoreLocationMixinAPI) -> SingleFileStatus:
        location = self._location_from_identifier(file_identifier)
        file_hash = location.parts[0]
        canonical_url = self._hash_file_url(file_hash)
        return SingleFileStatus(
            url=canonical_url,
            check_exists_function=lambda _url: self._blob_exists(file_hash),
            check_size_function=lambda _url: self._blob_size(file_hash),
            check_hash_function=lambda _url: file_hash if self._blob_exists(file_hash) else "",
        )

    def read_file_bytes(self, file_identifier: str | StoreLocationMixinAPI) -> bytes:
        location = self._location_from_identifier(file_identifier)
        return self._blob_bytes(location.parts[0])

    def iter_locations(self) -> Iterator[SingleFileSqliteStoreLocation]:
        with self._connect() as conn:
            rows = conn.execute("SELECT file_hash FROM files ORDER BY created_ts ASC, file_hash ASC").fetchall()
        for row in rows:
            yield self.location(str(row["file_hash"]))

    def write_bytes(
        self,
        file_bytes: bytes,
        *,
        metadata=None,
        location: str | None = None,
    ) -> SingleFileSqliteStoreLocation:
        file_hash = hashlib.sha256(file_bytes).hexdigest()
        if location is not None:
            requested_hash = self._extract_hash(location)
            if requested_hash is None:
                raise ValueError(
                    "SingleFileSqliteStorageBackend is content-addressed; explicit locations must be the canonical hash or URL."
                )
            if requested_hash != file_hash:
                raise ValueError(
                    "Explicit location does not match the payload hash for this content-addressed SQLite plugin."
                )
        with self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO files(file_hash, file_size, file_bytes, created_ts) VALUES (?, ?, ?, ?)",
                (file_hash, len(file_bytes), sqlite3.Binary(file_bytes), int(time.time())),
            )
        return self.location(file_hash)

    def copy_within_plugin(
        self,
        src_location: str | StoreLocationMixinAPI,
        dst_location: str | StoreLocationMixinAPI,
    ) -> SingleFileSqliteStoreLocation:
        payload = self.read_file_bytes(src_location)
        dst_text = dst_location.file_url if isinstance(dst_location, StoreLocationMixinAPI) else str(dst_location)
        return self.write_bytes(payload, location=dst_text)

    def delete(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        try:
            location = self._location_from_identifier(file_identifier)
        except ValueError:
            return False
        file_hash = location.parts[0]
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM files WHERE file_hash = ?", (file_hash,))
        return bool(cur.rowcount)

    def update_bytes(
        self,
        file_identifier: str | StoreLocationMixinAPI,
        file_bytes: bytes,
        *,
        append: bool = False,
    ) -> bool:
        raise PermissionError(
            "SingleFileSqliteStorageBackend is content-addressed; update by writing a new blob and using its new canonical location."
        )


__all__ = ["SingleFileSqliteStorageBackend"]
