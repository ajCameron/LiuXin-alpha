"""
Single-file SQLite-backed store backend.

This backend stores every payload in one SQLite database file as content-addressed
blobs keyed by SHA256.
"""

from __future__ import annotations

import hashlib
import pathlib
import re
import sqlite3
import time

from typing import Iterator, Optional, Type

from LiuXin_alpha.storage.api.file_api import SingleFileAPI
from LiuXin_alpha.storage.api import StoreAPI, StoreCheckStatus, StoreStatus
from LiuXin_alpha.storage.single_file import SingleFileStatus
from LiuXin_alpha.storage.store_backend_plugins.single_file_sqlite.single_file_sqlite_single_file import (
    SingleFileSqliteSingleFile,
)
from LiuXin_alpha.utils.logging.event_logs import DefaultEventLog
from LiuXin_alpha.utils.storage.local.local_store_properties import get_free_bytes
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name


class SingleFileSqliteStorageBackend(StoreAPI):
    """
    Read/write store that keeps all files inside one SQLite file.
    """

    single_file_cls: Type[SingleFileSqliteSingleFile] = SingleFileSqliteSingleFile
    _hash_re = re.compile(r"^[0-9a-f]{64}$")

    def __init__(self, url: str, name: Optional[str] = None, uuid: Optional[str] = None) -> None:
        super().__init__(url=url, name=name, uuid=uuid)
        self._db_path = pathlib.Path(self.url).expanduser().resolve()
        if self._db_path.exists() and self._db_path.is_dir():
            raise ValueError("SingleFileSqliteStorageBackend requires a file path, got directory: {!r}".format(url))
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self.set_url(str(self._db_path))
        self._event_log = DefaultEventLog()
        self._cached_status: Optional[StoreStatus] = None
        self._ensure_schema()

    @property
    def db_path(self) -> pathlib.Path:
        return self._db_path

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

    def _extract_hash(self, file_url: str) -> Optional[str]:
        if file_url is None:
            return None

        text = str(file_url).strip().lower()
        prefix = self.url.rstrip("/") + "/"
        if text.startswith(prefix):
            text = text[len(prefix) :]

        if "/" in text:
            return None
        if self._hash_re.fullmatch(text) is None:
            return None
        return text

    def _hash_file_url(self, file_hash: str) -> str:
        return "{}/{}".format(self.url.rstrip("/"), file_hash)

    def _blob_exists(self, file_hash: str) -> bool:
        with self._connect() as conn:
            row = conn.execute("SELECT 1 FROM files WHERE file_hash = ? LIMIT 1", (file_hash,)).fetchone()
        return row is not None

    def _blob_size(self, file_hash: str) -> int:
        with self._connect() as conn:
            row = conn.execute("SELECT file_size FROM files WHERE file_hash = ?", (file_hash,)).fetchone()
        if row is None:
            return 0
        return int(row["file_size"])

    def _blob_bytes(self, file_hash: str) -> bytes:
        with self._connect() as conn:
            row = conn.execute("SELECT file_bytes FROM files WHERE file_hash = ?", (file_hash,)).fetchone()
        if row is None:
            raise FileNotFoundError("Unknown file hash in single-file store: {!r}".format(file_hash))
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
            self._event_log.put("self_test read probe failed: {!r}".format(exc))

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
                self._event_log.put("self_test write probe failed: {!r}".format(exc))

        try:
            free_space = int(get_free_bytes(str(self._db_path.parent)))
        except Exception:
            free_space = None

        check_status = StoreCheckStatus(
            store_marker_file=marker_ok,
            read=read_ok,
            write=write_ok,
            sundry=sundry_ok,
        )
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
            details={"mode": "read_write", "container": "sqlite_single_file"},
        )
        self._cached_status = status
        return status

    def status(self) -> StoreStatus:
        if self._cached_status is None:
            return self.self_test()
        return self._cached_status

    def file_exists(self, file_url: str) -> bool:
        file_hash = self._extract_hash(file_url)
        if file_hash is None:
            return False
        return self._blob_exists(file_hash)

    def get_file_status(self, file_url: str) -> SingleFileStatus:
        file_hash = self._extract_hash(file_url)
        if file_hash is None:
            raise ValueError("Malformed file URL for single-file SQLite store: {!r}".format(file_url))

        canonical_url = self._hash_file_url(file_hash)

        def _exists(url: str) -> bool:
            extracted = self._extract_hash(url)
            if extracted is None:
                return False
            return self._blob_exists(extracted)

        def _size(url: str) -> int:
            extracted = self._extract_hash(url)
            if extracted is None:
                return 0
            return self._blob_size(extracted)

        def _hash(url: str) -> str:
            extracted = self._extract_hash(url)
            if extracted is None:
                return ""
            return extracted if self._blob_exists(extracted) else ""

        return SingleFileStatus(
            url=canonical_url,
            check_exists_function=_exists,
            check_size_function=_size,
            check_hash_function=_hash,
        )

    def get_file(self, file_url: str) -> SingleFileAPI:
        file_hash = self._extract_hash(file_url)
        if file_hash is None:
            raise ValueError("Malformed file URL for single-file SQLite store: {!r}".format(file_url))
        canonical_url = self._hash_file_url(file_hash)
        file_row = self.single_file_cls(
            file_url=canonical_url,
            backend=self,
            file_status=self.get_file_status(canonical_url),
        )
        file_row.store = self.name
        return file_row

    def read_file_bytes(self, file_url: str) -> bytes:
        file_hash = self._extract_hash(file_url)
        if file_hash is None:
            raise ValueError("Malformed file URL for single-file SQLite store: {!r}".format(file_url))
        return self._blob_bytes(file_hash)

    def true_files(self) -> Iterator[SingleFileAPI]:
        with self._connect() as conn:
            rows = conn.execute("SELECT file_hash FROM files ORDER BY created_ts ASC, file_hash ASC").fetchall()
        for row in rows:
            yield self.get_file(self._hash_file_url(str(row["file_hash"])))

    def add_file(self, file_bytes: bytes, *, metadata=None) -> SingleFileAPI:
        file_hash = hashlib.sha256(file_bytes).hexdigest()
        created_ts = int(time.time())
        with self._connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO files(file_hash, file_size, file_bytes, created_ts) VALUES (?, ?, ?, ?)",
                (file_hash, len(file_bytes), sqlite3.Binary(file_bytes), created_ts),
            )
        return self.get_file(self._hash_file_url(file_hash))

    def delete_file(self, file_url: str) -> bool:
        file_hash = self._extract_hash(file_url)
        if file_hash is None:
            return False
        with self._connect() as conn:
            cur = conn.execute("DELETE FROM files WHERE file_hash = ?", (file_hash,))
        return bool(cur.rowcount)
