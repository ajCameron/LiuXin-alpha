"""Transactional SQLite BLOB storage driver."""

from __future__ import annotations

import dataclasses
import hashlib
import io
import os
import sqlite3
import tempfile

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from types import TracebackType
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import (
    Digest,
    DriverCapabilities,
    DriverConcurrencyCapabilities,
    DriverInventoryEntry,
    DriverObjectAddress,
    DriverObjectAddressInput,
    DriverObjectInfo,
    DriverStatus,
    EnumerationCompleteness,
    ScopedDriverObjectAddressChecker,
    StorageAlreadyExists,
    StorageDriverAPI,
    StorageError,
    StorageIntegrityError,
    StorageInvalidAddress,
    StorageNotFound,
    StoragePreconditionFailed,
    StorageTimeout,
    StorageUnavailable,
    StorageUnsupportedOperation,
    WriteMode,
)
from LiuXin_alpha.storage.drivers._errors import (
    driver_failure_message,
    translate_os_error,
    translate_sqlite_error,
)


@dataclasses.dataclass(slots=True, frozen=True)
class SQLiteObjectAddress(DriverObjectAddress):
    """Opaque BLOB key inside one SQLite database."""


class _SQLiteWriteSession:
    def __init__(
        self,
        driver: SQLiteStorageDriver,
        address: SQLiteObjectAddress,
        *,
        mode: WriteMode,
        expected_size: int | None,
        expected_digest: Digest | None,
    ) -> None:
        self._driver = driver
        self._address = address
        self._mode = mode
        self._expected_size = expected_size
        self._expected_digest = expected_digest
        self._stream = tempfile.SpooledTemporaryFile(max_size=8 * 1024 * 1024)
        self._digest = hashlib.sha256()
        self._size = 0
        self._finished = False
        self._committed = False

    def write(self, data: bytes) -> int:
        if self._finished:
            raise StorageError("SQLite write session is finished.")
        if not isinstance(data, bytes):
            raise TypeError("write-session data must be bytes.")
        try:
            accepted = self._stream.write(data)
        except OSError as error:
            raise translate_os_error(
                error,
                backend="SQLite",
                operation="stage write",
                target=self._driver._target(self._address),
            ) from error
        self._digest.update(data[:accepted])
        self._size += accepted
        return accepted

    def commit(self) -> DriverObjectInfo[SQLiteObjectAddress]:
        if self._finished:
            raise StorageError("SQLite write session is finished.")
        try:
            if self._expected_size is not None and self._size != self._expected_size:
                raise StorageIntegrityError(
                    f"expected {self._expected_size} bytes, received {self._size}."
                )
            sha256 = self._digest.hexdigest()
            if self._expected_digest is not None:
                self._stream.seek(0)
                observed = hashlib.new(self._expected_digest.algorithm)
                while chunk := self._stream.read(1024 * 1024):
                    observed.update(chunk)
                if observed.hexdigest() != self._expected_digest.value:
                    raise StorageIntegrityError(
                        f"{self._expected_digest.algorithm} digest mismatch."
                    )
            self._stream.seek(0)
            payload = self._stream.read()
            self._driver._publish(
                self._address,
                payload,
                sha256=sha256,
                mode=self._mode,
            )
            self._finished = True
            self._committed = True
            return self._driver.stat(self._address)
        except OSError as error:
            self.abort()
            raise translate_os_error(
                error,
                backend="SQLite",
                operation="commit staged write",
                target=self._driver._target(self._address),
            ) from error
        except BaseException:
            self.abort()
            raise

    def abort(self) -> None:
        try:
            self._stream.close()
        except OSError:
            pass
        self._finished = True

    def __enter__(self) -> _SQLiteWriteSession:
        if self._finished:
            raise StorageError("SQLite write session is finished.")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if not self._committed:
            self.abort()


class SQLiteStorageDriver(StorageDriverAPI[SQLiteObjectAddress]):
    """Durable BLOB driver using one SQLite database file."""

    def __init__(self, path: str | os.PathLike[str], *, address_space_uuid: UUID):
        self._path = Path(path).expanduser().resolve(strict=False)
        self._checker = ScopedDriverObjectAddressChecker(
            SQLiteObjectAddress,
            address_space_uuid,
        )
        self._started = False

    @property
    def db_path(self) -> Path:
        return self._path

    @property
    def object_address_checker(self):
        return self._checker

    @property
    def root_uri(self) -> str:
        return self._path.as_uri()

    @property
    def capabilities(self) -> DriverCapabilities:
        return DriverCapabilities(
            range_reads=True,
            conditional_read=True,
            enumeration=EnumerationCompleteness.COMPLETE,
            stat_digest_authoritative=True,
            native_digest=True,
            create=True,
            replace=True,
            delete=True,
            conditional_delete=True,
            atomic_publish=True,
            capacity_reporting=True,
            object_address_allocation=True,
            hierarchical_object_addresses=False,
            prefix_enumeration=True,
            concurrency=DriverConcurrencyCapabilities(
                thread_safe=True,
                concurrent_reads=True,
                concurrent_writes=True,
                recommended_parallel_reads=4,
            ),
        )

    def startup(self) -> DriverStatus:
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
        except OSError as error:
            raise translate_os_error(
                error,
                backend="SQLite",
                operation="create container directory",
                target=self._path.parent,
            ) from error
        with self._connection("startup") as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS storage_objects (
                    object_key TEXT PRIMARY KEY,
                    object_size INTEGER NOT NULL CHECK (object_size >= 0),
                    sha256 TEXT NOT NULL,
                    object_bytes BLOB NOT NULL,
                    version INTEGER NOT NULL,
                    modified_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS storage_objects_sha256
                    ON storage_objects(sha256);
                """
            )
        self._started = True
        return self.status()

    def probe(self) -> DriverStatus:
        try:
            with self._connection("probe") as connection:
                connection.execute("SELECT 1").fetchone()
            return self.status()
        except (StorageUnavailable, StorageTimeout) as error:
            return DriverStatus(
                False,
                False,
                checked_at=datetime.now(timezone.utc),
                message=str(error),
            )

    def status(self) -> DriverStatus:
        if not self._path.exists():
            return DriverStatus(
                False,
                False,
                checked_at=datetime.now(timezone.utc),
                message=driver_failure_message(
                    "SQLite",
                    "status",
                    target=self._path,
                    reason="the database file does not exist",
                ),
            )
        try:
            with self._connection("status") as connection:
                count = int(
                    connection.execute(
                        "SELECT COUNT(*) FROM storage_objects"
                    ).fetchone()[0]
                )
            stat = os.statvfs(self._path.parent)
        except (OSError, StorageError) as error:
            failure = (
                error
                if isinstance(error, StorageError)
                else translate_os_error(
                    error,
                    backend="SQLite",
                    operation="capacity check",
                    target=self._path.parent,
                )
            )
            return DriverStatus(
                False,
                False,
                checked_at=datetime.now(timezone.utc),
                message=str(failure),
            )
        return DriverStatus(
            True,
            True,
            total_bytes=stat.f_blocks * stat.f_frsize,
            free_bytes=stat.f_bavail * stat.f_frsize,
            object_count=count,
            checked_at=datetime.now(timezone.utc),
            details=(("container", "sqlite"),),
        )

    def close(self) -> None:
        self._started = False

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[SQLiteObjectAddress],
    ) -> SQLiteObjectAddress:
        if isinstance(identifier, DriverObjectAddress):
            return self.check_object_address(identifier)
        if not isinstance(identifier, str):
            raise TypeError("SQLite object key must be a string.")
        value = identifier
        if not value or "\x00" in value or "/" in value or "\\" in value:
            raise StorageInvalidAddress(f"invalid SQLite object key: {identifier!r}")
        return SQLiteObjectAddress(value, self._checker.address_space_uuid)

    def stat(self, object_address: SQLiteObjectAddress) -> DriverObjectInfo[SQLiteObjectAddress]:
        checked = self.check_object_address(object_address)
        with self._connection("stat", target=self._target(checked)) as connection:
            row = connection.execute(
                "SELECT object_size, sha256, version, modified_at "
                "FROM storage_objects WHERE object_key = ?",
                (str(checked),),
            ).fetchone()
        if row is None:
            raise StorageNotFound(
                driver_failure_message(
                    "SQLite",
                    "stat",
                    target=self._target(checked),
                    reason="the object does not exist",
                )
            )
        return DriverObjectInfo(
            checked,
            size=int(row[0]),
            digest=Digest("sha256", str(row[1])),
            version=str(row[2]),
            modified_at=datetime.fromtimestamp(float(row[3]), timezone.utc),
        )

    def open_read(
        self,
        object_address: SQLiteObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> io.BytesIO:
        if offset < 0 or (length is not None and length < 0):
            raise StorageInvalidAddress("read ranges must not be negative.")
        checked = self.check_object_address(object_address)
        with self._connection("open read", target=self._target(checked)) as connection:
            row = connection.execute(
                "SELECT object_bytes, version FROM storage_objects WHERE object_key = ?",
                (str(checked),),
            ).fetchone()
        if row is None:
            raise StorageNotFound(
                driver_failure_message(
                    "SQLite",
                    "open read",
                    target=self._target(checked),
                    reason="the object does not exist",
                )
            )
        if if_version is not None and str(row[1]) != if_version:
            raise StoragePreconditionFailed(
                f"version changed for {checked!s}."
            )
        payload = bytes(row[0])[offset:]
        if length is not None:
            payload = payload[:length]
        return io.BytesIO(payload)

    def begin_write(
        self,
        object_address: SQLiteObjectAddress,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        metadata: tuple[tuple[str, str], ...] = (),
    ) -> _SQLiteWriteSession:
        if metadata:
            raise StorageUnsupportedOperation(
                "SQLite BLOB storage does not persist native metadata."
            )
        if expected_size is not None and expected_size < 0:
            raise ValueError("expected_size must not be negative.")
        checked = self.check_object_address(object_address)
        try:
            return _SQLiteWriteSession(
                self,
                checked,
                mode=mode,
                expected_size=expected_size,
                expected_digest=expected_digest,
            )
        except OSError as error:
            raise translate_os_error(
                error,
                backend="SQLite",
                operation="begin write",
                target=self._target(checked),
            ) from error

    def _publish(
        self,
        address: SQLiteObjectAddress,
        payload: bytes,
        *,
        sha256: str,
        mode: WriteMode,
    ) -> None:
        now = datetime.now(timezone.utc).timestamp()
        with self._connection("publish", target=self._target(address)) as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT version FROM storage_objects WHERE object_key = ?",
                (str(address),),
            ).fetchone()
            if mode is WriteMode.CREATE_ONLY and existing is not None:
                raise StorageAlreadyExists(str(address))
            if mode is WriteMode.REPLACE and existing is None:
                raise StorageNotFound(
                    driver_failure_message(
                        "SQLite",
                        "publish replacement",
                        target=self._target(address),
                        reason="the destination object does not exist",
                    )
                )
            version = 1 if existing is None else int(existing[0]) + 1
            connection.execute(
                "INSERT INTO storage_objects "
                "(object_key, object_size, sha256, object_bytes, version, modified_at) "
                "VALUES (?, ?, ?, ?, ?, ?) "
                "ON CONFLICT(object_key) DO UPDATE SET "
                "object_size=excluded.object_size, sha256=excluded.sha256, "
                "object_bytes=excluded.object_bytes, version=excluded.version, "
                "modified_at=excluded.modified_at",
                (
                    str(address),
                    len(payload),
                    sha256,
                    sqlite3.Binary(payload),
                    version,
                    now,
                ),
            )

    def delete(
        self,
        object_address: SQLiteObjectAddress,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        checked = self.check_object_address(object_address)
        with self._connection("delete", target=self._target(checked)) as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM storage_objects WHERE object_key = ?",
                (str(checked),),
            ).fetchone()
            if row is None:
                if missing_ok:
                    return
                raise StorageNotFound(
                    driver_failure_message(
                        "SQLite",
                        "delete",
                        target=self._target(checked),
                        reason="the object does not exist",
                    )
                )
            if if_version is not None and str(row[0]) != if_version:
                raise StoragePreconditionFailed(str(checked))
            connection.execute(
                "DELETE FROM storage_objects WHERE object_key = ?",
                (str(checked),),
            )

    def iter_inventory(
        self,
        *,
        prefix: SQLiteObjectAddress | None = None,
    ) -> Iterator[DriverInventoryEntry[SQLiteObjectAddress]]:
        prefix_value = "" if prefix is None else str(self.check_object_address(prefix))
        with self._connection("inventory") as connection:
            rows = connection.execute(
                "SELECT object_key, object_size, sha256, version, modified_at "
                "FROM storage_objects ORDER BY object_key"
            )
            for key, size, sha256, version, modified_at in rows:
                if prefix_value and not str(key).startswith(prefix_value):
                    continue
                yield DriverInventoryEntry(
                    self.parse_object_address(str(key)),
                    size=int(size),
                    digest=Digest("sha256", str(sha256)),
                    version=str(version),
                    modified_at=datetime.fromtimestamp(
                        float(modified_at), timezone.utc
                    ),
                )

    def allocate_object_address(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        name_hint: str | None = None,
    ) -> SQLiteObjectAddress:
        _ = (expected_size, name_hint)
        return self.parse_object_address(
            expected_digest.value if expected_digest is not None else uuid4().hex
        )

    def native_compute_digest(
        self,
        object_address: SQLiteObjectAddress,
        algorithm: str = "sha256",
    ) -> Digest:
        if algorithm.lower() == "sha256":
            info = self.stat(object_address)
            assert info.digest is not None
            return info.digest
        try:
            digest = hashlib.new(algorithm)
        except ValueError as error:
            raise StorageUnsupportedOperation(
                f"unsupported digest algorithm: {algorithm!r}"
            ) from error
        with self.open_read(object_address) as source:
            while chunk := source.read(1024 * 1024):
                digest.update(chunk)
        return Digest(algorithm, digest.hexdigest())

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._path, timeout=30)
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = FULL")
        return connection

    @contextmanager
    def _connection(
        self,
        operation: str,
        *,
        target: str | None = None,
    ):
        try:
            with self._connect() as connection:
                yield connection
        except sqlite3.Error as error:
            raise translate_sqlite_error(
                error,
                operation=operation,
                target=self.root_uri if target is None else target,
            ) from error

    def _target(self, address: SQLiteObjectAddress) -> str:
        return f"{self.root_uri} object {str(address)!r}"


__all__ = ["SQLiteObjectAddress", "SQLiteStorageDriver"]
