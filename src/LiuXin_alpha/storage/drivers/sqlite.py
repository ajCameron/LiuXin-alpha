"""
Transactional SQLite BLOB storage driver.
"""

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
    """
    Opaque BLOB key inside one SQLite database.

    Example:
        >>> SQLiteObjectAddress("object-42", UUID(int=1)).value
        'object-42'
    """


class _SQLiteWriteSession:
    """
    Stage bytes locally before publishing them in one SQLite transaction.

    Example:
        >>> session = driver.begin_write(address)  # doctest: +SKIP
        >>> session.write(b"book")  # doctest: +SKIP
        4
    """

    def __init__(
        self,
        driver: SQLiteStorageDriver,
        address: SQLiteObjectAddress,
        *,
        mode: WriteMode,
        expected_size: int | None,
        expected_digest: Digest | None,
    ) -> None:
        """
        Create a single-use write session with optional integrity expectations.

        Example:
            >>> _SQLiteWriteSession(driver, address, mode=WriteMode.CREATE_ONLY, expected_size=4, expected_digest=None)  # doctest: +SKIP


        :param driver:
        :param address:
        :param mode:
        :param expected_size:
        :param expected_digest:
        :return:
        """

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
        """
        Append bytes to the private spool and update staged integrity facts.

        Example:
            >>> session.write(b"book")  # doctest: +SKIP
            4


        :param data:
        :return:
        """

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
        """
        Validate and atomically publish the complete staged BLOB.

        Example:
            >>> session.commit().size  # doctest: +SKIP
            4


        :return:
        """

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
        """
        Discard staged bytes without changing the database object.

        Example:
            >>> session.abort()  # doctest: +SKIP


        :return:
        """

        try:
            self._stream.close()
        except OSError:
            pass
        self._finished = True

    def __enter__(self) -> _SQLiteWriteSession:
        """
        Return the active session for context-managed staging.

        Example:
            >>> with driver.begin_write(address) as session:  # doctest: +SKIP
            ...     session.write(b"book")


        :return:
        """

        if self._finished:
            raise StorageError("SQLite write session is finished.")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Abort automatically unless the context body committed explicitly.

        Example:
            >>> session.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type:
        :param exc:
        :param traceback:
        :return:
        """

        if not self._committed:
            self.abort()


class SQLiteStorageDriver(StorageDriverAPI[SQLiteObjectAddress]):
    """
    Durable BLOB driver using one SQLite database file.

    Example:
        >>> driver = SQLiteStorageDriver("objects.sqlite", address_space_uuid=UUID(int=1))  # doctest: +SKIP
    """

    def __init__(self, path: str | os.PathLike[str], *, address_space_uuid: UUID):
        """
        Configure a database path without opening or creating it yet.

        Example:
            >>> SQLiteStorageDriver("objects.sqlite", address_space_uuid=UUID(int=1))  # doctest: +SKIP


        :param path:
        :param address_space_uuid:
        :return:
        """

        self._path = Path(path).expanduser().resolve(strict=False)
        self._checker = ScopedDriverObjectAddressChecker(
            SQLiteObjectAddress,
            address_space_uuid,
        )
        self._started = False

    @property
    def db_path(self) -> Path:
        """
        Return the resolved SQLite database path.

        Example:
            >>> driver.db_path.name  # doctest: +SKIP
            'objects.sqlite'


        :return:
        """

        return self._path

    @property
    def object_address_checker(self):
        """
        Return the checker that scopes BLOB keys to this database.

        Example:
            >>> driver.object_address_checker.address_space_uuid  # doctest: +SKIP
            UUID('00000000-0000-0000-0000-000000000001')


        :return:
        """

        return self._checker

    @property
    def root_uri(self) -> str:
        """
        Return the credential-free file URI for the database.

        Example:
            >>> driver.root_uri.endswith("objects.sqlite")  # doctest: +SKIP
            True


        :return:
        """

        return self._path.as_uri()

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Describe transactional BLOB reads, writes, deletion, and enumeration.

        Example:
            >>> driver.capabilities.atomic_publish  # doctest: +SKIP
            True


        :return:
        """

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
        """
        Create the object schema if needed and report current status.

        Example:
            >>> driver.startup().available  # doctest: +SKIP
            True


        :return:
        """

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
        """
        Check that SQLite can open and query the configured database.

        Example:
            >>> driver.probe().available  # doctest: +SKIP
            True


        :return:
        """

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
        """
        Report availability, object count, and containing-volume capacity.

        Example:
            >>> driver.status().writable  # doctest: +SKIP
            True


        :return:
        """

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
        """
        Mark the lifecycle closed; operations use short-lived connections.

        Example:
            >>> driver.close()  # doctest: +SKIP


        :return:
        """

        self._started = False

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[SQLiteObjectAddress],
    ) -> SQLiteObjectAddress:
        """
        Validate an opaque, flat BLOB key in this address space.

        Example:
            >>> str(driver.parse_object_address("object-42"))  # doctest: +SKIP
            'object-42'


        :param identifier:
        :return:
        """

        if isinstance(identifier, DriverObjectAddress):
            return self.check_object_address(identifier)
        if not isinstance(identifier, str):
            raise TypeError("SQLite object key must be a string.")
        value = identifier
        if not value or "\x00" in value or "/" in value or "\\" in value:
            raise StorageInvalidAddress(f"invalid SQLite object key: {identifier!r}")
        return SQLiteObjectAddress(value, self._checker.address_space_uuid)

    def stat(self, object_address: SQLiteObjectAddress) -> DriverObjectInfo[SQLiteObjectAddress]:
        """
        Return authoritative size, SHA-256, version, and modification time.

        Example:
            >>> driver.stat(address).digest.algorithm  # doctest: +SKIP
            'sha256'


        :param object_address:
        :return:
        """

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
        """
        Return an in-memory stream for a checked object version and range.

        Example:
            >>> driver.open_read(address, offset=1, length=2).read()  # doctest: +SKIP
            b'oo'


        :param object_address:
        :param offset:
        :param length:
        :param if_version:
        :return:
        """

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
        """
        Begin a staged create or replacement of one BLOB.

        Example:
            >>> session = driver.begin_write(address, expected_size=4)  # doctest: +SKIP


        :param object_address:
        :param mode:
        :param expected_size:
        :param expected_digest:
        :param metadata:
        :return:
        """

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
        """
        Publish validated bytes with create-only or replace preconditions.

        Example:
            >>> driver._publish(address, b"book", sha256="00", mode=WriteMode.CREATE_ONLY)  # doctest: +SKIP


        :param address:
        :param payload:
        :param sha256:
        :param mode:
        :return:
        """

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
        """
        Delete one BLOB, optionally protecting a known version.

        Example:
            >>> driver.delete(address, if_version="1")  # doctest: +SKIP


        :param object_address:
        :param missing_ok:
        :param if_version:
        :return:
        """

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
        """
        Yield all BLOB records whose opaque keys share an optional prefix.

        Example:
            >>> [str(item.object_address) for item in driver.iter_inventory()]  # doctest: +SKIP
            ['object-42']


        :param prefix:
        :return:
        """

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
        """
        Allocate a digest key when known, otherwise a random flat key.

        Example:
            >>> len(str(driver.allocate_object_address()))  # doctest: +SKIP
            32


        :param expected_size:
        :param expected_digest:
        :param name_hint:
        :return:
        """

        _ = (expected_size, name_hint)
        return self.parse_object_address(
            expected_digest.value if expected_digest is not None else uuid4().hex
        )

    def native_compute_digest(
        self,
        object_address: SQLiteObjectAddress,
        algorithm: str = "sha256",
    ) -> Digest:
        """
        Return stored SHA-256 directly or stream another requested algorithm.

        Example:
            >>> driver.native_compute_digest(address).algorithm  # doctest: +SKIP
            'sha256'


        :param object_address:
        :param algorithm:
        :return:
        """

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
        """
        Open one fully durable WAL-mode SQLite connection.

        Example:
            >>> connection = driver._connect()  # doctest: +SKIP
            >>> connection.close()  # doctest: +SKIP


        :return:
        """

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
        """
        Yield a transaction-scoped connection and translate SQLite failures.

        Example:
            >>> with driver._connection("probe") as connection:  # doctest: +SKIP
            ...     connection.execute("SELECT 1").fetchone()
            (1,)


        :param operation:
        :param target:
        :return:
        """

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
        """
        Render a safe database-and-key target for diagnostic messages.

        Example:
            >>> driver._target(address).endswith("object 'object-42'")  # doctest: +SKIP
            True


        :param address:
        :return:
        """

        return f"{self.root_uri} object {str(address)!r}"


__all__ = ["SQLiteObjectAddress", "SQLiteStorageDriver"]
