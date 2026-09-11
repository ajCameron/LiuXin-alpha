"""
Implement opaque-key BLOB storage with transactional SQLite publication.

Rows retain complete payloads, sizes, SHA-256, integer versions, and modification
times. Writes spool before materializing a payload for one transaction; reads
load the full BLOB before applying ranges. Stored metadata is trusted rather
than revalidated on every read. Connection contexts manage transaction completion
without explicitly closing connections, and successful sessions retain their
spools until explicit or eventual cleanup.
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
    StorageCharacteristics,
    StorageDriverAPI,
    StorageError,
    StorageIntegrityError,
    StorageInvalidAddress,
    StorageNotFound,
    StoragePreconditionFailed,
    StoragePublicationModel,
    StorageTemporarySpaceRequirement,
    StorageTimeout,
    StorageUnavailable,
    StorageUnsupportedOperation,
    StorageWriteUsage,
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
    Carry an opaque BLOB key and its configured driver UUID.

    The inherited value object checks UUID, nonempty value, and NULs. Flat key syntax is validated
    by SQLiteStorageDriver when parsing text, not by directly constructing this record.

    Example:
        >>> SQLiteObjectAddress("object-42", UUID(int=1)).value
        'object-42'
    """


class _SQLiteWriteSession:
    """
    Spool accepted bytes and publish one complete BLOB through a SQLite transaction.

    The spool starts in memory and can spill to a temporary file above eight MiB. Commit checks
    expectations, then reads the entire spool into a bytes payload for publication. A successful
    commit and context exit leave the spool open; explicit abort closes it without deleting a
    published row. Post-publication stat failures can raise even though the BLOB transaction already
    committed.

    Example:
        >>> with driver.begin_write(address, expected_size=4) as session:  # doctest: +SKIP
        ...     session.write(b"book")
        ...     info = session.commit()
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
        Prepare an eight-MiB spooled temporary stream and SHA-256 accounting.

        This does not open the database or validate the optional expected digest algorithm. The
        caller supplies an already checked address and WriteMode member.

        Example:
            >>> session = _SQLiteWriteSession(driver, address, mode=WriteMode.CREATE_ONLY, expected_size=4, expected_digest=None)  # doctest: +SKIP


        :param driver: SQLite driver used later for publication and metadata lookup.
        :param address: Scoped destination address already checked by the driver.
        :param mode: WriteMode member passed unchanged to commit-time publication.
        :param expected_size: Optional accepted-byte count required at commit.
        :param expected_digest: Optional digest to verify by rereading the spool at commit.
        :return: None after creating the spool, SHA-256 accumulator, and unfinished state.
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
        Append bytes to the spool and update SHA-256 and size for the accepted prefix.

        Reject finished sessions and non-bytes input. Translate spool OSError into a contextual
        storage error. The expected size is a commit-time check rather than an acceptance cap.

        Example:
            >>> session.write(b"book")  # doctest: +SKIP
            4


        :param data: Object bytes to append; accounting uses the prefix accepted by the spool.
        :return: Accepted byte count; write failures leave cleanup to the caller/context.
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
        Check staged expectations, publish a complete BLOB, and stat the destination.

        Size and stored SHA-256 come from accepted-write accounting. An expected digest is
        separately computed by rereading the spool; unsupported hashlib names raise directly.
        Publication then receives the entire spooled payload in memory.

        After publication, mark the session committed before querying metadata. Errors attempt
        abort, translating OSError and propagating other exceptions. A later stat failure does not
        roll back the committed row, and returned metadata can reflect another writer. Success does
        not explicitly close the spool.

        Example:
            >>> session.commit().size  # doctest: +SKIP
            4


        :return: DriverObjectInfo obtained after publication, or an error that can follow a committed write.
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
        Close the spool, suppressing OSError, and mark the session finished without deleting
        database rows.

        Example:
            >>> session.abort()  # doctest: +SKIP


        :return: None after closing or suppressing an OS close failure and updating session state.
        """

        try:
            self._stream.close()
        except OSError:
            pass
        self._finished = True

    def __enter__(self) -> _SQLiteWriteSession:
        """
        Return this unfinished session for explicit commit or context-managed abort.

        Example:
            >>> with driver.begin_write(address) as session:  # doctest: +SKIP
            ...     session.write(b"temporary")


        :return: This session, or StorageError if it has already finished.
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
        Abort an uncommitted context while leaving committed-session spools untouched.

        Exception arguments are not inspected and body exceptions are not suppressed. Closing a
        committed spool requires explicit abort or eventual object cleanup.

        Example:
            >>> session.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type: Body exception type or None, unused by cleanup selection.
        :param exc: Body exception instance or None, unused.
        :param traceback: Body traceback or None, unused.
        :return: None; uncommitted sessions are aborted and body exceptions remain visible.
        """

        if not self._committed:
            self.abort()


class SQLiteStorageDriver(StorageDriverAPI[SQLiteObjectAddress]):
    """
    Store opaque-keyed BLOBs and metadata in one SQLite database.

    Startup creates the storage_objects schema; each database operation obtains its own connection
    with WAL and FULL synchronous settings. The connection context manages transaction completion,
    not explicit connection closure. Reads load full BLOBs before slicing, and writes publish a
    fully materialized spool. Neither path imposes an object-size memory cap.

    SHA-256 is persisted with each row and trusted by stat. Per-key integer versions start at one
    and increment on replacement; deleting and recreating a key can reuse a version. Close clears a
    flag rather than preventing later operations.

    Example:
        >>> driver = SQLiteStorageDriver("objects.sqlite", address_space_uuid=UUID(int=1))  # doctest: +SKIP
        >>> driver.startup()  # doctest: +SKIP
    """

    def __init__(self, path: str | os.PathLike[str], *, address_space_uuid: UUID):
        """
        Resolve a database path and configure scoped address ownership without opening the database.

        Example:
            >>> driver = SQLiteStorageDriver("objects.sqlite", address_space_uuid=UUID(int=1))  # doctest: +SKIP


        :param path: Database filesystem path, expanded and resolved with strict=False.
        :param address_space_uuid: UUID required on SQLite object addresses accepted by this driver.
        :return: None after retaining the path/checker and clearing the started flag.
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
        Return the expanded, resolved database filename without checking availability.

        Example:
            >>> driver.db_path.name  # doctest: +SKIP
            'objects.sqlite'


        :return: Absolute Path retained when constructing the driver.
        """

        return self._path

    @property
    def object_address_checker(self):
        """
        Return the subtype/UUID checker without adding flat-key parsing or existence checks.

        Example:
            >>> driver.object_address_checker.address_space_uuid  # doctest: +SKIP
            UUID('00000000-0000-0000-0000-000000000001')


        :return: Existing ScopedDriverObjectAddressChecker for this SQLite address space.
        """

        return self._checker

    @property
    def root_uri(self) -> str:
        """
        Render the configured database filename as a file URI.

        Example:
            >>> driver.root_uri.endswith("objects.sqlite")  # doctest: +SKIP
            True


        :return: Database file URI, not an externally renderable URI for an individual BLOB.
        """

        return self._path.as_uri()

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Declare transactional mutation, versioned reads/deletes, inventory, and stored checksums.

        Keys are flat with lexical prefix enumeration. No native copy/move, paging, or external
        object URI flags are enabled. Concurrency and writability are declared mechanics, not
        current lock, permission, or capacity observations.

        Example:
            >>> driver.capabilities.conditional_delete  # doctest: +SKIP
            True


        :return: DriverCapabilities with mutation support, authoritative stat digests, and recommended parallel reads of four.
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

    @property
    def storage_characteristics(self) -> StorageCharacteristics:
        """
        Describe per-object BLOB publication and object staging without whole-container rewriting.

        Example:
            >>> driver.storage_characteristics.temporary_space  # doctest: +SKIP
            <StorageTemporarySpaceRequirement.OBJECT_STAGE: 'object_stage'>


        :return: General-write characteristics preserving unmodelled entries, without a calculated size/capacity limit.
        """

        return StorageCharacteristics(
            publication_model=StoragePublicationModel.PER_OBJECT,
            temporary_space=StorageTemporarySpaceRequirement.OBJECT_STAGE,
            recommended_write_usage=StorageWriteUsage.GENERAL,
            preserves_unmodelled_entries=True,
            rewrites_container_format=False,
        )

    def startup(self) -> DriverStatus:
        """
        Create the containing directory and storage schema if absent, then report status.

        CREATE IF NOT EXISTS preserves existing rows but does not migrate or validate every aspect
        of a preexisting schema. Directory OS failures and SQLite failures follow their respective
        translators. The started flag is set before collecting status, whose later capacity or count
        failure may report unavailable.

        Example:
            >>> driver.startup().available  # doctest: +SKIP
            True


        :return: Fresh DriverStatus after schema setup; initialization errors may raise instead.
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
        Open a configured connection, execute SELECT 1, then obtain normal status.

        Connection setup can create a missing database file and change journal settings; this is not
        a read-only probe or schema initializer. StorageUnavailable and StorageTimeout become
        unavailable statuses, while other probe exceptions propagate.

        Example:
            >>> driver.probe().available  # doctest: +SKIP
            True


        :return: Status from normal collection or a caught availability/timeout failure.
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
        Count stored rows and query containing-volume capacity without caching the result.

        An absent file returns unavailable before connecting. Count/volume OS and storage errors
        become unavailable statuses. Success reports writable=True without an explicit write test.
        Capacity uses statvfs available blocks and is observed separately from the row count.

        Example:
            >>> driver.status().writable  # doctest: +SKIP
            True


        :return: Fresh availability/count/capacity status with a sqlite container detail on success.
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
        Clear the started flag without tracking or closing connections, spools, or later operations.

        Example:
            >>> driver.close()  # doctest: +SKIP


        :return: None after recording _started=False.
        """

        self._started = False

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[SQLiteObjectAddress],
    ) -> SQLiteObjectAddress:
        """
        Parse a nonempty flat key or check subtype/UUID of an already constructed address.

        Text may not contain NUL, slash, or backslash; it is not stripped or normalized for Unicode
        equivalence. Existing addresses take the ownership checker only, bypassing these
        parser-specific syntax checks.

        Example:
            >>> str(driver.parse_object_address("object-42"))  # doctest: +SKIP
            'object-42'


        :param identifier: Opaque key text or SQLiteObjectAddress expected to belong to this driver.
        :return: Scoped SQLiteObjectAddress; invalid text/type or ownership raises through the corresponding check.
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
        Read stored size, SHA-256, version, and modification time without reading BLOB bytes.

        A missing key raises StorageNotFound. Stored checksum/size columns are trusted rather than
        verified against current bytes, so out-of-band database mutations are outside this method's
        integrity guarantee.

        Example:
            >>> driver.stat(address).digest.algorithm  # doctest: +SKIP
            'sha256'


        :param object_address: Scoped key selecting the row whose metadata should be returned.
        :return: DriverObjectInfo with stored digest, stringified integer version, and UTC modification time.
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
        Load a whole BLOB/version row, check an optional version, then return an in-memory slice.

        Even a short or zero-length requested range fetches the full BLOB first. Version and bytes
        come from the same selected row; a mismatch raises StoragePreconditionFailed. No stored
        checksum is compared with those bytes. Negative ranges are invalid, while offsets past the
        object end produce an empty stream.

        Example:
            >>> with driver.open_read(address, offset=1, length=2) as source:  # doctest: +SKIP
            ...     source.read()
            b'oo'


        :param object_address: Scoped BLOB address whose complete bytes and version are selected.
        :param offset: Nonnegative byte offset applied after loading the BLOB.
        :param length: Optional maximum slice length in bytes, or None for the remaining bytes.
        :param if_version: Optional string version compared with the selected row before returning content.
        :return: Caller-owned BytesIO holding the selected payload slice.
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
        Reject unsupported metadata or negative size, check address ownership, and create a spool.

        Mode passes through unchanged and must be a WriteMode member. Database startup is not
        performed here; schema availability is discovered when commit publishes. Spool-setup OS
        failures are translated with the database/object target.

        Example:
            >>> session = driver.begin_write(address, expected_size=4)  # doctest: +SKIP


        :param object_address: Destination key with this driver subtype/UUID.
        :param mode: WriteMode member controlling publication preconditions; not string-coerced here.
        :param expected_size: Nonnegative accepted-byte count to require at commit, or None.
        :param expected_digest: Optional digest verified from spooled bytes at commit.
        :param metadata: Native metadata tuple; nonempty values are unsupported.
        :return: New uncommitted _SQLiteWriteSession owning its spool.
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
        Insert or replace the complete BLOB and metadata within a BEGIN IMMEDIATE transaction.

        Check CREATE_ONLY/REPLACE existence while holding the transaction, then upsert payload
        length, supplied SHA-256, and a timestamp. Existing rows increment their integer version;
        new rows start at one, including recreation after deletion. The supplied digest is not
        recomputed or validated here. Mode is compared by enum identity and is assumed to have been
        selected correctly by the caller.

        Example:
            >>> digest = hashlib.sha256(b"book").hexdigest()
            >>> driver._publish(address, b"book", sha256=digest, mode=WriteMode.CREATE_ONLY)  # doctest: +SKIP


        :param address: Already checked destination address used as the SQL object_key.
        :param payload: Complete materialized bytes to store in the transaction.
        :param sha256: SHA-256 text supplied by write-time accounting and persisted unchanged.
        :param mode: WriteMode member selecting create-only, replace-existing, or upsert behavior.
        :return: None after normal transaction exit commits the row; errors roll back through the connection context.
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
        Check existence/version and remove one row within a BEGIN IMMEDIATE transaction.

        The version test and deletion share the transaction. Missing tolerance is evaluated before
        any version condition; an absent key with missing_ok succeeds even when if_version was
        supplied. Version strings can be reused after recreation.

        Example:
            >>> driver.delete(address, if_version="1")  # doctest: +SKIP


        :param object_address: Scoped key of the BLOB to delete.
        :param missing_ok: Whether an absent row is accepted before checking a version condition.
        :param if_version: Optional string version required for an existing row.
        :return: None after committed deletion or accepted absence; stale existing rows raise StoragePreconditionFailed.
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
        Iterate stored metadata in object-key order, filtering an optional lexical prefix in Python.

        The query scans all metadata rows even with a prefix. Keep the query/context active while
        yielding; consumers that stop early should close the generator. Size/checksum/version
        columns are trusted and BLOB contents are not inspected.

        Example:
            >>> [str(item.object_address) for item in driver.iter_inventory()]  # doctest: +SKIP
            ['object-42']


        :param prefix: Optional scoped opaque key whose literal text is matched using startswith.
        :return: Iterator of checked row keys and stored metadata; malformed persisted keys can raise during iteration.
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
        Suggest the digest value as a flat key, or generate a random UUID hex key.

        Size and name hints are ignored. Digest algorithm is not part of the key, and no collision,
        reservation, or capacity check is performed.

        Example:
            >>> len(str(driver.allocate_object_address()))  # doctest: +SKIP
            32


        :param expected_size: Accepted hint but unused by this allocator.
        :param expected_digest: Optional digest whose value alone selects the key.
        :param name_hint: Accepted hint but unused for opaque SQLite keys.
        :return: Scoped flat address without publishing or reserving an object.
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
        Return persisted SHA-256 for that algorithm or hash a fully loaded BLOB for another.

        The SHA-256 path does not verify bytes. Other supported algorithms read through open_read,
        then update the hasher in one-MiB chunks over the in-memory stream. Invalid hashlib names
        become StorageUnsupportedOperation.

        Example:
            >>> driver.native_compute_digest(address).algorithm  # doctest: +SKIP
            'sha256'


        :param object_address: Scoped BLOB address to inspect or read.
        :param algorithm: Requested algorithm name; case-insensitive sha256 selects stored metadata.
        :return: Stored SHA-256 Digest or a newly computed digest for another algorithm.
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
        Open SQLite with a 30-second lock timeout and configure foreign keys, WAL, and FULL sync.

        Connection setup can create the database and change persistent journal mode. A PRAGMA
        failure has no explicit close guard here. Returning a connection does not itself start a
        storage write transaction or initialize the object schema.

        Example:
            >>> connection = driver._connect()  # doctest: +SKIP
            >>> connection.close()  # doctest: +SKIP


        :return: New sqlite3.Connection; the caller is responsible for explicit connection closure.
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
        Yield a connection under SQLite transaction handling and translate sqlite3.Error failures.

        Normal context exit commits an open transaction, while exceptional exit rolls it back. The
        sqlite3 connection context does not close the connection, and this wrapper adds no explicit
        close. Errors from setup, body, or transaction exit that are sqlite3.Error receive storage
        classification and cause chaining; other exception types propagate unchanged.

        Example:
            >>> with driver._connection("probe") as connection:  # doctest: +SKIP
            ...     connection.execute("SELECT 1").fetchone()
            (1,)


        :param operation: Context label used when translating SQLite failures.
        :param target: Optional diagnostic database/object text, or None to use the root URI.
        :return: Context manager yielding the transaction-managed connection without an explicit close guarantee.
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
        Append repr-formatted object-key text to the database URI for diagnostic context.

        This formatter does not validate ownership or perform secret filtering; callers pass its
        output to shared error-message helpers where applicable.

        Example:
            >>> driver._target(address).endswith("object 'object-42'")  # doctest: +SKIP
            True


        :param address: Object address whose string value should identify the diagnostic target.
        :return: Database URI followed by object and the repr of the key.
        """

        return f"{self.root_uri} object {str(address)!r}"


__all__ = ["SQLiteObjectAddress", "SQLiteStorageDriver"]
