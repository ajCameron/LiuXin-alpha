"""
Adapt rclone commands to owned addresses, streamed reads/inventory, and staged writes.

Injected runners own executable selection, credentials, and command deadlines.
The raw drivers validate selected address and response contracts, translate
failures, and expose remote metadata as evidence. Writable operations stage locally
or remotely before publication, without promising atomic backend moves or rollback
after publication. Process and incremental-parser helpers document their exact
completion, buffering, and cleanup boundaries.
"""

from __future__ import annotations

import dataclasses
import codecs
import hashlib
import io
import json
import mimetypes
import os
import pathlib
import re
import subprocess
import tempfile
import threading

from collections.abc import Callable, Iterator, Sequence
from datetime import datetime, timezone
from types import TracebackType
from typing import Any, BinaryIO, Protocol
from uuid import UUID, uuid4

from LiuXin_alpha.storage.api import (
    Digest,
    DriverCapabilities,
    DriverConcurrencyCapabilities,
    DriverInventoryEntry,
    DriverObjectAddress,
    DriverObjectAddressInput,
    DriverObjectHints,
    DriverObjectInfo,
    DriverStatus,
    EnumerationCompleteness,
    ScopedDriverObjectAddressChecker,
    StorageAuthenticationFailed,
    StorageAlreadyExists,
    StorageCharacteristics,
    StorageDriverAPI,
    StorageError,
    StorageIntegrityError,
    StorageInvalidAddress,
    StorageLimitation,
    StorageNotFound,
    StoragePermissionDenied,
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
)
from LiuXin_alpha.storage.drivers._validation import (
    best_effort_close,
    reject_malformed_unicode,
)


RcloneJsonRunner = Callable[[Sequence[str]], Any]
RcloneCommandRunner = Callable[[Sequence[str]], Any]
RcloneProcessSpawner = Callable[[Sequence[str]], Any]
RcloneProbe = Callable[[], None]


DEFAULT_MAX_RCLONE_INVENTORY_ENTRIES = 100_000
DEFAULT_MAX_RCLONE_JSON_TOKEN_CHARS = 8 * 1024 * 1024


@dataclasses.dataclass(slots=True, frozen=True)
class RcloneObjectAddress(DriverObjectAddress):
    """
    Carry an rclone-relative path and the UUID owning its address space.

    Inherited construction validates record text and identity, without applying the driver's
    canonical-key parser. Typed ownership checks likewise do not reparse stored path components.

    Example:
        >>> str(RcloneObjectAddress("authors/book.epub", UUID(int=1)))
        'authors/book.epub'
    """


class _ProcessAPI(Protocol):
    """
    Describe the process streams and lifecycle methods used by rclone readers and inventory.

    stdout must supply binary reads for those consumers; stderr may be absent as None. Wait/poll
    expose completion, while terminate/kill support cleanup. The driver's runtime shape checks cover
    only a subset of this protocol.

    Example:
        >>> process: _ProcessAPI = spawned  # doctest: +SKIP


    :ivar stdout: Binary output stream consumed by the reader or incremental inventory decoder.
    :ivar stderr: Diagnostic stream read after an unsuccessful exit, or None.
    """

    stdout: Any
    stderr: Any

    def wait(self, timeout: float | None = None) -> int:
        """
        Wait for command completion or report a timeout according to the process implementation.

        Example:
            >>> code = process.wait(timeout=1)  # doctest: +SKIP


        :param timeout: Optional maximum wait in seconds; None leaves the wait unbounded at this protocol boundary.
        :return: Process exit status when completion is observed; the implementation may raise on timeout.
        """
        ...

    def poll(self) -> int | None:
        """
        Inspect completion without waiting for a running command.

        Example:
            >>> running = process.poll() is None  # doctest: +SKIP


        :return: Exit status, or None while the process remains running.
        """
        ...

    def terminate(self) -> None:
        """
        Request process termination without requiring synchronous exit or stream cleanup.

        Example:
            >>> process.terminate()  # doctest: +SKIP


        :return: None after requesting termination; completion is observed separately.
        """
        ...

    def kill(self) -> None:
        """
        Request forceful process termination without waiting for it to exit.

        Example:
            >>> process.kill()  # doctest: +SKIP


        :return: None after requesting termination through the implementation kill operation.
        """
        ...


class _RcloneProcessReader(io.RawIOBase):
    """
    Adapt one process stdout to raw binary reads with optional exact-length accounting.

    EOF or a read after the remaining count reaches zero checks the process result. Returning a
    requested prefix does not itself prove successful process exit. Closing stops the process and
    streams without draining unread bytes or requiring a successful command. No independent timer or
    stderr-draining thread is added.

    Example:
        >>> reader = _RcloneProcessReader(process, "archive:book", remaining=4)  # doctest: +SKIP
    """

    def __init__(
        self,
        process: _ProcessAPI,
        target: str,
        remaining: int | None = None,
    ) -> None:
        """
        Retain process streams, diagnostic target, optional remaining count, and an unchecked EOF
        marker.

        Example:
            >>> reader = _RcloneProcessReader(process, "archive:book")  # doctest: +SKIP


        :param process: Owned process whose stdout and stderr attributes are read immediately without further validation.
        :param target: Remote identifier passed to shared diagnostic formatting.
        :param remaining: Exact expected bytes for a bounded response, or None to read until stdout EOF.
        :return: None after retaining collaborators; no process I/O or size validation occurs here.
        """
        self._process = process
        self._stdout = process.stdout
        self._stderr = process.stderr
        self._target = target
        self._remaining = remaining
        self._checked_eof = False

    def readable(self) -> bool:
        """
        Advertise raw read support without probing process state.

        Example:
            >>> reader.readable()  # doctest: +SKIP
            True


        :return: True, including after process completion or wrapper closure.
        """
        return True

    def readinto(self, buffer: bytearray | memoryview) -> int:
        """
        Copy at most the buffer capacity and remaining count from process stdout.

        Reject non-byte output and oversized chunks. Translate read timeouts separately from other
        ordinary read failures. EOF checks process success before rejecting a positive missing
        count; a zero-capacity buffer can therefore trigger that check without consuming bytes.
        Reaching the requested count is checked on a subsequent read, without inspecting output
        beyond that count.

        Example:
            >>> count = reader.readinto(bytearray(4096))  # doctest: +SKIP


        :param buffer: Writable destination receiving the returned bytes; its length limits the underlying read request.
        :return: Number of copied bytes, or zero after the applicable completion check; short bounded responses raise StorageUnavailable.
        """
        if self._remaining == 0:
            self._check_process_result()
            return 0
        requested = len(buffer)
        if self._remaining is not None:
            requested = min(requested, self._remaining)
        try:
            data = (
                self._stdout.read(requested)
                if self._stdout is not None
                else b""
            )
        except (TimeoutError, subprocess.TimeoutExpired) as error:
            raise StorageTimeout(
                driver_failure_message(
                    "rclone",
                    "read object",
                    target=self._target,
                    reason="the object stream timed out",
                )
            ) from error
        except OSError as error:
            raise StorageUnavailable(
                driver_failure_message(
                    "rclone",
                    "read object",
                    target=self._target,
                    reason=str(error) or type(error).__name__,
                )
            ) from error
        except Exception as error:
            raise StorageUnavailable(
                driver_failure_message(
                    "rclone",
                    "read object",
                    target=self._target,
                    reason=str(error) or type(error).__name__,
                )
            ) from error
        if not isinstance(data, bytes):
            raise StorageUnavailable(
                driver_failure_message(
                    "rclone",
                    "read object",
                    target=self._target,
                    reason="rclone cat returned non-byte output",
                )
            )
        if len(data) > requested:
            raise StorageUnavailable(
                driver_failure_message(
                    "rclone",
                    "read object",
                    target=self._target,
                    reason="rclone cat returned more bytes than requested",
                )
            )
        if not data:
            self._check_process_result()
            if self._remaining is not None and self._remaining > 0:
                raise StorageUnavailable(
                    driver_failure_message(
                        "rclone",
                        "read object",
                        target=self._target,
                        reason=(
                            "rclone cat ended before the requested byte count "
                            f"({self._remaining} bytes missing)"
                        ),
                    )
                )
        buffer[: len(data)] = data
        if self._remaining is not None:
            self._remaining -= len(data)
        return len(data)

    def _check_process_result(self) -> None:
        """
        Mark EOF checked, wait without an explicit timeout, and translate an unsuccessful exit.

        The marker is set before waiting, so later calls do not retry a failed check. A nonzero
        result reads stderr in full and classifies its text; diagnostic read failures use a fixed
        message naming the exception type. A supplied process wrapper may impose its own timeout.

        Example:
            >>> reader._check_process_result()  # doctest: +SKIP


        :return: None after a falsey exit status or an earlier check marker; first-check wait/exit failures propagate as storage errors.
        """
        if self._checked_eof:
            return
        self._checked_eof = True
        try:
            return_code = self._process.wait()
        except (TimeoutError, subprocess.TimeoutExpired) as error:
            raise StorageTimeout(
                driver_failure_message(
                    "rclone",
                    "read object",
                    target=self._target,
                    reason="the object process timed out while finishing",
                )
            ) from error
        except OSError as error:
            raise StorageUnavailable(
                driver_failure_message(
                    "rclone",
                    "read object",
                    target=self._target,
                    reason=str(error) or type(error).__name__,
                )
            ) from error
        except Exception as error:
            raise StorageUnavailable(
                driver_failure_message(
                    "rclone",
                    "read object",
                    target=self._target,
                    reason=str(error) or type(error).__name__,
                )
            ) from error
        if return_code:
            stderr = b""
            if self._stderr is not None:
                try:
                    stderr = self._stderr.read()
                except Exception as error:
                    stderr = (
                        "rclone exited unsuccessfully and its diagnostic "
                        f"stream failed: {type(error).__name__}"
                    )
            message = (
                stderr.decode(errors="replace")
                if isinstance(stderr, bytes)
                else str(stderr)
            )
            raise _translate_rclone_error(
                message,
                target=self._target,
                operation="read object",
            )

    def close(self) -> None:
        """
        Stop the owned process and close the raw wrapper, returning immediately if already closed.

        The base close runs in finally even if cleanup fails. This operation does not validate EOF,
        a requested byte count, or a successful process exit.

        Example:
            >>> reader.close()  # doctest: +SKIP


        :return: None after cleanup/base closure, unless a failure escapes those operations.
        """
        if self.closed:
            return
        try:
            _stop_rclone_process(self._process)
        finally:
            super().close()


class _RcloneWriteSession:
    """
    Accumulate a local staged file and publish it through the owning writable driver on commit.

    Expectations use accepted-write counters and an optional running digest. A failed commit can
    follow remote publication; abort removes local staging but does not reverse remote effects.
    Context exit aborts unless commit succeeded.

    Example:
        >>> with driver.begin_write(address) as session:  # doctest: +SKIP
        ...     session.write(b"book")
        ...     info = session.commit()
    """

    def __init__(
        self,
        driver: WritableRcloneStorageDriver,
        address: RcloneObjectAddress,
        *,
        mode: WriteMode,
        expected_size: int | None,
        expected_digest: Digest | None,
    ) -> None:
        """
        Create an optional digest accumulator and a local mkstemp staging stream.

        Unsupported hashlib algorithms raise StorageUnsupportedOperation. Creation OSErrors are
        translated, but fdopen follows mkstemp outside that creation guard. Mode, address, and
        expected-size validation belong to the caller.

        Example:
            >>> session = _RcloneWriteSession(driver, address, mode=WriteMode.CREATE_ONLY, expected_size=4, expected_digest=None)  # doctest: +SKIP


        :param driver: Writable driver supplying local staging location and remote publication behavior.
        :param address: Retained requested destination passed to publication on commit.
        :param mode: Retained WriteMode used by remote existence and publication checks.
        :param expected_size: Optional accepted-byte total to require at commit; None omits that comparison.
        :param expected_digest: Optional algorithm/value used to accumulate and compare accepted bytes.
        :return: None after creating the local stream and initializing unfinished/uncommitted session state.
        """
        self._driver = driver
        self._address = address
        self._mode = mode
        self._expected_size = expected_size
        self._expected_digest = expected_digest
        self._size = 0
        expected_algorithm = (
            None if expected_digest is None else expected_digest.algorithm
        )
        try:
            self._digest = (
                None
                if expected_algorithm is None
                else hashlib.new(expected_algorithm)
            )
        except ValueError as error:
            raise StorageUnsupportedOperation(
                f"unsupported digest algorithm: {expected_algorithm!r}"
            ) from error
        try:
            descriptor, temporary_name = tempfile.mkstemp(
                prefix="liuxin-rclone-",
                suffix=".part",
                dir=driver.local_staging_directory,
            )
        except OSError as error:
            raise translate_os_error(
                error,
                backend="rclone local staging",
                operation="begin write",
                target=driver.local_staging_directory,
            ) from error
        self._temporary_path = pathlib.Path(temporary_name)
        self._stream = os.fdopen(descriptor, "wb")
        self._finished = False
        self._committed = False

    def write(self, data: bytes) -> int:
        """
        Append bytes to an unfinished staging stream and account for its reported acceptance.

        Reject non-bytes input. A None write result means all input bytes were accepted; other
        counts are accumulated without an independent range check here. The optional digest sees the
        reported accepted prefix. Local OSErrors are translated.

        Example:
            >>> accepted = session.write(b"chapter")  # doctest: +SKIP


        :param data: Bytes offered to the local staging stream.
        :return: Reported accepted count, or the input length when the stream returns None.
        """
        if self._finished:
            raise StorageError("rclone write session is finished.")
        if not isinstance(data, bytes):
            raise TypeError("write-session data must be bytes.")
        try:
            accepted = self._stream.write(data)
        except OSError as error:
            raise translate_os_error(
                error,
                backend="rclone local staging",
                operation="write",
                target=self._temporary_path,
            ) from error
        if accepted is None:
            accepted = len(data)
        self._size += accepted
        if self._digest is not None and accepted:
            self._digest.update(data[:accepted])
        return accepted

    def commit(self) -> DriverObjectInfo[RcloneObjectAddress]:
        """
        Flush, fsync, and close local staging, check accumulated expectations, then publish
        remotely.

        Successful publication and its final stat precede setting finished/committed. On failure,
        abort is attempted; a remote object can already be visible. The local path is unlinked in
        finally with OSErrors suppressed. Expectation checks do not reread the staged file, and no
        remote rollback is promised.

        Example:
            >>> info = session.commit()  # doctest: +SKIP


        :return: DriverObjectInfo from final publication stat; reuse after a finished session raises StorageError.
        """
        if self._finished:
            raise StorageError("rclone write session is finished.")
        try:
            self._stream.flush()
            os.fsync(self._stream.fileno())
            self._stream.close()
            self._validate_expectations()
            info = self._driver._publish_local_file(
                self._temporary_path,
                self._address,
                mode=self._mode,
            )
            self._finished = True
            self._committed = True
            return info
        except OSError as error:
            self.abort()
            raise translate_os_error(
                error,
                backend="rclone local staging",
                operation="commit",
                target=self._temporary_path,
            ) from error
        except BaseException:
            self.abort()
            raise
        finally:
            try:
                self._temporary_path.unlink(missing_ok=True)
            except OSError:
                pass

    def _validate_expectations(self) -> None:
        """
        Compare retained size and digest expectations with accepted-write accumulators, without
        rereading local bytes.

        Example:
            >>> session._validate_expectations()  # doctest: +SKIP


        :return: None when supplied expectations match; size or digest mismatch raises StorageIntegrityError.
        """
        if self._expected_size is not None and self._size != self._expected_size:
            raise StorageIntegrityError(
                f"expected {self._expected_size} bytes, received {self._size}."
            )
        if self._expected_digest is not None:
            assert self._digest is not None
            if self._digest.hexdigest().lower() != self._expected_digest.value:
                raise StorageIntegrityError(
                    f"{self._expected_digest.algorithm} digest mismatch."
                )

    def abort(self) -> None:
        """
        Attempt stream closure and local-path removal, then mark the session finished.

        OSErrors from close/unlink are suppressed; other failures can escape before the finished
        marker. Remote staging/publication belongs to driver operations and cannot be undone by this
        local cleanup.

        Example:
            >>> session.abort()  # doctest: +SKIP


        :return: None after the attempted local cleanup and finished-state update.
        """
        try:
            if not self._stream.closed:
                self._stream.close()
        except OSError:
            pass
        try:
            self._temporary_path.unlink(missing_ok=True)
        except OSError:
            pass
        self._finished = True

    def __enter__(self) -> _RcloneWriteSession:
        """
        Require an unfinished session and return it without publishing or acquiring a driver write
        lock.

        Example:
            >>> with session as active:  # doctest: +SKIP
            ...     accepted = active.write(b"book")


        :return: This session; entering after finish raises StorageError.
        """
        if self._finished:
            raise StorageError("rclone write session is finished.")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """
        Abort locally unless a commit completed successfully, regardless of the escaping exception.

        Example:
            >>> session.__exit__(None, None, None)  # doctest: +SKIP


        :param exc_type: Context exception type, or None; ignored when deciding whether to abort.
        :param exc: Context exception instance, or None; not suppressed by this method.
        :param traceback: Context traceback, or None; unused by local cleanup.
        :return: None, allowing any context exception to propagate; abort failures can also escape.
        """
        if not self._committed:
            self.abort()


class RcloneStorageDriver(StorageDriverAPI[RcloneObjectAddress]):
    """
    Expose an injected rclone command interface through a read-only raw storage driver.

    Local construction establishes root/address identity and inventory limits. Stat trusts selected
    remote metadata; reads stream cat output, and inventory prefers incremental process output with
    a restricted legacy-runner fallback. Declared digest authority depends on backend evidence
    rather than an independent byte verification performed here.

    Example:
        >>> driver = RcloneStorageDriver("archive:", address_space_uuid=UUID(int=1), json_runner=lambda args: [], process_spawner=lambda args: None)
        >>> driver.root_uri, driver.status().available
        ('archive:', False)
    """

    def __init__(
        self,
        fs_root: str,
        *,
        address_space_uuid: UUID,
        json_runner: RcloneJsonRunner,
        process_spawner: RcloneProcessSpawner,
        probe: RcloneProbe | None = None,
        max_inventory_entries: int = DEFAULT_MAX_RCLONE_INVENTORY_ENTRIES,
        max_json_token_chars: int = DEFAULT_MAX_RCLONE_JSON_TOKEN_CHARS,
    ) -> None:
        """
        Validate the root and positive inventory bounds, then retain injected command collaborators.

        Strip outer root whitespace; reject empty roots, NUL/newline/carriage-return, malformed
        Unicode, and selected recognizable inline-secret options. The secret check is not a complete
        parser or redactor. Bounds are compared before int conversion. Construction runs no command
        and does not validate runner behavior.

        Example:
            >>> driver = RcloneStorageDriver("archive:", address_space_uuid=UUID(int=1), json_runner=lambda args: [], process_spawner=lambda args: None)
            >>> driver.status().writable
            False


        :param fs_root: Rclone filesystem identifier converted to stripped text and retained as the root.
        :param address_space_uuid: UUID used by the scoped RcloneObjectAddress checker.
        :param json_runner: Callable receiving rclone arguments without an executable and returning already-decoded output.
        :param process_spawner: Callable receiving streamed-command arguments and returning a process-like object.
        :param probe: Optional zero-argument health callback; None selects the default depth-one listing.
        :param max_inventory_entries: Positive bound on observed inventory values, including later skipped or duplicate entries.
        :param max_json_token_chars: Positive bound on newly buffered decoded inventory text before the next parsing pass.
        :return: None after initializing an unavailable cached status and the configured address/runners/limits.
        """
        root = str(fs_root).strip()
        reject_malformed_unicode(root, label="rclone filesystem root")
        if not root or "\x00" in root or "\n" in root or "\r" in root:
            raise StorageInvalidAddress("rclone filesystem root is invalid.")
        _reject_inline_rclone_secrets(root)
        if max_inventory_entries < 1:
            raise ValueError("max_inventory_entries must be positive.")
        if max_json_token_chars < 1:
            raise ValueError("max_json_token_chars must be positive.")
        self._fs_root = root
        self._json_runner = json_runner
        self._process_spawner = process_spawner
        self._probe_callback = probe
        self._max_inventory_entries = int(max_inventory_entries)
        self._max_json_token_chars = int(max_json_token_chars)
        self._checker = ScopedDriverObjectAddressChecker(
            RcloneObjectAddress,
            address_space_uuid,
        )
        self._last_status = DriverStatus(
            available=False,
            writable=False,
            message="rclone driver has not been started.",
        )

    @property
    def object_address_checker(
        self,
    ) -> ScopedDriverObjectAddressChecker[RcloneObjectAddress]:
        """
        Expose the retained checker for rclone address subtype and UUID ownership.

        Example:
            >>> driver = RcloneStorageDriver("archive:", address_space_uuid=UUID(int=1), json_runner=lambda args: [], process_spawner=lambda args: None)
            >>> driver.object_address_checker.address_space_uuid == UUID(int=1)
            True


        :return: Scoped checker instance; it does not reparse an existing address key.
        """
        return self._checker

    @property
    def root_uri(self) -> str:
        """
        Return the validated, outer-whitespace-stripped rclone filesystem identifier.

        Example:
            >>> driver = RcloneStorageDriver("archive:", address_space_uuid=UUID(int=1), json_runner=lambda args: [], process_spawner=lambda args: None)
            >>> driver.root_uri
            'archive:'


        :return: Retained root text, which need not be a conventional HTTP-style URI.
        """
        return self._fs_root

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Describe generic range reads, hierarchical addresses, complete enumeration, and remote
        digest evidence.

        Conditional reads, paging, and writes remain unsupported. Concurrent-read declarations
        assume the injected runners tolerate independent calls; no shared process or global
        synchronization is supplied by this property.

        Example:
            >>> driver = RcloneStorageDriver("archive:", address_space_uuid=UUID(int=1), json_runner=lambda args: [], process_spawner=lambda args: None)
            >>> driver.capabilities.enumeration is EnumerationCompleteness.COMPLETE
            True


        :return: New DriverCapabilities with read/inventory/address support and four recommended parallel reads.
        """
        return DriverCapabilities(
            range_reads=True,
            enumeration=EnumerationCompleteness.COMPLETE,
            stat_digest_authoritative=True,
            hierarchical_object_addresses=True,
            external_uri_parsing=True,
            external_uri_rendering=True,
            prefix_enumeration=True,
            concurrency=DriverConcurrencyCapabilities(
                thread_safe=True,
                concurrent_reads=True,
                recommended_parallel_reads=4,
            ),
        )

    @property
    def storage_characteristics(self) -> StorageCharacteristics:
        """
        Describe the read-only driver publication and write-space profile.

        Example:
            >>> driver = RcloneStorageDriver("archive:", address_space_uuid=UUID(int=1), json_runner=lambda args: [], process_spawner=lambda args: None)
            >>> driver.storage_characteristics.publication_model is StoragePublicationModel.READ_ONLY
            True


        :return: New read-only characteristics with no write-staging requirement and inapplicable write usage.
        """

        return StorageCharacteristics(
            publication_model=StoragePublicationModel.READ_ONLY,
            temporary_space=StorageTemporarySpaceRequirement.NONE,
            recommended_write_usage=StorageWriteUsage.NOT_APPLICABLE,
        )

    def startup(self) -> DriverStatus:
        """
        Run the active probe and return its freshly cached status.

        Example:
            >>> driver = RcloneStorageDriver("archive:", address_space_uuid=UUID(int=1), json_runner=lambda args: [], process_spawner=lambda args: None)
            >>> driver.startup().available
            True


        :return: DriverStatus from probe; injected callback or listing I/O may occur.
        """
        return self.probe()

    def probe(self) -> DriverStatus:
        """
        Run the optional health callback or a depth-one JSON listing and update cached availability.

        The callback is called directly and its return value is ignored. The default listing result
        is not checked for shape or content. Only StorageUnavailable and StorageTimeout become
        unavailable status; other failures propagate without a replacement status. Success does not
        enumerate the root or test writes.

        Example:
            >>> driver = RcloneStorageDriver("archive:", address_space_uuid=UUID(int=1), json_runner=lambda args: [], process_spawner=lambda args: None)
            >>> driver.probe().available, driver.status().writable
            (True, False)


        :return: New timestamped read-only DriverStatus after a successful or handled unavailable/timeout probe.
        """
        try:
            if self._probe_callback is not None:
                self._probe_callback()
            else:
                self._run_json(["lsjson", "--max-depth", "1", self._fs_root])
        except (StorageUnavailable, StorageTimeout) as error:
            self._last_status = DriverStatus(
                available=False,
                writable=False,
                checked_at=datetime.now(timezone.utc),
                message=str(error),
            )
            return self._last_status
        self._last_status = DriverStatus(
            available=True,
            writable=False,
            checked_at=datetime.now(timezone.utc),
            message="rclone filesystem is available (read-only).",
        )
        return self._last_status

    def status(self) -> DriverStatus:
        """
        Return the latest retained health observation without running a command.

        Example:
            >>> driver = RcloneStorageDriver("archive:", address_space_uuid=UUID(int=1), json_runner=lambda args: [], process_spawner=lambda args: None)
            >>> driver.status().available
            False


        :return: Cached initial or last-probe DriverStatus; access does not refresh it.
        """
        return self._last_status

    def close(self) -> None:
        """
        Return without retaining a driver-level process pool or changing cached status.

        Example:
            >>> driver = RcloneStorageDriver("archive:", address_space_uuid=UUID(int=1), json_runner=lambda args: [], process_spawner=lambda args: None)
            >>> driver.close()


        :return: None; individual reader/inventory consumers own their processes.
        """
        return None

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[RcloneObjectAddress],
    ) -> RcloneObjectAddress:
        """
        Check existing address ownership or validate text as a relative canonical rclone key.

        Example:
            >>> driver = RcloneStorageDriver("archive:", address_space_uuid=UUID(int=1), json_runner=lambda args: [], process_spawner=lambda args: None)
            >>> str(driver.parse_object_address("authors/café.epub"))
            'authors/café.epub'


        :param identifier: Existing driver address checked for subtype/UUID, or text passed through the canonical-key parser.
        :return: Owned RcloneObjectAddress; an existing typed key is not reparsed and no existence check occurs.
        """
        if isinstance(identifier, DriverObjectAddress):
            return self.check_object_address(identifier)
        key = _canonical_rclone_key(str(identifier))
        return RcloneObjectAddress(key, self._checker.address_space_uuid)

    def join_object_address(self, *tokens: str) -> RcloneObjectAddress:
        """
        Join one or more stringified tokens with slashes, then apply text-key validation.

        Example:
            >>> driver = RcloneStorageDriver("archive:", address_space_uuid=UUID(int=1), json_runner=lambda args: [], process_spawner=lambda args: None)
            >>> str(driver.join_object_address("authors", "book.epub"))
            'authors/book.epub'


        :param tokens: Nonempty sequence of relative path fragments; leading/trailing slashes and empty fragments are not trimmed.
        :return: Owned canonical address, or StorageInvalidAddress when the joined key is invalid.
        """
        if not tokens:
            raise StorageInvalidAddress("at least one rclone path token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def object_address_from_uri(self, uri: str) -> RcloneObjectAddress:
        """
        Remove the exact configured root prefix and validate the remaining relative key without URL
        decoding.

        Example:
            >>> driver = RcloneStorageDriver("archive:", address_space_uuid=UUID(int=1), json_runner=lambda args: [], process_spawner=lambda args: None)
            >>> str(driver.object_address_from_uri("archive:book%20one.epub"))
            'book%20one.epub'


        :param uri: Stringified rclone object identifier starting with the root and its applicable slash separator.
        :return: Owned address below the textual root; case and literal percent spelling remain significant.
        """
        text = str(uri)
        prefix = self._fs_root if self._fs_root.endswith(":") else self._fs_root.rstrip("/") + "/"
        if not text.startswith(prefix):
            raise StorageInvalidAddress("rclone object identifier belongs to another filesystem root.")
        return self.parse_object_address(text[len(prefix) :])

    def object_uri(self, object_address: RcloneObjectAddress) -> str:
        """
        Append owned address text to the root with the rclone-specific separator.

        Example:
            >>> driver = RcloneStorageDriver("archive:", address_space_uuid=UUID(int=1), json_runner=lambda args: [], process_spawner=lambda args: None)
            >>> driver.object_uri(driver.parse_object_address("book one.epub"))
            'archive:book one.epub'


        :param object_address: Address checked for subtype/UUID without reparsing its stored key.
        :return: Root-plus-key text; roots ending in a colon concatenate directly, and other roots use one slash without percent quoting.
        """
        checked = self.check_object_address(object_address)
        if self._fs_root.endswith(":"):
            return self._fs_root + str(checked)
        return self._fs_root.rstrip("/") + "/" + str(checked)

    def stat(
        self,
        object_address: RcloneObjectAddress,
    ) -> DriverObjectInfo[RcloneObjectAddress]:
        """
        Request lsjson --stat --hash and interpret the returned object record.

        Require a dictionary, falsey IsDir, and a nonnegative int-convertible Size. Prefer reported
        SHA-256, SHA-1, then MD5 without reading object bytes or checking digest length/hex syntax.
        The returned record remains bound to the requested address; response Path is not checked
        against it. Name Unicode is validated, while other optional metadata uses its respective
        coercion helper.

        Example:
            >>> info = driver.stat(address)  # doctest: +SKIP


        :param object_address: Owned file address rendered as the stat command target.
        :return: DriverObjectInfo with required size, selected digest/version/time, and filename/media/native hints; invalid remote facts may raise.
        """
        checked = self.check_object_address(object_address)
        blob = self._run_json(
            ["lsjson", "--stat", "--hash", self.object_uri(checked)]
        )
        if not isinstance(blob, dict):
            raise StorageUnavailable(
                driver_failure_message(
                    "rclone",
                    "stat object",
                    target=self.object_uri(checked),
                    reason="rclone returned a non-object response",
                )
            )
        if bool(blob.get("IsDir", False)):
            raise StorageInvalidAddress("rclone Store Locations identify files, not directories.")
        if "Size" not in blob:
            raise StorageUnavailable("rclone stat omitted the object's size.")
        try:
            size = int(blob["Size"])
        except (TypeError, ValueError) as error:
            raise StorageUnavailable("rclone stat returned an invalid size.") from error
        if size < 0:
            raise StorageUnavailable("rclone stat returned a negative size.")
        digest = _rclone_digest(blob.get("Hashes"))
        return DriverObjectInfo(
            object_address=checked,
            size=size,
            modified_at=_rclone_datetime(blob.get("ModTime")),
            digest=digest,
            version=_optional_text(blob.get("ID")),
            hints=DriverObjectHints(
                suggested_filename=(
                    _remote_rclone_text(
                        blob.get("Name"),
                        label="stat object name",
                    )
                    or pathlib.PurePosixPath(str(checked)).name
                ),
                media_type=_optional_text(blob.get("MimeType"))
                or mimetypes.guess_type(str(checked))[0],
                metadata=_rclone_native_metadata(blob),
            ),
        )

    def open_read(
        self,
        object_address: RcloneObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Spawn cat and return a buffered process reader for the selected byte range.

        Ownership, unsupported version conditions, and negative ranges are checked before a
        zero-length BytesIO shortcut. No stat preflight establishes existence or available length. A
        supplied count becomes an exact expected byte total, so a shorter successful response fails
        when consumed. Unbounded reads only check EOF/process outcome. Invalid process shape
        attempts terminate, without a full wait/stream cleanup. The process adapter supplies any
        command deadline.

        Example:
            >>> with driver.open_read(address, offset=2, length=4) as stream:  # doctest: +SKIP
            ...     payload = stream.read()


        :param object_address: Owned address used as the cat target, without reparsing typed key text.
        :param offset: Nonnegative byte offset sent as --offset when nonzero.
        :param length: Exact expected response count sent as --count, None for EOF-driven reads, or zero for an immediate empty stream.
        :param if_version: Must be None; generic rclone remotes have no supported conditional-read contract here.
        :return: Caller-owned BufferedReader over the process, or an empty BytesIO for length zero; callers must close it.
        """
        checked = self.check_object_address(object_address)
        if if_version is not None:
            raise StorageUnsupportedOperation(
                "generic rclone remotes do not provide conditional reads."
            )
        if offset < 0 or (length is not None and length < 0):
            raise StorageInvalidAddress("rclone read ranges must not be negative.")
        if length == 0:
            return io.BytesIO()
        target = self.object_uri(checked)
        arguments = ["cat", target]
        if offset:
            arguments.extend(["--offset", str(offset)])
        if length is not None:
            arguments.extend(["--count", str(length)])
        try:
            process = self._process_spawner(arguments)
        except subprocess.TimeoutExpired as error:
            raise StorageTimeout(
                driver_failure_message(
                    "rclone",
                    "open read",
                    target=target,
                    reason="the command timed out",
                )
            ) from error
        except Exception as error:
            raise _translate_rclone_error(
                str(error),
                target=target,
                operation="open read",
            ) from error
        if (
            process is None
            or not callable(getattr(process, "wait", None))
            or not callable(getattr(process, "poll", None))
            or getattr(process, "stdout", None) is None
            or not callable(getattr(process.stdout, "read", None))
        ):
            if process is not None:
                terminate = getattr(process, "terminate", None)
                if callable(terminate):
                    try:
                        terminate()
                    except Exception:
                        pass
            raise StorageUnavailable(
                driver_failure_message(
                    "rclone",
                    "open read",
                    target=target,
                    reason="rclone returned an invalid process stream",
                )
            )
        return io.BufferedReader(
            _RcloneProcessReader(process, target, remaining=length)
        )

    def iter_inventory(
        self,
        *,
        prefix: RcloneObjectAddress | None = None,
    ) -> Iterator[DriverInventoryEntry[RcloneObjectAddress]]:
        """
        Request recursive file/hash listings and yield unique, optionally prefix-filtered addresses.

        Count every observed value before shape/path/filter/deduplication handling. Missing/falsey
        paths are skipped, duplicate accepted addresses are suppressed, and prefix matching is
        lexical startswith. The command requests --files-only, but returned IsDir fields are not
        checked here. Earlier entries may escape before later parse, metadata, count, or exit
        failures. Process parsing and legacy-list fallback have different buffering behavior.

        Example:
            >>> entries = list(driver.iter_inventory(prefix=prefix))  # doctest: +SKIP


        :param prefix: Optional owned address whose text filters file keys lexically, or None for all accepted entries.
        :return: Lazy DriverInventoryEntry iterator with optional listing-derived size/digest/version/time and filename/media hints.
        """
        prefix_key = None if prefix is None else str(self.check_object_address(prefix))
        arguments = ["lsjson", "-R", "--files-only", "--hash", self._fs_root]
        process = None
        try:
            process = self._process_spawner(arguments)
        except subprocess.TimeoutExpired as error:
            raise StorageTimeout(
                driver_failure_message(
                    "rclone",
                    "start inventory",
                    target=self._fs_root,
                    reason="the command timed out",
                )
            ) from error
        except Exception:
            # Retain compatibility with injected or legacy runners that do not
            # expose a process form.  A successful production scan streams.
            process = None

        if process is not None and not _valid_rclone_process(process):
            terminate = getattr(process, "terminate", None)
            if callable(terminate):
                try:
                    terminate()
                except Exception:
                    pass
            raise StorageUnavailable(
                driver_failure_message(
                    "rclone",
                    "start inventory",
                    target=self._fs_root,
                    reason="rclone returned an invalid process stream",
                )
            )

        def _items() -> Iterator[Any]:
            """
            Yield streamed JSON values or select the restricted decoded-list compatibility fallback.

            A missing process permits fallback. After streamed StorageError, fallback is allowed
            only before any item was yielded, with a known nonzero poll result, and excluding
            StorageTimeout. Successful malformed output and failures after yielding remain fatal. A
            legacy None result is empty; other legacy results must be lists and may already occupy
            memory in full.

            Example:
                >>> raw_items = list(_items())  # doctest: +SKIP


            :return: Iterator over unnormalized remote values, with process cleanup owned by the streamed decoder.
            """
            if process is not None:
                yielded = False
                try:
                    for item in _iter_json_array_process(
                        process,
                        target=self._fs_root,
                        max_token_chars=self._max_json_token_chars,
                    ):
                        yielded = True
                        yield item
                    return
                except StorageError as error:
                    # Some injected or legacy runners expose only the JSON call.
                    # Fall back only when the process failed before yielding;
                    # malformed successful output remains fatal.
                    try:
                        process_status = process.poll()
                    except Exception:
                        process_status = None
                    if (
                        isinstance(error, StorageTimeout)
                        or yielded
                        or process_status in {None, 0}
                    ):
                        raise
            payload = self._run_json(arguments)
            if payload is None:
                return
            if not isinstance(payload, list):
                raise StorageUnavailable(
                    "rclone inventory did not return a JSON array."
                )
            yield from payload

        seen: set[RcloneObjectAddress] = set()
        observed = 0
        for item in _items():
            observed += 1
            if observed > self._max_inventory_entries:
                raise StorageUnavailable(
                    driver_failure_message(
                        "rclone",
                        "inventory",
                        target=self._fs_root,
                        reason="the configured inventory entry limit was exceeded",
                    )
                )
            if not isinstance(item, dict):
                raise StorageUnavailable("rclone inventory contained a non-object entry.")
            raw_path = item.get("Path") or item.get("Name")
            if not raw_path:
                continue
            try:
                address = self.parse_object_address(
                    _remote_rclone_text(
                        raw_path,
                        label="inventory object path",
                    )
                    or ""
                )
            except StorageInvalidAddress as error:
                raise StorageUnavailable(
                    driver_failure_message(
                        "rclone",
                        "inventory",
                        target=self._fs_root,
                        reason="rclone returned a malformed object path",
                    )
                ) from error
            if prefix_key is not None and not str(address).startswith(prefix_key):
                continue
            if address in seen:
                continue
            seen.add(address)
            size = None
            if "Size" in item:
                try:
                    size = int(item["Size"])
                except (TypeError, ValueError) as error:
                    raise StorageUnavailable("rclone inventory returned an invalid size.") from error
                if size < 0:
                    raise StorageUnavailable("rclone inventory returned a negative size.")
            yield DriverInventoryEntry(
                object_address=address,
                size=size,
                modified_at=_rclone_datetime(item.get("ModTime")),
                digest=_rclone_digest(item.get("Hashes")),
                version=_optional_text(item.get("ID")),
                hints=DriverObjectHints(
                    suggested_filename=(
                        _remote_rclone_text(
                            item.get("Name"),
                            label="inventory object name",
                        )
                        or pathlib.PurePosixPath(str(address)).name
                    ),
                    media_type=_optional_text(item.get("MimeType"))
                    or mimetypes.guess_type(str(address))[0],
                ),
            )

    def _run_json(self, arguments: Sequence[str]) -> Any:
        """
        Forward a JSON command and retain typed storage errors while translating timeout or other
        runner failures.

        Example:
            >>> driver = RcloneStorageDriver("archive:", address_space_uuid=UUID(int=1), json_runner=lambda args: [], process_spawner=lambda args: None)
            >>> driver._run_json(["lsjson", "archive:"])
            []


        :param arguments: Argument sequence including a command name, but not the executable; the first item labels failures.
        :return: Runner result unchanged, without JSON decoding or response-shape validation at this boundary.
        """
        try:
            return self._json_runner(arguments)
        except subprocess.TimeoutExpired as error:
            raise StorageTimeout(
                driver_failure_message(
                    "rclone",
                    f"run {arguments[0] if arguments else 'command'}",
                    target=self._fs_root,
                    reason="the command timed out",
                )
            ) from error
        except StorageError:
            raise
        except Exception as error:
            raise _translate_rclone_error(
                str(error),
                target=self._fs_root,
                operation=f"run {arguments[0] if arguments else 'command'}",
            ) from error


class WritableRcloneStorageDriver(RcloneStorageDriver):
    """
    Add local staging, remote staging, create/replace publication, and deletion to generic rclone
    reads.

    Publication uses copyto followed by moveto with existence checks under an instance lock. Backend
    moves may copy and delete, so publication is not guaranteed atomic. A completed remote change
    can precede a reported failure during final metadata inspection. The driver does not provide a
    cross-object transaction or independent verification of reported remote hashes.

    Example:
        >>> driver = WritableRcloneStorageDriver("archive:", address_space_uuid=UUID(int=1), json_runner=run_json, command_runner=run, process_spawner=spawn)  # doctest: +SKIP
    """

    def __init__(
        self,
        fs_root: str,
        *,
        address_space_uuid: UUID,
        json_runner: RcloneJsonRunner,
        command_runner: RcloneCommandRunner,
        process_spawner: RcloneProcessSpawner,
        probe: RcloneProbe | None = None,
        local_staging_directory: str | os.PathLike[str] | None = None,
        max_inventory_entries: int = DEFAULT_MAX_RCLONE_INVENTORY_ENTRIES,
        max_json_token_chars: int = DEFAULT_MAX_RCLONE_JSON_TOKEN_CHARS,
    ) -> None:
        """
        Configure inherited read operations, a command runner, an instance lock, and local write
        staging.

        None staging creates an owned TemporaryDirectory. A supplied path is expanded, resolved
        non-strictly, and created with parents; existing permissions are not rewritten. Construction
        performs local filesystem I/O without testing remote availability or write permission.

        Example:
            >>> driver = WritableRcloneStorageDriver("archive:", address_space_uuid=UUID(int=1), json_runner=run_json, command_runner=run, process_spawner=spawn)  # doctest: +SKIP


        :param fs_root: Rclone root validated and retained by the read-only base constructor.
        :param address_space_uuid: UUID owning both public and internally generated staging addresses.
        :param json_runner: Injected command-to-decoded-output callable used for probe/stat and legacy inventory.
        :param command_runner: Injected callable executing copyto/moveto/deletefile and signaling failure by exception.
        :param process_spawner: Injected process factory for cat and streamed inventory arguments.
        :param probe: Optional zero-argument health callback, or None for the base JSON probe.
        :param local_staging_directory: Caller-managed local path, or None to create an automatically cleaned temporary directory.
        :param max_inventory_entries: Positive bound on observed inventory values, captured by the base driver.
        :param max_json_token_chars: Positive bound on buffered decoded inventory text before a parsing pass.
        :return: None after configuring local staging and command collaborators; local setup OSErrors become storage failures.
        """
        super().__init__(
            fs_root,
            address_space_uuid=address_space_uuid,
            json_runner=json_runner,
            process_spawner=process_spawner,
            probe=probe,
            max_inventory_entries=max_inventory_entries,
            max_json_token_chars=max_json_token_chars,
        )
        self._command_runner = command_runner
        self._write_lock = threading.RLock()
        try:
            if local_staging_directory is None:
                self._temporary_directory = tempfile.TemporaryDirectory(
                    prefix="liuxin-rclone-writes-"
                )
                self._local_staging_directory = pathlib.Path(
                    self._temporary_directory.name
                )
            else:
                self._temporary_directory = None
                self._local_staging_directory = pathlib.Path(
                    local_staging_directory
                ).expanduser().resolve(strict=False)
                self._local_staging_directory.mkdir(
                    mode=0o700,
                    parents=True,
                    exist_ok=True,
                )
        except OSError as error:
            raise translate_os_error(
                error,
                backend="rclone local staging",
                operation="configure",
                target=(
                    tempfile.gettempdir()
                    if local_staging_directory is None
                    else local_staging_directory
                ),
            ) from error

    @property
    def local_staging_directory(self) -> pathlib.Path:
        """
        Expose the configured local directory used when creating complete-file write sessions.

        Example:
            >>> directory = driver.local_staging_directory  # doctest: +SKIP


        :return: Retained pathlib.Path; access neither reserves capacity nor checks continued existence.
        """
        return self._local_staging_directory

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Extend generic read capabilities with create, replace, delete, and address allocation while
        retaining conservative publication guarantees.

        Example:
            >>> driver.capabilities.atomic_publish  # doctest: +SKIP
            False


        :return: New capabilities with non-atomic publication, no conditional deletion, and concurrent_writes=False.
        """
        return dataclasses.replace(
            super().capabilities,
            create=True,
            replace=True,
            delete=True,
            conditional_delete=False,
            atomic_publish=False,
            object_address_allocation=True,
            concurrency=DriverConcurrencyCapabilities(
                thread_safe=True,
                concurrent_reads=True,
                concurrent_writes=False,
                recommended_parallel_reads=4,
            ),
        )

    @property
    def storage_characteristics(self) -> StorageCharacteristics:
        """
        Describe per-object publication and complete local staging with backend-dependent size and
        atomicity limits.

        Example:
            >>> driver.storage_characteristics.temporary_space is StorageTemporarySpaceRequirement.OBJECT_STAGE  # doctest: +SKIP
            True


        :return: New characteristics declaring object staging, general write usage, and an explicit backend-dependent limitation.
        """

        return StorageCharacteristics(
            publication_model=StoragePublicationModel.PER_OBJECT,
            temporary_space=StorageTemporarySpaceRequirement.OBJECT_STAGE,
            recommended_write_usage=StorageWriteUsage.GENERAL,
            preserves_unmodelled_entries=True,
            rewrites_container_format=False,
            limitations=(
                StorageLimitation(
                    "rclone_backend_dependent_limits",
                    "Object limits and publication atomicity depend on the selected rclone backend.",
                ),
            ),
        )

    def probe(self) -> DriverStatus:
        """
        Run the inherited readability probe and mark a successful result as configured for staged
        writes.

        Example:
            >>> status = driver.probe()  # doctest: +SKIP


        :return: Cached status with writable=True after availability succeeds; no write permission test or publication is performed.
        """
        status = super().probe()
        if not status.available:
            return status
        self._last_status = dataclasses.replace(
            status,
            writable=True,
            message="rclone filesystem is available for staged writes.",
        )
        return self._last_status

    def begin_write(
        self,
        object_address: RcloneObjectAddress,
        *,
        mode: WriteMode = WriteMode.CREATE_ONLY,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        metadata: tuple[tuple[str, str], ...] = (),
    ) -> _RcloneWriteSession:
        """
        Validate public destination, empty native metadata, nonnegative expected size, and WriteMode
        before creating a local session.

        Example:
            >>> session = driver.begin_write(address, expected_size=4)  # doctest: +SKIP


        :param object_address: Owned destination outside the reserved .liuxin-staging/ prefix.
        :param mode: WriteMode or accepted enum input converted before session construction.
        :param expected_size: Nonnegative expected accepted-byte total, or None to omit the size comparison.
        :param expected_digest: Optional expected accepted-byte digest checked before remote upload.
        :param metadata: Must be empty because generic rclone writes have no shared native metadata contract.
        :return: Uncommitted local staging session; destination existence is checked later during publication.
        """
        self._require_public_address(object_address)
        if metadata:
            raise StorageUnsupportedOperation(
                "generic rclone writes do not persist native metadata."
            )
        if expected_size is not None and expected_size < 0:
            raise ValueError("expected_size must not be negative.")
        return _RcloneWriteSession(
            self,
            self.check_object_address(object_address),
            mode=WriteMode(mode),
            expected_size=expected_size,
            expected_digest=expected_digest,
        )

    def delete(
        self,
        object_address: RcloneObjectAddress,
        *,
        missing_ok: bool = False,
        if_version: str | None = None,
    ) -> None:
        """
        Delete an owned public key and optionally suppress typed not-found failure.

        Example:
            >>> driver.delete(address, missing_ok=True)  # doctest: +SKIP


        :param object_address: Owned key rejected when its text starts with the reserved staging prefix.
        :param missing_ok: Whether a translated StorageNotFound from deletefile is suppressed.
        :param if_version: Must be None; generic conditional deletion is unsupported.
        :return: None after command success or an allowed missing object; no publication lock is acquired here.
        """
        checked = self.check_object_address(object_address)
        self._require_public_address(checked)
        if if_version is not None:
            raise StorageUnsupportedOperation(
                "generic rclone remotes do not provide conditional deletion."
            )
        try:
            self._run_command(["deletefile", self.object_uri(checked)])
        except StorageNotFound:
            if not missing_ok:
                raise

    def allocate_object_address(
        self,
        *,
        expected_size: int | None = None,
        expected_digest: Digest | None = None,
        name_hint: str | None = None,
    ) -> RcloneObjectAddress:
        """
        Suggest a digest-derived key or a UUID-prefixed filename without checking remote existence
        or reserving space.

        Example:
            >>> address = driver.allocate_object_address(name_hint="book.epub")  # doctest: +SKIP


        :param expected_size: Unused sizing hint; this helper does not enforce a capacity or size bound.
        :param expected_digest: Optional digest forming objects/algorithm/prefix/value deterministically.
        :param name_hint: Optional basename hint for random allocation; ignored when a digest is supplied.
        :return: Owned canonical address suggestion, without collision, capacity, or reservation guarantees.
        """
        _ = expected_size
        if expected_digest is not None:
            return self.join_object_address(
                "objects",
                expected_digest.algorithm,
                expected_digest.value[:2],
                expected_digest.value,
            )
        name = _safe_rclone_name(name_hint)
        return self.join_object_address("objects", f"{uuid4().hex}-{name}")

    def stat(
        self,
        object_address: RcloneObjectAddress,
    ) -> DriverObjectInfo[RcloneObjectAddress]:
        """
        Reject reserved staging access before using the inherited remote metadata inspection.

        Example:
            >>> info = driver.stat(address)  # doctest: +SKIP


        :param object_address: Owned public destination whose metadata is requested.
        :return: DriverObjectInfo from the base stat implementation, with its reported-fact validation and limitations.
        """
        self._require_public_address(object_address)
        return super().stat(object_address)

    def open_read(
        self,
        object_address: RcloneObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        """
        Reject reserved staging access before using the inherited cat-backed read contract.

        Example:
            >>> with driver.open_read(address) as stream:  # doctest: +SKIP
            ...     payload = stream.read()


        :param object_address: Owned public address accepted by the staging-prefix guard.
        :param offset: Nonnegative starting byte offset forwarded to the base reader.
        :param length: Exact expected bounded response count, None for EOF-driven reading, or zero for an empty stream.
        :param if_version: Must be None because the inherited conditional-read contract is unsupported.
        :return: Caller-owned binary stream with the base process/count/cleanup behavior.
        """
        self._require_public_address(object_address)
        return super().open_read(
            object_address,
            offset=offset,
            length=length,
            if_version=if_version,
        )

    def iter_inventory(
        self,
        *,
        prefix: RcloneObjectAddress | None = None,
    ) -> Iterator[DriverInventoryEntry[RcloneObjectAddress]]:
        """
        Validate an optional public prefix and omit reserved staging entries from the inherited
        inventory.

        Example:
            >>> entries = list(driver.iter_inventory())  # doctest: +SKIP


        :param prefix: Optional owned public address used by inherited lexical prefix filtering.
        :return: Lazy iterator of entries outside .liuxin-staging/; hidden entries still consume inherited observation limits.
        """
        if prefix is not None:
            self._require_public_address(prefix)
        for entry in super().iter_inventory(prefix=prefix):
            if not str(entry.object_address).startswith(".liuxin-staging/"):
                yield entry

    def _require_public_address(
        self,
        object_address: RcloneObjectAddress,
    ) -> RcloneObjectAddress:
        """
        Check address ownership and reject keys starting with the reserved staging prefix.

        Example:
            >>> checked = driver._require_public_address(address)  # doctest: +SKIP


        :param object_address: Typed candidate checked for subtype and UUID before its stored text prefix is examined.
        :return: Owned address when its text does not start with .liuxin-staging/; this does not reparse arbitrary typed keys.
        """
        checked = self.check_object_address(object_address)
        if str(checked).startswith(".liuxin-staging/"):
            raise StorageInvalidAddress(
                "the .liuxin-staging namespace is reserved for transactional writes."
            )
        return checked

    def _publish_local_file(
        self,
        local_path: pathlib.Path,
        destination: RcloneObjectAddress,
        *,
        mode: WriteMode,
    ) -> DriverObjectInfo[RcloneObjectAddress]:
        """
        Copy local bytes to a unique remote staging key, recheck existence, and move into place.

        An instance lock covers preflight, upload, second preflight, and publication. CREATE_ONLY
        uses --immutable; REPLACE checks presence without a version-pinned condition. The uploaded
        marker is set only after copyto returns, so a failed copy with remote side effects may leave
        staging. Marked staging receives a best-effort delete on later failure. Final stat occurs
        outside the lock after publication; its failure cannot undo destination visibility.

        Example:
            >>> info = driver._publish_local_file(path, address, mode=WriteMode.CREATE_ONLY)  # doctest: +SKIP


        :param local_path: Closed local file whose current bytes are offered to copyto; this helper does not recompute expectations.
        :param destination: Owned destination inspected by inherited public stat checks.
        :param mode: Retained WriteMode compared by enum identity for create/replace existence policy.
        :return: Final public stat result after moveto; failures may leave remote effects despite local cleanup.
        """
        checked = self.check_object_address(destination)
        staging = super().parse_object_address(
            f".liuxin-staging/{uuid4().hex}.part"
        )
        staging_uri = self.object_uri(staging)
        destination_uri = self.object_uri(checked)
        uploaded = False
        with self._write_lock:
            existing = self.try_stat(checked)
            if mode is WriteMode.CREATE_ONLY and existing is not None:
                raise StorageAlreadyExists(str(checked))
            if mode is WriteMode.REPLACE and existing is None:
                raise StorageNotFound(str(checked))
            try:
                self._run_command(
                    ["copyto", str(local_path), staging_uri, "--immutable"]
                )
                uploaded = True
                current = self.try_stat(checked)
                if mode is WriteMode.CREATE_ONLY and current is not None:
                    raise StorageAlreadyExists(str(checked))
                if mode is WriteMode.REPLACE and current is None:
                    raise StorageNotFound(str(checked))
                publish = ["moveto", staging_uri, destination_uri]
                if mode is WriteMode.CREATE_ONLY:
                    publish.append("--immutable")
                self._run_command(publish)
                uploaded = False
            finally:
                if uploaded:
                    try:
                        self._run_command(["deletefile", staging_uri])
                    except StorageError:
                        pass
        return self.stat(checked)

    def import_from_uri(
        self,
        source_uri: str,
        destination: RcloneObjectAddress,
        *,
        mode: WriteMode,
        expected_size: int,
        expected_digest: Digest,
    ) -> DriverObjectInfo[RcloneObjectAddress]:
        """
        Copy a remote source to staging, compare reported identity, and publish with a final
        identity check.

        Size and digest comparisons use remote stat evidence, not independently read bytes. The
        instance lock and upload marker follow local-file publication; final stat/identity
        comparison occur outside the lock after moveto. A missing or differently named digest is
        unsupported, while size/value mismatch is an integrity failure. No failure after publication
        rolls back the destination.

        Example:
            >>> info = driver.import_from_uri("source:book", address, mode=WriteMode.CREATE_ONLY, expected_size=4, expected_digest=digest)  # doctest: +SKIP


        :param source_uri: Source identifier forwarded directly to copyto without local source parsing or byte retrieval.
        :param destination: Owned destination used for preflight checks and publication.
        :param mode: WriteMode compared by enum identity; this method does not coerce it.
        :param expected_size: Required size compared to staging and final reported object sizes.
        :param expected_digest: Required algorithm/value compared to staging and final reported digests.
        :return: Final DriverObjectInfo after both remote-evidence comparisons pass; this is not independent byte authentication.
        """

        checked = self.check_object_address(destination)
        staging = super().parse_object_address(
            f".liuxin-staging/{uuid4().hex}.part"
        )
        staging_uri = self.object_uri(staging)
        destination_uri = self.object_uri(checked)
        uploaded = False
        with self._write_lock:
            existing = self.try_stat(checked)
            if mode is WriteMode.CREATE_ONLY and existing is not None:
                raise StorageAlreadyExists(str(checked))
            if mode is WriteMode.REPLACE and existing is None:
                raise StorageNotFound(str(checked))
            try:
                self._run_command(
                    ["copyto", source_uri, staging_uri, "--immutable"]
                )
                uploaded = True
                staging_info = RcloneStorageDriver.stat(self, staging)
                _require_rclone_identity(
                    staging_info,
                    expected_size=expected_size,
                    expected_digest=expected_digest,
                )
                current = self.try_stat(checked)
                if mode is WriteMode.CREATE_ONLY and current is not None:
                    raise StorageAlreadyExists(str(checked))
                if mode is WriteMode.REPLACE and current is None:
                    raise StorageNotFound(str(checked))
                publish = ["moveto", staging_uri, destination_uri]
                if mode is WriteMode.CREATE_ONLY:
                    publish.append("--immutable")
                self._run_command(publish)
                uploaded = False
            finally:
                if uploaded:
                    try:
                        self._run_command(["deletefile", staging_uri])
                    except StorageError:
                        pass
        result = self.stat(checked)
        _require_rclone_identity(
            result,
            expected_size=expected_size,
            expected_digest=expected_digest,
        )
        return result

    def _run_command(self, arguments: Sequence[str]) -> Any:
        """
        Execute injected non-streamed arguments, preserving StorageError and translating timeout or
        other failures.

        Example:
            >>> result = driver._run_command(["deletefile", "archive:book"])  # doctest: +SKIP


        :param arguments: Command-name-and-arguments sequence without an executable; the first item labels translated failures.
        :return: Runner-specific result unchanged; a returned nonzero status is not inspected here and must be signaled by the runner.
        """
        try:
            return self._command_runner(arguments)
        except StorageError:
            raise
        except subprocess.TimeoutExpired as error:
            raise StorageTimeout(
                driver_failure_message(
                    "rclone",
                    f"run {arguments[0] if arguments else 'command'}",
                    target=self._fs_root,
                    reason="the command timed out",
                )
            ) from error
        except Exception as error:
            raise _translate_rclone_error(
                str(error),
                target=self._fs_root,
                operation=f"run {arguments[0] if arguments else 'command'}",
            ) from error

    def close(self) -> None:
        """
        Clean the automatically owned staging directory when one was created.

        Example:
            >>> driver.close()  # doctest: +SKIP


        :return: None after owned-directory cleanup; caller-managed directories, remote staging, active streams, and cached status are not managed here.
        """
        if self._temporary_directory is not None:
            self._temporary_directory.cleanup()


def _iter_json_array_process(
    process: _ProcessAPI,
    *,
    target: str,
    max_token_chars: int = DEFAULT_MAX_RCLONE_JSON_TOKEN_CHARS,
) -> Iterator[Any]:
    """
    Incrementally yield array values from process stdout and attempt process cleanup on exit.

    Read 64 KiB chunks, decode UTF-8 incrementally, and bound the full newly buffered text before
    the next parsing pass. Yielded values precede final syntax/exit checks. Stream and wait timeouts
    receive typed errors; stderr is collected only after unsuccessful completion. The process is
    stopped when the guarded parser body exits or its generator is closed; initial stream lookup
    precedes that guard.

    This state machine accepts a trailing comma and stops reading on a closing bracket. It checks
    trailing text already buffered, without draining later stdout or finalizing a pending UTF-8
    suffix on that path. A number split across chunks can be decoded prematurely and cause a later
    syntax failure. These limits distinguish this helper from strict whole-document JSON validation.

    Example:
        >>> values = list(_iter_json_array_process(process, target="archive:"))  # doctest: +SKIP


    :param process: Owned process with stdout/stderr attributes, binary stdout.read, and applicable lifecycle methods.
    :param target: Remote identifier included in translated stream/process diagnostics.
    :param max_token_chars: Maximum buffered decoded character count after reading a chunk; can include several complete elements.
    :return: Lazy iterator of raw JSON values, without entry-shape validation; earlier values may precede a later failure.
    """

    stdout = process.stdout
    stderr = process.stderr
    if stdout is None:
        raise StorageUnavailable("rclone inventory process omitted stdout.")
    decoder = json.JSONDecoder()
    utf8 = codecs.getincrementaldecoder("utf-8")()
    buffer = ""
    position = 0
    started = False
    after_item = False
    finished = False
    exhausted = False
    try:
        while not finished:
            while True:
                while position < len(buffer) and buffer[position].isspace():
                    position += 1
                if not started:
                    if position >= len(buffer):
                        break
                    if buffer[position] != "[":
                        raise StorageUnavailable(
                            "rclone inventory did not return a JSON array."
                        )
                    position += 1
                    started = True
                    continue
                while position < len(buffer) and buffer[position].isspace():
                    position += 1
                if after_item:
                    if position >= len(buffer):
                        break
                    if buffer[position] == ",":
                        position += 1
                        after_item = False
                        continue
                    if buffer[position] == "]":
                        position += 1
                        finished = True
                        break
                    raise StorageUnavailable(
                        "rclone inventory returned malformed JSON."
                    )
                if position < len(buffer) and buffer[position] == "]":
                    position += 1
                    finished = True
                    break
                try:
                    item, end = decoder.raw_decode(buffer, position)
                except json.JSONDecodeError:
                    break
                except (RecursionError, OverflowError) as error:
                    raise StorageUnavailable(
                        "rclone inventory JSON exceeded safe decoder limits."
                    ) from error
                position = end
                after_item = True
                yield item

            if finished:
                break
            try:
                raw = stdout.read(64 * 1024)
            except (TimeoutError, subprocess.TimeoutExpired) as error:
                raise StorageTimeout(
                    driver_failure_message(
                        "rclone",
                        "inventory",
                        target=target,
                        reason="the inventory stream timed out",
                    )
                ) from error
            except Exception as error:
                raise StorageUnavailable(
                    driver_failure_message(
                        "rclone",
                        "inventory",
                        target=target,
                        reason=str(error) or type(error).__name__,
                    )
                ) from error
            if not isinstance(raw, bytes):
                raise StorageUnavailable(
                    "rclone inventory stdout returned non-byte data."
                )
            if raw:
                if position:
                    buffer = buffer[position:]
                    position = 0
                try:
                    buffer += utf8.decode(raw)
                except UnicodeDecodeError as error:
                    raise StorageUnavailable(
                        "rclone inventory returned malformed UTF-8."
                    ) from error
                if len(buffer) > max_token_chars:
                    raise StorageUnavailable(
                        driver_failure_message(
                            "rclone",
                            "inventory",
                            target=target,
                            reason=(
                                "the configured JSON token size limit was exceeded"
                            ),
                        )
                    )
                continue
            if exhausted:
                _require_rclone_process_success(
                    process,
                    target=target,
                    operation="inventory",
                )
                raise StorageUnavailable(
                    "rclone inventory returned truncated JSON."
                )
            exhausted = True
            try:
                buffer += utf8.decode(b"", final=True)
            except UnicodeDecodeError as error:
                raise StorageUnavailable(
                    "rclone inventory returned malformed UTF-8."
                ) from error

        while position < len(buffer) and buffer[position].isspace():
            position += 1
        if position != len(buffer):
            raise StorageUnavailable(
                "rclone inventory returned trailing JSON data."
            )
        _require_rclone_process_success(
            process,
            target=target,
            operation="inventory",
        )
    finally:
        _stop_rclone_process(process)


_RCLONE_SECRET_OPTION = re.compile(
    r"(?:^|,)\s*(?:access_key(?:_id)?|api_key|bearer_token|client_secret|"
    r"credential(?:s)?|password|pass|secret(?:_access_key)?|service_account_"
    r"credentials|token)\s*=",
    re.IGNORECASE,
)


def _reject_inline_rclone_secrets(root: str) -> None:
    """
    Reject recognized secret option assignments only in colon-prefixed connection-string roots.

    Matching is a case-insensitive textual pattern at the start or after a comma, not a complete
    rclone option/credential parser. Named remotes and unmatched text are retained; the helper
    neither rewrites nor redacts input.

    Example:
        >>> _reject_inline_rclone_secrets("archive:")


    :param root: Configured root text checked for the leading colon and selected secret-option names.
    :return: None when no recognized assignment is found; a match raises StorageInvalidAddress.
    """
    if root.startswith(":") and _RCLONE_SECRET_OPTION.search(root[1:]):
        raise StorageInvalidAddress(
            "rclone roots must not embed secret configuration; supply secrets "
            "through an rclone config file or runtime environment."
        )


def _canonical_rclone_key(value: str) -> str:
    """
    Validate a relative slash-separated key while preserving accepted text spelling.

    Reject empty text, malformed Unicode, NUL, backslashes, leading slash, and empty/dot/dot-dot
    components. Other controls, whitespace, and literal percent characters remain allowed. No
    normalization, decoding, or remote lookup occurs.

    Example:
        >>> _canonical_rclone_key("authors/café book.epub")
        'authors/café book.epub'


    :param value: Candidate converted to text before canonical component checks.
    :return: Accepted slash-joined key text, or a typed address failure.
    """
    key = str(value)
    reject_malformed_unicode(key, label="rclone object address")
    if not key or "\x00" in key or "\\" in key or key.startswith("/"):
        raise StorageInvalidAddress("rclone object address must be a relative POSIX path.")
    parts = key.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise StorageInvalidAddress("rclone object address is not canonical.")
    return "/".join(parts)


def _valid_rclone_process(process: object) -> bool:
    """
    Check for callable wait/poll and a non-None stdout with callable read, without invoking them.

    Example:
        >>> _valid_rclone_process(object())
        False


    :param process: Candidate whose attributes are inspected; hostile attribute lookup can itself raise.
    :return: Whether this limited shape check passes; stderr, termination methods, read results, and real liveness are not validated.
    """

    stdout = getattr(process, "stdout", None)
    return (
        callable(getattr(process, "wait", None))
        and callable(getattr(process, "poll", None))
        and stdout is not None
        and callable(getattr(stdout, "read", None))
    )


def _stop_rclone_process(process: _ProcessAPI) -> None:
    """
    Attempt stdout closure, stop a possibly running process, and attempt stderr closure.

    A poll exception is treated as running. Try terminate and a one-second wait; failure of that
    wait triggers kill without a second wait. Ordinary failures from those calls and stream close
    attempts are suppressed. Outer stdout/stderr attribute lookup and BaseException failures can
    still escape this helper.

    Example:
        >>> _stop_rclone_process(process)  # doctest: +SKIP


    :param process: Process-like object whose output streams and lifecycle methods are used for best-effort cleanup.
    :return: None after attempted cleanup, without proving process reaping or successful stream/command completion.
    """

    best_effort_close(getattr(process, "stdout", None))
    try:
        running = process.poll() is None
    except Exception:
        running = True
    if running:
        try:
            process.terminate()
        except Exception:
            pass
        try:
            process.wait(timeout=1)
        except Exception:
            try:
                process.kill()
            except Exception:
                pass
    best_effort_close(getattr(process, "stderr", None))


def _require_rclone_process_success(
    process: _ProcessAPI,
    *,
    target: str,
    operation: str,
) -> None:
    """
    Wait without an explicit timeout and classify diagnostics from a truthy exit status.

    The process wrapper supplies any deadline. Wait timeouts are distinct from other ordinary
    failures. After nonzero exit, read stderr in full, replacing diagnostic-read failures with the
    exception type; process exit codes are not used to choose the storage exception category.

    Example:
        >>> _require_rclone_process_success(process, target="archive:", operation="inventory")  # doctest: +SKIP


    :param process: Process whose completion is awaited and whose stderr may be consumed.
    :param target: Remote context passed to diagnostic formatting.
    :param operation: Operation label attached to timeout, wait, or backend failure messages.
    :return: None for a falsey exit status; unsuccessful completion raises the translated storage exception.
    """

    try:
        return_code = process.wait()
    except (TimeoutError, subprocess.TimeoutExpired) as error:
        raise StorageTimeout(
            driver_failure_message(
                "rclone",
                operation,
                target=target,
                reason="the command timed out while finishing",
            )
        ) from error
    except OSError as error:
        raise StorageUnavailable(
            driver_failure_message(
                "rclone",
                operation,
                target=target,
                reason=str(error) or type(error).__name__,
            )
        ) from error
    except Exception as error:
        raise StorageUnavailable(
            driver_failure_message(
                "rclone",
                operation,
                target=target,
                reason=str(error) or type(error).__name__,
            )
        ) from error
    if not return_code:
        return
    stderr = process.stderr
    try:
        detail = stderr.read() if stderr is not None else b""
    except Exception as error:
        detail = (
            "rclone exited unsuccessfully and its diagnostic stream failed: "
            f"{type(error).__name__}"
        )
    message = (
        detail.decode(errors="replace")
        if isinstance(detail, bytes)
        else str(detail)
    )
    raise _translate_rclone_error(
        message,
        target=target,
        operation=operation,
    )


def _rclone_datetime(value: Any) -> datetime | None:
    """
    Convert stripped optional ISO timestamp text to UTC, treating naive parsed values as UTC.

    Example:
        >>> _rclone_datetime("2020-01-01T00:00:00Z").tzinfo is timezone.utc
        True


    :param value: Optional backend value stringified and stripped; every literal Z is replaced before ISO parsing.
    :return: Aware UTC datetime, or None for absent/blank text or ValueError during ISO parsing; other conversion failures may propagate.
    """
    text = _optional_text(value)
    if text is None:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _rclone_digest(value: Any) -> Digest | None:
    """
    Select the first truthy normalized SHA-256, SHA-1, or MD5 mapping value in that preference
    order.

    Hash names are lowercased with hyphens/underscores removed. Selected values are stringified and
    passed through Digest's nonempty/lowercase normalization; neither that record nor this helper
    validates digest length or hex syntax. Backend bytes are not read or independently hashed.

    Example:
        >>> _rclone_digest({"SHA-256": "AB"})
        Digest(algorithm='sha256', value='ab')


    :param value: Candidate dictionary of reported hashes; non-dictionaries provide no digest evidence.
    :return: Selected Digest or None when no recognized truthy value exists; invalid selected record text may raise ValueError.
    """
    if not isinstance(value, dict):
        return None
    normalized = {
        str(key).lower().replace("-", "").replace("_", ""): str(digest)
        for key, digest in value.items()
        if digest
    }
    for algorithm in ("sha256", "sha1", "md5"):
        digest = normalized.get(algorithm)
        if digest:
            return Digest(algorithm, digest)
    return None


def _rclone_native_metadata(blob: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    """
    Retain non-None Tier, Encrypted, and OrigID fields in that fixed order as text pairs.

    Example:
        >>> _rclone_native_metadata({"Tier": "cold", "Size": 4})
        (('Tier', 'cold'),)


    :param blob: Decoded object dictionary whose selected native fields are inspected.
    :return: Tuple of recognized key/string-value pairs, preserving falsey non-None values and ignoring other fields.
    """
    allowed = ("Tier", "Encrypted", "OrigID")
    return tuple(
        (key, str(blob[key]))
        for key in allowed
        if key in blob and blob[key] is not None
    )


def _require_rclone_identity(
    info: DriverObjectInfo[RcloneObjectAddress],
    *,
    expected_size: int,
    expected_digest: Digest,
) -> None:
    """
    Compare reported object size and digest with the required source identity.

    Size mismatch is an integrity failure. Missing/differently named digest evidence is unsupported;
    matching-algorithm value mismatch is an integrity failure. The comparison examines metadata
    records, not object bytes.

    Example:
        >>> _require_rclone_identity(info, expected_size=4, expected_digest=digest)  # doctest: +SKIP


    :param info: Reported staging or published object metadata to compare.
    :param expected_size: Required byte count compared exactly to info.size.
    :param expected_digest: Required algorithm and normalized value compared to info.digest.
    :return: None when both reported identity components match, otherwise a typed integrity or unsupported-operation error.
    """
    if info.size != expected_size:
        raise StorageIntegrityError(
            "rclone native transfer size does not match its source identity."
        )
    if info.digest is None or info.digest.algorithm != expected_digest.algorithm:
        raise StorageUnsupportedOperation(
            "rclone native transfer cannot verify the required digest algorithm."
        )
    if info.digest.value != expected_digest.value:
        raise StorageIntegrityError(
            "rclone native transfer digest does not match its source identity."
        )


def _optional_text(value: Any) -> str | None:
    """
    Stringify and strip a present value, collapsing an empty result to absence.

    Example:
        >>> _optional_text("  token  ")
        'token'


    :param value: Backend value, or None for immediate absence; conversion does not validate Unicode.
    :return: Stripped nonempty text or None; failures of string conversion are not suppressed.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _opaque_text(value: Any) -> str | None:
    """
    Stringify a present opaque value without trimming whitespace or normalizing its spelling.

    Example:
        >>> _opaque_text("  filename  ")
        '  filename  '


    :param value: Backend value, or None for absence; only an empty string conversion also becomes None.
    :return: Unstripped text or None, without Unicode/path validation.
    """

    if value is None:
        return None
    text = str(value)
    return text or None


def _remote_rclone_text(value: Any, *, label: str) -> str | None:
    """
    Preserve opaque backend text and translate malformed-Unicode rejection into a remote-data
    failure.

    Example:
        >>> _remote_rclone_text(" café.epub ", label="object name")
        ' café.epub '


    :param value: Optional backend value converted to opaque text without trimming.
    :param label: Field description used in the malformed-Unicode diagnostic.
    :return: Unicode-valid nonempty text or None; this helper does not enforce canonical path components.
    """

    text = _opaque_text(value)
    if text is None:
        return None
    try:
        reject_malformed_unicode(text, label=f"rclone {label}")
    except StorageInvalidAddress as error:
        raise StorageUnavailable(
            f"rclone returned malformed Unicode in its {label}."
        ) from error
    return text


def _safe_rclone_name(value: str | None) -> str:
    """
    Select a stripped POSIX basename, substitute a default for unusable names, and replace
    separators.

    Example:
        >>> _safe_rclone_name("incoming/book.epub")
        'book.epub'


    :param value: Optional filename hint; falsey input, empty/dot/parent names, or NUL select payload.bin.
    :return: Suggested component with slashes/backslashes replaced, without general Unicode/control validation or a length bound.
    """
    name = pathlib.PurePosixPath(str(value or "payload.bin")).name.strip()
    if not name or name in {".", ".."} or "\x00" in name:
        return "payload.bin"
    return name.replace("/", "_").replace("\\", "_")


def _translate_rclone_error(
    message: str,
    *,
    target: str,
    operation: str,
) -> Exception:
    """
    Classify ordered diagnostic-text markers and construct a contextual storage exception.

    Check existing-destination markers first, then not-found, authentication, permission, timeout,
    and network-unavailable wording. Recognized categories generally use fixed explanations; network
    and fallback cases retain message text through the shared selective formatter. This is not
    exit-code parsing or exhaustive credential redaction.

    Example:
        >>> type(_translate_rclone_error("not found", target="archive:book", operation="stat")).__name__
        'StorageNotFound'


    :param message: Diagnostic text lowercased for marker matching and selectively retained in formatted reasons.
    :param target: Root/object identifier supplied to shared diagnostic formatting.
    :param operation: Caller operation label included in the resulting exception message.
    :return: Constructed StorageError subtype to raise; this helper itself does not raise the selected exception.
    """
    lowered = message.lower()
    def contextual(reason: str) -> str:
        """
        Format one reason with the enclosing rclone operation and target using the shared diagnostic
        helper.

        Example:
            >>> explanation = contextual("object not found")  # doctest: +SKIP


        :param reason: Fixed category explanation or backend diagnostic selected by the enclosing classifier.
        :return: Contextual failure text after the shared formatter applies its selective redaction and length policy.
        """
        return driver_failure_message(
            "rclone",
            operation,
            target=target,
            reason=reason,
        )

    if any(
        marker in lowered
        for marker in (
            "already exists",
            "immutable file modified",
            "can't modify existing",
        )
    ):
        return StorageAlreadyExists(contextual("the destination already exists"))
    if any(marker in lowered for marker in ("not found", "doesn't exist", "couldn't find", "error 404")):
        return StorageNotFound(contextual("the object or remote was not found"))
    if any(marker in lowered for marker in ("unauthorized", "authentication", "invalid credentials", "error 401")):
        return StorageAuthenticationFailed(contextual("authentication failed"))
    if any(marker in lowered for marker in ("permission denied", "forbidden", "error 403")):
        return StoragePermissionDenied(contextual("permission denied"))
    if "timeout" in lowered or "timed out" in lowered:
        return StorageTimeout(contextual("the command timed out"))
    if any(
        marker in lowered
        for marker in (
            "connection refused",
            "connection reset",
            "network is unreachable",
            "no route to host",
            "service unavailable",
            "temporarily unavailable",
        )
    ):
        return StorageUnavailable(contextual(message))
    return StorageError(contextual(message or "the backend command failed"))


__all__ = [
    "RcloneObjectAddress",
    "RcloneStorageDriver",
    "WritableRcloneStorageDriver",
]
