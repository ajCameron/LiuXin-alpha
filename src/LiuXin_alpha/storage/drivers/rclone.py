"""Read-only storage driver backed by rclone commands."""

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
    StorageDriverAPI,
    StorageError,
    StorageIntegrityError,
    StorageInvalidAddress,
    StorageNotFound,
    StoragePermissionDenied,
    StoragePreconditionFailed,
    StorageTimeout,
    StorageUnavailable,
    StorageUnsupportedOperation,
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
    """Canonical relative POSIX path within one rclone filesystem root."""


class _ProcessAPI(Protocol):
    stdout: Any
    stderr: Any

    def wait(self, timeout: float | None = None) -> int: ...
    def poll(self) -> int | None: ...
    def terminate(self) -> None: ...
    def kill(self) -> None: ...


class _RcloneProcessReader(io.RawIOBase):
    """Own an ``rclone cat`` process and validate its eventual exit status."""

    def __init__(
        self,
        process: _ProcessAPI,
        target: str,
        remaining: int | None = None,
    ) -> None:
        self._process = process
        self._stdout = process.stdout
        self._stderr = process.stderr
        self._target = target
        self._remaining = remaining
        self._checked_eof = False

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: bytearray | memoryview) -> int:
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
        if self.closed:
            return
        try:
            _stop_rclone_process(self._process)
        finally:
            super().close()


class _RcloneWriteSession:
    """Stage bytes locally and publish them through rclone only on commit."""

    def __init__(
        self,
        driver: WritableRcloneStorageDriver,
        address: RcloneObjectAddress,
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
        if self._finished:
            raise StorageError("rclone write session is finished.")
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if not self._committed:
            self.abort()


class RcloneStorageDriver(StorageDriverAPI[RcloneObjectAddress]):
    """Expose one rclone filesystem as a Store-neutral read-only driver."""

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
        return self._checker

    @property
    def root_uri(self) -> str:
        return self._fs_root

    @property
    def capabilities(self) -> DriverCapabilities:
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

    def startup(self) -> DriverStatus:
        return self.probe()

    def probe(self) -> DriverStatus:
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
        return self._last_status

    def close(self) -> None:
        return None

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[RcloneObjectAddress],
    ) -> RcloneObjectAddress:
        if isinstance(identifier, DriverObjectAddress):
            return self.check_object_address(identifier)
        key = _canonical_rclone_key(str(identifier))
        return RcloneObjectAddress(key, self._checker.address_space_uuid)

    def join_object_address(self, *tokens: str) -> RcloneObjectAddress:
        if not tokens:
            raise StorageInvalidAddress("at least one rclone path token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def object_address_from_uri(self, uri: str) -> RcloneObjectAddress:
        text = str(uri)
        prefix = self._fs_root if self._fs_root.endswith(":") else self._fs_root.rstrip("/") + "/"
        if not text.startswith(prefix):
            raise StorageInvalidAddress("rclone object identifier belongs to another filesystem root.")
        return self.parse_object_address(text[len(prefix) :])

    def object_uri(self, object_address: RcloneObjectAddress) -> str:
        checked = self.check_object_address(object_address)
        if self._fs_root.endswith(":"):
            return self._fs_root + str(checked)
        return self._fs_root.rstrip("/") + "/" + str(checked)

    def stat(
        self,
        object_address: RcloneObjectAddress,
    ) -> DriverObjectInfo[RcloneObjectAddress]:
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
            # Compatibility for injected runners that do not expose a process
            # form. Healthy production scans always take the streaming path.
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
                    # Some injected/legacy runners expose only the JSON call.
                    # Fall back solely when process startup failed before any
                    # output; malformed successful output must remain fatal.
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
    """Transactional writable rclone driver with conservative capabilities.

    Bytes are first staged in a private local file. Commit uploads to a unique
    remote staging key and then asks rclone to move that complete object into
    place. Some rclone remotes implement that move as copy-and-delete, so the
    driver deliberately advertises ``atomic_publish=False``.
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
        return self._local_staging_directory

    @property
    def capabilities(self) -> DriverCapabilities:
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

    def probe(self) -> DriverStatus:
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
        if prefix is not None:
            self._require_public_address(prefix)
        for entry in super().iter_inventory(prefix=prefix):
            if not str(entry.object_address).startswith(".liuxin-staging/"):
                yield entry

    def _require_public_address(
        self,
        object_address: RcloneObjectAddress,
    ) -> RcloneObjectAddress:
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
        """Copy through rclone's remote-to-remote path and verify staging."""

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
        if self._temporary_directory is not None:
            self._temporary_directory.cleanup()


def _iter_json_array_process(
    process: _ProcessAPI,
    *,
    target: str,
    max_token_chars: int = DEFAULT_MAX_RCLONE_JSON_TOKEN_CHARS,
) -> Iterator[Any]:
    """Incrementally decode one JSON array while owning an rclone process."""

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
    if root.startswith(":") and _RCLONE_SECRET_OPTION.search(root[1:]):
        raise StorageInvalidAddress(
            "rclone roots must not embed secret configuration; supply secrets "
            "through an rclone config file or runtime environment."
        )


def _canonical_rclone_key(value: str) -> str:
    key = str(value)
    reject_malformed_unicode(key, label="rclone object address")
    if not key or "\x00" in key or "\\" in key or key.startswith("/"):
        raise StorageInvalidAddress("rclone object address must be a relative POSIX path.")
    parts = key.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise StorageInvalidAddress("rclone object address is not canonical.")
    return "/".join(parts)


def _valid_rclone_process(process: object) -> bool:
    """Return whether a spawned process exposes the stream contract we use."""

    stdout = getattr(process, "stdout", None)
    return (
        callable(getattr(process, "wait", None))
        and callable(getattr(process, "poll", None))
        and stdout is not None
        and callable(getattr(stdout, "read", None))
    )


def _stop_rclone_process(process: _ProcessAPI) -> None:
    """Best-effort cleanup that cannot replace a transfer's real outcome."""

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
    """Wait for a streamed command and translate timeout/exit failures."""

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
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _opaque_text(value: Any) -> str | None:
    """Return backend-supplied opaque text without changing its identity."""

    if value is None:
        return None
    text = str(value)
    return text or None


def _remote_rclone_text(value: Any, *, label: str) -> str | None:
    """Validate text emitted by rclone before exposing it to callers."""

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
    lowered = message.lower()
    def contextual(reason: str) -> str:
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
