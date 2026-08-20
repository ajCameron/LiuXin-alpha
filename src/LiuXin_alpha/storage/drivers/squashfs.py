"""Read-only SquashFS archive storage driver."""

from __future__ import annotations

import dataclasses
import io
import os
import pathlib
import re
import shutil
import subprocess
import threading

from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any, BinaryIO
from uuid import UUID

from LiuXin_alpha.storage.api import (
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
    StorageDriverAPI,
    StorageInvalidAddress,
    StorageNotFound,
    StorageTimeout,
    StorageUnavailable,
    StorageUnsupportedOperation,
)
from LiuXin_alpha.storage.drivers._errors import (
    driver_failure_message,
    translate_os_error,
)


@dataclasses.dataclass(slots=True, frozen=True)
class SquashfsObjectAddress(DriverObjectAddress):
    """Canonical relative POSIX path inside one SquashFS archive."""


@dataclasses.dataclass(slots=True, frozen=True)
class _SquashfsEntry:
    size: int
    modified_at: datetime | None


class _SquashfsProcessReader(io.RawIOBase):
    """Read an exact range while owning one ``unsquashfs -cat`` process."""

    def __init__(
        self,
        process: Any,
        *,
        archive_path: pathlib.Path,
        internal_path: str,
        offset: int,
        length: int | None,
        timeout_s: float,
    ) -> None:
        self._process = process
        self._stdout = process.stdout
        self._stderr = process.stderr
        self._archive_path = archive_path
        self._internal_path = internal_path
        self._skip = offset
        self._remaining = length
        self._timeout_s = timeout_s
        self._checked_eof = False

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: bytearray | memoryview) -> int:
        if self._remaining == 0:
            return 0
        while self._skip:
            discarded = self._stdout.read(min(1024 * 1024, self._skip))
            if not discarded:
                self._check_eof()
                return 0
            self._skip -= len(discarded)
        count = len(buffer)
        if self._remaining is not None:
            count = min(count, self._remaining)
        data = self._stdout.read(count)
        if not isinstance(data, bytes):
            raise TypeError("unsquashfs stdout must be a binary stream.")
        if not data:
            self._check_eof()
            return 0
        buffer[: len(data)] = data
        if self._remaining is not None:
            self._remaining -= len(data)
        return len(data)

    def _check_eof(self) -> None:
        if self._checked_eof:
            return
        self._checked_eof = True
        try:
            return_code = self._process.wait(timeout=self._timeout_s)
        except subprocess.TimeoutExpired as error:
            self._process.kill()
            raise StorageTimeout(
                driver_failure_message(
                    "SquashFS",
                    "read object",
                    target=f"{self._archive_path}::{self._internal_path}",
                    reason="the unsquashfs command timed out",
                )
            ) from error
        if return_code:
            stderr = self._stderr.read() if self._stderr is not None else b""
            detail = stderr.decode("utf-8", "replace") if isinstance(stderr, bytes) else str(stderr)
            raise StorageUnavailable(
                driver_failure_message(
                    "SquashFS",
                    "read object",
                    target=f"{self._archive_path}::{self._internal_path}",
                    reason=detail.strip() or f"unsquashfs exited with status {return_code}",
                )
            )

    def close(self) -> None:
        if self.closed:
            return
        try:
            if self._stdout is not None:
                self._stdout.close()
            if self._process.poll() is None:
                self._process.terminate()
                try:
                    self._process.wait(timeout=1)
                except Exception:
                    self._process.kill()
            if self._stderr is not None:
                self._stderr.close()
        finally:
            super().close()


class SquashfsStorageDriver(StorageDriverAPI[SquashfsObjectAddress]):
    """Read and completely enumerate one immutable SquashFS image."""

    _line_re = re.compile(
        r"^(?P<mode>\S+)\s+\S+\s+(?P<size>\d+)\s+"
        r"(?P<date>\d{4}-\d{2}-\d{2})\s+(?P<time>\d{2}:\d{2})\s+"
        r"(?P<path>.+)$"
    )
    _root_prefix = "squashfs-root/"

    def __init__(
        self,
        archive_path: str | pathlib.Path,
        *,
        address_space_uuid: UUID,
        unsquashfs_exe: str = "unsquashfs",
        timeout_s: float = 60.0,
    ) -> None:
        self._archive_path = pathlib.Path(archive_path).expanduser().resolve(strict=False)
        if not self._archive_path.is_file():
            raise StorageNotFound(
                driver_failure_message(
                    "SquashFS",
                    "configure",
                    target=self._archive_path,
                    reason="the archive does not exist or is not a regular file",
                )
            )
        if timeout_s <= 0:
            raise ValueError("timeout_s must be positive.")
        self._unsquashfs_exe = str(unsquashfs_exe)
        self._timeout_s = float(timeout_s)
        self._checker = ScopedDriverObjectAddressChecker(
            SquashfsObjectAddress,
            address_space_uuid,
        )
        self._index: dict[str, _SquashfsEntry] = {}
        self._indexed_mtime_ns: int | None = None
        self._index_lock = threading.RLock()
        self._last_status = DriverStatus(
            available=False,
            writable=False,
            message="SquashFS driver has not been started.",
        )

    @property
    def archive_path(self) -> pathlib.Path:
        return self._archive_path

    @property
    def object_address_checker(
        self,
    ) -> ScopedDriverObjectAddressChecker[SquashfsObjectAddress]:
        return self._checker

    @property
    def root_uri(self) -> str:
        return self._archive_path.as_uri()

    @property
    def capabilities(self) -> DriverCapabilities:
        return DriverCapabilities(
            range_reads=True,
            enumeration=EnumerationCompleteness.COMPLETE,
            hierarchical_object_addresses=True,
            prefix_enumeration=True,
            concurrency=DriverConcurrencyCapabilities(
                thread_safe=True,
                concurrent_reads=True,
                recommended_parallel_reads=2,
            ),
        )

    def startup(self) -> DriverStatus:
        return self.probe()

    def probe(self) -> DriverStatus:
        try:
            index = self._get_index(force=True)
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
            object_count=len(index),
            checked_at=datetime.now(timezone.utc),
            message="SquashFS archive is available (read-only).",
            details=(("archive", str(self._archive_path)),),
        )
        return self._last_status

    def status(self) -> DriverStatus:
        return self._last_status

    def close(self) -> None:
        return None

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[SquashfsObjectAddress],
    ) -> SquashfsObjectAddress:
        if isinstance(identifier, DriverObjectAddress):
            return self.check_object_address(identifier)
        key = _canonical_squashfs_key(str(identifier))
        return SquashfsObjectAddress(key, self._checker.address_space_uuid)

    def join_object_address(self, *tokens: str) -> SquashfsObjectAddress:
        if not tokens:
            raise StorageInvalidAddress("at least one archive path token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def stat(
        self,
        object_address: SquashfsObjectAddress,
    ) -> DriverObjectInfo[SquashfsObjectAddress]:
        checked = self.check_object_address(object_address)
        entry = self._get_index().get(str(checked))
        if entry is None:
            raise StorageNotFound(
                driver_failure_message(
                    "SquashFS",
                    "stat object",
                    target=f"{self._archive_path}::{str(checked)}",
                    reason="the object is absent from the archive index",
                )
            )
        return DriverObjectInfo(
            object_address=checked,
            size=entry.size,
            modified_at=entry.modified_at,
            hints=DriverObjectHints(
                suggested_filename=pathlib.PurePosixPath(str(checked)).name
            ),
        )

    def open_read(
        self,
        object_address: SquashfsObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        checked = self.check_object_address(object_address)
        if if_version is not None:
            raise StorageUnsupportedOperation(
                "SquashFS command reads cannot atomically pin an archive version."
            )
        if offset < 0 or (length is not None and length < 0):
            raise StorageInvalidAddress("SquashFS read ranges must not be negative.")
        if str(checked) not in self._get_index():
            raise StorageNotFound(
                driver_failure_message(
                    "SquashFS",
                    "open read",
                    target=f"{self._archive_path}::{str(checked)}",
                    reason="the object is absent from the archive index",
                )
            )
        if length == 0:
            return io.BytesIO()
        executable = shutil.which(self._unsquashfs_exe) or self._unsquashfs_exe
        try:
            process = subprocess.Popen(
                [
                    executable,
                    "-no-wildcards",
                    "-cat",
                    str(self._archive_path),
                    str(checked),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        except OSError as error:
            raise translate_os_error(
                error,
                backend="SquashFS",
                operation="start object read",
                target=f"{self._archive_path}::{str(checked)}",
            ) from error
        if process.stdout is None:
            process.kill()
            raise StorageUnavailable(
                driver_failure_message(
                    "SquashFS",
                    "start object read",
                    target=f"{self._archive_path}::{str(checked)}",
                    reason="unsquashfs did not provide a stdout pipe",
                )
            )
        return io.BufferedReader(
            _SquashfsProcessReader(
                process,
                archive_path=self._archive_path,
                internal_path=str(checked),
                offset=offset,
                length=length,
                timeout_s=self._timeout_s,
            )
        )

    def iter_inventory(
        self,
        *,
        prefix: SquashfsObjectAddress | None = None,
    ) -> Iterator[DriverInventoryEntry[SquashfsObjectAddress]]:
        prefix_key = None if prefix is None else str(self.check_object_address(prefix))
        for key, entry in sorted(self._get_index().items()):
            if prefix_key is not None and not key.startswith(prefix_key):
                continue
            address = self.parse_object_address(key)
            yield DriverInventoryEntry(
                object_address=address,
                size=entry.size,
                modified_at=entry.modified_at,
                hints=DriverObjectHints(
                    suggested_filename=pathlib.PurePosixPath(key).name
                ),
            )

    def _get_index(self, *, force: bool = False) -> dict[str, _SquashfsEntry]:
        with self._index_lock:
            try:
                mtime_ns = self._archive_path.stat().st_mtime_ns
            except OSError as error:
                raise translate_os_error(
                    error,
                    backend="SquashFS",
                    operation="stat archive",
                    target=self._archive_path,
                ) from error
            if force or self._indexed_mtime_ns != mtime_ns:
                self._index = self._build_index()
                self._indexed_mtime_ns = mtime_ns
            return dict(self._index)

    def _build_index(self) -> dict[str, _SquashfsEntry]:
        process = self._run_unsquashfs(
            ["-llc", str(self._archive_path)]
        )
        # unsquashfs reports archive names as raw filesystem bytes. Preserve
        # undecodable names using the same surrogateescape representation as
        # os.walk rather than irreversibly replacing them with U+FFFD.
        output = os.fsdecode(process.stdout)
        index: dict[str, _SquashfsEntry] = {}
        for raw_line in output.splitlines():
            match = self._line_re.match(raw_line)
            if match is None or not match.group("mode").startswith("-"):
                continue
            path = match.group("path")
            if not path.startswith(self._root_prefix):
                continue
            relative = path[len(self._root_prefix) :].lstrip("/")
            if not relative:
                continue
            try:
                canonical = str(self.parse_object_address(relative))
            except StorageInvalidAddress:
                continue
            index[canonical] = _SquashfsEntry(
                size=int(match.group("size")),
                modified_at=_listing_datetime(
                    match.group("date"),
                    match.group("time"),
                ),
            )
        return index

    def _run_unsquashfs(
        self,
        arguments: list[str],
    ) -> subprocess.CompletedProcess[bytes]:
        executable = shutil.which(self._unsquashfs_exe) or self._unsquashfs_exe
        try:
            process = subprocess.run(
                [executable, *arguments],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=self._timeout_s,
                check=False,
            )
        except subprocess.TimeoutExpired as error:
            raise StorageTimeout(
                driver_failure_message(
                    "SquashFS",
                    "build inventory",
                    target=self._archive_path,
                    reason="the unsquashfs command timed out",
                )
            ) from error
        except OSError as error:
            raise translate_os_error(
                error,
                backend="SquashFS",
                operation="build inventory",
                target=self._archive_path,
            ) from error
        if process.returncode:
            detail = process.stderr.decode("utf-8", "replace").strip()
            raise StorageUnavailable(
                driver_failure_message(
                    "SquashFS",
                    "build inventory",
                    target=self._archive_path,
                    reason=detail or f"unsquashfs exited with status {process.returncode}",
                )
            )
        return process


def _canonical_squashfs_key(value: str) -> str:
    key = str(value)
    if not key or "\x00" in key or "\\" in key or key.startswith("/"):
        raise StorageInvalidAddress(
            "SquashFS object address must be a relative POSIX path."
        )
    parts = key.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise StorageInvalidAddress("SquashFS object address is not canonical.")
    return "/".join(parts)


def _listing_datetime(date: str, clock: str) -> datetime | None:
    try:
        return datetime.strptime(
            f"{date} {clock}",
            "%Y-%m-%d %H:%M",
        ).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


__all__ = ["SquashfsObjectAddress", "SquashfsStorageDriver"]
