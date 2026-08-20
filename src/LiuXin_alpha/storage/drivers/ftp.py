"""Read-only FTP/FTPS storage driver."""

from __future__ import annotations

import dataclasses
import ftplib
import io
import mimetypes
import posixpath
import socket
import tempfile

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, BinaryIO
from urllib.parse import quote, unquote, urlsplit, urlunsplit
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
    StorageAuthenticationFailed,
    StorageDriverAPI,
    StorageError,
    StorageInvalidAddress,
    StorageNotFound,
    StoragePermissionDenied,
    StorageTimeout,
    StorageUnavailable,
    StorageUnsupportedOperation,
)
from LiuXin_alpha.storage.drivers._errors import (
    driver_failure_message,
    translate_os_error,
)


@dataclasses.dataclass(slots=True)
class FtpDriverOptions:
    """Connection options for one FTP-family endpoint."""

    timeout_s: float | None = 30.0
    passive: bool = True
    secure_data_channel: bool = True
    encoding: str = "utf-8"
    client_factory: Callable[[], Any] | None = None
    spool_limit_bytes: int = 8 * 1024 * 1024

    def __post_init__(self) -> None:
        if self.spool_limit_bytes < 0:
            raise ValueError("spool_limit_bytes must not be negative.")


@dataclasses.dataclass(slots=True, frozen=True)
class FtpObjectAddress(DriverObjectAddress):
    """Canonical POSIX relative path beneath one configured FTP root."""


class FtpStorageDriver(StorageDriverAPI[FtpObjectAddress]):
    """Enumerate, stat, and retrieve files from one FTP/FTPS root."""

    def __init__(
        self,
        url: str,
        *,
        address_space_uuid: UUID,
        options: FtpDriverOptions | None = None,
    ) -> None:
        self.options = options or FtpDriverOptions()
        parsed = urlsplit(str(url))
        if parsed.scheme.lower() not in {"ftp", "ftps"}:
            raise StorageInvalidAddress("FTP driver requires an ftp(s) URL.")
        if not parsed.hostname:
            raise StorageInvalidAddress("FTP URL must include a hostname.")
        if parsed.query or parsed.fragment:
            raise StorageInvalidAddress("FTP root URL must not contain query or fragment data.")

        self._scheme = parsed.scheme.lower()
        self._host = parsed.hostname
        self._port = parsed.port or (990 if self._scheme == "ftps" else 21)
        self._username = unquote(parsed.username) if parsed.username else "anonymous"
        self._password = unquote(parsed.password) if parsed.password else "anonymous@"
        root = posixpath.normpath(unquote(parsed.path or "/"))
        self._ftp_root_path = "/" if root in {"", "."} else "/" + root.strip("/")
        rendered_path = quote(self._ftp_root_path, safe="/-._~")
        if not rendered_path.endswith("/"):
            rendered_path += "/"
        host = self._host
        default_port = 990 if self._scheme == "ftps" else 21
        authority = host if self._port == default_port else f"{host}:{self._port}"
        self._root_uri = urlunsplit((self._scheme, authority, rendered_path, "", ""))
        self._checker = ScopedDriverObjectAddressChecker(
            FtpObjectAddress,
            address_space_uuid,
        )
        self._last_status = DriverStatus(
            available=False,
            writable=False,
            message="FTP driver has not been started.",
        )

    @property
    def object_address_checker(
        self,
    ) -> ScopedDriverObjectAddressChecker[FtpObjectAddress]:
        return self._checker

    @property
    def root_uri(self) -> str:
        """Return a credential-free endpoint URI."""

        return self._root_uri

    @property
    def ftp_root_path(self) -> str:
        return self._ftp_root_path

    @property
    def capabilities(self) -> DriverCapabilities:
        return DriverCapabilities(
            range_reads=True,
            enumeration=EnumerationCompleteness.COMPLETE,
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
            with self._connected_client(operation="probe") as client:
                noop = getattr(client, "voidcmd", None)
                if callable(noop):
                    noop("NOOP")
                # Consume the iterator so access errors cannot masquerade as
                # a successful lazy listing.
                list(client.mlsd("."))
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
            message="FTP endpoint is available (read-only).",
            details=(
                ("scheme", self._scheme),
                ("host", self._host),
                ("port", str(self._port)),
                ("root_path", self._ftp_root_path),
            ),
        )
        return self._last_status

    def status(self) -> DriverStatus:
        return self._last_status

    def close(self) -> None:
        return None

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[FtpObjectAddress],
    ) -> FtpObjectAddress:
        if isinstance(identifier, DriverObjectAddress):
            return self.check_object_address(identifier)
        key = _canonical_ftp_key(str(identifier))
        return FtpObjectAddress(key, self._checker.address_space_uuid)

    def join_object_address(self, *tokens: str) -> FtpObjectAddress:
        if not tokens:
            raise StorageInvalidAddress("at least one FTP path token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def object_address_from_uri(self, uri: str) -> FtpObjectAddress:
        parsed = urlsplit(str(uri))
        if parsed.scheme.lower() != self._scheme or parsed.hostname != self._host:
            raise StorageInvalidAddress("FTP object URI belongs to another endpoint.")
        candidate_port = parsed.port or (990 if self._scheme == "ftps" else 21)
        if candidate_port != self._port:
            raise StorageInvalidAddress("FTP object URI uses another endpoint port.")
        if parsed.query or parsed.fragment:
            raise StorageInvalidAddress("FTP object URIs must not contain query or fragment data.")
        path = posixpath.normpath(unquote(parsed.path or "/"))
        root_prefix = self._ftp_root_path.rstrip("/") + "/"
        if not path.startswith(root_prefix):
            raise StorageInvalidAddress("FTP object URI lies outside the configured root.")
        return self.parse_object_address(path[len(root_prefix) :])

    def object_uri(self, object_address: FtpObjectAddress) -> str:
        checked = self.check_object_address(object_address)
        encoded = "/".join(quote(part, safe="-._~") for part in str(checked).split("/"))
        return self._root_uri + encoded

    def stat(
        self,
        object_address: FtpObjectAddress,
    ) -> DriverObjectInfo[FtpObjectAddress]:
        checked = self.check_object_address(object_address)
        with self._connected_client(
            operation="stat",
            target=self.object_uri(checked),
            missing_as_not_found=True,
        ) as client:
            facts = self._entry_for(client, str(checked))
            if facts is None:
                raise StorageNotFound(
                    driver_failure_message(
                        "FTP",
                        "stat",
                        target=self.object_uri(checked),
                        reason="the object does not exist",
                    )
                )
            if facts.get("type") == "dir":
                raise StorageInvalidAddress("FTP Store Locations identify files, not directories.")
            size = _optional_int(facts.get("size"))
            if size is None:
                try:
                    size = _optional_int(client.size(str(checked)))
                except Exception:
                    size = None
            if size is None:
                raise StorageUnsupportedOperation(
                    "FTP endpoint did not provide an authoritative object size."
                )
            return DriverObjectInfo(
                object_address=checked,
                size=size,
                modified_at=_ftp_modified_at(facts.get("modify")),
                version=facts.get("unique") or None,
                hints=DriverObjectHints(
                    suggested_filename=posixpath.basename(str(checked)),
                    media_type=mimetypes.guess_type(str(checked))[0],
                    metadata=tuple(
                        sorted(
                            (str(key), str(value))
                            for key, value in facts.items()
                            if key not in {"size", "modify", "unique", "type"}
                        )
                    ),
                ),
            )

    def open_read(
        self,
        object_address: FtpObjectAddress,
        *,
        offset: int = 0,
        length: int | None = None,
        if_version: str | None = None,
    ) -> BinaryIO:
        checked = self.check_object_address(object_address)
        if if_version is not None:
            raise StorageUnsupportedOperation(
                "FTP does not provide an atomic conditional-read primitive."
            )
        if offset < 0 or (length is not None and length < 0):
            raise StorageInvalidAddress("FTP read ranges must not be negative.")
        try:
            output = tempfile.SpooledTemporaryFile(
                max_size=self.options.spool_limit_bytes,
                mode="w+b",
            )
        except OSError as error:
            raise translate_os_error(
                error,
                backend="FTP local staging",
                operation="prepare retrieval",
                target=self.object_uri(checked),
            ) from error
        if length == 0:
            return output
        remaining = length

        def _receive(chunk: bytes) -> None:
            nonlocal remaining
            try:
                if remaining is None:
                    output.write(chunk)
                    return
                if remaining > 0:
                    accepted = chunk[:remaining]
                    output.write(accepted)
                    remaining -= len(accepted)
            except OSError as error:
                raise translate_os_error(
                    error,
                    backend="FTP local staging",
                    operation="retrieve",
                    target=self.object_uri(checked),
                ) from error

        try:
            with self._connected_client(
                operation="retrieve",
                target=self.object_uri(checked),
                missing_as_not_found=True,
            ) as client:
                command = f"RETR {str(checked)}"
                try:
                    client.retrbinary(command, _receive, rest=offset or None)
                except TypeError:
                    # Some small or legacy clients do not accept REST. Fall
                    # back to streaming and discarding the prefix exactly.
                    skip = offset

                    def _receive_without_rest(chunk: bytes) -> None:
                        nonlocal skip
                        if skip:
                            discarded = min(skip, len(chunk))
                            skip -= discarded
                            chunk = chunk[discarded:]
                        if chunk:
                            _receive(chunk)

                    client.retrbinary(command, _receive_without_rest)
        except BaseException:
            try:
                output.close()
            except OSError:
                pass
            raise
        try:
            output.seek(0)
        except OSError as error:
            output.close()
            raise translate_os_error(
                error,
                backend="FTP local staging",
                operation="rewind retrieval",
                target=self.object_uri(checked),
            ) from error
        return output

    def iter_inventory(
        self,
        *,
        prefix: FtpObjectAddress | None = None,
    ) -> Iterator[DriverInventoryEntry[FtpObjectAddress]]:
        prefix_key = None if prefix is None else str(self.check_object_address(prefix))
        with self._connected_client(
            operation="inventory",
            missing_as_not_found=True,
        ) as client:
            for path, facts in self._walk_entries(client):
                if facts.get("type") == "dir":
                    continue
                if prefix_key is not None and not path.startswith(prefix_key):
                    continue
                address = self.parse_object_address(path)
                yield DriverInventoryEntry(
                    object_address=address,
                    size=_optional_int(facts.get("size")),
                    modified_at=_ftp_modified_at(facts.get("modify")),
                    version=facts.get("unique") or None,
                    hints=DriverObjectHints(
                        suggested_filename=posixpath.basename(path),
                        media_type=mimetypes.guess_type(path)[0],
                    ),
                )

    @contextmanager
    def _connected_client(
        self,
        *,
        operation: str,
        target: str | None = None,
        missing_as_not_found: bool = False,
    ):
        client = None
        failure_target = self._root_uri if target is None else target
        try:
            client = (
                self.options.client_factory()
                if self.options.client_factory is not None
                else self._default_client_factory()
            )
            connect = getattr(client, "connect", None)
            if callable(connect):
                connect(self._host, self._port, timeout=self.options.timeout_s)
            login = getattr(client, "login", None)
            if callable(login):
                login(self._username, self._password)
            set_pasv = getattr(client, "set_pasv", None)
            if callable(set_pasv):
                set_pasv(bool(self.options.passive))
            if self._scheme == "ftps" and self.options.secure_data_channel:
                prot_p = getattr(client, "prot_p", None)
                if callable(prot_p):
                    prot_p()
            if self._ftp_root_path != "/":
                client.cwd(self._ftp_root_path)
            yield client
        except StorageError:
            raise
        except ftplib.error_perm as error:
            raise _translated_ftp_permission_error(
                error,
                operation=operation,
                target=failure_target,
                missing_as_not_found=missing_as_not_found,
            ) from error
        except (TimeoutError, socket.timeout) as error:
            raise StorageTimeout(
                driver_failure_message(
                    "FTP",
                    operation,
                    target=failure_target,
                    reason="the operation timed out",
                )
            ) from error
        except (ftplib.Error, EOFError, OSError) as error:
            raise StorageUnavailable(
                driver_failure_message(
                    "FTP",
                    operation,
                    target=failure_target,
                    reason=str(error) or type(error).__name__,
                )
            ) from error
        except Exception as error:
            raise StorageError(
                driver_failure_message(
                    "FTP",
                    operation,
                    target=failure_target,
                    reason=str(error) or type(error).__name__,
                )
            ) from error
        finally:
            if client is not None:
                for closer_name in ("quit", "close"):
                    closer = getattr(client, closer_name, None)
                    if callable(closer):
                        try:
                            closer()
                        except Exception:
                            pass
                        break

    def _default_client_factory(self) -> Any:
        if self._scheme == "ftps":
            return ftplib.FTP_TLS(
                timeout=self.options.timeout_s,
                encoding=self.options.encoding,
            )
        return ftplib.FTP(
            timeout=self.options.timeout_s,
            encoding=self.options.encoding,
        )

    def _walk_entries(
        self,
        client: Any,
        relative_directory: str = "",
    ) -> Iterator[tuple[str, dict[str, str]]]:
        for name, facts in self._list_dir(client, relative_directory):
            path = posixpath.join(relative_directory, name) if relative_directory else name
            normalized = dict(facts)
            entry_type = str(normalized.get("type") or "").lower()
            if entry_type in {"dir", "cdir", "pdir"}:
                normalized["type"] = "dir"
                yield path, normalized
                yield from self._walk_entries(client, path)
            else:
                normalized["type"] = "file"
                yield path, normalized

    def _list_dir(self, client: Any, relative_directory: str) -> list[tuple[str, dict[str, str]]]:
        target = relative_directory or "."
        try:
            return [
                (str(name), {str(key): str(value) for key, value in dict(facts).items()})
                for name, facts in client.mlsd(target)
                if str(name) not in {"", ".", ".."}
            ]
        except (AttributeError, NotImplementedError, ftplib.error_perm):
            names = list(client.nlst(target))
            entries: list[tuple[str, dict[str, str]]] = []
            for raw_name in names:
                name = str(raw_name).rstrip("/").rsplit("/", 1)[-1]
                if not name or name in {".", ".."}:
                    continue
                candidate = posixpath.join(relative_directory, name) if relative_directory else name
                current = client.pwd() if hasattr(client, "pwd") else None
                is_directory = False
                try:
                    client.cwd(candidate)
                    is_directory = True
                except Exception:
                    pass
                finally:
                    if is_directory and current is not None:
                        client.cwd(current)
                facts = {"type": "dir" if is_directory else "file"}
                if not is_directory:
                    try:
                        size = client.size(candidate)
                    except Exception:
                        size = None
                    if size is not None:
                        facts["size"] = str(size)
                entries.append((name, facts))
            return entries

    def _entry_for(self, client: Any, path: str) -> dict[str, str] | None:
        parent = posixpath.dirname(path)
        basename = posixpath.basename(path)
        for name, facts in self._list_dir(client, parent):
            if name != basename:
                continue
            normalized = dict(facts)
            entry_type = str(normalized.get("type") or "").lower()
            normalized["type"] = (
                "dir" if entry_type in {"dir", "cdir", "pdir"} else "file"
            )
            return normalized
        return None


def _canonical_ftp_key(value: str) -> str:
    key = str(value)
    if not key or "\x00" in key:
        raise StorageInvalidAddress("FTP object address must not be empty or contain NUL.")
    if any(ord(character) < 32 or ord(character) == 127 for character in key):
        raise StorageInvalidAddress("FTP object addresses must not contain controls.")
    if key.startswith("/") or "\\" in key:
        raise StorageInvalidAddress("FTP object addresses must be relative POSIX paths.")
    parts = key.split("/")
    if any(part in {"", ".", ".."} for part in parts):
        raise StorageInvalidAddress("FTP object address is not canonical.")
    return "/".join(parts)


def _optional_int(value: Any) -> int | None:
    if value in {None, ""}:
        return None
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return result if result >= 0 else None


def _ftp_modified_at(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = value.split(".", 1)[0]
    try:
        return datetime.strptime(raw, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _translated_ftp_permission_error(
    error: ftplib.error_perm,
    *,
    operation: str,
    target: str,
    missing_as_not_found: bool,
) -> Exception:
    message = str(error)
    code = message[:3]
    lowered = message.lower()
    reason = f"FTP {code or 'permission reply'}"
    if code == "530":
        return StorageAuthenticationFailed(
            driver_failure_message(
                "FTP",
                operation,
                target=target,
                reason=f"authentication failed ({reason})",
            )
        )
    if "permission" in lowered or "denied" in lowered:
        return StoragePermissionDenied(
            driver_failure_message(
                "FTP",
                operation,
                target=target,
                reason=f"permission denied ({reason})",
            )
        )
    if code == "550" and missing_as_not_found:
        return StorageNotFound(
            driver_failure_message(
                "FTP",
                operation,
                target=target,
                reason=f"object not found ({reason})",
            )
        )
    return StoragePermissionDenied(
        driver_failure_message(
            "FTP",
            operation,
            target=target,
            reason=f"operation refused ({reason})",
        )
    )


__all__ = ["FtpDriverOptions", "FtpObjectAddress", "FtpStorageDriver"]
