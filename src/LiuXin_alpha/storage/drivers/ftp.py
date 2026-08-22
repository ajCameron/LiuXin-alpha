"""
Read-only FTP/FTPS storage driver.
"""

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
from urllib.parse import SplitResult, quote, unquote, urlsplit, urlunsplit
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
from LiuXin_alpha.storage.drivers._validation import (
    reject_malformed_percent_escapes,
    reject_malformed_unicode,
)


@dataclasses.dataclass(slots=True)
class FtpDriverOptions:
    """
    Connection options for one FTP-family endpoint.

    Example:
        >>> FtpDriverOptions(timeout_s=10).passive
        True
    """

    timeout_s: float | None = 30.0
    passive: bool = True
    secure_data_channel: bool = True
    encoding: str = "utf-8"
    client_factory: Callable[[], Any] | None = None
    spool_limit_bytes: int = 8 * 1024 * 1024
    max_inventory_depth: int = 256
    max_directory_entries: int = 100_000
    max_inventory_entries: int = 100_000

    def __post_init__(self) -> None:
        """
        Reject resource limits that cannot bound an inventory or spool.

        Example:
            >>> FtpDriverOptions(max_inventory_depth=2).max_inventory_depth
            2


        :return:
        """

        if self.spool_limit_bytes < 0:
            raise ValueError("spool_limit_bytes must not be negative.")
        if self.max_inventory_depth < 1:
            raise ValueError("max_inventory_depth must be at least one.")
        if self.max_directory_entries < 1:
            raise ValueError("max_directory_entries must be at least one.")
        if self.max_inventory_entries < 1:
            raise ValueError("max_inventory_entries must be at least one.")


@dataclasses.dataclass(slots=True, frozen=True)
class FtpObjectAddress(DriverObjectAddress):
    """
    Canonical POSIX relative path beneath one configured FTP root.

    Example:
        >>> FtpObjectAddress("books/novel.epub", UUID(int=1)).value
        'books/novel.epub'
    """


class FtpStorageDriver(StorageDriverAPI[FtpObjectAddress]):
    """
    Enumerate, stat, and retrieve files from one FTP/FTPS root.

    Example:
        >>> driver = FtpStorageDriver("ftp://example.test/books/", address_space_uuid=UUID(int=1))
        >>> driver.root_uri
        'ftp://example.test/books/'
    """

    def __init__(
        self,
        url: str,
        *,
        address_space_uuid: UUID,
        options: FtpDriverOptions | None = None,
    ) -> None:
        """
        Parse one endpoint, retaining credentials only for live connections.

        Example:
            >>> FtpStorageDriver("ftps://example.test/books/", address_space_uuid=UUID(int=1)).root_uri
            'ftps://example.test/books/'


        :param url:
        :param address_space_uuid:
        :param options:
        :return:
        """

        self.options = options or FtpDriverOptions()
        url_text = str(url)
        reject_malformed_unicode(url_text, label="FTP root URL")
        try:
            parsed = urlsplit(url_text)
            host = _canonical_ftp_hostname(parsed)
            port = parsed.port
        except (TypeError, ValueError) as error:
            raise StorageInvalidAddress("FTP root URL authority is malformed.") from error
        if parsed.scheme.lower() not in {"ftp", "ftps"}:
            raise StorageInvalidAddress("FTP driver requires an ftp(s) URL.")
        if not parsed.hostname:
            raise StorageInvalidAddress("FTP URL must include a hostname.")
        if parsed.query or parsed.fragment:
            raise StorageInvalidAddress("FTP root URL must not contain query or fragment data.")

        self._scheme = parsed.scheme.lower()
        reject_malformed_percent_escapes(parsed.path, label="FTP root URL path")
        self._host = host
        self._port = port or (990 if self._scheme == "ftps" else 21)
        self._username = unquote(parsed.username) if parsed.username else "anonymous"
        self._password = unquote(parsed.password) if parsed.password else "anonymous@"
        decoded_root = unquote(parsed.path or "/")
        if "\\" in decoded_root or any(
            ord(character) < 32 or ord(character) == 127
            for character in decoded_root
        ):
            raise StorageInvalidAddress("FTP root URL path is malformed.")
        root = posixpath.normpath(decoded_root)
        self._ftp_root_path = "/" if root in {"", "."} else "/" + root.strip("/")
        rendered_path = quote(self._ftp_root_path, safe="/-._~")
        if not rendered_path.endswith("/"):
            rendered_path += "/"
        host = f"[{self._host}]" if ":" in self._host else self._host
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
        """
        Return the checker that scopes paths to this endpoint.

        Example:
            >>> driver.object_address_checker.address_space_uuid  # doctest: +SKIP
            UUID('00000000-0000-0000-0000-000000000001')


        :return:
        """

        return self._checker

    @property
    def root_uri(self) -> str:
        """
        Return a credential-free endpoint URI.

        Example:
            >>> driver.root_uri  # doctest: +SKIP
            'ftp://example.test/books/'


        :return:
        """

        return self._root_uri

    @property
    def ftp_root_path(self) -> str:
        """
        Return the decoded absolute path used after login.

        Example:
            >>> driver.ftp_root_path  # doctest: +SKIP
            '/books'


        :return:
        """

        return self._ftp_root_path

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Describe read-only range, hierarchy, URI, and inventory support.

        Example:
            >>> driver.capabilities.enumeration is EnumerationCompleteness.COMPLETE  # doctest: +SKIP
            True


        :return:
        """

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
        """
        Connect and perform the initial endpoint probe.

        Example:
            >>> driver.startup().available  # doctest: +SKIP
            True


        :return:
        """

        return self.probe()

    def probe(self) -> DriverStatus:
        """
        Verify login, command access, and root-directory listing.

        Example:
            >>> driver.probe().writable  # doctest: +SKIP
            False


        :return:
        """

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
        """
        Return the most recently observed endpoint status.

        Example:
            >>> driver.status().available  # doctest: +SKIP
            True


        :return:
        """

        return self._last_status

    def close(self) -> None:
        """
        Complete lifecycle cleanup; each operation owns its connection.

        Example:
            >>> driver.close()  # doctest: +SKIP


        :return:
        """

        return None

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[FtpObjectAddress],
    ) -> FtpObjectAddress:
        """
        Validate a persisted canonical path in this endpoint's address space.

        Example:
            >>> str(driver.parse_object_address("authors/book.epub"))  # doctest: +SKIP
            'authors/book.epub'


        :param identifier:
        :return:
        """

        if isinstance(identifier, DriverObjectAddress):
            return self.check_object_address(identifier)
        key = _canonical_ftp_key(str(identifier))
        return FtpObjectAddress(key, self._checker.address_space_uuid)

    def join_object_address(self, *tokens: str) -> FtpObjectAddress:
        """
        Join hierarchical tokens and validate the resulting FTP path.

        Example:
            >>> str(driver.join_object_address("authors", "book.epub"))  # doctest: +SKIP
            'authors/book.epub'


        :param tokens:
        :return:
        """

        if not tokens:
            raise StorageInvalidAddress("at least one FTP path token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def object_address_from_uri(self, uri: str) -> FtpObjectAddress:
        """
        Convert one credential-free, in-root FTP URI to an object address.

        Example:
            >>> str(driver.object_address_from_uri("ftp://example.test/books/a.epub"))  # doctest: +SKIP
            'a.epub'


        :param uri:
        :return:
        """

        uri_text = str(uri)
        reject_malformed_unicode(uri_text, label="FTP object URI")
        try:
            parsed = urlsplit(uri_text)
            candidate_host = _canonical_ftp_hostname(parsed)
            candidate_port_value = parsed.port
        except (TypeError, ValueError) as error:
            raise StorageInvalidAddress("FTP object URI authority is malformed.") from error
        if parsed.scheme.lower() != self._scheme or candidate_host != self._host:
            raise StorageInvalidAddress("FTP object URI belongs to another endpoint.")
        candidate_port = candidate_port_value or (990 if self._scheme == "ftps" else 21)
        if candidate_port != self._port:
            raise StorageInvalidAddress("FTP object URI uses another endpoint port.")
        if parsed.query or parsed.fragment:
            raise StorageInvalidAddress("FTP object URIs must not contain query or fragment data.")
        reject_malformed_percent_escapes(parsed.path, label="FTP object URI path")
        path = posixpath.normpath(unquote(parsed.path or "/"))
        root_prefix = self._ftp_root_path.rstrip("/") + "/"
        if not path.startswith(root_prefix):
            raise StorageInvalidAddress("FTP object URI lies outside the configured root.")
        return self.parse_object_address(path[len(root_prefix) :])

    def object_uri(self, object_address: FtpObjectAddress) -> str:
        """
        Render a checked address as a credential-free FTP URI.

        Example:
            >>> driver.object_uri(driver.parse_object_address("a.epub"))  # doctest: +SKIP
            'ftp://example.test/books/a.epub'


        :param object_address:
        :return:
        """

        checked = self.check_object_address(object_address)
        encoded = "/".join(quote(part, safe="-._~") for part in str(checked).split("/"))
        return self._root_uri + encoded

    def stat(
        self,
        object_address: FtpObjectAddress,
    ) -> DriverObjectInfo[FtpObjectAddress]:
        """
        Return authoritative size and available MLSD facts for one file.

        Example:
            >>> driver.stat(address).size  # doctest: +SKIP
            42


        :param object_address:
        :return:
        """

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
        """
        Retrieve a selected range into an owned local spool.

        Example:
            >>> with driver.open_read(address, length=4) as source:  # doctest: +SKIP
            ...     source.read()
            b'book'


        :param object_address:
        :param offset:
        :param length:
        :param if_version:
        :return:
        """

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
        received = 0

        def _receive(chunk: bytes) -> None:
            """
            Append one transfer callback chunk within the selected length.

            Example:
                >>> _receive(b"book")  # doctest: +SKIP


            :param chunk:
            :return:
            """

            nonlocal received, remaining
            if not isinstance(chunk, bytes):
                raise StorageUnavailable(
                    driver_failure_message(
                        "FTP",
                        "retrieve",
                        target=self.object_uri(checked),
                        reason="the server returned non-byte transfer data",
                    )
                )
            try:
                if remaining is None:
                    output.write(chunk)
                    received += len(chunk)
                    return
                if remaining > 0:
                    accepted = chunk[:remaining]
                    output.write(accepted)
                    received += len(accepted)
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
                expected_bytes: int | None = None
                try:
                    remote_size = _optional_int(client.size(str(checked)))
                except Exception:
                    remote_size = None
                if remote_size is not None:
                    available = max(0, remote_size - offset)
                    expected_bytes = (
                        available
                        if length is None
                        else min(length, available)
                    )
                try:
                    client.retrbinary(command, _receive, rest=offset or None)
                except TypeError:
                    # Some small or legacy clients do not accept REST. Fall
                    # back to streaming and discarding the prefix exactly.
                    skip = offset

                    def _receive_without_rest(chunk: bytes) -> None:
                        """
                        Discard the requested prefix for clients lacking REST.

                        Example:
                            >>> _receive_without_rest(b"prefixbook")  # doctest: +SKIP


                        :param chunk:
                        :return:
                        """

                        nonlocal skip
                        if skip:
                            discarded = min(skip, len(chunk))
                            skip -= discarded
                            chunk = chunk[discarded:]
                        if chunk:
                            _receive(chunk)

                    client.retrbinary(command, _receive_without_rest)
                if expected_bytes is not None and received != expected_bytes:
                    raise StorageUnavailable(
                        driver_failure_message(
                            "FTP",
                            "retrieve",
                            target=self.object_uri(checked),
                            reason=(
                                "the server completed a transfer with the wrong "
                                f"length (expected {expected_bytes}, received {received})"
                            ),
                        )
                    )
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
        """
        Walk and yield file entries beneath an optional path prefix.

        Example:
            >>> [str(item.object_address) for item in driver.iter_inventory()]  # doctest: +SKIP
            ['authors/book.epub']


        :param prefix:
        :return:
        """

        prefix_key = None if prefix is None else str(self.check_object_address(prefix))
        with self._connected_client(
            operation="inventory",
            missing_as_not_found=True,
        ) as client:
            observed = 0
            for path, facts in self._walk_entries(client):
                observed += 1
                if observed > self.options.max_inventory_entries:
                    raise StorageUnavailable(
                        driver_failure_message(
                            "FTP",
                            "inventory",
                            target=self._root_uri,
                            reason="the configured inventory entry limit was exceeded",
                        )
                    )
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
        """
        Yield one logged-in client and translate connection-level failures.

        Example:
            >>> with driver._connected_client(operation="probe") as client:  # doctest: +SKIP
            ...     client.voidcmd("NOOP")


        :param operation:
        :param target:
        :param missing_as_not_found:
        :return:
        """

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
        """
        Construct an FTP or FTP_TLS client from endpoint options.

        Example:
            >>> type(driver._default_client_factory()).__name__  # doctest: +SKIP
            'FTP'


        :return:
        """

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
        depth: int = 0,
    ) -> Iterator[tuple[str, dict[str, str]]]:
        """
        Recursively yield normalized directory and file facts within limits.

        Example:
            >>> list(driver._walk_entries(client))  # doctest: +SKIP
            [('a.epub', {'type': 'file', 'size': '42'})]


        :param client:
        :param relative_directory:
        :param depth:
        :return:
        """

        for name, facts in self._list_dir(client, relative_directory):
            path = posixpath.join(relative_directory, name) if relative_directory else name
            normalized = dict(facts)
            entry_type = str(normalized.get("type") or "").lower()
            if entry_type in {"cdir", "pdir"}:
                continue
            if entry_type == "dir":
                normalized["type"] = "dir"
                yield path, normalized
                if depth >= self.options.max_inventory_depth:
                    raise StorageUnavailable(
                        "FTP inventory exceeded its configured directory-depth limit."
                    )
                yield from self._walk_entries(client, path, depth + 1)
            else:
                normalized["type"] = "file"
                yield path, normalized

    def _list_dir(self, client: Any, relative_directory: str) -> list[tuple[str, dict[str, str]]]:
        """
        List one directory through MLSD, with a conservative NLST fallback.

        Example:
            >>> driver._list_dir(client, "")  # doctest: +SKIP
            [('a.epub', {'type': 'file', 'size': '42'})]


        :param client:
        :param relative_directory:
        :return:
        """

        target = relative_directory or "."
        try:
            entries: list[tuple[str, dict[str, str]]] = []
            seen_names: set[str] = set()
            observed = 0
            for raw_name, facts in client.mlsd(target):
                observed += 1
                if observed > self.options.max_directory_entries:
                    raise StorageUnavailable(
                        "FTP inventory exceeded its configured per-directory entry limit."
                    )
                name = _validated_ftp_listing_name(raw_name)
                if name is None:
                    continue
                if name in seen_names:
                    raise StorageUnavailable(
                        "FTP inventory returned a duplicate directory-entry name."
                    )
                seen_names.add(name)
                entries.append(
                    (
                        name,
                        {
                            str(key): str(value)
                            for key, value in dict(facts).items()
                        },
                    )
                )
            return entries
        except (AttributeError, NotImplementedError, ftplib.error_perm):
            entries: list[tuple[str, dict[str, str]]] = []
            seen_names: set[str] = set()
            observed = 0
            for raw_name in client.nlst(target):
                observed += 1
                if observed > self.options.max_directory_entries:
                    raise StorageUnavailable(
                        "FTP inventory exceeded its configured per-directory entry limit."
                    )
                candidate_name = str(raw_name).rstrip("/").rsplit("/", 1)[-1]
                name = _validated_ftp_listing_name(candidate_name)
                if name is None:
                    continue
                if name in seen_names:
                    raise StorageUnavailable(
                        "FTP inventory returned a duplicate directory-entry name."
                    )
                seen_names.add(name)
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
        """
        Find one exact basename in its parent directory listing.

        Example:
            >>> driver._entry_for(client, "authors/a.epub")  # doctest: +SKIP
            {'type': 'file', 'size': '42'}


        :param client:
        :param path:
        :return:
        """

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
    """
    Require one control-free canonical relative POSIX FTP path.

    Example:
        >>> _canonical_ftp_key("authors/book.epub")
        'authors/book.epub'


    :param value:
    :return:
    """

    key = str(value)
    reject_malformed_unicode(key, label="FTP object address")
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


def _canonical_ftp_hostname(parsed: SplitResult) -> str:
    """
    Return an unbracketed canonical hostname suitable for FTP clients.

    Example:
        >>> _canonical_ftp_hostname(urlsplit("ftp://BÜCHER.example/books"))
        'xn--bcher-kva.example'


    :param parsed:
    :return:
    """

    hostname = parsed.hostname
    if not hostname:
        raise StorageInvalidAddress("FTP URL must include a hostname.")
    reject_malformed_unicode(hostname, label="FTP URL hostname")
    if ":" in hostname:
        return hostname.lower()
    try:
        return hostname.encode("idna").decode("ascii").lower()
    except UnicodeError as error:
        raise StorageInvalidAddress("FTP URL hostname is malformed.") from error


def _validated_ftp_listing_name(value: object) -> str | None:
    """
    Accept one safe directory-entry component or ignore dot entries.

    Example:
        >>> _validated_ftp_listing_name("book.epub")
        'book.epub'
        >>> _validated_ftp_listing_name(".") is None
        True


    :param value:
    :return:
    """

    name = str(value)
    if name in {"", ".", ".."}:
        return None
    try:
        reject_malformed_unicode(name, label="FTP inventory name")
    except StorageInvalidAddress as error:
        raise StorageUnavailable(
            "FTP inventory returned a name containing malformed Unicode."
        ) from error
    if (
        "/" in name
        or "\\" in name
        or "\x00" in name
        or any(ord(character) < 32 or ord(character) == 127 for character in name)
    ):
        raise StorageUnavailable(
            "FTP inventory returned a malformed directory-entry name."
        )
    return name


def _optional_int(value: Any) -> int | None:
    """
    Parse a non-negative integer fact, returning ``None`` when unusable.

    Example:
        >>> _optional_int("42")
        42
        >>> _optional_int("unknown") is None
        True


    :param value:
    :return:
    """

    if value in {None, ""}:
        return None
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return result if result >= 0 else None


def _ftp_modified_at(value: str | None) -> datetime | None:
    """
    Parse an MLSD UTC modification timestamp when present and valid.

    Example:
        >>> _ftp_modified_at("20260822123045").isoformat()
        '2026-08-22T12:30:45+00:00'


    :param value:
    :return:
    """

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
    """
    Classify an FTP permission reply without exposing credentials.

    Example:
        >>> error = ftplib.error_perm("530 Login incorrect")
        >>> type(_translated_ftp_permission_error(error, operation="probe", target="ftp://example.test/", missing_as_not_found=False))
        <class 'LiuXin_alpha.storage.api.errors.StorageAuthenticationFailed'>


    :param error:
    :param operation:
    :param target:
    :param missing_as_not_found:
    :return:
    """

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
