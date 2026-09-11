"""
Inspect and retrieve FTP/FTPS files through scoped paths and per-operation clients.

The driver separates normalized public URLs from retained login credentials, uses
bounded MLSD/NLST traversal for inventory, and retrieves reads into local spools.
Probe has its own direct listing path. Helpers here validate path/fact evidence and
translate selected failures; listing metadata and transfer lengths do not provide
version-pinned reads, content authentication, or a remote filesystem snapshot.
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
    StorageCharacteristics,
    StorageDriverAPI,
    StorageError,
    StorageInvalidAddress,
    StorageNotFound,
    StoragePermissionDenied,
    StoragePublicationModel,
    StorageTemporarySpaceRequirement,
    StorageTimeout,
    StorageUnavailable,
    StorageUnsupportedOperation,
    StorageWriteUsage,
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
    Retain mutable FTP connection settings, read-spool policy, and inventory bounds.

    Construction checks the spool/depth/entry bounds but does not validate every option type or
    connection setting. Drivers retain this object by reference; later field assignment is not
    revalidated automatically. A spool threshold controls memory-to-file rollover, not maximum
    retrieved object size.

    Example:
        >>> options = FtpDriverOptions(timeout_s=10, max_inventory_depth=2)
        >>> options.passive, options.max_inventory_depth
        (True, 2)


    :ivar timeout_s: Seconds passed to client construction/connect, or None; not an overall transfer deadline.
    :ivar passive: Truth value passed to set_pasv when that method is callable.
    :ivar secure_data_channel: Whether FTPS setup calls an available prot_p method.
    :ivar encoding: Command/path text encoding passed to the default ftplib client constructor.
    :ivar client_factory: Optional zero-argument client factory, or None for the FTP/FTP_TLS factory.
    :ivar spool_limit_bytes: In-memory spool rollover threshold in bytes; zero disables automatic size-based rollover.
    :ivar max_inventory_depth: Recursion bound checked when a directory would be descended into.
    :ivar max_directory_entries: Maximum observed names in one directory listing, including ignored dot entries.
    :ivar max_inventory_entries: Maximum walk observations per inventory, including directories and filtered files.
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
        Reject a negative spool threshold or depth/directory/total inventory limits below one.

        Example:
            >>> FtpDriverOptions(spool_limit_bytes=0).spool_limit_bytes
            0


        :return: None when these comparisons pass; invalid bounds raise ValueError without coercing option values.
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
    Carry a relative FTP path and the UUID identifying its driver address space.

    Inherited record construction validates ownership metadata, nonempty text, and NULs without
    applying FTP canonical-path checks. Use the driver's parsers for external identifiers; the
    runtime checker verifies subtype and UUID only.

    Example:
        >>> FtpObjectAddress("books/novel.epub", UUID(int=1)).value
        'books/novel.epub'
    """


class FtpStorageDriver(StorageDriverAPI[FtpObjectAddress]):
    """
    Inspect and retrieve files under an FTP-family root using per-operation clients.

    Construction normalizes root/credential data without connecting. Text addresses are validated
    relative paths; existing typed addresses receive ownership checks only. Reads finish retrieval
    into a local spool before returning it. Inventory walks bounded directory listings, while probe
    follows a separate MLSD path. Injected factories supply the client behavior and connection
    isolation.

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
        Normalize one FTP/FTPS endpoint and retain runtime login credentials and options.

        Root path escapes are checked, decoded with replacement behavior, normalized as a POSIX
        path, and requoted for the public URI. Query/fragment data and decoded path
        controls/backslashes are rejected. Host spelling uses IDNA or lowercase IPv6 text. A missing
        or zero port selects 21 for FTP or 990 for FTPS; default ports are omitted from the rendered
        URI. User information is decoded and retained privately for login, not included in that URI.
        Construction does not perform network I/O or a probe.

        Example:
            >>> driver = FtpStorageDriver("ftp://user:pass@EXAMPLE.TEST/books/../archive", address_space_uuid=UUID(int=1))
            >>> driver.root_uri, driver.ftp_root_path
            ('ftp://example.test/archive/', '/archive')


        :param url: FTP or FTPS root URL, optionally including login user/password; absent values select anonymous defaults.
        :param address_space_uuid: UUID used to scope runtime object-address ownership.
        :param options: Truthy FtpDriverOptions retained by reference, otherwise a new default option record.
        :return: None after retaining normalized endpoint state, the checker, and an initially unavailable cached status.
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
        Return the retained subtype-and-UUID checker for this FTP address space.

        Example:
            >>> driver.object_address_checker.address_space_uuid  # doctest: +SKIP


        :return: Shared ownership checker; it does not canonicalize stored path text.
        """

        return self._checker

    @property
    def root_uri(self) -> str:
        """
        Return the normalized root URI with login user information omitted.

        Example:
            >>> FtpStorageDriver("ftp://user:pass@example.test/books", address_space_uuid=UUID(int=1)).root_uri
            'ftp://example.test/books/'


        :return: Retained FTP-family root URI ending in a slash, without a connection or status refresh.
        """

        return self._root_uri

    @property
    def ftp_root_path(self) -> str:
        """
        Return the decoded normalized absolute path used for the client working directory.

        Example:
            >>> FtpStorageDriver("ftp://example.test/books/", address_space_uuid=UUID(int=1)).ftp_root_path
            '/books'


        :return: Remote POSIX root path, not a local filesystem path or a percent-encoded URI.
        """

        return self._ftp_root_path

    @property
    def capabilities(self) -> DriverCapabilities:
        """
        Describe read-only ranges, complete inventory, hierarchical keys, and URI conversion.

        Range support is implemented through completed retrieval/spooling; conditional reads are not
        supported. Complete enumeration requires traversal to finish within its limits. Concurrency
        advertises concurrent reads and recommends four readers, but an injected factory must
        provide appropriate client isolation.

        Example:
            >>> driver = FtpStorageDriver("ftp://example.test/", address_space_uuid=UUID(int=1))
            >>> driver.capabilities.enumeration is EnumerationCompleteness.COMPLETE
            True


        :return: Fresh capability record without checking whether a particular server implements the required operations.
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

    @property
    def storage_characteristics(self) -> StorageCharacteristics:
        """
        Describe read-only publication without temporary write-space requirements.

        Example:
            >>> driver = FtpStorageDriver("ftp://example.test/", address_space_uuid=UUID(int=1))
            >>> driver.storage_characteristics.publication_model is StoragePublicationModel.READ_ONLY
            True


        :return: Fresh READ_ONLY/NONE/NOT_APPLICABLE characteristics; read operations can still use local spools.
        """

        return StorageCharacteristics(
            publication_model=StoragePublicationModel.READ_ONLY,
            temporary_space=StorageTemporarySpaceRequirement.NONE,
            recommended_write_usage=StorageWriteUsage.NOT_APPLICABLE,
        )

    def startup(self) -> DriverStatus:
        """
        Run the active endpoint probe and return its updated status.

        Example:
            >>> status = driver.startup()  # doctest: +SKIP


        :return: DriverStatus from probe; startup does not enable an operation-gating flag.
        """

        return self.probe()

    def probe(self) -> DriverStatus:
        """
        Open a client, optionally issue NOOP, and consume the root MLSD listing.

        Probe directly materializes mlsd(".") without the inventory name/entry bounds or NLST
        fallback. Only StorageUnavailable and StorageTimeout become cached unavailable status; other
        failures propagate. Success records read-only availability, UTC check time, and normalized
        endpoint details without counting inventory entries or testing object reads.

        Example:
            >>> driver.probe().writable  # doctest: +SKIP
            False


        :return: New cached DriverStatus for successful reachability or a handled unavailable/timeout failure.
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
        Return the last probe observation without contacting the endpoint.

        Example:
            >>> driver = FtpStorageDriver("ftp://example.test/", address_space_uuid=UUID(int=1))
            >>> driver.status().available
            False


        :return: Retained status object, initially unavailable before a successful probe.
        """

        return self._last_status

    def close(self) -> None:
        """
        Complete the lifecycle hook without changing driver state or closing returned spools.

        Each network operation handles its own client cleanup. The hook does not reset cached
        status, invalidate later operations, or manage caller-owned reads.

        Example:
            >>> driver = FtpStorageDriver("ftp://example.test/", address_space_uuid=UUID(int=1))
            >>> driver.close()


        :return: None; this lifecycle hook performs no work.
        """

        return None

    def parse_object_address(
        self,
        identifier: DriverObjectAddressInput[FtpObjectAddress],
    ) -> FtpObjectAddress:
        """
        Validate relative path text, or ownership-check an existing typed address.

        Text is stringified and checked for canonical slash components, valid Unicode, and absence
        of protocol controls/backslashes. Existing DriverObjectAddress values bypass text validation
        and receive only subtype/UUID checks. Parsing does not establish remote existence or resolve
        server filesystem links.

        Example:
            >>> driver = FtpStorageDriver("ftp://example.test/books/", address_space_uuid=UUID(int=1))
            >>> str(driver.parse_object_address("authors/café.epub"))
            'authors/café.epub'


        :param identifier: Relative FTP key to validate, or an existing address belonging to this driver.
        :return: Scoped FtpObjectAddress after key validation or ownership checking.
        """

        if isinstance(identifier, DriverObjectAddress):
            return self.check_object_address(identifier)
        key = _canonical_ftp_key(str(identifier))
        return FtpObjectAddress(key, self._checker.address_space_uuid)

    def join_object_address(self, *tokens: str) -> FtpObjectAddress:
        """
        Join stringified tokens with slashes and validate the complete relative path.

        Example:
            >>> driver = FtpStorageDriver("ftp://example.test/", address_space_uuid=UUID(int=1))
            >>> str(driver.join_object_address("authors", "book.epub"))
            'authors/book.epub'


        :param tokens: One or more relative path tokens; boundary slashes are retained and can make the joined key invalid.
        :return: Parsed owned FTP address; no tokens or invalid joined syntax raises StorageInvalidAddress.
        """

        if not tokens:
            raise StorageInvalidAddress("at least one FTP path token is required.")
        return self.parse_object_address("/".join(str(token) for token in tokens))

    def object_address_from_uri(self, uri: str) -> FtpObjectAddress:
        """
        Normalize an FTP object URI and derive its relative path within the configured root.

        Canonical host, scheme, and effective port must match. Query/fragment data and malformed
        path escapes are rejected. Decoding uses replacement behavior and POSIX normalization
        precedes root membership checks, so in-root dot segments can normalize away. URI user
        information is accepted but does not replace the driver's configured login credentials. No
        request is made.

        Example:
            >>> driver = FtpStorageDriver("ftp://example.test/books/", address_space_uuid=UUID(int=1))
            >>> str(driver.object_address_from_uri("ftp://other:login@example.test/books/a.epub"))
            'a.epub'


        :param uri: Absolute FTP-family object URI whose normalized endpoint/path must belong to this driver root.
        :return: Parsed relative owned address; the root itself does not identify a file.
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
        Append percent-quoted address components to the retained root URI without user information.

        Example:
            >>> driver = FtpStorageDriver("ftp://example.test/books/", address_space_uuid=UUID(int=1))
            >>> driver.object_uri(driver.parse_object_address("café.epub"))
            'ftp://example.test/books/caf%C3%A9.epub'


        :param object_address: Address whose subtype and UUID must match; stored key text is not reparsed here.
        :return: Rendered object URL without a remote lookup or renewed canonical-path validation.
        """

        checked = self.check_object_address(object_address)
        encoded = "/".join(quote(part, safe="-._~") for part in str(checked).split("/"))
        return self._root_uri + encoded

    def stat(
        self,
        object_address: FtpObjectAddress,
    ) -> DriverObjectInfo[FtpObjectAddress]:
        """
        Find a file in its parent listing and interpret size, time, unique token, and advisory
        facts.

        Missing entries raise StorageNotFound and directory entries are invalid file addresses.
        Missing/unusable size facts trigger SIZE; ordinary failures of that fallback are swallowed
        and absent size becomes StorageUnsupportedOperation. The unique fact is reported as a
        version hint without enabling conditional reads. Metadata does not verify body bytes or
        provide a filesystem snapshot.

        Example:
            >>> info = driver.stat(address)  # doctest: +SKIP
            >>> info.size  # doctest: +SKIP
            42


        :param object_address: Owned file address inspected using a fresh connected client and parent directory listing.
        :return: DriverObjectInfo with required size, optional UTC modification time/unique token, and remaining stringified facts as hints.
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
        Retrieve selected bytes into a caller-owned local spool before returning.

        Ownership is checked first; any version condition is unsupported, and negative ranges are
        rejected. The spool uses the retained rollover threshold, with zero disabling automatic
        size-based rollover. A zero-length read returns that empty spool without connecting. Other
        reads request REST for a nonzero offset and retain at most length bytes, but the transfer
        continues even after that count is collected. No hard transfer-size limit or overall
        deadline is added here.

        Available SIZE evidence determines the expected collected count. Its absence omits the final
        length comparison; matching length does not prove the server honored REST or authenticate
        bytes. Any retrbinary TypeError retries without REST and discards a prefix in the callback,
        without resetting already collected bytes/counters. Successful retrieval and connection
        cleanup precede rewinding the spool. Failed retrieval attempts close the spool before
        propagating.

        Example:
            >>> with driver.open_read(address, offset=3, length=2) as source:  # doctest: +SKIP
            ...     selected = source.read()


        :param object_address: Owned file address used in the RETR command; typed ownership checking does not reparse its path.
        :param offset: Nonnegative starting byte offset, passed as REST or discarded locally for clients rejecting that argument.
        :param length: Maximum bytes retained in the spool, None for the remaining transfer, or zero for an immediate empty spool.
        :param if_version: Must be None because FTP has no supported atomic conditional-read primitive here.
        :return: Seekable binary SpooledTemporaryFile positioned at zero; the caller must close it.
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
            Append a byte chunk or its selected prefix and advance the enclosing read counters.

            Non-bytes data raises StorageUnavailable even after the selected count is full. With no
            length bound, all chunk bytes are written. With a bound, excess bytes are discarded
            without stopping the transfer. Counters use the selected input length rather than
            checking the spool write return value. Local OSErrors are translated into staging
            failures.

            Example:
                >>> _receive(b"book")  # doctest: +SKIP


            :param chunk: Bytes delivered by retrbinary or the no-REST adapter callback.
            :return: None after appending the selected bytes or discarding bytes beyond the retained length.
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
                    # Some injected or legacy client adapters do not accept the
                    # ``rest`` argument.  Discard the streamed prefix exactly.
                    skip = offset

                    def _receive_without_rest(chunk: bytes) -> None:
                        """
                        Discard the requested offset across chunks, then forward remaining data to
                        the collector.

                        This callback is used after retrbinary raises TypeError with a REST
                        argument. The enclosing spool and collection counters are not reset before
                        retry. Entirely discarded chunks do not reach the collector's byte-type
                        validation.

                        Example:
                            >>> _receive_without_rest(b"prefixbook")  # doctest: +SKIP


                        :param chunk: Transfer chunk whose leading bytes may satisfy the still-unskipped offset.
                        :return: None after reducing the remaining skip and forwarding any nonempty suffix.
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
        Walk the configured root through one client and yield optionally prefix-filtered files.

        The total observation bound counts directories and files before directory exclusion and
        lexical startswith filtering. Per-directory and recursion bounds belong to the walk helpers.
        Inventory metadata is advisory listing evidence, not a snapshot; earlier entries can be
        yielded before a later traversal failure. The generator retains its connection context while
        suspended during iteration.

        Example:
            >>> prefix = driver.parse_object_address("books")  # doctest: +SKIP
            >>> entries = list(driver.iter_inventory(prefix=prefix))  # doctest: +SKIP


        :param prefix: Optional owned address text used as a lexical file-path prefix, or None for all walked files.
        :return: Lazy DriverInventoryEntry iterator with listing-derived size/time/unique token and filename/media hints.
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
        Yield an operation client after optional connection/login setup and root selection.

        A configured factory replaces default construction. Callable connect, login, set_pasv, and
        applicable FTPS prot_p methods are invoked when present; a non-root path requires cwd. The
        exception guard covers both setup and the caller's yielded operation. Existing StorageError
        propagates; permission, timeout, FTP/EOF/OS, and other ordinary failures are translated by
        category.

        Cleanup selects the first callable of quit and close. An ordinary failure from that call is
        suppressed and does not trigger the other closer. Attribute lookup and BaseException cleanup
        failures may still propagate or mask an earlier failure. No connection pool or closed-state
        cache is retained.

        Example:
            >>> with driver._connected_client(operation="probe") as client:  # doctest: +SKIP
            ...     reply = client.voidcmd("NOOP")


        :param operation: Operation label included in translated diagnostics for setup and the yielded body.
        :param target: Optional diagnostic target, or None to use the normalized root URI.
        :param missing_as_not_found: Allow a 550 reply without denial wording to become StorageNotFound rather than permission denied.
        :return: Context manager yielding the selected client and attempting one supported closer on exit.
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
        Construct an unconnected ftplib FTP or FTP_TLS client using timeout and encoding options.

        The endpoint scheme selects the class. Host, port, login, passive mode, and data-channel
        protection are handled later by the connection context. No custom SSL context or alternative
        TLS transport is supplied by this helper.

        Example:
            >>> driver = FtpStorageDriver("ftp://example.test/", address_space_uuid=UUID(int=1))
            >>> client = driver._default_client_factory()
            >>> type(client).__name__
            'FTP'
            >>> client.close()


        :return: New unconnected ftplib client; creating it does not test endpoint availability.
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
        Yield normalized directory/file facts and recurse in each listing's order.

        cdir/pdir facts are skipped. Directories are yielded before the depth check and recursion;
        all other types are normalized to file. The depth argument starts at zero, and a directory
        at the configured bound is yielded before traversal raises. This helper has no unique-fact
        or filesystem-link cycle set; depth and outer observation bounds limit problematic
        traversal.

        Example:
            >>> entries = list(driver._walk_entries(client))  # doctest: +SKIP


        :param client: Connected client positioned at the configured remote root.
        :param relative_directory: Directory path relative to that root, or empty text for the root listing.
        :param depth: Current recursion depth, initially zero and incremented for each descent.
        :return: Lazy iterator of relative paths and copied facts with type normalized to dir or file.
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
        Materialize one validated directory listing through MLSD or a limited NLST fallback.

        MLSD names are checked as single components, duplicate accepted names fail, and facts are
        stringified. Every observed name consumes the per-directory bound, including ignored dot
        entries. AttributeError, NotImplementedError, or ftplib.error_perm anywhere in that MLSD
        attempt starts a fresh NLST attempt.

        NLST reduces returned paths to basenames before validation. A successful cwd probe marks a
        directory; ordinary probe failures mark it a file. A saved pwd is restored after successful
        directory probing, but a client without pwd can retain the changed working directory. File
        SIZE failures omit size evidence. Neither path guarantees a stable or complete view of
        changing remote state.

        Example:
            >>> names_and_facts = driver._list_dir(client, "books")  # doctest: +SKIP


        :param client: Connected FTP-like client supplying MLSD or NLST and applicable cwd/pwd/size operations.
        :param relative_directory: Relative directory path to list; empty text is sent as ".".
        :return: List of accepted names and string facts in backend order, after the selected listing attempt completes.
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
        Find an exact basename in its complete parent listing and normalize the reported entry type.

        Example:
            >>> facts = driver._entry_for(client, "books/a.epub")  # doctest: +SKIP


        :param client: Connected client used to list the candidate parent directory.
        :param path: Remote path whose POSIX parent and basename select the lookup.
        :return: Copied facts with dir/cdir/pdir normalized to dir and other types to file, or None when no exact name is found.
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
    Validate relative FTP path components without normalizing accepted Unicode or whitespace.

    Reject malformed Unicode, empty text, C0/DEL controls, leading slash, backslashes, and
    empty/dot/dot-dot components. Other whitespace and literal percent characters are retained; no
    percent decoding or server lookup occurs.

    Example:
        >>> _canonical_ftp_key("authors/café book.epub")
        'authors/café book.epub'


    :param value: Relative path candidate to stringify and validate.
    :return: Accepted slash-joined key text; explicit validation failures raise StorageInvalidAddress.
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
    Render a parsed host as lowercase IDNA ASCII or lowercase unbracketed IPv6 text.

    Example:
        >>> _canonical_ftp_hostname(urlsplit("ftp://BÜCHER.example/books"))
        'xn--bcher-kva.example'


    :param parsed: SplitResult supplying a hostname; scheme, port, user information, and path are not checked here.
    :return: Normalized host text; absent or malformed Unicode/IDNA hostname data raises StorageInvalidAddress.
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
    Accept one stringified directory-entry component, ignoring empty and dot names.

    Example:
        >>> _validated_ftp_listing_name("book.epub")
        'book.epub'
        >>> _validated_ftp_listing_name(".") is None
        True


    :param value: Remote name to stringify; accepted names must contain no slash, backslash, C0/DEL control, or malformed Unicode.
    :return: Accepted name without normalization, or None for empty/dot/dot-dot text; malformed names raise StorageUnavailable.
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
    Interpret optional nonnegative size evidence using int conversion.

    None/empty text and conversion TypeError/ValueError produce None, as do negative results. Normal
    integer coercions apply, including finite-float truncation. The initial set-membership check can
    raise for unhashable values, and overflow or arbitrary conversion failures are not caught.

    Example:
        >>> _optional_int("42")
        42
        >>> _optional_int("unknown") is None
        True


    :param value: Candidate size fact; hashable None or empty text denotes absent evidence.
    :return: Nonnegative converted integer or None for absent/rejected evidence, subject to the uncaught conversion cases above.
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
    Parse an MLSD modification timestamp as UTC after dropping any fractional-second suffix.

    Example:
        >>> _ftp_modified_at("20260822123045.987").isoformat()
        '2026-08-22T12:30:45+00:00'


    :param value: Optional YYYYMMDDHHMMSS text, possibly followed by a dot and fractional seconds.
    :return: Aware UTC datetime, or None for falsey input or a date-parsing ValueError; non-string method failures propagate.
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
    Classify a permanent FTP reply using its leading code and selected denial wording.

    Code 530 becomes authentication failure. Permission/denied wording then takes precedence over
    optional 550-as-missing classification; remaining replies are permission failures. Diagnostics
    retain only the first three reply characters alongside fixed reason text, then use shared
    selective target/detail formatting.

    Example:
        >>> error = ftplib.error_perm("530 Login incorrect")
        >>> isinstance(_translated_ftp_permission_error(error, operation="probe", target="ftp://example.test/", missing_as_not_found=False), StorageAuthenticationFailed)
        True


    :param error: Permanent-reply exception whose text supplies the first three characters and denial-wording checks.
    :param operation: Operation label for shared diagnostic formatting.
    :param target: Endpoint or object description passed to the shared target formatter.
    :param missing_as_not_found: Map code 550 without denial wording to StorageNotFound when True.
    :return: New storage authentication, permission, or missing-object exception for the caller to raise.
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
