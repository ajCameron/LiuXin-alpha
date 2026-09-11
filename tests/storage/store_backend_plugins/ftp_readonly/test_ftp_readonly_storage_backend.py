"""
Exercise configured FTP/FTPS Stores, raw-driver failure boundaries, and ingest publication.

    Shared-tree clients simulate connection setup, listings, metadata, and transfers;
    TLS markers and FTP replies are test doubles, not live-service verification.
    Ingest cases use real local filesystem destinations with in-memory manager
    metadata. The helper facts can intentionally disagree with delivered bytes to
    expose truncation and malformed-response behavior.
"""

from __future__ import annotations

import ftplib
import socket

from dataclasses import dataclass

import pytest

from LiuXin_alpha.ingest import ingest_store
from LiuXin_alpha.storage.api import (
    EnumerationCompleteness,
    Location,
    StorageAuthenticationFailed,
    StorageInvalidAddress,
    StorageNotFound,
    StoragePublicationModel,
    StorageTemporarySpaceRequirement,
    StorageTimeout,
    StorageUnavailable,
    StoreReadOnly,
)
from LiuXin_alpha.storage.storage_manager import InMemoryStorageManager
from LiuXin_alpha.storage.stores import FilesystemStore
from LiuXin_alpha.storage.store_backend_plugins.ftp_readonly import (
    FtpBackendOptions,
    FtpReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.ftp_readonly.ftp_location import (
    FtpReadOnlyStoreLocation,
)
from tests.fixtures.storage_unicode import (
    TORTURED_UNICODE_PATH_CASES,
    UNICODE_DIRECTORY,
    UNICODE_FILENAME,
    UNICODE_KEY,
    UNICODE_PAYLOAD,
    UNICODE_URL_KEY,
)
from tests.storage.contracts.unicode_paths import exercise_unicode_path_cases


@dataclass(slots=True)
class _Node:
    """
    Describe one synthetic FTP tree entry without validating its facts against payload bytes.

    Directory/file type and size are independently supplied, allowing tests to represent malformed
    metadata or a truncated transfer. Optional modification and unique facts are emitted by the fake
    MLSD implementation.

    Example:
        >>> node = _Node("file", size=12, payload=b"short")
        >>> node.size, len(node.payload)
        (12, 5)


    :ivar node_type: Entry-kind marker, conventionally dir or file, interpreted by the fake client.
    :ivar size: Declared file length, independent of actual payload length.
    :ivar payload: Bytes returned by the ordinary fake retrieval operation.
    :ivar modified: Optional raw MLSD timestamp text.
    :ivar unique: Optional opaque MLSD unique fact used as a version hint.
    """
    node_type: str
    size: int = 0
    payload: bytes = b""
    modified: str | None = None
    unique: str | None = None


class _FakeFtpClient:
    """
    Model selected FTP commands against a shared absolute-path tree without network I/O.

    Instances retain independent working directories and setup/closure markers. MLSD derives
    immediate children from sorted tree keys; SIZE trusts declared node lengths and RETR sends the
    selected payload in one callback. This fake does not implement TLS, permissions, command
    framing, or a complete FTP server.

    Example:
        >>> client = _FakeFtpClient(_tree())
        >>> client.cwd("/library")
        >>> client.size("books/one.epub")
        7
    """
    def __init__(self, tree: dict[str, _Node]) -> None:
        """
        Retain the shared tree and initialize working-directory, setup, closure, and retrieval
        records.

        Example:
            >>> client = _FakeFtpClient(_tree())
            >>> client.pwd(), client.closed, client.retrievals
            ('/', False, [])


        :param tree: Absolute-path mapping retained by reference; nodes can deliberately disagree about size and payload.
        :return: None after setting the initial working directory to root and clearing observable setup state.
        """
        self._tree = tree
        self._cwd = "/"
        self.connected = None
        self.logged_in = None
        self.passive = None
        self.secured = False
        self.closed = False
        self.retrievals: list[tuple[str, int | None]] = []

    def connect(self, host: str, port: int, timeout=None):
        """
        Record the requested endpoint and timeout without opening a socket.

        Example:
            >>> client = _FakeFtpClient(_tree())
            >>> client.connect("example.test", 21, timeout=10)
            'ok'
            >>> client.connected
            ('example.test', 21, 10)


        :param host: Host text to record as the first endpoint component.
        :param port: Port value recorded without validation or connection attempts.
        :param timeout: Optional timeout recorded verbatim without implementing waiting behavior.
        :return: Fixed "ok" response after recording the connection tuple.
        """
        self.connected = (host, port, timeout)
        return "ok"

    def login(self, user: str, passwd: str):
        """
        Record supplied credentials without authentication or permission checks.

        Example:
            >>> client = _FakeFtpClient(_tree())
            >>> client.login("anonymous", "anonymous@")
            'ok'


        :param user: Login username retained in the logged_in tuple.
        :param passwd: Login password retained in the logged_in tuple for setup assertions.
        :return: Fixed "ok" response after recording the credentials.
        """
        self.logged_in = (user, passwd)
        return "ok"

    def set_pasv(self, passive: bool):
        """
        Record the passive-mode value without configuring a data connection.

        Example:
            >>> client = _FakeFtpClient(_tree())
            >>> client.set_pasv(True)
            >>> client.passive
            True


        :param passive: Requested passive-mode value retained for assertions.
        :return: None after updating the passive marker.
        """
        self.passive = passive

    def prot_p(self):
        """
        Record that data-channel protection was requested without performing TLS negotiation.

        Example:
            >>> client = _FakeFtpClient(_tree())
            >>> client.prot_p()
            >>> client.secured
            True


        :return: None after setting the secured marker to True.
        """
        self.secured = True

    def cwd(self, path: str):
        """
        Normalize a path and select it only when the tree contains an explicit directory node.

        Example:
            >>> client = _FakeFtpClient(_tree())
            >>> client.cwd("/library/books")
            >>> client.pwd()
            '/library/books'


        :param path: Absolute or current-directory-relative path to normalize against the fake tree.
        :return: None after updating the working directory; absent/non-directory nodes raise FileNotFoundError.
        """
        normalized = self._normalize(path)
        node = self._tree.get(normalized)
        if node is None or node.node_type != "dir":
            raise FileNotFoundError(normalized)
        self._cwd = normalized

    def pwd(self) -> str:
        """
        Return the fake client current working directory without consulting the tree again.

        Example:
            >>> _FakeFtpClient(_tree()).pwd()
            '/'


        :return: Retained normalized absolute working-directory text.
        """
        return self._cwd

    def voidcmd(self, cmd: str):
        """
        Accept any command text and return the fixed successful control reply.

        Example:
            >>> _FakeFtpClient(_tree()).voidcmd("NOOP")
            '200 OK'


        :param cmd: Command text ignored by the fake control-channel implementation.
        :return: The literal "200 OK" without parsing or recording the command.
        """
        return "200 OK"

    def mlsd(self, target: str = "."):
        """
        Yield immediate child names and selected facts from sorted shared-tree entries.

        The target must have an explicit directory node. Deeper descendants synthesize a directory
        child once, while direct file children expose declared size and truthy modification/unique
        facts. Listing uses the tree's current contents without network transport or
        service-specific validation.

        Example:
            >>> client = _FakeFtpClient(_tree())
            >>> [name for name, facts in client.mlsd("/library/books")]
            ['one.epub', 'two.mobi']


        :param target: Directory path relative to current state or absolute, defaulting to the current directory.
        :return: Generator of immediate names and synthetic string facts; invalid targets raise FileNotFoundError during iteration.
        """
        base = self._normalize(target)
        node = self._tree.get(base)
        if node is None or node.node_type != "dir":
            raise FileNotFoundError(base)
        prefix = "/" if base == "/" else base.rstrip("/") + "/"
        seen: set[str] = set()
        for path, one in sorted(self._tree.items()):
            if path == base or not path.startswith(prefix):
                continue
            remainder = path[len(prefix):]
            if "/" in remainder:
                name = remainder.split("/", 1)[0]
                if name in seen:
                    continue
                seen.add(name)
                yield name, {"type": "dir"}
                continue
            name = remainder
            seen.add(name)
            facts = {"type": "dir" if one.node_type == "dir" else "file"}
            if one.node_type == "file":
                facts["size"] = str(one.size)
                if one.modified:
                    facts["modify"] = one.modified
                if one.unique:
                    facts["unique"] = one.unique
            yield name, facts

    def nlst(self, target: str = "."):
        """
        Materialize only the names from this fake client MLSD result.

        Example:
            >>> _FakeFtpClient(_tree()).nlst("/library/books")
            ['one.epub', 'two.mobi']


        :param target: Directory target passed unchanged to mlsd, defaulting to the current directory.
        :return: List of names; this base fake NLST depends on MLSD rather than independently modeling its fallback protocol.
        """
        return [name for name, _facts in self.mlsd(target)]

    def size(self, target: str):
        """
        Return the declared size of an explicit file node without measuring payload bytes.

        Example:
            >>> _FakeFtpClient(_tree()).size("/library/books/one.epub")
            7


        :param target: File path normalized against the current working directory and shared tree.
        :return: Node size field, or FileNotFoundError for an absent/non-file entry.
        """
        path = self._normalize(target)
        node = self._tree.get(path)
        if node is None or node.node_type != "file":
            raise FileNotFoundError(path)
        return node.size

    def retrbinary(self, cmd: str, callback, blocksize: int = 8192, rest=None):
        """
        Record a retrieval and deliver the payload suffix in one callback invocation.

        The command is split at its first space but its verb is not validated. REST is
        integer-converted and used as a Python slice offset; blocksize is ignored. Missing/non-file
        nodes raise an FTP 550 reply. Callback exceptions propagate after the retrieval record has
        been appended.

        Example:
            >>> client = _FakeFtpClient(_tree())
            >>> chunks = []
            >>> client.retrbinary("RETR /library/books/one.epub", chunks.append, rest=3)
            '226 Transfer complete'
            >>> chunks
            [b'BOOK']


        :param cmd: Command text containing a verb and tree path separated by one space.
        :param callback: Callable receiving all payload bytes from the selected offset as one chunk.
        :param blocksize: Client-compatible chunk-size argument deliberately ignored.
        :param rest: Optional integer-convertible slice offset; falsey values select zero.
        :return: Fixed "226 Transfer complete" after the callback returns normally.
        """
        del blocksize
        _verb, rel = cmd.split(" ", 1)
        path = self._normalize(rel)
        node = self._tree.get(path)
        if node is None or node.node_type != "file":
            raise ftplib.error_perm("550 File not found")
        start = int(rest or 0)
        self.retrievals.append((path, start or None))
        callback(node.payload[start:])
        return "226 Transfer complete"

    def quit(self):
        """
        Mark the client closed without sending QUIT or disabling its other fake methods.

        Example:
            >>> client = _FakeFtpClient(_tree())
            >>> client.quit()
            >>> client.closed
            True


        :return: None after setting closed to True.
        """
        self.closed = True

    def close(self):
        """
        Set the same closure marker used by quit without clearing tree or working-directory state.

        Example:
            >>> client = _FakeFtpClient(_tree())
            >>> client.close()
            >>> client.closed
            True


        :return: None after setting closed to True.
        """
        self.closed = True

    def _normalize(self, path: str) -> str:
        """
        Resolve fake POSIX path components relative to cwd, collapsing dots and clamping parent
        traversal at root.

        Example:
            >>> client = _FakeFtpClient(_tree())
            >>> client.cwd("/library/books")
            >>> client._normalize("../readme.txt")
            '/library/readme.txt'


        :param path: Absolute or relative path; empty text and dot preserve the current working directory.
        :return: Normalized absolute path without checking whether a tree node exists.
        """
        if path in {"", "."}:
            return self._cwd
        if path.startswith("/"):
            base = path
        else:
            base = self._cwd.rstrip("/") + "/" + path if self._cwd != "/" else "/" + path
        parts = []
        for part in base.split("/"):
            if part in {"", "."}:
                continue
            if part == "..":
                if parts:
                    parts.pop()
                continue
            parts.append(part)
        return "/" + "/".join(parts)


def _tree() -> dict[str, _Node]:
    """
    Build a fresh library tree containing two book files and one readme with fixed declared facts.

    Example:
        >>> tree = _tree()
        >>> tree["/library/books/one.epub"].payload
        b'ONEBOOK'


    :return: New absolute-path dictionary and mutable nodes used by ordinary FTP behavior tests.
    """
    return {
        "/": _Node("dir"),
        "/library": _Node("dir"),
        "/library/books": _Node("dir"),
        "/library/books/one.epub": _Node(
            "file",
            size=7,
            payload=b"ONEBOOK",
            modified="20260816100000",
            unique="ftp-v1",
        ),
        "/library/books/two.mobi": _Node("file", size=3, payload=b"TWO"),
        "/library/readme.txt": _Node("file", size=6, payload=b"README"),
    }


def _make_store(
    *,
    scheme: str = "ftp",
    clients: list[_FakeFtpClient] | None = None,
    tree: dict[str, _Node] | None = None,
):
    """
    Configure an unstarted FTP-family Store with a fresh-client factory over a shared test tree.

    None selects the standard tree; an explicitly supplied empty mapping is retained. The closure
    creates clients only when operations request them and optionally appends them to a caller list.
    The URL supplies fixed test credentials and root; all transport remains simulated.

    Example:
        >>> clients = []
        >>> store = _make_store(clients=clients)
        >>> len(clients), store.configuration.store_root_uri
        (0, 'ftp://example.com/library/')


    :param scheme: Scheme inserted into the test URL, normally ftp or ftps.
    :param clients: Optional list receiving each newly created fake client for setup/cleanup inspection.
    :param tree: Shared custom absolute-path mapping, or None to create the standard library tree.
    :return: FtpReadOnlyStorageBackend using the selected in-memory tree and per-operation fake clients.
    """
    selected_tree = _tree() if tree is None else tree

    def _factory():
        """
        Create one fake client over the retained tree and optionally append it to the caller
        recorder.

        Example:
            >>> client = store.options.client_factory()  # doctest: +SKIP


        :return: New _FakeFtpClient with independent working-directory/setup state and shared node data.
        """
        client = _FakeFtpClient(selected_tree)
        if clients is not None:
            clients.append(client)
        return client

    return FtpReadOnlyStorageBackend(
        url=f"{scheme}://user:pass@example.com/library",
        options=FtpBackendOptions(client_factory=_factory),
    )


def test_ftp_backend_preserves_unicode_names_uris_hints_and_bytes() -> None:
    """
    Preserve Unicode keys, filename/version hints, exact payload bytes, and encoded URI round trips.

    The source is an injected memory tree. The characteristic assertions describe read-only
    publication and write-space requirements; they do not assert that retrieval avoids local
    spooling.

    Example:
        >>> test_ftp_backend_preserves_unicode_names_uris_hints_and_bytes()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    tree = {
        "/": _Node("dir"),
        "/library": _Node("dir"),
        f"/library/{UNICODE_DIRECTORY}": _Node("dir"),
        f"/library/{UNICODE_KEY}": _Node(
            "file",
            size=len(UNICODE_PAYLOAD),
            payload=UNICODE_PAYLOAD,
            modified="20260816100000",
            unique="unicode-v1",
        ),
    }
    store = _make_store(tree=tree)

    [location] = list(store.iter_locations())
    info = store.stat_file(location)

    assert location.key == UNICODE_KEY
    assert info.hints.suggested_filename == UNICODE_FILENAME
    assert info.version == "unicode-v1"
    assert store.read_file(info) == UNICODE_PAYLOAD
    assert store.characteristics.publication_model is StoragePublicationModel.READ_ONLY
    assert (
        store.characteristics.temporary_space
        is StorageTemporarySpaceRequirement.NONE
    )
    uri = store.location_uri(location)
    assert uri == f"ftp://example.com/library/{UNICODE_URL_KEY}"
    assert store.location_from_uri(uri) == location


def test_ftp_backend_reads_tortured_unicode_paths_without_normalizing_them() -> None:
    """
    Exercise the shared hostile-Unicode corpus through simulated FTP reads and URI round trips.

    Seed each case with its original spelling, then check the shared harness results against exact
    key and encoded-URI sets. No live FTP service is involved.

    Example:
        >>> test_ftp_backend_reads_tortured_unicode_paths_without_normalizing_them()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    tree = {
        "/": _Node("dir"),
        "/library": _Node("dir"),
    }
    for case in TORTURED_UNICODE_PATH_CASES:
        parent, _filename = case.key.rsplit("/", 1)
        tree[f"/library/{parent}"] = _Node("dir")
        tree[f"/library/{case.key}"] = _Node(
            "file",
            size=len(case.payload),
            payload=case.payload,
            unique=f"{case.case_id}-v1",
        )
    store = _make_store(tree=tree)

    results = exercise_unicode_path_cases(
        store,
        TORTURED_UNICODE_PATH_CASES,
        check_uri_round_trip=True,
    )

    assert {result.location.key for result in results} == {
        case.key for case in TORTURED_UNICODE_PATH_CASES
    }
    assert {result.uri for result in results} == {
        f"ftp://example.com/library/{case.url_key}"
        for case in TORTURED_UNICODE_PATH_CASES
    }


def test_ftp_unicode_object_ingests_end_to_end(tmp_path) -> None:
    """
    Copy a Unicode-named fake FTP object into a real filesystem Store through the ingest pipeline.

    Assert successful reporting, retained source key and original filename metadata, and bytes read
    back through the manager. Destination bytes are real local files; asset and replica metadata
    belongs to an in-memory manager.

    Example:
        >>> test_ftp_unicode_object_ingests_end_to_end(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary parent of the real filesystem destination used with in-memory manager metadata.
    :return: None after the stated regression assertions pass.
    """
    tree = {
        "/": _Node("dir"),
        "/library": _Node("dir"),
        f"/library/{UNICODE_DIRECTORY}": _Node("dir"),
        f"/library/{UNICODE_KEY}": _Node(
            "file",
            size=len(UNICODE_PAYLOAD),
            payload=UNICODE_PAYLOAD,
        ),
    }
    source = _make_store(tree=tree)
    destination = FilesystemStore(tmp_path / "ftp-ingest-destination")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    report = ingest_store(manager, source)

    assert report.ok and report.ingested_files == 1
    [item] = report.items
    assert item.source_info.location.key == UNICODE_KEY
    assert item.result.asset_record.metadata.original_name == UNICODE_FILENAME
    assert manager.read_file(item.result.asset_record) == UNICODE_PAYLOAD


def test_truncated_ftp_ingest_publishes_no_manager_state(tmp_path) -> None:
    """
    Reject a nominally completed short FTP transfer before destination or manager publication.

    The fake declares twelve bytes but delivers five. Verify a failed ingest report, its length
    diagnostic, and empty destination, asset, and replica inventories.

    Example:
        >>> test_truncated_ftp_ingest_publishes_no_manager_state(tmp_path)  # doctest: +SKIP


    :param tmp_path: Temporary parent of the filesystem destination checked for absence of published objects.
    :return: None after the stated regression assertions pass.
    """
    class _TruncatedTransfer(_FakeFtpClient):
        """
        Model a successful FTP reply after delivering only the short payload used by the ingest
        publication regression.

        Example:
            >>> client = _TruncatedTransfer(_tree())  # doctest: +SKIP
        """
        def retrbinary(self, cmd: str, callback, blocksize: int = 8192, rest=None):
            """
            Deliver five bytes regardless of command/range arguments, then report transfer
            completion.

            Example:
                >>> chunks = []
                >>> client.retrbinary("RETR books/one.epub", chunks.append)  # doctest: +SKIP
                >>> chunks == [b"short"]  # doctest: +SKIP
                True


            :param cmd: FTP command argument deliberately ignored by this pathological transfer fake.
            :param callback: Driver collector called with the deliberately supplied transfer chunk.
            :param blocksize: Client-compatible block-size argument ignored by this override.
            :param rest: Client-compatible restart offset ignored so the override always emits the same chunk.
            :return: Fixed "226 Transfer complete" if the driver callback returns normally.
            """
            del cmd, blocksize, rest
            callback(b"short")
            return "226 Transfer complete"

    tree = {
        "/": _Node("dir"),
        "/library": _Node("dir"),
        "/library/book.epub": _Node("file", size=12, payload=b"short"),
    }
    source = FtpReadOnlyStorageBackend(
        "ftp://example.test/library/",
        options=FtpBackendOptions(
            client_factory=lambda: _TruncatedTransfer(tree)
        ),
    )
    destination = FilesystemStore(tmp_path / "ftp-truncated-destination")
    manager = InMemoryStorageManager(
        store_registrations=((destination.configuration, destination),),
        default_store_ref=destination.store_ref,
    )

    report = ingest_store(manager, source)

    assert not report.ok and report.ingested_files == 0
    assert "wrong length" in report.failures[0].message
    assert tuple(manager.iter_digital_asset_records()) == ()
    assert tuple(manager.iter_replica_records()) == ()
    assert tuple(destination.iter_locations()) == ()


@pytest.mark.parametrize("control", ["line\nbreak.epub", "carriage\rreturn.epub", "tab\tname.epub"])
def test_ftp_backend_rejects_protocol_control_characters(control: str) -> None:
    """
    Reject newline, carriage-return, and tab characters while locating supplied FTP keys.

    Example:
        >>> test_ftp_backend_rejects_protocol_control_characters(control)  # doctest: +SKIP


    :param control: Parameterized relative filename containing one protocol control character.
    :return: None after the stated regression assertions pass.
    """
    store = _make_store()
    with pytest.raises(StorageInvalidAddress):
        store.locate(control)


def test_ftp_backend_iter_locations_returns_only_real_files() -> None:
    """
    Enumerate the three synthetic file entries in order with the configured Store identity.

    Directory nodes are omitted and the declared enumeration capability is complete. The historical
    test name refers to file entries in the memory tree, not live server files or a separately
    verified remote snapshot.

    Example:
        >>> test_ftp_backend_iter_locations_returns_only_real_files()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _make_store()

    locations = list(store.iter_locations())

    assert [location.key for location in locations] == [
        "books/one.epub",
        "books/two.mobi",
        "readme.txt",
    ]
    assert all(location.store_ref == store.store_ref for location in locations)
    assert store.capabilities.enumeration is EnumerationCompleteness.COMPLETE


def test_ftp_backend_stat_read_digest_and_ranges_follow_new_store_api() -> None:
    """
    Exercise Location compatibility, file metadata, full reads, and an exact selected byte range.

    Check size, version, and presence of modification time for the standard memory object. The
    digest assertion checks its 64-character representation length; it does not compare against an
    independently computed expected hash.

    Example:
        >>> test_ftp_backend_stat_read_digest_and_ranges_follow_new_store_api()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _make_store()
    location = store.locate("books/one.epub")

    assert isinstance(location, FtpReadOnlyStoreLocation)
    assert isinstance(location, Location)
    assert store.file_exists(location) is True
    info = store.stat_file(location)
    assert info.size == 7
    assert info.version == "ftp-v1"
    assert info.modified_at is not None
    assert store.read_file(info) == b"ONEBOOK"
    assert store.read_file(info, offset=3, length=2) == b"BO"
    assert len(store.compute_digest(info.location).value) == 64


def test_ftp_prefix_inventory_replaces_path_like_directory_locations() -> None:
    """
    Use an opaque key prefix to select both book files without inspecting a directory Location.

    Locating the prefix only constructs an identifier; the inventory operation performs the
    simulated directory traversal.

    Example:
        >>> test_ftp_prefix_inventory_replaces_path_like_directory_locations()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _make_store()
    prefix = store.locate("books")

    assert [location.key for location in store.iter_locations(prefix=prefix)] == [
        "books/one.epub",
        "books/two.mobi",
    ]


def test_ftp_backend_is_truthfully_read_only() -> None:
    """
    Require false write/delete capabilities and typed read-only rejection of byte publication and
    deletion.

    Example:
        >>> test_ftp_backend_is_truthfully_read_only()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _make_store()
    location = store.locate("books/one.epub")

    assert store.capabilities.create is False
    assert store.capabilities.replace is False
    assert store.capabilities.delete is False
    with pytest.raises(StoreReadOnly):
        store.store_bytes(b"abc", location=location)
    with pytest.raises(StoreReadOnly):
        store.delete_file(location)


def test_ftps_backend_enables_secure_data_channel() -> None:
    """
    Require successful read-only startup to request data-channel protection on the FTPS fake.

    The secured marker proves that prot_p was called. It does not establish TLS negotiation,
    certificate verification, or compatibility with a live endpoint.

    Example:
        >>> test_ftps_backend_enables_secure_data_channel()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    clients: list[_FakeFtpClient] = []
    store = _make_store(scheme="ftps", clients=clients)

    status = store.startup()

    assert status.available is True
    assert status.writable is False
    assert clients and clients[0].secured is True


def test_ftp_locate_accepts_owned_full_urls_but_never_exposes_credentials() -> None:
    """
    Accept alternate URI user information for the same endpoint while rendering public roots and
    object URLs without it.

    Assert exact object URI output and absence of the configured test username and password from the
    root URI. These checks cover those public values, rather than every possible diagnostic or
    arbitrary caller-supplied label.

    Example:
        >>> test_ftp_locate_accepts_owned_full_urls_but_never_exposes_credentials()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _make_store()

    location = store.locate(
        "ftp://different:credentials@example.com/library/books/two.mobi"
    )

    assert location.key == "books/two.mobi"
    assert store.driver.object_uri(
        store.driver.parse_object_address(location.key)
    ) == "ftp://example.com/library/books/two.mobi"
    assert "user" not in store.configuration.store_root_uri
    assert "pass" not in store.configuration.store_root_uri


@pytest.mark.parametrize(
    "invalid",
    [
        "../escape.epub",
        "/absolute.epub",
        "books//one.epub",
        "books/./one.epub",
        "books\\one.epub",
    ],
)
def test_ftp_backend_rejects_escaping_or_noncanonical_keys(invalid: str) -> None:
    """
    Reject parent traversal, absolute paths, empty/dot components, and backslashes in text keys.

    Example:
        >>> test_ftp_backend_rejects_escaping_or_noncanonical_keys(invalid)  # doctest: +SKIP


    :param invalid: Parameterized key containing one unsupported path form.
    :return: None after the stated regression assertions pass.
    """
    store = _make_store()
    with pytest.raises(StorageInvalidAddress):
        store.locate(invalid)


def test_ftp_backend_rejects_urls_from_another_endpoint_or_root() -> None:
    """
    Reject object URLs with a foreign host, an outside-root path, or a different FTP-family scheme.

    Example:
        >>> test_ftp_backend_rejects_urls_from_another_endpoint_or_root()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _make_store()
    for invalid in (
        "ftp://other.example/library/books/one.epub",
        "ftp://example.com/elsewhere/one.epub",
        "ftps://example.com/library/books/one.epub",
    ):
        with pytest.raises(StorageInvalidAddress):
            store.locate(invalid)


def test_ftp_missing_object_is_not_converted_to_empty_metadata() -> None:
    """
    Preserve typed not-found failure when the parent listing contains no matching file.

    Example:
        >>> test_ftp_missing_object_is_not_converted_to_empty_metadata()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = _make_store()
    with pytest.raises(StorageNotFound):
        store.stat_file("books/missing.epub")


def test_ftp_authentication_failure_remains_typed() -> None:
    """
    Translate the injected FTP 530 login rejection into StorageAuthenticationFailed during startup.

    Example:
        >>> test_ftp_authentication_failure_remains_typed()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    class _AuthFailure(_FakeFtpClient):
        """
        Force the connection setup path to receive an FTP authentication rejection.

        Example:
            >>> client = _AuthFailure(_tree())  # doctest: +SKIP
        """
        def login(self, user: str, passwd: str):
            """
            Raise the same FTP 530 login error for any supplied credentials.

            Example:
                >>> client.login("bad", "secret")  # doctest: +SKIP


            :param user: Username ignored because this fake always rejects login.
            :param passwd: Password ignored because this fake always rejects login.
            :return: Does not return; raises ftplib.error_perm with a 530 Login incorrect reply.
            """
            raise ftplib.error_perm("530 Login incorrect")

    store = FtpReadOnlyStorageBackend(
        "ftp://bad:secret@example.com/library",
        options=FtpBackendOptions(client_factory=lambda: _AuthFailure(_tree())),
    )

    with pytest.raises(StorageAuthenticationFailed):
        store.startup()


@pytest.mark.parametrize(
    "invalid",
    ["bad\ud800.epub", "folder/bad\udfff.epub"],
)
def test_ftp_rejects_unpaired_surrogate_object_paths(invalid: str) -> None:
    """
    Reject malformed Unicode in text keys with an address error before any remote operation.

    Example:
        >>> test_ftp_rejects_unpaired_surrogate_object_paths(invalid)  # doctest: +SKIP


    :param invalid: Parameterized relative key containing an unpaired high or low surrogate.
    :return: None after the stated regression assertions pass.
    """
    store = _make_store()

    with pytest.raises(StorageInvalidAddress, match="malformed Unicode"):
        store.locate(invalid)


def test_ftp_rejects_unpaired_surrogate_root_urls() -> None:
    """
    Reject a literal unpaired surrogate in a root URL during backend construction.

    Example:
        >>> test_ftp_rejects_unpaired_surrogate_root_urls()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    with pytest.raises(StorageInvalidAddress, match="malformed Unicode"):
        FtpReadOnlyStorageBackend(
            "ftp://example.test/library/\ud800/",
            options=FtpBackendOptions(client_factory=lambda: _FakeFtpClient(_tree())),
        )


@pytest.mark.parametrize(
    "root",
    [
        "ftp://example.test:not-a-port/library/",
        "ftp://example.test:99999/library/",
        "ftp://example.test/library/%GG/",
        "ftp://example.test/library/bad%00path/",
        "ftp://example.test/library/folder%5Cname/",
    ],
)
def test_ftp_rejects_malformed_root_url_encoding(root: str) -> None:
    """
    Reject invalid ports, malformed percent escapes, and decoded NUL or backslash root paths.

    Example:
        >>> test_ftp_rejects_malformed_root_url_encoding(root)  # doctest: +SKIP


    :param root: Parameterized FTP URL containing invalid authority or encoded-path input.
    :return: None after the stated regression assertions pass.
    """
    with pytest.raises(StorageInvalidAddress):
        FtpReadOnlyStorageBackend(
            root,
            options=FtpBackendOptions(client_factory=lambda: _FakeFtpClient(_tree())),
        )


def test_ftp_canonicalizes_idn_roots_and_matching_object_uris() -> None:
    """
    Render the internationalized root host as IDNA and accept an object URI using its Unicode
    spelling.

    Example:
        >>> test_ftp_canonicalizes_idn_roots_and_matching_object_uris()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    store = FtpReadOnlyStorageBackend(
        "ftp://例え.テスト/library/",
        options=FtpBackendOptions(client_factory=lambda: _FakeFtpClient(_tree())),
    )

    location = store.locate("ftp://例え.テスト/library/books/one.epub")

    assert store.configuration.store_root_uri.startswith(
        "ftp://xn--r8jz45g.xn--zckzah/library/"
    )
    assert location.key == "books/one.epub"


@pytest.mark.parametrize(
    "bad_name",
    ["bad\ud800.epub", "slash/name.epub", "back\\slash.epub", "line\nbreak.epub"],
)
def test_ftp_inventory_rejects_malformed_names_returned_by_the_server(
    bad_name: str,
) -> None:
    """
    Reject malformed Unicode, separators, or controls supplied as a single MLSD entry name.

    Example:
        >>> test_ftp_inventory_rejects_malformed_names_returned_by_the_server(bad_name)  # doctest: +SKIP


    :param bad_name: Parameterized malformed single-component filename yielded by the injected MLSD implementation.
    :return: None after the stated regression assertions pass.
    """
    class _MalformedListing(_FakeFtpClient):
        """
        Expose the enclosing parameterized malformed filename through an otherwise minimal MLSD
        result.

        Example:
            >>> client = _MalformedListing(_tree())  # doctest: +SKIP
        """
        def mlsd(self, target: str = "."):
            """
            Yield the captured malformed name with one-byte file facts, ignoring the requested
            directory.

            Example:
                >>> name, facts = next(client.mlsd())  # doctest: +SKIP


            :param target: Directory argument ignored so every request receives the enclosing malformed name.
            :return: Generator yielding one name/facts pair for validation by the real driver.
            """
            del target
            yield bad_name, {"type": "file", "size": "1"}

    store = FtpReadOnlyStorageBackend(
        "ftp://example.test/",
        options=FtpBackendOptions(
            client_factory=lambda: _MalformedListing(_tree())
        ),
    )

    with pytest.raises(StorageUnavailable, match="malformed"):
        list(store.driver.iter_inventory())


def test_ftp_inventory_ignores_protocol_self_entries_but_rejects_duplicates() -> None:
    """
    Ignore MLSD self/parent entries and reject repeated file names within one listing.

    The same fake client is reused, first without duplication and then with its duplicate flag
    enabled. Its close marker does not disable later fake calls.

    Example:
        >>> test_ftp_inventory_ignores_protocol_self_entries_but_rejects_duplicates()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    class _Listing(_FakeFtpClient):
        """
        Emit protocol self entries and one file, optionally repeating that file for duplicate
        detection.

        Example:
            >>> client = _Listing(_tree())  # doctest: +SKIP
            >>> client.duplicate = True  # doctest: +SKIP


        :ivar duplicate: Flag controlling whether the same book entry is emitted a second time.
        """
        duplicate = False

        def mlsd(self, target: str = "."):
            """
            Yield dot and parent markers followed by one book entry and its optional duplicate.

            Example:
                >>> names = [name for name, facts in client.mlsd()]  # doctest: +SKIP


            :param target: Directory argument ignored by the fixed synthetic listing.
            :return: Generator of three entries, or four when the duplicate flag is true.
            """
            del target
            yield ".", {"type": "cdir"}
            yield "..", {"type": "pdir"}
            yield "book.epub", {"type": "file", "size": "4"}
            if self.duplicate:
                yield "book.epub", {"type": "file", "size": "4"}

    client = _Listing(_tree())
    store = FtpReadOnlyStorageBackend(
        "ftp://example.test/",
        options=FtpBackendOptions(client_factory=lambda: client),
    )
    assert [
        str(entry.object_address)
        for entry in store.driver.iter_inventory()
    ] == ["book.epub"]

    client.duplicate = True
    with pytest.raises(StorageUnavailable, match="duplicate"):
        list(store.driver.iter_inventory())


def test_ftp_inventory_stops_pathological_directory_depth() -> None:
    """
    Raise a typed inventory failure before descending beyond the configured one-level recursion
    bound.

    Example:
        >>> test_ftp_inventory_stops_pathological_directory_depth()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    tree = {
        "/": _Node("dir"),
        "/one": _Node("dir"),
        "/one/two": _Node("dir"),
        "/one/two/book.epub": _Node("file", size=4, payload=b"book"),
    }
    store = FtpReadOnlyStorageBackend(
        "ftp://example.test/",
        options=FtpBackendOptions(
            client_factory=lambda: _FakeFtpClient(tree),
            max_inventory_depth=1,
        ),
    )

    with pytest.raises(StorageUnavailable, match="depth limit"):
        list(store.driver.iter_inventory())


def test_ftp_detects_a_successfully_completed_but_truncated_transfer() -> None:
    """
    Reject a two-byte RETR result despite its success reply when SIZE declares seven bytes.

    Example:
        >>> test_ftp_detects_a_successfully_completed_but_truncated_transfer()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    class _TruncatingClient(_FakeFtpClient):
        """
        Keep standard seven-byte SIZE evidence while shortening the delivered body to two bytes.

        Example:
            >>> client = _TruncatingClient(_tree())  # doctest: +SKIP
        """
        def retrbinary(self, cmd: str, callback, blocksize: int = 8192, rest=None):
            """
            Deliver only the two-byte prefix and return the nominally successful FTP completion
            reply.

            Example:
                >>> chunks = []
                >>> client.retrbinary("RETR books/one.epub", chunks.append)  # doctest: +SKIP
                >>> chunks == [b"ON"]  # doctest: +SKIP
                True


            :param cmd: FTP command argument deliberately ignored by this pathological transfer fake.
            :param callback: Driver collector called with the deliberately supplied transfer chunk.
            :param blocksize: Client-compatible block-size argument ignored by this override.
            :param rest: Client-compatible restart offset ignored so the override always emits the same chunk.
            :return: Fixed "226 Transfer complete" if the driver callback returns normally.
            """
            del cmd, blocksize, rest
            callback(b"ON")
            return "226 Transfer complete"

    store = FtpReadOnlyStorageBackend(
        "ftp://example.test/library",
        options=FtpBackendOptions(
            client_factory=lambda: _TruncatingClient(_tree())
        ),
    )
    address = store.driver.parse_object_address("books/one.epub")

    with pytest.raises(StorageUnavailable, match="wrong length"):
        store.driver.open_read(address)


def test_ftp_translates_mid_transfer_timeout_and_discards_staging() -> None:
    """
    Expose a typed timeout when the fake fails after delivering a partial chunk.

    The assertion checks the propagated error and its diagnostic. It does not directly inspect the
    spool cleanup named by the historical test function.

    Example:
        >>> test_ftp_translates_mid_transfer_timeout_and_discards_staging()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    class _TimingOutClient(_FakeFtpClient):
        """
        Model a transfer that delivers a chunk and then fails with a socket timeout.

        Example:
            >>> client = _TimingOutClient(_tree())  # doctest: +SKIP
        """
        def retrbinary(self, cmd: str, callback, blocksize: int = 8192, rest=None):
            """
            Deliver the fixed partial chunk before raising the simulated stalled-remote timeout.

            Example:
                >>> chunks = []
                >>> client.retrbinary("RETR books/one.epub", chunks.append)  # doctest: +SKIP
                >>> chunks == [b"partial"]  # doctest: +SKIP
                True


            :param cmd: FTP command argument deliberately ignored by this pathological transfer fake.
            :param callback: Driver collector called with the deliberately supplied transfer chunk.
            :param blocksize: Client-compatible block-size argument ignored by this override.
            :param rest: Client-compatible restart offset ignored so the override always emits the same chunk.
            :return: Does not return normally; raises socket.timeout after the callback, unless that callback fails first.
            """
            del cmd, blocksize, rest
            callback(b"partial")
            raise socket.timeout("remote stalled")

    store = FtpReadOnlyStorageBackend(
        "ftp://example.test/library",
        options=FtpBackendOptions(
            client_factory=lambda: _TimingOutClient(_tree())
        ),
    )
    address = store.driver.parse_object_address("books/one.epub")

    with pytest.raises(StorageTimeout, match="timed out"):
        store.driver.open_read(address)


def test_ftp_rejects_nonbyte_transfer_chunks() -> None:
    """
    Reject a string chunk delivered by retrbinary with a typed non-byte transfer failure.

    Example:
        >>> test_ftp_rejects_nonbyte_transfer_chunks()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    class _TextTransferClient(_FakeFtpClient):
        """
        Violate the retrbinary callback byte contract by delivering text through an otherwise
        ordinary fake client.

        Example:
            >>> client = _TextTransferClient(_tree())  # doctest: +SKIP
        """
        def retrbinary(self, cmd: str, callback, blocksize: int = 8192, rest=None):
            """
            Pass a string to the collector and report success only if that invalid chunk is
            accepted.

            Example:
                >>> chunks = []
                >>> client.retrbinary("RETR books/one.epub", chunks.append)  # doctest: +SKIP
                >>> chunks == ["not bytes"]  # doctest: +SKIP
                True


            :param cmd: FTP command argument deliberately ignored by this pathological transfer fake.
            :param callback: Driver collector called with the deliberately supplied transfer chunk.
            :param blocksize: Client-compatible block-size argument ignored by this override.
            :param rest: Client-compatible restart offset ignored so the override always emits the same chunk.
            :return: Fixed "226 Transfer complete" only if the callback returns; the real driver rejects the text chunk.
            """
            del cmd, blocksize, rest
            callback("not bytes")
            return "226 Transfer complete"

    store = FtpReadOnlyStorageBackend(
        "ftp://example.test/library",
        options=FtpBackendOptions(
            client_factory=lambda: _TextTransferClient(_tree())
        ),
    )

    with pytest.raises(StorageUnavailable, match="non-byte"):
        store.driver.open_read(
            store.driver.parse_object_address("books/one.epub")
        )


def test_ftp_nlst_fallback_validates_server_names() -> None:
    """
    Reject an unpaired surrogate returned by NLST after the client explicitly lacks MLSD support.

    Example:
        >>> test_ftp_nlst_fallback_validates_server_names()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    class _BadNlstClient(_FakeFtpClient):
        """
        Lack MLSD support and return malformed Unicode from the independent NLST override.

        Example:
            >>> client = _BadNlstClient(_tree())  # doctest: +SKIP
        """
        def mlsd(self, target: str = "."):
            """
            Reject MLSD immediately so the real driver selects its NLST fallback.

            Example:
                >>> client.mlsd()  # doctest: +SKIP


            :param target: Directory argument ignored because MLSD is always unsupported.
            :return: Does not return; raises NotImplementedError before producing any listing.
            """
            del target
            raise NotImplementedError

        def nlst(self, target: str = "."):
            """
            Return one filename containing an unpaired surrogate without delegating to MLSD.

            Example:
                >>> names = client.nlst()  # doctest: +SKIP


            :param target: Directory argument ignored by this malformed-name response.
            :return: Single-element list containing the malformed Unicode filename.
            """
            del target
            return ["bad\ud800.epub"]

    store = FtpReadOnlyStorageBackend(
        "ftp://example.test/",
        options=FtpBackendOptions(
            client_factory=lambda: _BadNlstClient(_tree())
        ),
    )

    with pytest.raises(StorageUnavailable, match="malformed Unicode"):
        list(store.driver.iter_inventory())


def test_ftp_inventory_enforces_per_directory_and_total_entry_limits() -> None:
    """
    Exercise directory and whole-inventory limits independently against a four-entry synthetic
    listing.

    A two-name directory bound fails first; with that bound raised, the two-entry total bound fails
    instead and retains the configured root in its diagnostic.

    Example:
        >>> test_ftp_inventory_enforces_per_directory_and_total_entry_limits()  # doctest: +SKIP


    :return: None after the stated regression assertions pass.
    """
    class _FloodingClient(_FakeFtpClient):
        """
        Supply four distinct file entries for independent directory and total inventory-bound
        checks.

        Example:
            >>> client = _FloodingClient(_tree())  # doctest: +SKIP
        """
        def mlsd(self, target: str = "."):
            """
            Yield four numbered one-byte file facts without looking at the backing tree.

            Example:
                >>> names = [name for name, facts in client.mlsd()]  # doctest: +SKIP


            :param target: Directory argument ignored by the fixed four-entry response.
            :return: Generator of book-0.epub through book-3.epub with file type and declared size one.
            """
            del target
            for index in range(4):
                yield f"book-{index}.epub", {"type": "file", "size": "1"}

    per_directory = FtpReadOnlyStorageBackend(
        "ftp://example.test/",
        options=FtpBackendOptions(
            client_factory=lambda: _FloodingClient(_tree()),
            max_directory_entries=2,
        ),
    )
    with pytest.raises(StorageUnavailable, match="per-directory entry limit"):
        list(per_directory.driver.iter_inventory())

    total = FtpReadOnlyStorageBackend(
        "ftp://example.test/",
        options=FtpBackendOptions(
            client_factory=lambda: _FloodingClient(_tree()),
            max_directory_entries=10,
            max_inventory_entries=2,
        ),
    )
    with pytest.raises(StorageUnavailable, match="inventory entry limit") as failure:
        list(total.driver.iter_inventory())
    assert "ftp://example.test/" in str(failure.value)
