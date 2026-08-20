from __future__ import annotations

import ftplib

from dataclasses import dataclass

import pytest

from LiuXin_alpha.storage.api import (
    EnumerationCompleteness,
    Location,
    StorageAuthenticationFailed,
    StorageInvalidAddress,
    StorageNotFound,
    StoreReadOnly,
)
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


@dataclass(slots=True)
class _Node:
    node_type: str
    size: int = 0
    payload: bytes = b""
    modified: str | None = None
    unique: str | None = None


class _FakeFtpClient:
    def __init__(self, tree: dict[str, _Node]) -> None:
        self._tree = tree
        self._cwd = "/"
        self.connected = None
        self.logged_in = None
        self.passive = None
        self.secured = False
        self.closed = False
        self.retrievals: list[tuple[str, int | None]] = []

    def connect(self, host: str, port: int, timeout=None):
        self.connected = (host, port, timeout)
        return "ok"

    def login(self, user: str, passwd: str):
        self.logged_in = (user, passwd)
        return "ok"

    def set_pasv(self, passive: bool):
        self.passive = passive

    def prot_p(self):
        self.secured = True

    def cwd(self, path: str):
        normalized = self._normalize(path)
        node = self._tree.get(normalized)
        if node is None or node.node_type != "dir":
            raise FileNotFoundError(normalized)
        self._cwd = normalized

    def pwd(self) -> str:
        return self._cwd

    def voidcmd(self, cmd: str):
        return "200 OK"

    def mlsd(self, target: str = "."):
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
        return [name for name, _facts in self.mlsd(target)]

    def size(self, target: str):
        path = self._normalize(target)
        node = self._tree.get(path)
        if node is None or node.node_type != "file":
            raise FileNotFoundError(path)
        return node.size

    def retrbinary(self, cmd: str, callback, blocksize: int = 8192, rest=None):
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
        self.closed = True

    def close(self):
        self.closed = True

    def _normalize(self, path: str) -> str:
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
    selected_tree = _tree() if tree is None else tree

    def _factory():
        client = _FakeFtpClient(selected_tree)
        if clients is not None:
            clients.append(client)
        return client

    return FtpReadOnlyStorageBackend(
        url=f"{scheme}://user:pass@example.com/library",
        options=FtpBackendOptions(client_factory=_factory),
    )


def test_ftp_backend_preserves_unicode_names_uris_hints_and_bytes() -> None:
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
    uri = store.location_uri(location)
    assert uri == f"ftp://example.com/library/{UNICODE_URL_KEY}"
    assert store.location_from_uri(uri) == location


def test_ftp_backend_reads_tortured_unicode_paths_without_normalizing_them() -> None:
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

    discovered = {location.key: location for location in store.iter_locations()}

    assert set(discovered) == {case.key for case in TORTURED_UNICODE_PATH_CASES}
    for case in TORTURED_UNICODE_PATH_CASES:
        location = discovered[case.key]
        info = store.stat_file(location)
        uri = store.location_uri(location)
        assert info.hints.suggested_filename == case.filename
        assert store.read_file(info) == case.payload
        assert uri == f"ftp://example.com/library/{case.url_key}"
        assert store.location_from_uri(uri) == location


@pytest.mark.parametrize("control", ["line\nbreak.epub", "carriage\rreturn.epub", "tab\tname.epub"])
def test_ftp_backend_rejects_protocol_control_characters(control: str) -> None:
    store = _make_store()
    with pytest.raises(StorageInvalidAddress):
        store.locate(control)


def test_ftp_backend_iter_locations_returns_only_real_files() -> None:
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
    store = _make_store()
    prefix = store.locate("books")

    assert [location.key for location in store.iter_locations(prefix=prefix)] == [
        "books/one.epub",
        "books/two.mobi",
    ]


def test_ftp_backend_is_truthfully_read_only() -> None:
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
    clients: list[_FakeFtpClient] = []
    store = _make_store(scheme="ftps", clients=clients)

    status = store.startup()

    assert status.available is True
    assert status.writable is False
    assert clients and clients[0].secured is True


def test_ftp_locate_accepts_owned_full_urls_but_never_exposes_credentials() -> None:
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
    store = _make_store()
    with pytest.raises(StorageInvalidAddress):
        store.locate(invalid)


def test_ftp_backend_rejects_urls_from_another_endpoint_or_root() -> None:
    store = _make_store()
    for invalid in (
        "ftp://other.example/library/books/one.epub",
        "ftp://example.com/elsewhere/one.epub",
        "ftps://example.com/library/books/one.epub",
    ):
        with pytest.raises(StorageInvalidAddress):
            store.locate(invalid)


def test_ftp_missing_object_is_not_converted_to_empty_metadata() -> None:
    store = _make_store()
    with pytest.raises(StorageNotFound):
        store.stat_file("books/missing.epub")


def test_ftp_authentication_failure_remains_typed() -> None:
    class _AuthFailure(_FakeFtpClient):
        def login(self, user: str, passwd: str):
            raise ftplib.error_perm("530 Login incorrect")

    store = FtpReadOnlyStorageBackend(
        "ftp://bad:secret@example.com/library",
        options=FtpBackendOptions(client_factory=lambda: _AuthFailure(_tree())),
    )

    with pytest.raises(StorageAuthenticationFailed):
        store.startup()
