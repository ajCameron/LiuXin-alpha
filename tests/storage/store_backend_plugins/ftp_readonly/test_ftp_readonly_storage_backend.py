from __future__ import annotations

from dataclasses import dataclass

from LiuXin_alpha.storage.store_backend_plugins.ftp_readonly import (
    FtpBackendOptions,
    FtpReadOnlyStorageBackend,
)
from LiuXin_alpha.storage.store_backend_plugins.ftp_readonly.ftp_location import (
    FtpReadOnlyStoreLocation,
)


@dataclass(slots=True)
class _Node:
    node_type: str
    size: int = 0
    payload: bytes = b""


class _FakeFtpClient:
    def __init__(self, tree: dict[str, _Node]) -> None:
        self._tree = tree
        self._cwd = "/"
        self.connected = None
        self.logged_in = None
        self.passive = None
        self.secured = False
        self.closed = False

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
            yield name, facts

    def nlst(self, target: str = "."):
        return [name for name, _facts in self.mlsd(target)]

    def size(self, target: str):
        path = self._normalize(target)
        node = self._tree.get(path)
        if node is None or node.node_type != "file":
            raise FileNotFoundError(path)
        return node.size

    def retrbinary(self, cmd: str, callback):
        _verb, rel = cmd.split(" ", 1)
        path = self._normalize(rel)
        node = self._tree.get(path)
        if node is None or node.node_type != "file":
            raise FileNotFoundError(path)
        callback(node.payload)
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


def _make_store(*, scheme: str = "ftp") -> FtpReadOnlyStorageBackend:
    tree = {
        "/": _Node("dir"),
        "/library": _Node("dir"),
        "/library/books": _Node("dir"),
        "/library/books/one.epub": _Node("file", size=3, payload=b"ONE"),
        "/library/books/two.mobi": _Node("file", size=3, payload=b"TWO"),
        "/library/readme.txt": _Node("file", size=6, payload=b"README"),
    }
    return FtpReadOnlyStorageBackend(
        url=f"{scheme}://user:pass@example.com/library",
        options=FtpBackendOptions(client_factory=lambda: _FakeFtpClient(tree)),
    )


def test_ftp_backend_iter_locations_returns_real_files() -> None:
    store = _make_store()

    urls = [loc.file_url for loc in store.iter_locations()]

    assert urls == [
        "ftp://user:pass@example.com/library/books/one.epub",
        "ftp://user:pass@example.com/library/books/two.mobi",
        "ftp://user:pass@example.com/library/readme.txt",
    ]


def test_ftp_backend_exists_stat_and_read_follow_new_plugin_api() -> None:
    store = _make_store()
    location = store.locate("books/one.epub")

    assert isinstance(location, FtpReadOnlyStoreLocation)
    assert store.exists(location) is True
    assert store.file_size(location) == 3
    assert location.as_bytes() == b"ONE"
    assert location.as_string() == "ONE"

    status = store.stat(location)
    assert status._exists is True
    assert status.size == 3
    assert len(status.hash) == 64


def test_ftp_location_iterdir_and_glob_work_from_store_root() -> None:
    store = _make_store()
    root = store.location()
    books = store.locate("books")

    assert [child.as_posix() for child in root.iterdir()] == ["books", "readme.txt"]
    assert [child.as_posix() for child in books.iterdir()] == ["books/one.epub", "books/two.mobi"]
    assert [loc.as_posix() for loc in root.rglob("*.mobi")] == ["books/two.mobi"]


def test_ftp_backend_is_read_only() -> None:
    store = _make_store()

    for action in (
        lambda: store.write_bytes(b"abc"),
        lambda: store.delete("books/one.epub"),
        lambda: store.update_bytes("books/one.epub", b"new"),
    ):
        try:
            action()
        except PermissionError:
            pass
        else:  # pragma: no cover
            raise AssertionError("Expected PermissionError")


def test_ftps_backend_enables_secure_data_channel() -> None:
    clients: list[_FakeFtpClient] = []

    def _factory():
        client = _FakeFtpClient({"/": _Node("dir"), "/library": _Node("dir")})
        clients.append(client)
        return client

    store = FtpReadOnlyStorageBackend(
        url="ftps://user:pass@example.com/library",
        options=FtpBackendOptions(client_factory=_factory),
    )

    status = store.startup()

    assert status.checked is True
    assert clients and clients[0].secured is True


def test_ftp_locate_accepts_full_urls_under_store_root() -> None:
    store = _make_store()

    location = store.locate("ftp://user:pass@example.com/library/books/two.mobi")

    assert location.as_posix() == "books/two.mobi"
    assert location.file_url.endswith("/books/two.mobi")
