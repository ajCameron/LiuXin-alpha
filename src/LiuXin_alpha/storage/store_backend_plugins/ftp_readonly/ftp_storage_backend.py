"""Read-only FTP/FTPS storage plugin.

This plugin exposes one FTP-family root as `Location` objects. It is intentionally
read-only: it can enumerate, stat, and stream files, but all mutation methods fail.
"""

from __future__ import annotations

import ftplib
import hashlib
import posixpath
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator, Optional, Type
from urllib.parse import unquote, urlsplit

from LiuXin_alpha.storage.api import StoreCheckStatus, StoreLocationMixinAPI, StorePluginAPI, StoreStatus
from LiuXin_alpha.storage.single_file import SingleFileStatus
from LiuXin_alpha.utils.logging.event_logs.in_memory_list import InMemoryEventLog
from LiuXin_alpha.utils.text.safe_path_to_name import safe_path_to_name

from .ftp_location import FtpReadOnlyStoreLocation


@dataclass(slots=True)
class FtpBackendOptions:
    """Runtime options for FTP-family read-only stores."""

    timeout_s: float | None = 30.0
    passive: bool = True
    secure_data_channel: bool = True
    encoding: str = "utf-8"
    client_factory: Callable[[], Any] | None = None


class FtpReadOnlyStorageBackend(StorePluginAPI):
    """Read-only storage plugin over FTP or FTPS.

    Supported URL forms:
    - ftp://user:pass@example.com:21/path/root
    - ftps://user:pass@example.com:990/path/root

    The configured URL identifies the store root; all returned locations are
    relative to that root.
    """

    location_cls: Type[FtpReadOnlyStoreLocation] = FtpReadOnlyStoreLocation

    def __init__(
        self,
        url: str,
        *,
        name: Optional[str] = None,
        uuid: Optional[str] = None,
        options: FtpBackendOptions | None = None,
    ) -> None:
        super().__init__(url=url, name=name, uuid=uuid)
        self.options = options or FtpBackendOptions()
        self._event_log = InMemoryEventLog()
        parsed = urlsplit(url)
        if parsed.scheme not in {"ftp", "ftps"}:
            raise ValueError(f"Unsupported FTP-family scheme: {parsed.scheme!r}")
        if not parsed.hostname:
            raise ValueError("FTP store URL must include a hostname.")
        self._scheme = parsed.scheme
        self._host = parsed.hostname
        self._port = parsed.port or (990 if parsed.scheme == "ftps" else 21)
        self._username = unquote(parsed.username) if parsed.username is not None else "anonymous"
        self._password = unquote(parsed.password) if parsed.password is not None else "anonymous@"
        raw_root = parsed.path or "/"
        self._root_path = posixpath.normpath(raw_root)
        if raw_root.endswith("/") and self._root_path != "/":
            self._root_path += "/"

    def url_to_name(self, url: str) -> str:
        return safe_path_to_name(url)

    @property
    def root_path(self) -> str:
        return self._root_path

    def _default_client_factory(self) -> Any:
        if self._scheme == "ftps":
            return ftplib.FTP_TLS(timeout=self.options.timeout_s, encoding=self.options.encoding)
        return ftplib.FTP(timeout=self.options.timeout_s, encoding=self.options.encoding)

    @contextmanager
    def _connected_client(self):
        client = self.options.client_factory() if self.options.client_factory is not None else self._default_client_factory()
        try:
            connect = getattr(client, "connect", None)
            if callable(connect):
                connect(self._host, self._port, timeout=self.options.timeout_s)
            login = getattr(client, "login", None)
            if callable(login):
                login(self._username, self._password)
            set_pasv = getattr(client, "set_pasv", None)
            if callable(set_pasv):
                set_pasv(bool(self.options.passive))
            if self._scheme == "ftps" and bool(self.options.secure_data_channel):
                prot_p = getattr(client, "prot_p", None)
                if callable(prot_p):
                    prot_p()
            if self._root_path not in {"", "/", "."}:
                client.cwd(self._root_path)
            yield client
        finally:
            for closer_name in ("quit", "close"):
                closer = getattr(client, closer_name, None)
                if callable(closer):
                    try:
                        closer()
                    except Exception:
                        pass
                    break

    def _normalize_rel(self, file_identifier: str | StoreLocationMixinAPI) -> str:
        if isinstance(file_identifier, StoreLocationMixinAPI):
            if file_identifier.store is self:
                return file_identifier.as_posix().strip("/")
            file_identifier = file_identifier.file_url
        raw = str(file_identifier).strip()
        if not raw:
            return ""
        if raw.startswith(("ftp://", "ftps://")):
            base = self.url.rstrip("/")
            if raw == base:
                return ""
            if raw.startswith(base + "/"):
                raw = raw[len(base) + 1:]
            else:
                parsed = urlsplit(raw)
                raw = parsed.path.lstrip("/")
                root = self._root_path.strip("/")
                if root and raw.startswith(root + "/"):
                    raw = raw[len(root) + 1:]
                elif raw == root:
                    raw = ""
        raw = raw.replace("\\", "/").lstrip("/")
        if raw in {"", "."}:
            return ""
        normalized = posixpath.normpath(raw)
        if normalized == ".":
            return ""
        if normalized.startswith("../") or normalized == "..":
            raise ValueError("FTP locations must remain under the configured store root.")
        return normalized.strip("/")

    def _rel_parts(self, file_identifier: str | StoreLocationMixinAPI) -> list[str]:
        rel = self._normalize_rel(file_identifier)
        return [] if not rel else [part for part in rel.split("/") if part]

    def location(self, *tokens: str) -> FtpReadOnlyStoreLocation:
        return self.location_cls(*tokens, store=self)

    def locate(self, file_identifier: str | StoreLocationMixinAPI) -> FtpReadOnlyStoreLocation:
        return self.location(*self._rel_parts(file_identifier))

    def startup(self) -> StoreStatus:
        return self.self_test()

    def self_test(self) -> StoreStatus:
        cs = StoreCheckStatus(store_marker_file=True, read=False, write=False, update=False, sundry=False)
        good: str | bool = "unknown"
        try:
            with self._connected_client() as client:
                noop = getattr(client, "voidcmd", None)
                if callable(noop):
                    noop("NOOP")
                list(client.mlsd("."))
            cs.read = True
            cs.sundry = True
            good = "ok (read-only)"
        except Exception as exc:
            self._event_log.put(f"ftp self_test failed: {exc!r}")
            good = "unhealthy"
        return StoreStatus(
            name=self.name,
            uuid=self.uuid or self.name,
            url=self.url,
            file_count=None,
            store_free_space=None,
            check_status=cs,
            checked=bool(cs.read),
            good=good,
            event_log=self._event_log,
            details={
                "scheme": self._scheme,
                "host": self._host,
                "port": self._port,
                "root_path": self._root_path,
                "mode": "read_only",
            },
        )

    def status(self) -> StoreStatus:
        return self.self_test()

    def _list_dir(self, rel_dir: str) -> list[tuple[str, dict[str, str]]]:
        target = rel_dir or "."
        with self._connected_client() as client:
            try:
                return [(str(name), dict(facts)) for name, facts in client.mlsd(target)]
            except Exception:
                # Fallback for older servers lacking MLSD.
                names = list(client.nlst(target))
                out: list[tuple[str, dict[str, str]]] = []
                for raw_name in names:
                    name = raw_name.rstrip("/").split("/")[-1]
                    if not name or name in {".", ".."}:
                        continue
                    candidate = posixpath.join(rel_dir, name) if rel_dir else name
                    is_dir = False
                    current = client.pwd() if hasattr(client, "pwd") else None
                    try:
                        client.cwd(candidate)
                        is_dir = True
                    except Exception:
                        is_dir = False
                    finally:
                        if is_dir and current is not None:
                            try:
                                client.cwd(current)
                            except Exception:
                                pass
                    facts = {"type": "dir" if is_dir else "file"}
                    if not is_dir:
                        try:
                            size = client.size(candidate)
                        except Exception:
                            size = None
                        if size is not None:
                            facts["size"] = str(size)
                    out.append((name, facts))
                return out

    def _walk_entries(self, rel_dir: str = "") -> Iterator[tuple[str, dict[str, str]]]:
        for name, facts in self._list_dir(rel_dir):
            rel_path = posixpath.join(rel_dir, name) if rel_dir else name
            facts_type = str((facts or {}).get("type") or "").lower()
            normalized_facts = dict(facts)
            if facts_type in {"dir", "cdir", "pdir"}:
                normalized_facts["type"] = "dir"
                yield rel_path, normalized_facts
                yield from self._walk_entries(rel_path)
            else:
                normalized_facts["type"] = "file"
                yield rel_path, normalized_facts

    def _entry_for(self, file_identifier: str | StoreLocationMixinAPI) -> dict[str, str] | None:
        rel = self._normalize_rel(file_identifier)
        if rel == "":
            return {"type": "dir"}
        parent = posixpath.dirname(rel)
        name = posixpath.basename(rel)
        for child_name, facts in self._list_dir(parent):
            if child_name == name:
                facts_type = str((facts or {}).get("type") or "").lower()
                normalized = dict(facts)
                normalized["type"] = "dir" if facts_type in {"dir", "cdir", "pdir"} else "file"
                return normalized
        return None

    def _iter_children(self, file_identifier: str | StoreLocationMixinAPI):
        rel = self._normalize_rel(file_identifier)
        for name, _facts in self._list_dir(rel):
            child_rel = posixpath.join(rel, name) if rel else name
            yield self.locate(child_rel)

    def exists(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        return self._entry_for(file_identifier) is not None

    def file_size(self, file_identifier: str | StoreLocationMixinAPI) -> int | None:
        entry = self._entry_for(file_identifier)
        if entry is None or entry.get("type") == "dir":
            return None
        size = entry.get("size")
        return None if size in {None, ""} else int(size)

    def _compute_hash(self, file_identifier: str | StoreLocationMixinAPI) -> str:
        digest = hashlib.sha256()
        with self.locate(file_identifier).open("rb") as handle:
            while True:
                chunk = handle.read(1024 * 1024)
                if not chunk:
                    break
                digest.update(chunk)
        return digest.hexdigest()

    def stat(self, file_identifier: str | StoreLocationMixinAPI) -> SingleFileStatus:
        location = self.locate(file_identifier)
        exists = self.exists(location)
        size = self.file_size(location) or 0

        def _exists(_url: str) -> bool:
            return self.exists(location)

        def _size(_url: str) -> int:
            return self.file_size(location) or 0

        def _hash(_url: str) -> str:
            return self._compute_hash(location) if self.exists(location) and self.file_size(location) is not None else ""

        return SingleFileStatus(
            url=location.file_url,
            exists=exists,
            size=size,
            file_hash=_hash(location.file_url) if exists and self.file_size(location) is not None else "",
            check_exists_function=_exists,
            check_size_function=_size,
            check_hash_function=_hash,
        )

    def iter_locations(self) -> Iterator[FtpReadOnlyStoreLocation]:
        for rel_path, facts in self._walk_entries(""):
            if facts.get("type") == "file":
                yield self.locate(rel_path)

    def read_file_bytes(self, file_identifier: str | StoreLocationMixinAPI) -> bytes:
        rel = self._normalize_rel(file_identifier)
        if rel == "":
            raise IsADirectoryError(self.url)
        chunks: list[bytes] = []
        with self._connected_client() as client:
            client.retrbinary(f"RETR {rel}", chunks.append)
        return b"".join(chunks)

    def write_bytes(self, file_bytes: bytes, *, metadata=None, location: str | None = None) -> FtpReadOnlyStoreLocation:
        raise PermissionError("FtpReadOnlyStorageBackend is read-only.")

    def copy_within_plugin(
        self,
        src_location: str | StoreLocationMixinAPI,
        dst_location: str | StoreLocationMixinAPI,
    ) -> FtpReadOnlyStoreLocation:
        raise PermissionError("FtpReadOnlyStorageBackend is read-only.")

    def delete(self, file_identifier: str | StoreLocationMixinAPI) -> bool:
        raise PermissionError("FtpReadOnlyStorageBackend is read-only.")

    def update_bytes(
        self,
        file_identifier: str | StoreLocationMixinAPI,
        file_bytes: bytes,
        *,
        append: bool = False,
    ) -> bool:
        raise PermissionError("FtpReadOnlyStorageBackend is read-only.")
