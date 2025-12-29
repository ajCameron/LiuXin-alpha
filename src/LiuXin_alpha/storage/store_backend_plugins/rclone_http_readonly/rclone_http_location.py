from __future__ import annotations

import io
import os
import stat as statmod
from datetime import datetime
import pathlib
from typing import Any, Dict, Iterator, Self

from LiuXin_alpha.storage.api.location_api import SyncNativePretendAsyncLocation

from .rclone_utils import run_rclone_json, run_rclone, which_rclone


class _RcloneCatStream(io.RawIOBase):
    """A streaming binary reader backed by `rclone cat`.

    This is *not* seekable. It is designed to be used under a `with` block.
    """

    def __init__(self, proc: Any) -> None:
        self._proc = proc
        self._stdout = proc.stdout  # type: ignore[attr-defined]
        self._stderr = proc.stderr  # type: ignore[attr-defined]
        self._eof_checked = False
        super().__init__()

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False

    def writable(self) -> bool:
        return False

    def readinto(self, b: bytearray) -> int | None:
        data = self.read(len(b))
        if data is None:
            return None
        n = len(data)
        b[:n] = data
        return n

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            return b""
        if self._stdout is None:
            return b""
        chunk = self._stdout.read(size)
        if chunk == b"" and not self._eof_checked:
            self._eof_checked = True
            rc = self._proc.wait()
            if rc != 0:
                err = b""
                try:
                    err = self._stderr.read() if self._stderr else b""
                except Exception:
                    pass
                raise OSError(f"rclone cat failed (rc={rc}): {err.decode(errors='ignore').strip()}")
        return chunk

    def close(self) -> None:
        if self.closed:
            return
        try:
            if self._stdout:
                try:
                    self._stdout.close()
                except Exception:
                    pass
            if self._proc and getattr(self._proc, "poll", lambda: 0)() is None:
                try:
                    self._proc.terminate()
                except Exception:
                    pass
                try:
                    self._proc.wait(timeout=1)
                except Exception:
                    try:
                        self._proc.kill()
                    except Exception:
                        pass
            # drain stderr
            if self._stderr:
                try:
                    self._stderr.close()
                except Exception:
                    pass
        finally:
            super().close()


class RcloneHttpReadOnlyStoreLocation(SyncNativePretendAsyncLocation):
    """A Path-like Location over an rclone filesystem (HTTP remote is the primary target).

    This implements the *full* Location contract surface, but mutating methods
    raise PermissionError because HTTP is read-only.
    """

    def _fs_root(self) -> str:
        root = getattr(self.store, "url", None) or ""
        if not isinstance(root, str) or not root:
            raise ValueError("Store must provide a .url string usable as an rclone fs root")
        return root

    def _rel_posix(self) -> str:
        # Tokens are already normalized by StoreLocationMixinAPI, but we join here.
        toks = [t for t in self._normalized_tokens() if t not in (".", "")]
        return "/".join(toks)

    def _join(self, rel: str) -> str:
        base = self._fs_root()
        if not rel:
            return base
        if base.endswith(":"):
            return f"{base}{rel}"
        return f"{base.rstrip('/')}/{rel}"

    def _rclone_path(self) -> str:
        return self._join(self._rel_posix())

    def _rclone_dir(self) -> str:
        p = self._rclone_path()
        if not p.endswith("/"):
            p += "/"
        return p

    def as_store_key(self) -> str:
        return self._rclone_path()

    # --- stat / existence ---

    def _stat_blob(self) -> Dict[str, Any] | None:
        # rclone lsjson --stat returns a JSON object; raises on missing.
        p = self._rclone_path()
        try:
            blob = run_rclone_json(["lsjson", "--stat", p], rclone_exe=getattr(self.store, "options", None).rclone_exe if getattr(self.store, "options", None) else "rclone",
                                   extra_args=getattr(self.store, "options", None).rclone_args if getattr(self.store, "options", None) else (),
                                   env=getattr(self.store, "options", None).env if getattr(self.store, "options", None) else None,
                                   timeout_s=getattr(self.store, "options", None).timeout_s if getattr(self.store, "options", None) else 60.0,
                                   check=True)
            if isinstance(blob, dict):
                return blob
        except Exception:
            return None
        return None

    def exists(self) -> bool:
        return self._stat_blob() is not None

    def is_file(self) -> bool:
        b = self._stat_blob()
        if not b:
            return False
        return not bool(b.get("IsDir", False))

    def is_dir(self) -> bool:
        b = self._stat_blob()
        if not b:
            return False
        return bool(b.get("IsDir", False))

    def stat(self) -> os.stat_result:
        b = self._stat_blob()
        if not b:
            raise FileNotFoundError(self._rclone_path())
        size = int(b.get("Size") or 0)
        mt = b.get("ModTime") or ""
        try:
            # RFC3339-ish; Python handles offset.
            mtime = datetime.fromisoformat(mt.replace("Z", "+00:00")).timestamp() if mt else 0.0
        except Exception:
            mtime = 0.0
        is_dir = bool(b.get("IsDir", False))
        mode = (statmod.S_IFDIR if is_dir else statmod.S_IFREG) | 0o444
        return os.stat_result((mode, 0, 0, 1, 0, 0, size, mtime, mtime, mtime))

    # --- traversal ---

    def iterdir(self) -> Iterator[Self]:
        items = run_rclone_json(
            ["lsjson", self._rclone_dir()],
            rclone_exe=getattr(self.store, "options", None).rclone_exe if getattr(self.store, "options", None) else "rclone",
            extra_args=getattr(self.store, "options", None).rclone_args if getattr(self.store, "options", None) else (),
            env=getattr(self.store, "options", None).env if getattr(self.store, "options", None) else None,
            timeout_s=getattr(self.store, "options", None).timeout_s if getattr(self.store, "options", None) else 60.0,
            check=True,
        ) or []
        for it in items:
            name = it.get("Name")
            if not name:
                continue
            yield self.joinpath(name)  # type: ignore[return-value]

    def glob(self, pattern: str) -> Iterator[Self]:
        for child in self.iterdir():
            if pathlib.PurePosixPath(child.name).match(pattern):
                yield child

    def rglob(self, pattern: str) -> Iterator[Self]:
        items = run_rclone_json(
            ["lsjson", "-R", self._rclone_dir()],
            rclone_exe=getattr(self.store, "options", None).rclone_exe if getattr(self.store, "options", None) else "rclone",
            extra_args=getattr(self.store, "options", None).rclone_args if getattr(self.store, "options", None) else (),
            env=getattr(self.store, "options", None).env if getattr(self.store, "options", None) else None,
            timeout_s=getattr(self.store, "options", None).timeout_s if getattr(self.store, "options", None) else 60.0,
            check=True,
        ) or []
        base = self._rel_posix()
        for it in items:
            path = it.get("Path") or it.get("Name") or ""
            if not path:
                continue
            # Path is relative to the remote path we listed from.
            rel = pathlib.PurePosixPath(path)
            if rel.match(pattern):
                yield self.joinpath(*rel.parts)  # type: ignore[return-value]

    # --- mutation (blocked) ---

    def mkdir(self, mode: int = 0o777, parents: bool = False, exist_ok: bool = False) -> None:
        raise PermissionError("HTTP backend is read-only")

    def unlink(self, missing_ok: bool = False) -> None:
        raise PermissionError("HTTP backend is read-only")

    def rmdir(self) -> None:
        raise PermissionError("HTTP backend is read-only")

    def rename(self, target: str | os.PathLike[str]) -> Self:
        raise PermissionError("HTTP backend is read-only")

    def replace(self, target: str | os.PathLike[str]) -> Self:
        raise PermissionError("HTTP backend is read-only")

    def touch(self, mode: int = 0o666, exist_ok: bool = True) -> None:
        raise PermissionError("HTTP backend is read-only")

    # --- IO ---

    def open(
        self,
        mode: str = "r",
        buffering: int = -1,
        encoding: str | None = None,
        errors: str | None = None,
        newline: str | None = None,
    ) -> Any:
        # Read-only: allow r / rb only.
        if "w" in mode or "a" in mode or "+" in mode or "x" in mode:
            raise PermissionError("HTTP backend is read-only")
        binary = "b" in mode

        opts = getattr(self.store, "options", None)
        rclone_exe = getattr(opts, "rclone_exe", "rclone")
        rclone_args = list(getattr(opts, "rclone_args", ()))
        env = getattr(opts, "env", None)
        timeout_s = getattr(opts, "timeout_s", None)

        exe = which_rclone(rclone_exe)

        cmd = [exe, *rclone_args, "cat", self._rclone_path()]
        # Note: we do not implement buffering control here; Python will buffer in higher layers.
        import subprocess

        env_map = dict(os.environ)
        if env:
            env_map.update(dict(env))
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env_map)
        raw = _RcloneCatStream(proc)
        if binary:
            return io.BufferedReader(raw)
        return io.TextIOWrapper(io.BufferedReader(raw), encoding=encoding or "utf-8", errors=errors or "strict", newline=newline)
