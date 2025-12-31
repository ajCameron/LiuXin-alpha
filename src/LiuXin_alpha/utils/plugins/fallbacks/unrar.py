# -*- coding: utf-8 -*-
"""
Pure-python fallback for the compiled ``unrar`` extension.

The compiled extension reads RAR archives from a Python stream and exposes:
    - UNRARError exception
    - RARArchive(stream, stream_name, callback, get_comment=False)
        .comment : str
        .current_item() -> dict | None
        .process_item(extract: bool) -> object

This fallback shells out to an external extractor (preferably `unrar`) by first
spooling the stream to a temporary file. It is slower, but avoids compilation.

Notes:
- Requires `unrar` to be available on PATH.
- The header dict is intentionally minimal: it contains only the keys used by
  the existing Python wrapper in LiuXin/calibre.
"""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from typing import BinaryIO, Dict, List, Optional, Sequence


class UNRARError(Exception):
    pass


@dataclass
class _Item:
    name: str
    is_directory: bool = False
    is_symlink: bool = False
    is_label: bool = False
    has_password: bool = False


class RARArchive:
    def __init__(self, stream, stream_name: str, callback, get_comment: bool = False):
        self._stream = stream
        self._stream_name = stream_name
        self._callback = callback
        self._tmp = tempfile.NamedTemporaryFile(prefix="liuxin_rar_", suffix=".rar", delete=False)
        self._tmp_path = self._tmp.name
        self._tmp.close()

        self._items: List[_Item] = []
        self._idx = 0
        self.comment = ""

        try:
            self._spool_stream()
            if get_comment:
                self.comment = self._get_comment()
            self._items = self._list_items()
        except Exception as e:
            self.close()
            raise

    def close(self) -> None:
        try:
            os.remove(self._tmp_path)
        except Exception:
            pass

    def __del__(self) -> None:
        self.close()

    def _spool_stream(self) -> None:
        s = self._stream
        # Best-effort preserve position
        pos = None
        try:
            pos = s.tell()
        except Exception:
            pos = None
        try:
            try:
                s.seek(0)
            except Exception:
                pass
            with open(self._tmp_path, "wb") as f:
                while True:
                    chunk = s.read(1024 * 1024)
                    if not chunk:
                        break
                    if isinstance(chunk, str):
                        chunk = chunk.encode("utf-8", "ignore")
                    f.write(chunk)
        finally:
            if pos is not None:
                try:
                    s.seek(pos)
                except Exception:
                    pass

    def _require_unrar(self) -> str:
        exe = shutil.which("unrar")
        if exe:
            return exe
        exe = shutil.which("rar")
        if exe:
            return exe
        raise UNRARError("No `unrar` executable found on PATH (needed for RAR extraction).")

    def _run(self, args: Sequence[str], *, capture: bool = True) -> subprocess.CompletedProcess:
        try:
            return subprocess.run(
                list(args),
                check=False,
                stdout=subprocess.PIPE if capture else None,
                stderr=subprocess.PIPE if capture else None,
            )
        except OSError as e:
            raise UNRARError(str(e)) from e

    def _list_items(self) -> List[_Item]:
        exe = self._require_unrar()
        # `lb` prints only file names, one per line.
        cp = self._run([exe, "lb", "-p-", "-c-", self._tmp_path], capture=True)
        if cp.returncode != 0:
            msg = (cp.stderr or b"").decode("utf-8", "ignore").strip()
            raise UNRARError(msg or f"unrar failed with code {cp.returncode}")
        names = (cp.stdout or b"").decode("utf-8", "replace").splitlines()
        items: List[_Item] = []
        for nm in names:
            nm = nm.strip()
            if not nm:
                continue
            # Normalize to forward slashes
            nm_norm = nm.replace("\\", "/")
            is_dir = nm_norm.endswith("/")
            items.append(_Item(name=nm_norm, is_directory=is_dir))
        return items

    def _get_comment(self) -> str:
        # Comments are uncommon; best-effort: `vc` prints the comment if present
        try:
            exe = self._require_unrar()
            cp = self._run([exe, "vc", "-p-", "-c-", self._tmp_path], capture=True)
            if cp.returncode == 0:
                return (cp.stdout or b"").decode("utf-8", "ignore")
        except Exception:
            pass
        return ""

    def current_item(self) -> Optional[Dict[str, object]]:
        if self._idx >= len(self._items):
            return None
        it = self._items[self._idx]
        # Provide a dict compatible with the Python wrapper in LiuXin/calibre.
        # It expects raw bytes in "filename" and optional "filenamew" as bytes/None.
        return {
            "filename": it.name.encode("utf-8", "surrogatepass"),
            "filenamew": None,
            "is_directory": bool(it.is_directory),
            "is_symlink": bool(it.is_symlink),
            "is_label": bool(it.is_label),
            "has_password": bool(it.has_password),
        }

    def process_item(self, extract: bool) -> None:
        if self._idx >= len(self._items):
            return None
        it = self._items[self._idx]
        self._idx += 1
        if not extract or it.is_directory:
            return None

        exe = self._require_unrar()
        # `p` prints file to stdout
        cmd = [exe, "p", "-inul", "-p-", self._tmp_path, it.name]
        try:
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        except OSError as e:
            raise UNRARError(str(e)) from e

        assert p.stdout is not None
        try:
            while True:
                chunk = p.stdout.read(1024 * 256)
                if not chunk:
                    break
                try:
                    self._callback.handle_data(chunk)
                except Exception:
                    # If the callback blows up, terminate the child and re-raise
                    try:
                        p.kill()
                    except Exception:
                        pass
                    raise
        finally:
            # Collect exit status
            _, err = p.communicate()
            rc = p.returncode

        if rc != 0:
            msg = (err or b"").decode("utf-8", "ignore").strip()
            raise UNRARError(msg or f"unrar failed with code {rc}")
        return None
