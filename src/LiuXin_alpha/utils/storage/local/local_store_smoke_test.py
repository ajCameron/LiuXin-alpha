
"""
Class used to test to see if smoke comes out when testing local(ish) storage.
"""

from __future__ import annotations

import hashlib
import os
import shutil
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, Optional, Union


class StorageIOSmokeTest:
    """
    Functional smoke-test for local-ish disk IO (standard lib only).

    - Creates isolated run directory under `root/test_subdir/...`
    - Every filename includes `delete_me_tmp__{fuzz}__...` to avoid collisions + make cleanup obvious
    - Returns a report dict; optionally raises if strict=True and any check fails
    """

    def __init__(
        self,
        root: Union[str, os.PathLike[str]],
        test_subdir: str = "delete_me_tmp__io_smoketest",
        name_fuzz: Optional[str] = None,
        strict: bool = True,
        cleanup: bool = True,
        big_file_mb: int = 16,
        concurrent_files: int = 6,
        concurrent_file_mb: int = 2,
        random_access_mb: int = 8,
        chunk_size: int = 1024 * 1024,
    ) -> None:
        """
        Startup the class and define properties for the run.

        :param root:
        :param test_subdir:
        :param name_fuzz:
        :param strict:
        :param cleanup:
        :param big_file_mb:
        :param concurrent_files:
        :param concurrent_file_mb:
        :param random_access_mb:
        :param chunk_size:
        """
        self.root_path = Path(root).expanduser().resolve()
        self.test_subdir = test_subdir
        self.strict = strict
        self.cleanup = cleanup

        self.big_file_mb = max(1, big_file_mb)
        self.concurrent_files = max(1, concurrent_files)
        self.concurrent_file_mb = max(1, concurrent_file_mb)
        self.random_access_mb = max(1, random_access_mb)
        self.chunk_size = max(4 * 1024, chunk_size)

        fuzz = (name_fuzz or uuid.uuid4().hex[:12]).strip()
        fuzz = "".join(ch for ch in fuzz if ch.isalnum() or ch in ("-", "_")) or uuid.uuid4().hex[:12]
        self.fuzz = fuzz

        pid = os.getpid()
        stamp = int(time.time() * 1000)
        self.run_dir = self.root_path / self.test_subdir / f"delete_me_tmp__run__{self.fuzz}__{pid}__{stamp}"

        self.started = time.time()
        self.report: Dict[str, Any] = {
            "ok": True,
            "root": str(self.root_path),
            "test_dir": str(self.run_dir),
            "fuzz": self.fuzz,
            "started": self.started,
            "finished": None,
            "checks": [],
            "errors": None,
            "warnings": None,
        }

    # ---------- public API ----------

    def run(self) -> Dict[str, Any]:
        try:
            # writable_dir is foundational; if it fails, bail out early
            self.check_writable_dir()

            # remaining checks are individually guarded; failures accumulate in report
            self.check_small_text_roundtrip()
            self.check_small_binary_roundtrip()
            self.check_append_semantics()
            self.check_fsync_and_reopen()
            self.check_random_access_writes()
            self.check_atomic_replace()
            self.check_directory_ops()
            self.check_concurrent_writes_hashes()
            self.check_sequential_big_roundtrip()

        finally:
            self.report["finished"] = time.time()
            if self.cleanup:
                self._cleanup()

        if self.strict and not self.report["ok"]:
            failed = [c["name"] for c in self.report["checks"] if not c.get("ok", False)]
            raise IOError(
                f"Storage IO smoketest failed: {failed} (test_dir={self.report['test_dir']}, fuzz={self.fuzz})"
            )

        return self.report

    # ---------- check helpers (no nested functions) ----------

    def add_check(self, name: str, ok: bool, **details: Any) -> None:
        self.report["checks"].append({"name": name, "ok": ok, **details})
        if not ok:
            self.report["ok"] = False

    def fail(self, name: str, exc: BaseException, **details: Any) -> None:
        self.add_check(name, False, error=repr(exc), **details)

    def nm(self, stem: str, ext: str = "") -> str:
        # Every file name includes delete_me_tmp + fuzz
        return f"delete_me_tmp__{self.fuzz}__{stem}{ext}"

    def fsync_file(self, path: Path) -> None:
        with path.open("rb+") as f:
            f.flush()
            os.fsync(f.fileno())

    def fsync_dir(self, path: Path) -> None:
        # Best-effort: POSIX supports fsync on directories; Windows often doesn't.
        try:
            fd = os.open(str(path), os.O_RDONLY)
        except Exception:
            return
        try:
            os.fsync(fd)
        except Exception:
            pass
        finally:
            try:
                os.close(fd)
            except Exception:
                pass

    def sha256_file(self, path: Path) -> str:
        h = hashlib.sha256()
        with path.open("rb") as f:
            while True:
                b = f.read(self.chunk_size)
                if not b:
                    break
                h.update(b)
        return h.hexdigest()

    def _cleanup(self) -> None:
        try:
            shutil.rmtree(self.run_dir, ignore_errors=True)

            # Remove test_subdir only if empty (safe)
            parent = self.run_dir.parent  # root/test_subdir
            try:
                parent.rmdir()
            except OSError:
                pass

            try:
                (self.root_path / self.test_subdir).rmdir()
            except OSError:
                pass
        except Exception:
            # Cleanup errors shouldn't mask IO failures.
            pass

    # ---------- individual checks ----------

    def check_writable_dir(self) -> None:
        try:
            self.run_dir.mkdir(parents=True, exist_ok=False)
            probe = self.run_dir / self.nm("probe", ".txt")
            probe.write_text("ok", encoding="utf-8")
            probe.unlink()
            self.add_check("writable_dir", True)
        except Exception as e:
            self.fail("writable_dir", e)
            # foundational; re-raise so run() can bail early
            raise

    def check_small_text_roundtrip(self) -> None:
        name = "small_text_roundtrip"
        try:
            p = self.run_dir / self.nm("hello", ".txt")
            msg = "hello local disk\n"
            p.write_text(msg, encoding="utf-8")
            got = p.read_text(encoding="utf-8")
            self.add_check(name, got == msg)
        except Exception as e:
            self.fail(name, e)

    def check_small_binary_roundtrip(self) -> None:
        name = "small_binary_roundtrip"
        try:
            p = self.run_dir / self.nm("blob", ".bin")
            data = os.urandom(256 * 1024)  # 256 KiB
            p.write_bytes(data)
            got = p.read_bytes()
            self.add_check(name, got == data, size=len(data))
        except Exception as e:
            self.fail(name, e)

    def check_append_semantics(self) -> None:
        name = "append_semantics"
        try:
            p = self.run_dir / self.nm("append", ".txt")
            p.write_text("a\n", encoding="utf-8")
            with p.open("a", encoding="utf-8", newline="") as f:
                f.write("b\n")
                f.write("c\n")
            got = p.read_text(encoding="utf-8")
            self.add_check(name, got == "a\nb\nc\n")
        except Exception as e:
            self.fail(name, e)

    def check_fsync_and_reopen(self) -> None:
        name = "fsync_and_reopen"
        try:
            p = self.run_dir / self.nm("durable", ".bin")
            data = os.urandom(4 * 1024 * 1024)  # 4 MiB
            p.write_bytes(data)
            self.fsync_file(p)
            got = p.read_bytes()
            self.add_check(name, got == data, size=len(data))
        except Exception as e:
            self.fail(name, e)

    def check_random_access_writes(self) -> None:
        name = "random_access_writes"
        try:
            p = self.run_dir / self.nm("random_access", ".bin")
            size = self.random_access_mb * 1024 * 1024

            # create file (sparse-ish)
            with p.open("wb") as f:
                f.seek(size - 1)
                f.write(b"\0")
                f.flush()
                os.fsync(f.fileno())

            writes = [
                (0, b"HEAD"),
                (1024, os.urandom(64)),
                (size // 2, os.urandom(4096)),
                (size - 4, b"TAIL"),
            ]

            with p.open("rb+") as f:
                for off, blob in writes:
                    f.seek(off)
                    f.write(blob)
                f.flush()
                os.fsync(f.fileno())

            ok = True
            with p.open("rb") as f:
                for off, blob in writes:
                    f.seek(off)
                    got = f.read(len(blob))
                    if got != blob:
                        ok = False
                        break

            self.add_check(name, ok, size=size)
        except Exception as e:
            self.fail(name, e)

    def check_atomic_replace(self) -> None:
        name = "atomic_replace"
        try:
            target = self.run_dir / self.nm("atomic_target", ".txt")
            tmp = self.run_dir / self.nm("atomic_tmp", ".tmp")
            target.write_text("old", encoding="utf-8")
            tmp.write_text("new", encoding="utf-8")
            os.replace(tmp, target)  # atomic within filesystem
            ok = (target.read_text(encoding="utf-8") == "new") and (not tmp.exists())
            self.fsync_dir(self.run_dir)
            self.add_check(name, ok)
        except Exception as e:
            self.fail(name, e)

    def check_directory_ops(self) -> None:
        name = "directory_ops"
        try:
            d = self.run_dir / self.nm("nested_dir") / "dir" / "structure"
            d.mkdir(parents=True, exist_ok=True)
            a = d / self.nm("a", ".txt")
            b = d / self.nm("b", ".txt")
            a.write_text("A", encoding="utf-8")
            b.write_text("B", encoding="utf-8")

            names = sorted(p.name for p in d.iterdir() if p.is_file())
            ok = set(names) == {a.name, b.name}

            a.unlink()
            b.unlink()

            # best-effort cleanup of empty dirs
            for sub in (d, d.parent, d.parent.parent, d.parent.parent.parent):
                try:
                    sub.rmdir()
                except OSError:
                    pass

            self.add_check(name, ok)
        except Exception as e:
            self.fail(name, e)

    def _write_and_verify_concurrent(self, i: int) -> Dict[str, Any]:
        p = self.run_dir / self.nm(f"concurrent_{i}", ".bin")
        total = self.concurrent_file_mb * 1024 * 1024

        h_write = hashlib.sha256()
        with p.open("wb") as f:
            remaining = total
            while remaining > 0:
                take = min(self.chunk_size, remaining)
                buf = os.urandom(take)
                f.write(buf)
                h_write.update(buf)
                remaining -= take
            f.flush()
            os.fsync(f.fileno())

        h_read = hashlib.sha256()
        with p.open("rb") as f:
            while True:
                buf = f.read(self.chunk_size)
                if not buf:
                    break
                h_read.update(buf)

        return {
            "file": str(p),
            "write_hash": h_write.hexdigest(),
            "read_hash": h_read.hexdigest(),
            "ok": h_write.digest() == h_read.digest(),
        }

    def check_concurrent_writes_hashes(self) -> None:
        name = "concurrent_writes_hashes"
        try:
            results = []
            ok = True
            n = self.concurrent_files

            with ThreadPoolExecutor(max_workers=min(32, n)) as ex:
                futs = [ex.submit(self._write_and_verify_concurrent, i) for i in range(n)]
                for fut in as_completed(futs):
                    r = fut.result()
                    results.append(r)
                    if not r["ok"]:
                        ok = False

            self.add_check(
                name,
                ok,
                files=n,
                file_mb=self.concurrent_file_mb,
                details=results if not ok else None,
            )
        except Exception as e:
            self.fail(name, e)

    def check_sequential_big_roundtrip(self) -> None:
        name = "sequential_big_roundtrip"
        try:
            mb = self.big_file_mb
            total = mb * 1024 * 1024
            p = self.run_dir / self.nm(f"sequential_{mb}mb", ".bin")

            h_write = hashlib.sha256()
            t0 = time.time()
            with p.open("wb") as f:
                remaining = total
                while remaining > 0:
                    take = min(self.chunk_size, remaining)
                    buf = os.urandom(take)
                    f.write(buf)
                    h_write.update(buf)
                    remaining -= take
                f.flush()
                os.fsync(f.fileno())
            t_write = time.time() - t0

            t1 = time.time()
            h_read = hashlib.sha256()
            with p.open("rb") as f:
                while True:
                    buf = f.read(self.chunk_size)
                    if not buf:
                        break
                    h_read.update(buf)
            t_read = time.time() - t1

            ok = h_write.digest() == h_read.digest()
            self.add_check(
                name,
                ok,
                mb=mb,
                write_s=round(t_write, 4),
                read_s=round(t_read, 4),
            )
        except Exception as e:
            self.fail(name, e)
