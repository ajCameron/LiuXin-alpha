
"""
Provide local store smoke test utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise local store smoke test through a consuming regression::

        python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py
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

    Example:
        Exercise StorageIOSmokeTest through a consuming regression::

            python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py
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

        Example:
            Exercise StorageIOSmokeTest.  init   through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :param root: Root directory that bounds path resolution or traversal.
        :param test_subdir: Value supplied for test subdir under the utility contract.
        :param name_fuzz: Value supplied for name fuzz under the utility contract.
        :param strict: Value supplied for strict under the utility contract.
        :param cleanup: Value supplied for cleanup under the utility contract.
        :param big_file_mb: Value supplied for big file mb under the utility contract.
        :param concurrent_files: Value supplied for concurrent files under the utility
            contract.
        :param concurrent_file_mb: Value supplied for concurrent file mb under the utility
            contract.
        :param random_access_mb: Value supplied for random access mb under the utility
            contract.
        :param chunk_size: Value supplied for chunk size under the utility contract.
        :return: None; validated state is stored on the receiving object.
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
        """
        Perform the run utility operation under explicit compatibility rules.

        Example:
            Exercise StorageIOSmokeTest.run through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Add check under the documented compatibility and safety rules.

        Example:
            Exercise StorageIOSmokeTest.add check through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param ok: Value supplied for ok under the utility contract.
        :param details: Value supplied for details under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.report["checks"].append({"name": name, "ok": ok, **details})
        if not ok:
            self.report["ok"] = False

    def fail(self, name: str, exc: BaseException, **details: Any) -> None:
        """
        Perform the fail utility operation under explicit compatibility rules.

        Example:
            Exercise StorageIOSmokeTest.fail through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :param name: Field, file, function or resource name addressed by the operation.
        :param exc: Value supplied for exc under the utility contract.
        :param details: Value supplied for details under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.add_check(name, False, error=repr(exc), **details)

    def nm(self, stem: str, ext: str = "") -> str:
        # Every file name includes delete_me_tmp + fuzz
        """
        Perform the nm utility operation under explicit compatibility rules.

        Example:
            Exercise StorageIOSmokeTest.nm through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :param stem: Value supplied for stem under the utility contract.
        :param ext: Value supplied for ext under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return f"delete_me_tmp__{self.fuzz}__{stem}{ext}"

    def fsync_file(self, path: Path) -> None:
        """
        Perform the fsync file utility operation under explicit compatibility rules.

        Example:
            Exercise StorageIOSmokeTest.fsync file through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        with path.open("rb+") as f:
            f.flush()
            os.fsync(f.fileno())

    def fsync_dir(self, path: Path) -> None:
        # Best-effort: POSIX supports fsync on directories; Windows often doesn't.
        """
        Perform the fsync dir utility operation under explicit compatibility rules.

        Example:
            Exercise StorageIOSmokeTest.fsync dir through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
        """
        Perform the sha256 file utility operation under explicit compatibility rules.

        Example:
            Exercise StorageIOSmokeTest.sha256 file through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :param path: Filesystem path read, written, normalized or validated by the
            operation.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        h = hashlib.sha256()
        with path.open("rb") as f:
            while True:
                b = f.read(self.chunk_size)
                if not b:
                    break
                h.update(b)
        return h.hexdigest()

    def _cleanup(self) -> None:
        """
        Perform the cleanup utility operation under explicit compatibility rules.

        Example:
            Exercise StorageIOSmokeTest. cleanup through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
        """
        Perform the check writable dir utility operation under explicit compatibility rules.

        Example:
            Exercise StorageIOSmokeTest.check writable dir through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
        """
        Perform the check small text roundtrip utility operation under explicit compatibility rules.

        Example:
            Exercise StorageIOSmokeTest.check small text roundtrip through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
        """
        Perform the check small binary roundtrip utility operation under explicit compatibility rules.

        Example:
            Exercise StorageIOSmokeTest.check small binary roundtrip through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
        """
        Perform the check append semantics utility operation under explicit compatibility rules.

        Example:
            Exercise StorageIOSmokeTest.check append semantics through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
        """
        Perform the check fsync and reopen utility operation under explicit compatibility rules.

        Example:
            Exercise StorageIOSmokeTest.check fsync and reopen through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
        """
        Perform the check random access writes utility operation under explicit compatibility rules.

        Example:
            Exercise StorageIOSmokeTest.check random access writes through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
        """
        Perform the check atomic replace utility operation under explicit compatibility rules.

        Example:
            Exercise StorageIOSmokeTest.check atomic replace through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
        """
        Perform the check directory ops utility operation under explicit compatibility rules.

        Example:
            Exercise StorageIOSmokeTest.check directory ops through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
        """
        Perform the write and verify concurrent utility operation under explicit compatibility rules.

        Example:
            Exercise StorageIOSmokeTest. write and verify concurrent through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :param i: Value supplied for i under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
        """
        Perform the check concurrent writes hashes utility operation under explicit compatibility rules.

        Example:
            Exercise StorageIOSmokeTest.check concurrent writes hashes through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
        """
        Perform the check sequential big roundtrip utility operation under explicit compatibility rules.

        Example:
            Exercise StorageIOSmokeTest.check sequential big roundtrip through a consuming regression::

                python -m pytest -q tests/utils/storage/local/test_file_ops_smoke.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
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
