"""Tests for :mod:`LiuXin_alpha.utils.lock`.

The module provides cross-process file locking helpers.

We primarily test the POSIX implementation (the normal case for CI here).
For the Windows branch of ``singleinstance`` we use small fakes so the logic
is exercised without requiring pywin32.
"""

from __future__ import annotations

import errno
import os
import sys
import time
from multiprocessing import get_context
from pathlib import Path
from typing import Any, Callable, List

import pytest


def _import_lock_module():
    """Import helper.

    On Windows without pywin32, the module can raise at import-time.
    We skip rather than failing the entire suite.
    """

    try:
        from LiuXin_alpha.utils import lock as lock_mod
    except RuntimeError as e:
        pytest.skip(f"lock module unavailable in this environment: {e}")
    return lock_mod


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Uses POSIX fcntl semantics")
def test_unix_open_sets_cloexec_via_fcntl_when_no_speedup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lock = _import_lock_module()

    # Force the code-path where speedup provides no O_CLOEXEC.
    from LiuXin_alpha.utils import plugins as plugins_mod

    orig_getitem = type(plugins_mod.plugins).__getitem__

    def fake_getitem(self: Any, name: str):
        assert name == "speedup"
        return None, "no-speedup"

    monkeypatch.setattr(type(plugins_mod.plugins), "__getitem__", fake_getitem)

    calls: List[tuple] = []
    real_fcntl = lock.fcntl.fcntl

    def spy_fcntl(fd: int, op: int, arg: int):
        calls.append((fd, op, arg))
        return real_fcntl(fd, op, arg)

    monkeypatch.setattr(lock.fcntl, "fcntl", spy_fcntl)

    p = tmp_path / "a.lock"
    f = lock.unix_open(str(p))
    try:
        assert p.exists()
        assert f.mode == "rb+"
        # When not using O_CLOEXEC at open time, the code must set it via fcntl.
        assert any(op == lock.fcntl.F_SETFD and arg == lock.fcntl.FD_CLOEXEC for _, op, arg in calls)
    finally:
        f.close()

    # Restore class method to avoid leaking to other tests.
    monkeypatch.setattr(type(plugins_mod.plugins), "__getitem__", orig_getitem)


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Uses POSIX fcntl semantics")
def test_unix_open_uses_speedup_o_cloexec_when_available(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lock = _import_lock_module()

    from LiuXin_alpha.utils import plugins as plugins_mod

    class Speedup:
        # Use the platform constant when present; otherwise pick a plausible bit.
        O_CLOEXEC = getattr(os, "O_CLOEXEC", 0x80000)

    orig_getitem = type(plugins_mod.plugins).__getitem__

    def fake_getitem(self: Any, name: str):
        assert name == "speedup"
        return Speedup(), None

    monkeypatch.setattr(type(plugins_mod.plugins), "__getitem__", fake_getitem)

    calls: List[tuple] = []
    real_fcntl = lock.fcntl.fcntl

    def spy_fcntl(fd: int, op: int, arg: int):
        calls.append((fd, op, arg))
        return real_fcntl(fd, op, arg)

    monkeypatch.setattr(lock.fcntl, "fcntl", spy_fcntl)

    p = tmp_path / "b.lock"
    f = lock.unix_open(str(p))
    try:
        assert p.exists()
        # When O_CLOEXEC is used at open time, the fcntl path should not be needed.
        assert not any(op == lock.fcntl.F_SETFD for _, op, _ in calls)
    finally:
        f.close()

    monkeypatch.setattr(type(plugins_mod.plugins), "__getitem__", orig_getitem)


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Uses POSIX fcntl semantics")
def test_unix_open_falls_back_when_kernel_rejects_o_cloexec(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lock = _import_lock_module()

    from LiuXin_alpha.utils import plugins as plugins_mod

    class Speedup:
        O_CLOEXEC = getattr(os, "O_CLOEXEC", 0x80000)

    orig_getitem = type(plugins_mod.plugins).__getitem__
    monkeypatch.setattr(type(plugins_mod.plugins), "__getitem__", lambda self, n: (Speedup(), None))

    real_os_open = lock.os.open
    seen: List[int] = []

    def flaky_open(path: str, flags: int, mode: int):
        # First call (with O_CLOEXEC) fails with EINVAL, subsequent call succeeds.
        seen.append(flags)
        if len(seen) == 1 and (flags & Speedup.O_CLOEXEC):
            raise OSError(errno.EINVAL, "EINVAL")
        return real_os_open(path, flags, mode)

    monkeypatch.setattr(lock.os, "open", flaky_open)

    calls: List[tuple] = []
    real_fcntl = lock.fcntl.fcntl
    monkeypatch.setattr(lock.fcntl, "fcntl", lambda fd, op, arg: (calls.append((fd, op, arg)) or real_fcntl(fd, op, arg)))

    p = tmp_path / "c.lock"
    f = lock.unix_open(str(p))
    try:
        assert len(seen) >= 2, "expected retry without O_CLOEXEC"
        assert any(op == lock.fcntl.F_SETFD and arg == lock.fcntl.FD_CLOEXEC for _, op, arg in calls)
    finally:
        f.close()

    monkeypatch.setattr(type(plugins_mod.plugins), "__getitem__", orig_getitem)


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Uses POSIX fcntl semantics")
def test_exclusivefile_allows_basic_read_write_and_reacquire(tmp_path: Path) -> None:
    lock = _import_lock_module()
    p = tmp_path / "data.bin"

    with lock.ExclusiveFile(str(p), timeout=1) as f:
        f.write(b"abc")
        f.flush()

    # After leaving the context, the lock and fd must be released.
    with lock.ExclusiveFile(str(p), timeout=1) as f2:
        f2.seek(0)
        assert f2.read() == b"abc"


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Uses POSIX fcntl semantics")
def test_exclusivefile_prevents_reentrant_lock_in_same_process(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lock = _import_lock_module()
    p = tmp_path / "reentrant.bin"

    # Make timeouts fast.
    monkeypatch.setattr(lock.time, "sleep", lambda _: None)

    with lock.ExclusiveFile(str(p), timeout=1):
        with pytest.raises(lock.LockError):
            with lock.ExclusiveFile(str(p), timeout=0):
                pass


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Uses POSIX fork-based locking semantics")
def test_exclusivefile_prevents_lock_in_other_process(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lock = _import_lock_module()
    p = tmp_path / "xproc.bin"

    ctx = get_context("fork")
    ready = ctx.Event()
    release = ctx.Event()

    def child() -> None:
        from LiuXin_alpha.utils.lock import ExclusiveFile

        with ExclusiveFile(str(p), timeout=1):
            ready.set()
            release.wait(10)

    proc = ctx.Process(target=child)
    proc.start()
    try:
        assert ready.wait(5), "child did not acquire lock in time"

        # Speed up the retry loop.
        monkeypatch.setattr(lock.time, "sleep", lambda _: None)

        with pytest.raises(lock.LockError):
            with lock.ExclusiveFile(str(p), timeout=0):
                pass
    finally:
        release.set()
        proc.join(5)
        if proc.is_alive():
            proc.terminate()


@pytest.mark.skipif(sys.platform.startswith("win"), reason="Uses POSIX fcntl semantics")
def test_singleinstance_posix_across_processes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lock = _import_lock_module()

    # Ensure the lock file lives under tmp_path rather than the user's home.
    monkeypatch.setattr(lock.os.path, "expanduser", lambda p: str(tmp_path / p[2:]))

    ctx = get_context("fork")
    parent_ok = lock.singleinstance("pytest")
    assert parent_ok is True

    q = ctx.Queue()

    def child() -> None:
        from LiuXin_alpha.utils.lock import singleinstance

        q.put(singleinstance("pytest"))

    proc = ctx.Process(target=child)
    proc.start()
    proc.join(5)
    if proc.is_alive():
        proc.terminate()
        pytest.fail("child process hung")

    child_ok = q.get_nowait()
    assert child_ok is False


def test_clean_lock_file_is_best_effort(tmp_path: Path) -> None:
    lock = _import_lock_module()

    p = tmp_path / "cleanme"
    p.write_text("x")
    f = open(p, "r")
    try:
        lock._clean_lock_file(f)
        # Should not raise even if close/remove fail.
        assert not os.path.exists(p)
    finally:
        try:
            f.close()
        except Exception:
            pass


def test_singleinstance_windows_branch_with_fakes(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise Windows singleinstance logic without pywin32.

    This is purely a unit test of control flow.
    """

    lock = _import_lock_module()

    # Force Windows branch.
    monkeypatch.setattr(lock, "iswindows", True)

    closed: List[Any] = []
    registered: List[tuple] = []

    class FakeWinError:
        ERROR_ALREADY_EXISTS = 183
        ERROR_INVALID_HANDLE = 6

    class FakeWin32API:
        def __init__(self) -> None:
            self._err = 0

        def GetLastError(self) -> int:
            return self._err

        def CloseHandle(self, h: Any) -> None:
            closed.append(h)

    class FakeWin32Event:
        def CreateMutex(self, *_args: Any, **_kwargs: Any) -> str:
            return "mutex"

    api = FakeWin32API()
    evt = FakeWin32Event()

    monkeypatch.setattr(lock, "winerror", FakeWinError)
    monkeypatch.setattr(lock, "win32api", api)
    monkeypatch.setattr(lock, "win32event", evt)
    monkeypatch.setattr(lock.atexit, "register", lambda fn, *a: registered.append((fn, a)))

    # Case 1: no prior instance
    api._err = 0
    assert lock.singleinstance("abc") is True
    assert registered, "expected CloseHandle to be registered"

    # Case 2: already exists
    registered.clear()
    closed.clear()
    api._err = FakeWinError.ERROR_ALREADY_EXISTS
    assert lock.singleinstance("abc") is False
    assert closed == ["mutex"]
