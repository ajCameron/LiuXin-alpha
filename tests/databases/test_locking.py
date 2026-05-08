"""Tests for LiuXin_alpha.databases.locking.

Covers SHLock (shared/exclusive acquire/release, reentrant, blocking/non-blocking),
RWLockWrapper, DebugRWLockWrapper, SafeReadLock, create_locks, and wrap_simple.
"""
from __future__ import annotations

import threading
import time

import pytest

from LiuXin_alpha.databases.locking import (
    DowngradeLockError,
    LockingError,
    RWLockWrapper,
    SafeReadLock,
    SHLock,
    create_locks,
    wrap_simple,
)


# ---------------------------------------------------------------------------
# SHLock – shared mode
# ---------------------------------------------------------------------------


class TestSHLockShared:
    def test_acquire_and_release_shared(self) -> None:
        lock = SHLock()
        assert lock.acquire(shared=True) is True
        assert lock.is_shared == 1
        lock.release()
        assert lock.is_shared == 0

    def test_multiple_threads_can_hold_shared_lock(self) -> None:
        """Two threads should both be able to hold a shared lock simultaneously."""
        lock = SHLock()
        ready = threading.Event()
        both_acquired = threading.Event()
        errors: list[Exception] = []

        def _thread() -> None:
            try:
                lock.acquire(shared=True)
                ready.set()
                both_acquired.wait(timeout=2)
                lock.release()
            except Exception as exc:
                errors.append(exc)

        t = threading.Thread(target=_thread)
        t.start()
        ready.wait(timeout=2)

        # Main thread also acquires shared
        assert lock.acquire(shared=True) is True
        both_acquired.set()
        lock.release()
        t.join(timeout=2)
        assert not errors

    def test_reentrant_shared_lock_same_thread(self) -> None:
        lock = SHLock()
        lock.acquire(shared=True)
        lock.acquire(shared=True)  # reentrant – should succeed
        assert lock.is_shared == 2
        lock.release()
        lock.release()
        assert lock.is_shared == 0

    def test_release_unheld_shared_lock_raises(self) -> None:
        lock = SHLock()
        with pytest.raises(LockingError):
            lock.release()


# ---------------------------------------------------------------------------
# SHLock – exclusive mode
# ---------------------------------------------------------------------------


class TestSHLockExclusive:
    def test_acquire_and_release_exclusive(self) -> None:
        lock = SHLock()
        assert lock.acquire(shared=False) is True
        assert lock.is_exclusive == 1
        lock.release()
        assert lock.is_exclusive == 0

    def test_reentrant_exclusive_same_thread(self) -> None:
        lock = SHLock()
        lock.acquire(shared=False)
        lock.acquire(shared=False)  # reentrant
        assert lock.is_exclusive == 2
        lock.release()
        lock.release()
        assert lock.is_exclusive == 0

    def test_exclusive_blocks_second_thread(self) -> None:
        """A thread holding an exclusive lock should block another thread."""
        lock = SHLock()
        acquired_in_thread = threading.Event()
        unblock = threading.Event()
        second_acquired = threading.Event()

        def _main_holder() -> None:
            lock.acquire(shared=False)
            acquired_in_thread.set()
            unblock.wait(timeout=3)
            lock.release()

        def _waiter() -> None:
            acquired_in_thread.wait(timeout=3)
            lock.acquire(shared=False)
            second_acquired.set()
            lock.release()

        t1 = threading.Thread(target=_main_holder)
        t2 = threading.Thread(target=_waiter)
        t1.start()
        t2.start()

        acquired_in_thread.wait(timeout=2)
        # t2 should be blocked on the exclusive lock
        assert not second_acquired.is_set()
        unblock.set()
        second_acquired.wait(timeout=2)
        assert second_acquired.is_set()
        t1.join(timeout=2)
        t2.join(timeout=2)

    def test_nonblocking_exclusive_fails_when_another_thread_holds_shared(self) -> None:
        """Non-blocking exclusive acquire should return False when a different thread holds shared."""
        lock = SHLock()
        ready = threading.Event()
        release = threading.Event()

        def _holder() -> None:
            lock.acquire(shared=True)
            ready.set()
            release.wait(timeout=3)
            lock.release()

        t = threading.Thread(target=_holder)
        t.start()
        ready.wait(timeout=2)

        try:
            result = lock.acquire(shared=False, blocking=False)
            assert result is False
        finally:
            release.set()
            t.join(timeout=2)

    def test_downgrade_raises(self) -> None:
        lock = SHLock()
        lock.acquire(shared=False)
        try:
            with pytest.raises(LockingError):
                # Trying to acquire shared while holding exclusive should raise
                lock.acquire(shared=True)
        finally:
            lock.release()

    def test_upgrade_raises(self) -> None:
        lock = SHLock()
        lock.acquire(shared=True)
        try:
            with pytest.raises(LockingError):
                # Trying to acquire exclusive while holding shared (same thread) raises
                lock.acquire(shared=False)
        finally:
            lock.release()

    def test_release_unheld_exclusive_lock_raises(self) -> None:
        lock = SHLock()
        with pytest.raises(LockingError):
            lock.release()

    def test_owns_lock_exclusive(self) -> None:
        lock = SHLock()
        lock.acquire(shared=False)
        assert lock.owns_lock() is True
        lock.release()
        assert lock.owns_lock() is False

    def test_owns_lock_shared(self) -> None:
        lock = SHLock()
        lock.acquire(shared=True)
        assert lock.owns_lock() is True
        lock.release()
        assert lock.owns_lock() is False


# ---------------------------------------------------------------------------
# RWLockWrapper
# ---------------------------------------------------------------------------


class TestRWLockWrapper:
    def test_shared_wrapper_context_manager(self) -> None:
        shlock = SHLock()
        wrapper = RWLockWrapper(shlock, is_shared=True)
        with wrapper:
            assert shlock.is_shared == 1
        assert shlock.is_shared == 0

    def test_exclusive_wrapper_context_manager(self) -> None:
        shlock = SHLock()
        wrapper = RWLockWrapper(shlock, is_shared=False)
        with wrapper:
            assert shlock.is_exclusive == 1
        assert shlock.is_exclusive == 0

    def test_owns_lock(self) -> None:
        shlock = SHLock()
        wrapper = RWLockWrapper(shlock, is_shared=True)
        assert not wrapper.owns_lock()
        wrapper.acquire()
        assert wrapper.owns_lock()
        wrapper.release()
        assert not wrapper.owns_lock()


# ---------------------------------------------------------------------------
# SafeReadLock
# ---------------------------------------------------------------------------


class TestSafeReadLock:
    def test_acquire_and_release_in_context_manager(self) -> None:
        shlock = SHLock()
        read_lock = RWLockWrapper(shlock, is_shared=True)
        safe = SafeReadLock(read_lock)
        with safe:
            assert shlock.is_shared == 1
        assert shlock.is_shared == 0

    def test_acquire_returns_self(self) -> None:
        shlock = SHLock()
        read_lock = RWLockWrapper(shlock, is_shared=True)
        safe = SafeReadLock(read_lock)
        result = safe.acquire()
        assert result is safe
        safe.release()

    def test_suppresses_downgrade_lock_error(self) -> None:
        """SafeReadLock should silently swallow DowngradeLockError."""

        class _AlwaysDowngrade:
            def acquire(self) -> None:
                raise DowngradeLockError("test downgrade")

            def release(self) -> None:
                pass

        safe = SafeReadLock(_AlwaysDowngrade())
        safe.acquire()  # must not raise
        # acquired should be False since downgrade was swallowed
        assert safe.acquired is False

    def test_release_only_if_acquired(self) -> None:
        """Release must be idempotent when acquired=False."""

        class _AlwaysDowngrade:
            def acquire(self) -> None:
                raise DowngradeLockError("test")

            def release(self) -> None:
                raise AssertionError("release should not be called")

        safe = SafeReadLock(_AlwaysDowngrade())
        safe.acquire()
        safe.release()  # must not call underlying release (acquired==False)


# ---------------------------------------------------------------------------
# create_locks
# ---------------------------------------------------------------------------


class TestCreateLocks:
    def test_returns_two_wrappers(self) -> None:
        read_lock, write_lock = create_locks()
        assert isinstance(read_lock, RWLockWrapper)
        assert isinstance(write_lock, RWLockWrapper)

    def test_read_lock_is_shared(self) -> None:
        read_lock, _ = create_locks()
        assert read_lock._is_shared is True

    def test_write_lock_is_exclusive(self) -> None:
        _, write_lock = create_locks()
        assert write_lock._is_shared is False

    def test_both_locks_share_same_underlying_shlock(self) -> None:
        read_lock, write_lock = create_locks()
        assert read_lock._shlock is write_lock._shlock

    def test_read_lock_context_manager_works(self) -> None:
        read_lock, _ = create_locks()
        with read_lock:
            assert read_lock._shlock.is_shared == 1
        assert read_lock._shlock.is_shared == 0

    def test_write_lock_context_manager_works(self) -> None:
        _, write_lock = create_locks()
        with write_lock:
            assert write_lock._shlock.is_exclusive == 1
        assert write_lock._shlock.is_exclusive == 0


# ---------------------------------------------------------------------------
# wrap_simple
# ---------------------------------------------------------------------------


class TestWrapSimple:
    def test_wrapped_function_called_under_lock(self) -> None:
        from threading import Lock

        call_log: list[str] = []
        lock = Lock()

        def _fn(x: int, y: int) -> int:
            call_log.append("called")
            return x + y

        wrapped = wrap_simple(lock, _fn)
        result = wrapped(2, 3)
        assert result == 5
        assert call_log == ["called"]

    def test_wrapped_function_preserves_name(self) -> None:
        from threading import Lock

        def _my_function() -> None:
            pass

        wrapped = wrap_simple(Lock(), _my_function)
        assert wrapped.__name__ == "_my_function"

    def test_downgrade_lock_error_reruns_without_lock(self) -> None:
        """When DowngradeLockError is raised, wrap_simple re-calls without locking."""
        call_count = [0]

        class _FakeLock:
            def __enter__(self) -> "_FakeLock":
                raise DowngradeLockError("test")

            def __exit__(self, *args) -> None:
                pass

        def _fn() -> str:
            call_count[0] += 1
            return "ok"

        wrapped = wrap_simple(_FakeLock(), _fn)
        result = wrapped()
        assert result == "ok"
        assert call_count[0] == 1

    def test_wrapped_function_raises_other_exceptions(self) -> None:
        from threading import Lock

        def _fn() -> None:
            raise ValueError("test error")

        wrapped = wrap_simple(Lock(), _fn)
        with pytest.raises(ValueError, match="test error"):
            wrapped()
