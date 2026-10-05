"""
Check shared/exclusive locking, wrappers and safe-read behavior with bounded thread probes.

The module also contains lock-factory and function-wrapper tests. Thread cases
coordinate events with timeouts; event waits and timed joins are not all asserted,
so these tests are bounded observations rather than exhaustive scheduling proofs.

Example:
    Run with pytest::

        python -m pytest -q tests/databases/test_locking.py
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
    """
    Check shared lock counters, reentrancy and concurrent acquisition.

    Example:
        >>> TestSHLockShared().test_acquire_and_release_shared()
    """
    def test_acquire_and_release_shared(self) -> None:
        """
        Require shared acquisition to succeed and its count to return from one to zero on release.

        Example:
            >>> TestSHLockShared().test_acquire_and_release_shared()


        :return: None; failed expectations raise AssertionError.
        """
        lock = SHLock()
        assert lock.acquire(shared=True) is True
        assert lock.is_shared == 1
        lock.release()
        assert lock.is_shared == 0

    def test_multiple_threads_can_hold_shared_lock(self) -> None:
        """
        Acquire a shared lock in a worker and the main thread without a recorded worker exception.

        Uses two-second event waits and a timed join; timeout results and worker termination
        are not asserted.

        Example:
            Run with pytest::

                python -m pytest -q tests/databases/test_locking.py::TestSHLockShared::test_multiple_threads_can_hold_shared_lock


        :return: None; failed expectations raise AssertionError.
        """
        lock = SHLock()
        ready = threading.Event()
        both_acquired = threading.Event()
        errors: list[Exception] = []

        def _thread() -> None:
            """
            Hold shared access until the main thread signals or a two-second wait expires.

            Signals readiness after acquisition, releases afterward and records any Exception in
            the enclosing error list; release is not protected by finally.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/test_locking.py


            :return: None; updates enclosing events and possibly the error list.
            """
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
        """
        Acquire a shared lock twice on one thread and balance its count with two releases.

        Example:
            >>> TestSHLockShared().test_reentrant_shared_lock_same_thread()


        :return: None; failed expectations raise AssertionError.
        """
        lock = SHLock()
        lock.acquire(shared=True)
        lock.acquire(shared=True)  # reentrant – should succeed
        assert lock.is_shared == 2
        lock.release()
        lock.release()
        assert lock.is_shared == 0

    def test_release_unheld_shared_lock_raises(self) -> None:
        """
        Reject release of an unheld lock with LockingError.

        Example:
            >>> TestSHLockShared().test_release_unheld_shared_lock_raises()


        :return: None; failed expectations raise AssertionError.
        """
        lock = SHLock()
        with pytest.raises(LockingError):
            lock.release()


# ---------------------------------------------------------------------------
# SHLock – exclusive mode
# ---------------------------------------------------------------------------


class TestSHLockExclusive:
    """
    Check exclusive counters, contention, ownership and forbidden mode changes.

    Example:
        >>> TestSHLockExclusive().test_acquire_and_release_exclusive()
    """
    def test_acquire_and_release_exclusive(self) -> None:
        """
        Require exclusive acquisition to succeed and its count to return from one to zero on release.

        Example:
            >>> TestSHLockExclusive().test_acquire_and_release_exclusive()


        :return: None; failed expectations raise AssertionError.
        """
        lock = SHLock()
        assert lock.acquire(shared=False) is True
        assert lock.is_exclusive == 1
        lock.release()
        assert lock.is_exclusive == 0

    def test_reentrant_exclusive_same_thread(self) -> None:
        """
        Acquire an exclusive lock twice on one thread and balance both releases.

        Example:
            >>> TestSHLockExclusive().test_reentrant_exclusive_same_thread()


        :return: None; failed expectations raise AssertionError.
        """
        lock = SHLock()
        lock.acquire(shared=False)
        lock.acquire(shared=False)  # reentrant
        assert lock.is_exclusive == 2
        lock.release()
        lock.release()
        assert lock.is_exclusive == 0

    def test_exclusive_blocks_second_thread(self) -> None:
        """
        Observe an exclusive waiter before and after releasing the holder.

        Checks the acquisition event on both sides of the unblock signal and uses bounded
        waits/joins; it does not assert every wait result or thread termination.

        Example:
            Run with pytest::

                python -m pytest -q tests/databases/test_locking.py::TestSHLockExclusive::test_exclusive_blocks_second_thread


        :return: None; failed expectations raise AssertionError.
        """
        lock = SHLock()
        acquired_in_thread = threading.Event()
        unblock = threading.Event()
        second_acquired = threading.Event()

        def _main_holder() -> None:
            """
            Acquire exclusive access, signal readiness and release after a signal or three-second timeout.

            Uses the enclosing lock and events; exceptions are not captured in a result list.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/test_locking.py


            :return: None; controls the holder-ready event and lock state.
            """
            lock.acquire(shared=False)
            acquired_in_thread.set()
            unblock.wait(timeout=3)
            lock.release()

        def _waiter() -> None:
            """
            Wait up to three seconds for the holder, then acquire and release exclusive access.

            Sets second_acquired after obtaining the enclosing lock; the readiness-wait result
            is not checked.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/test_locking.py


            :return: None; signals successful waiter acquisition.
            """
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
        """
        Require a nonblocking exclusive attempt to return False while a worker holds shared access.

        Signals worker release in finally and joins with a timeout; the initial ready-wait
        result is not asserted.

        Example:
            Run with pytest::

                python -m pytest -q tests/databases/test_locking.py::TestSHLockExclusive::test_nonblocking_exclusive_fails_when_another_thread_holds_shared


        :return: None; failed expectations raise AssertionError.
        """
        lock = SHLock()
        ready = threading.Event()
        release = threading.Event()

        def _holder() -> None:
            """
            Hold shared access until signaled or a three-second release wait expires.

            Signals the enclosing ready event after acquisition and releases normally afterward.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/test_locking.py


            :return: None; updates readiness and the enclosing lock state.
            """
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
        """
        Reject a shared acquisition while the same thread holds exclusive access, then release it.

        Example:
            >>> TestSHLockExclusive().test_downgrade_raises()


        :return: None; failed expectations raise AssertionError.
        """
        lock = SHLock()
        lock.acquire(shared=False)
        with pytest.raises(LockingError):
            # Trying to acquire shared while holding exclusive should raise
            lock.acquire(shared=True)
        lock.release()

    def test_upgrade_raises(self) -> None:
        """
        Reject an exclusive acquisition while the same thread holds shared access, then release it.

        Example:
            >>> TestSHLockExclusive().test_upgrade_raises()


        :return: None; failed expectations raise AssertionError.
        """
        lock = SHLock()
        lock.acquire(shared=True)
        with pytest.raises(LockingError):
            # Trying to acquire exclusive while holding shared (same thread) raises
            lock.acquire(shared=False)
        lock.release()

    def test_release_unheld_exclusive_lock_raises(self) -> None:
        """
        Reject release when no lock mode is held.

        Example:
            >>> TestSHLockExclusive().test_release_unheld_exclusive_lock_raises()


        :return: None; failed expectations raise AssertionError.
        """
        lock = SHLock()
        with pytest.raises(LockingError):
            lock.release()

    def test_owns_lock_exclusive(self) -> None:
        """
        Check exclusive ownership before and after releasing the held lock.

        Example:
            >>> TestSHLockExclusive().test_owns_lock_exclusive()


        :return: None; failed expectations raise AssertionError.
        """
        lock = SHLock()
        lock.acquire(shared=False)
        assert lock.owns_lock() is True
        lock.release()
        assert lock.owns_lock() is False

    def test_owns_lock_shared(self) -> None:
        """
        Check shared ownership before and after releasing the held lock.

        Example:
            >>> TestSHLockExclusive().test_owns_lock_shared()


        :return: None; failed expectations raise AssertionError.
        """
        lock = SHLock()
        lock.acquire(shared=True)
        assert lock.owns_lock() is True
        lock.release()
        assert lock.owns_lock() is False


# ---------------------------------------------------------------------------
# RWLockWrapper
# ---------------------------------------------------------------------------


class TestRWLockWrapper:
    """
    Check wrapper context management and ownership forwarding.

    Example:
        >>> TestRWLockWrapper().test_shared_wrapper_context_manager()
    """
    def test_shared_wrapper_context_manager(self) -> None:
        """
        Acquire one shared hold within a wrapper context and release it on exit.

        Example:
            >>> TestRWLockWrapper().test_shared_wrapper_context_manager()


        :return: None; failed expectations raise AssertionError.
        """
        shlock = SHLock()
        wrapper = RWLockWrapper(shlock, is_shared=True)
        with wrapper:
            assert shlock.is_shared == 1
        assert shlock.is_shared == 0

    def test_exclusive_wrapper_context_manager(self) -> None:
        """
        Acquire one exclusive hold within a wrapper context and release it on exit.

        Example:
            >>> TestRWLockWrapper().test_exclusive_wrapper_context_manager()


        :return: None; failed expectations raise AssertionError.
        """
        shlock = SHLock()
        wrapper = RWLockWrapper(shlock, is_shared=False)
        with wrapper:
            assert shlock.is_exclusive == 1
        assert shlock.is_exclusive == 0

    def test_owns_lock(self) -> None:
        """
        Check wrapper ownership across an explicit shared acquire/release pair.

        Example:
            >>> TestRWLockWrapper().test_owns_lock()


        :return: None; failed expectations raise AssertionError.
        """
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
    """
    Check safe-read contexts, fluent acquire and suppressed downgrade failures.

    Example:
        >>> TestSafeReadLock().test_acquire_and_release_in_context_manager()
        (None, None, None)
    """
    def test_acquire_and_release_in_context_manager(self) -> None:
        """
        Acquire and release a shared hold through a SafeReadLock context.

        Example:
            >>> TestSafeReadLock().test_acquire_and_release_in_context_manager()
            (None, None, None)


        :return: None; failed expectations raise AssertionError.
        """
        shlock = SHLock()
        read_lock = RWLockWrapper(shlock, is_shared=True)
        safe = SafeReadLock(read_lock)
        with safe:
            assert shlock.is_shared == 1
        assert shlock.is_shared == 0

    def test_acquire_returns_self(self) -> None:
        """
        Require SafeReadLock.acquire to return the same safe wrapper, then release it.

        Example:
            >>> TestSafeReadLock().test_acquire_returns_self()


        :return: None; failed expectations raise AssertionError.
        """
        shlock = SHLock()
        read_lock = RWLockWrapper(shlock, is_shared=True)
        safe = SafeReadLock(read_lock)
        result = safe.acquire()
        assert result is safe
        safe.release()

    def test_suppresses_downgrade_lock_error(self) -> None:
        """
        Suppress DowngradeLockError and leave the safe wrapper marked unacquired.

        Example:
            >>> TestSafeReadLock().test_suppresses_downgrade_lock_error()


        :return: None; failed expectations raise AssertionError.
        """

        class _AlwaysDowngrade:
            """
            Provide an acquire method that always rejects downgrade and a harmless release.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/test_locking.py
            """
            def acquire(self) -> None:
                """
                Raise the test downgrade error without acquiring any underlying resource.

                Example:
                    Run the owning tests with pytest::

                        python -m pytest -q tests/databases/test_locking.py


                :return: Never returns; always raises DowngradeLockError.
                """
                raise DowngradeLockError("test downgrade")

            def release(self) -> None:
                """
                Do nothing when the downgrade-only stub is released.

                Example:
                    Run the owning tests with pytest::

                        python -m pytest -q tests/databases/test_locking.py


                :return: None; no state is changed.
                """
                pass

        safe = SafeReadLock(_AlwaysDowngrade())
        safe.acquire()  # must not raise
        # acquired should be False since downgrade was swallowed
        assert safe.acquired is False

    def test_release_only_if_acquired(self) -> None:
        """
        Avoid calling the underlying release after a suppressed downgrade failure.

        Example:
            >>> TestSafeReadLock().test_release_only_if_acquired()


        :return: None; failed expectations raise AssertionError.
        """

        class _AlwaysDowngrade:
            """
            Reject downgrade on acquire and fail if release is incorrectly forwarded.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/test_locking.py
            """
            def acquire(self) -> None:
                """
                Raise the test downgrade error without acquiring any underlying resource.

                Example:
                    Run the owning tests with pytest::

                        python -m pytest -q tests/databases/test_locking.py


                :return: Never returns; always raises DowngradeLockError.
                """
                raise DowngradeLockError("test")

            def release(self) -> None:
                """
                Fail if SafeReadLock forwards release despite never acquiring.

                Example:
                    Run the owning tests with pytest::

                        python -m pytest -q tests/databases/test_locking.py


                :return: Never returns; always raises AssertionError.
                """
                raise AssertionError("release should not be called")

        safe = SafeReadLock(_AlwaysDowngrade())
        safe.acquire()
        safe.release()  # must not call underlying release (acquired==False)


# ---------------------------------------------------------------------------
# create_locks
# ---------------------------------------------------------------------------


class TestCreateLocks:
    """
    Check the read/write wrappers produced around one shared lock.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_locking.py
    """
    def test_returns_two_wrappers(self) -> None:
        """
        Check that the lock factory returns two RWLockWrapper instances.

        Example:
            >>> TestCreateLocks().test_returns_two_wrappers()


        :return: None; failed expectations raise AssertionError.
        """
        read_lock, write_lock = create_locks()
        assert isinstance(read_lock, RWLockWrapper)
        assert isinstance(write_lock, RWLockWrapper)

    def test_read_lock_is_shared(self) -> None:
        """
        Check that the read wrapper is marked shared.

        Example:
            >>> TestCreateLocks().test_read_lock_is_shared()


        :return: None; failed expectations raise AssertionError.
        """
        read_lock, _ = create_locks()
        assert read_lock._is_shared is True

    def test_write_lock_is_exclusive(self) -> None:
        """
        Check that the write wrapper is marked exclusive.

        Example:
            >>> TestCreateLocks().test_write_lock_is_exclusive()


        :return: None; failed expectations raise AssertionError.
        """
        _, write_lock = create_locks()
        assert write_lock._is_shared is False

    def test_both_locks_share_same_underlying_shlock(self) -> None:
        """
        Check that both wrappers reference the identical underlying shared lock.

        Example:
            >>> TestCreateLocks().test_both_locks_share_same_underlying_shlock()


        :return: None; failed expectations raise AssertionError.
        """
        read_lock, write_lock = create_locks()
        assert read_lock._shlock is write_lock._shlock

    def test_read_lock_context_manager_works(self) -> None:
        """
        Check that entering and leaving the read context increments then clears the shared count.

        Example:
            >>> TestCreateLocks().test_read_lock_context_manager_works()


        :return: None; failed expectations raise AssertionError.
        """
        read_lock, _ = create_locks()
        with read_lock:
            assert read_lock._shlock.is_shared == 1
        assert read_lock._shlock.is_shared == 0

    def test_write_lock_context_manager_works(self) -> None:
        """
        Check that entering and leaving the write context increments then clears the exclusive count.

        Example:
            >>> TestCreateLocks().test_write_lock_context_manager_works()


        :return: None; failed expectations raise AssertionError.
        """
        _, write_lock = create_locks()
        with write_lock:
            assert write_lock._shlock.is_exclusive == 1
        assert write_lock._shlock.is_exclusive == 0


# ---------------------------------------------------------------------------
# wrap_simple
# ---------------------------------------------------------------------------


class TestWrapSimple:
    """
    Check wrapped calls, preserved names, and lock-acquisition error handling.

    Example:
        Run the owning tests with pytest::

            python -m pytest -q tests/databases/test_locking.py
    """
    def test_wrapped_function_called_under_lock(self) -> None:
        """
        Check that a lock-wrapped sum returns five and records one invocation.

        Example:
            >>> TestWrapSimple().test_wrapped_function_called_under_lock()


        :return: None; failed expectations raise AssertionError.
        """
        from threading import Lock

        call_log: list[str] = []
        lock = Lock()

        def _fn(x: int, y: int) -> int:
            """
            Record one invocation in the enclosing list and add the operands.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/test_locking.py


            :param x: First operand, passed as two by this test.
            :param y: Second operand, passed as three by this test.
            :return: The sum of x and y.
            """
            call_log.append("called")
            return x + y

        wrapped = wrap_simple(lock, _fn)
        result = wrapped(2, 3)
        assert result == 5
        assert call_log == ["called"]

    def test_wrapped_function_preserves_name(self) -> None:
        """
        Check that the wrapper retains the wrapped function name.

        Example:
            >>> TestWrapSimple().test_wrapped_function_preserves_name()


        :return: None; failed expectations raise AssertionError.
        """
        from threading import Lock

        def _my_function() -> None:
            """
            Provide a named no-op for the wrapper-name assertion.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/test_locking.py


            :return: None.
            """
            pass

        wrapped = wrap_simple(Lock(), _my_function)
        assert wrapped.__name__ == "_my_function"

    def test_downgrade_lock_error_reruns_without_lock(self) -> None:
        """
        Check that failed downgrade acquisition falls back to one unlocked function call.

        Example:
            >>> TestWrapSimple().test_downgrade_lock_error_reruns_without_lock()


        :return: None; failed expectations raise AssertionError.
        """
        call_count = [0]

        class _FakeLock:
            """
            Provide a context manager whose entry always rejects a downgrade.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/test_locking.py
            """
            def __enter__(self) -> "_FakeLock":
                """
                Raise DowngradeLockError before the protected body is entered.

                Example:
                    Run the owning tests with pytest::

                        python -m pytest -q tests/databases/test_locking.py


                :return: Never returns normally; raises DowngradeLockError with the test message.
                """
                raise DowngradeLockError("test")

            def __exit__(self, *args) -> None:
                """
                Ignore context-exit arguments in the acquisition-failure double.

                Example:
                    Run the owning tests with pytest::

                        python -m pytest -q tests/databases/test_locking.py


                :param args: Context-exit arguments, accepted but unused; normal entry always
                    raises.
                :return: None; does not suppress an exception.
                """
                pass

        def _fn() -> str:
            """
            Increment the enclosing call counter after downgrade acquisition fails.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/test_locking.py


            :return: The string "ok".
            """
            call_count[0] += 1
            return "ok"

        wrapped = wrap_simple(_FakeLock(), _fn)
        result = wrapped()
        assert result == "ok"
        assert call_count[0] == 1

    def test_wrapped_function_raises_other_exceptions(self) -> None:
        """
        Check that a wrapped ValueError propagates with its original message.

        Example:
            >>> TestWrapSimple().test_wrapped_function_raises_other_exceptions()


        :return: None; failed expectations raise AssertionError.
        """
        from threading import Lock

        def _fn() -> None:
            """
            Raise the ValueError used to check wrapper exception propagation.

            Example:
                Run the owning tests with pytest::

                    python -m pytest -q tests/databases/test_locking.py


            :return: Never returns normally; raises ValueError with the test error message.
            """
            raise ValueError("test error")

        wrapped = wrap_simple(Lock(), _fn)
        with pytest.raises(ValueError, match="test error"):
            wrapped()
