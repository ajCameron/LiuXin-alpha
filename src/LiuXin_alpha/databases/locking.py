#!/usr/bin/env python
# vim:fileencoding=UTF-8:ts=4:sw=4:sta:et:sts=4:ai

"""
Provide reentrant shared/exclusive thread locks and database lock adapters.

SHLock coordinates readers and writers in one process; it is separate from database transaction/file locks. Wrappers supply context-manager entry/exit, optional diagnostics and guarded shared access while a writer is already owned. wrap_simple copies the wrapped callable metadata with functools.wraps.
"""

# Todo: Move this over to utils? Not really database specific.

from __future__ import unicode_literals, division, absolute_import, print_function, annotations

import threading
import traceback
import sys
from functools import wraps
from threading import Lock, Condition, current_thread

from typing import ParamSpec, TypeVar, Callable, Any, Self, Union

from LiuXin_alpha.preferences import preferences as tweaks

__license__ = "GPL v3"
__copyright__ = "2011, Kovid Goyal <kovid@kovidgoyal.net>"
__docformat__ = "restructuredtext en"


class LockingError(RuntimeError):
    """
    Report invalid lock ownership or an unsupported mode transition.

    A RuntimeError carrying is_locking_error=True and optional locking_debug_msg context. The extra context is stored separately from the exception message.

    Example:
        >>> error = LockingError("unheld", extra="worker")
        >>> (str(error), error.locking_debug_msg)
        ('unheld', 'worker')
    """

    is_locking_error = True

    def __init__(self, msg, extra=None) -> None:
        """
        Store the runtime error message and optional lock diagnostics.

        Example:
            >>> LockingError("unheld").locking_debug_msg is None
            True


        :param msg: Message passed to RuntimeError.
        :param extra: Additional context stored unchanged as locking_debug_msg.
        :return: None.
        """
        RuntimeError.__init__(self, msg)
        self.locking_debug_msg = extra


class DowngradeLockError(LockingError):
    """
    Signal an attempt to acquire a shared lock while owning its exclusive mode.

    This LockingError subtype allows safe-read adapters to recognize an already-held writer. Shared-to-exclusive upgrades raise the base LockingError instead.

    Example:
        >>> issubclass(DowngradeLockError, LockingError)
        True
    """
    pass


def create_locks() \
        -> tuple[Union["DebugRWLockWrapper", "RWLockWrapper"], Union["DebugRWLockWrapper", "RWLockWrapper"]]:
    """
    Create read and write context-manager wrappers over one new SHLock.

    Select debug wrappers when newdb_debug_locking is enabled in preferences. Both modes are reentrant within one thread, but ordinary wrappers reject switching modes while the other mode is held. Release every acquisition before changing mode, or use the dedicated safe-read adapter when already holding the writer.

    Example:
        >>> read_lock, write_lock = create_locks()
        >>> read_lock._shlock is write_lock._shlock
        True


    :return: A (read_lock, write_lock) pair, sharing the same ownership counters and wait queues.
    """
    l = SHLock()
    wrapper = DebugRWLockWrapper if tweaks.get("newdb_debug_locking", False) else RWLockWrapper
    return wrapper(l), wrapper(l, is_shared=False)


class SHLock:  # {{{
    """
    Coordinate multiple readers or one writer with per-thread reentrancy.

    A thread may repeatedly acquire its current mode, with one release required per acquisition. Upgrades and downgrades are rejected. New readers wait behind queued writers; a departing writer hands ownership to all queued readers before the next writer. Queues manage handoff without a timeout API; these rules are not a formal starvation guarantee.

    Example:
        >>> lock = SHLock()
        >>> lock.acquire(shared=True)
        True
        >>> lock.is_shared
        1
        >>> lock.release()
    """

    def __init__(self) -> None:
        """
        Initialize an unlocked coordinator with empty ownership and waiter pools.

        Example:
            >>> lock = SHLock()
            >>> (lock.is_shared, lock.is_exclusive)
            (0, 0)


        :return: None; creates the internal mutex without acquiring either public mode.
        """
        self._lock = Lock()
        #  When a shared lock is held, is_shared will give the cumulative
        #  number of locks and _shared_owners maps each owning thread to
        #  the number of locks is holds.
        self.is_shared = 0
        self._shared_owners = {}
        #  When an exclusive lock is held, is_exclusive will give the number
        #  of locks held and _exclusive_owner will give the owning thread
        self.is_exclusive = 0
        self._exclusive_owner = None
        #  When someone is forced to wait for a lock, they add themselves
        #  to one of these queues along with a "waiter" condition that
        #  is used to wake them up.
        self._shared_queue = []
        self._exclusive_queue = []
        #  This is for recycling waiter objects.
        self._free_waiters = []

    def acquire(self, blocking: bool = True, shared: bool = False) -> bool:
        """
        Acquire shared or exclusive ownership for the current thread.

        Serialize ownership bookkeeping with the internal mutex, then delegate to the selected mode helper. Reentrant acquisition succeeds immediately. Mode-change errors still raise when blocking=False.

        Example:
            >>> lock = SHLock()
            >>> lock.acquire(blocking=False)
            True
            >>> lock.release()


        :param blocking: Whether to wait when another owner or an eligible queue prevents acquisition.
        :param shared: True for reader mode; False, the default, for exclusive writer mode.
        :return: True on acquisition, or False for a nonblocking contention failure.
        :raises DowngradeLockError: This thread owns the exclusive mode and requests shared mode.
        :raises LockingError: This thread owns shared mode and requests exclusive mode.
        """
        with self._lock:
            if shared:
                return self._acquire_shared(blocking)
            else:
                return self._acquire_exclusive(blocking)

    def owns_lock(self) -> bool:
        """
        Check whether the current thread holds either mode of this coordinator.

        Read ownership while holding the internal mutex; ownership by another thread is insufficient.

        Example:
            >>> SHLock().owns_lock()
            False


        :return: True for the exclusive owner or a member of the shared-owner map.
        """
        me = current_thread()
        with self._lock:
            return self._exclusive_owner is me or me in self._shared_owners

    def release(self) -> None:
        """
        Release one acquisition owned by the current thread and hand off if necessary.

        The last writer release grants ownership to all waiting readers, or the first waiting writer if no readers wait. The last reader release grants ownership to the first waiting writer. A remaining reentrant count keeps ownership without waking another group.

        Example:
            >>> lock = SHLock()
            >>> lock.acquire(shared=True)
            True
            >>> lock.release()
            >>> lock.owns_lock()
            False


        :return: None; repeated acquisitions require matching releases.
        :raises LockingError: No mode is held by the current thread.
        """
        me = current_thread()
        with self._lock:
            if self.is_exclusive:
                if self._exclusive_owner is not me:
                    raise LockingError("release() called on unheld lock")
                self.is_exclusive -= 1
                if not self.is_exclusive:
                    self._exclusive_owner = None
                    #  If there are waiting shared locks, issue them
                    #  all and them wake everyone up.
                    if self._shared_queue:
                        for (thread, waiter) in self._shared_queue:
                            self.is_shared += 1
                            self._shared_owners[thread] = 1
                            waiter.notify()
                        del self._shared_queue[:]
                    #  Otherwise, if there are waiting exclusive locks,
                    #  they get first dibbs on the lock.
                    elif self._exclusive_queue:
                        (thread, waiter) = self._exclusive_queue.pop(0)
                        self._exclusive_owner = thread
                        self.is_exclusive += 1
                        waiter.notify()
            elif self.is_shared:
                try:
                    self._shared_owners[me] -= 1
                    if self._shared_owners[me] == 0:
                        del self._shared_owners[me]
                except KeyError:
                    raise LockingError("release() called on unheld lock")
                self.is_shared -= 1
                if not self.is_shared:
                    #  If there are waiting exclusive locks,
                    #  they get first dibbs on the lock.
                    if self._exclusive_queue:
                        (thread, waiter) = self._exclusive_queue.pop(0)
                        self._exclusive_owner = thread
                        self.is_exclusive += 1
                        waiter.notify()
                    else:
                        assert not self._shared_queue
            else:
                raise LockingError("release() called on unheld lock")

    def _acquire_shared(self, blocking: bool = True) -> bool:
        """
        Acquire reader ownership while the caller holds the internal mutex.

        Existing readers may reenter despite queued writers. Other readers wait on a recycled condition and rely on release to assign their ownership before notification. Do not call this helper without self._lock held.

        Example:
            >>> lock = SHLock()
            >>> with lock._lock:
            ...     acquired = lock._acquire_shared(False)
            >>> acquired
            True
            >>> lock.release()


        :param blocking: Whether to queue and wait when a writer owns or is waiting for the lock.
        :return: True after acquiring or reentering reader mode; False for nonblocking contention.
        :raises DowngradeLockError: The calling thread already owns exclusive mode.
        """
        me = current_thread()
        #  Each case: acquiring a lock we already hold.
        if self.is_shared and me in self._shared_owners:
            self.is_shared += 1
            self._shared_owners[me] += 1
            return True
        #  If the lock is already spoken for by an exclusive, add us
        #  to the shared queue and it will give us the lock eventually.
        if self.is_exclusive or self._exclusive_queue:
            if self._exclusive_owner is me:
                raise DowngradeLockError("can't downgrade SHLock object")
            if not blocking:
                return False
            waiter = self._take_waiter()
            try:
                self._shared_queue.append((me, waiter))
                waiter.wait()
                assert not self.is_exclusive
            finally:
                self._return_waiter(waiter)
        else:
            self.is_shared += 1
            self._shared_owners[me] = 1
        return True

    def _acquire_exclusive(self, blocking: bool = True) -> bool:
        """
        Acquire writer ownership while the caller holds the internal mutex.

        Reenter for the current exclusive owner; otherwise reject a current reader before considering blocking. Waiting writers enter a FIFO queue and rely on release to assign ownership before notification.

        Example:
            >>> lock = SHLock()
            >>> with lock._lock:
            ...     acquired = lock._acquire_exclusive(False)
            >>> acquired
            True
            >>> lock.release()


        :param blocking: Whether to queue and wait for existing readers or another writer.
        :return: True after acquisition or reentry; False for nonblocking contention.
        :raises LockingError: The calling thread already owns shared mode.
        """
        me = current_thread()
        #  Each case: acquiring a lock we already hold.
        if self._exclusive_owner is me:
            assert self.is_exclusive
            self.is_exclusive += 1
            return True
        # Do not allow upgrade of lock
        if self.is_shared and me in self._shared_owners:
            raise LockingError("can't upgrade SHLock object")
        #  If the lock is already spoken for, add us to the exclusive queue.
        #  This will eventually give us the lock when it's our turn.
        if self.is_shared or self.is_exclusive:
            if not blocking:
                return False
            waiter = self._take_waiter()
            try:
                self._exclusive_queue.append((me, waiter))
                waiter.wait()
            finally:
                self._return_waiter(waiter)
        else:
            self._exclusive_owner = me
            self.is_exclusive += 1
        return True

    def _take_waiter(self) -> "threading.Condition":
        """
        Take a reusable condition or create one bound to the internal mutex.

        Caller must synchronize access to the pool with self._lock. Conditions are reused in last-returned-first order.

        Example:
            >>> lock = SHLock()
            >>> with lock._lock:
            ...     waiter = lock._take_waiter()
            ...     lock._return_waiter(waiter)


        :return: A Condition using self._lock; it is not yet added to an ownership queue.
        """
        try:
            return self._free_waiters.pop()
        except IndexError:
            return Condition(self._lock)

    def _return_waiter(self, waiter: "threading.Condition") -> None:
        """
        Put a finished condition back in the reusable waiter pool.

        Caller must hold self._lock. This helper only appends to the pool; it does not notify threads or remove queue entries.

        Example:
            >>> lock = SHLock()
            >>> with lock._lock:
            ...     waiter = lock._take_waiter()
            ...     lock._return_waiter(waiter)
            >>> len(lock._free_waiters)
            1


        :param waiter: Condition previously taken from this coordinator and no longer actively waiting.
        :return: None.
        """
        self._free_waiters.append(waiter)


# }}}


class RWLockWrapper:
    """
    Expose one fixed SHLock mode as a blocking context manager.

    Read and write wrappers may share one coordinator. Entry returns None; exit releases one acquisition and does not suppress body exceptions. owns_lock asks about either underlying mode, not only this wrapper’s selected mode.

    Example:
        >>> wrapper = RWLockWrapper(SHLock())
        >>> with wrapper as entered:
        ...     owned = wrapper.owns_lock()
        >>> (entered, owned, wrapper.owns_lock())
        (None, True, False)
    """
    def __init__(self, shlock: "SHLock", is_shared: bool = True) -> None:
        """
        Bind a wrapper to a coordinator and fixed acquisition mode.

        Example:
            >>> wrapper = RWLockWrapper(SHLock(), is_shared=False)
            >>> wrapper.owns_lock()
            False


        :param shlock: Shared coordinator, possibly also used by other wrappers.
        :param is_shared: True for shared reads; False for exclusive writes.
        :return: None.
        """
        self._shlock = shlock
        self._is_shared = is_shared

    def acquire(self) -> None:
        """
        Block until the underlying coordinator grants this wrapper’s mode.

        The coordinator’s boolean result is discarded. Reentrancy and mode-transition errors follow SHLock.acquire.

        Example:
            >>> wrapper = RWLockWrapper(SHLock())
            >>> wrapper.acquire()
            >>> wrapper.owns_lock()
            True
            >>> wrapper.release()


        :return: None; also the value returned by context-manager entry.
        """
        self._shlock.acquire(shared=self._is_shared)

    def release(self, *args: Any) -> None:
        """
        Release one underlying acquisition and ignore context-exit arguments.

        Example:
            >>> wrapper = RWLockWrapper(SHLock())
            >>> wrapper.acquire()
            >>> wrapper.release(None, None, None)


        :param args: Optional exception triple or other positional arguments; ignored.
        :return: None; context-manager exceptions are not suppressed.
        :raises LockingError: The current thread does not own the coordinator.
        """

        self._shlock.release()

    __enter__ = acquire
    __exit__ = release

    def owns_lock(self) -> bool:
        """
        Ask whether this thread owns either mode of the shared coordinator.

        Example:
            >>> lock = SHLock()
            >>> reader, writer = RWLockWrapper(lock), RWLockWrapper(lock, False)
            >>> writer.acquire()
            >>> reader.owns_lock()
            True
            >>> writer.release()


        :return: Underlying SHLock.owns_lock result, irrespective of this wrapper’s fixed mode.
        """

        return self._shlock.owns_lock()


class DebugRWLockWrapper(RWLockWrapper):
    """
    Add stderr ownership diagnostics and stack traces around wrapper operations.

    Acquire/release delegate to RWLockWrapper after printing their call context, then print completion details on success. Errors propagate and omit the completion message. Context-manager aliases use these diagnostic methods.

    Example:
        debug_lock = DebugRWLockWrapper(SHLock(), is_shared=False)
        with debug_lock:
            update_cache()
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """
        Forward coordinator and mode arguments to RWLockWrapper.

        Example:
            >>> debug_lock = DebugRWLockWrapper(SHLock())
            >>> debug_lock.owns_lock()
            False


        :param args: Positional shlock and optional is_shared arguments.
        :param kwargs: Keyword shlock or is_shared arguments.
        :return: None.
        """
        RWLockWrapper.__init__(self, *args, **kwargs)

    def acquire(self) -> None:
        """
        Print the calling thread and stack, then acquire the configured mode.

        Diagnostics go to stderr and include whether this wrapper requests shared access.

        Example:
            debug_lock.acquire()
            try:
                inspect_cache()
            finally:
                debug_lock.release()


        :return: None after successful acquisition; ownership errors propagate.
        """
        print("#" * 120, file=sys.stderr)
        print(
            "acquire called: thread id:",
            current_thread(),
            "shared:",
            self._is_shared,
            file=sys.stderr,
        )
        traceback.print_stack()
        RWLockWrapper.acquire(self)
        print("acquire done: thread id:", current_thread(), file=sys.stderr)
        print("_" * 120, file=sys.stderr)

    def release(self, *args: Any) -> None:
        """
        Print release diagnostics around one underlying release.

        After a successful release, print shared/exclusive counts. Invalid-owner errors propagate after the initial diagnostics.

        Example:
            debug_lock.acquire()
            debug_lock.release()


        :param args: Optional context-exit arguments; ignored.
        :return: None; does not suppress an exception from the protected body.
        """
        print("*" * 120, file=sys.stderr)
        print(
            "release called: thread id:",
            current_thread(),
            "shared:",
            self._is_shared,
            file=sys.stderr,
        )
        traceback.print_stack()
        RWLockWrapper.release(self)
        print(
            "release done: thread id:",
            current_thread(),
            "is_shared:",
            self._shlock.is_shared,
            "is_exclusive:",
            self._shlock.is_exclusive,
            file=sys.stderr,
        )
        print("_" * 120, file=sys.stderr)

    __enter__ = acquire
    __exit__ = release


class SafeReadLock:
    """
    Skip a redundant read acquisition when this thread already owns the writer.

    Only DowngradeLockError from read acquisition is swallowed. Body exceptions still propagate; exit prints its positional exception arguments to stdout. The acquired flag tracks a single outstanding acquisition, so use a separate adapter for nested entries rather than reentering one instance.

    Example:
        >>> reader = RWLockWrapper(SHLock())
        >>> safe = SafeReadLock(reader)
        >>> safe.acquire() is safe
        True
        >>> safe.release()
    """
    def __init__(self, read_lock: Union["DebugRWLockWrapper", "RWLockWrapper"]) -> None:
        """
        Store the read wrapper without acquiring it.

        Example:
            >>> SafeReadLock(RWLockWrapper(SHLock())).acquired
            False


        :param read_lock: Shared-mode RWLockWrapper or DebugRWLockWrapper.
        :return: None; starts with acquired=False.
        """
        self.read_lock = read_lock
        self.acquired = False

    def acquire(self) -> "Self":
        """
        Acquire the reader unless the coordinator reports an already-held writer.

        Set acquired=True after a successful underlying acquire. A caught DowngradeLockError leaves the flag unchanged; use a fresh or fully released adapter. Other errors propagate.

        Example:
            >>> coordinator = SHLock()
            >>> writer = RWLockWrapper(coordinator, False)
            >>> safe = SafeReadLock(RWLockWrapper(coordinator))
            >>> writer.acquire()
            >>> safe.acquire() is safe
            True
            >>> safe.acquired
            False
            >>> safe.release()
            >>> writer.release()


        :return: This SafeReadLock instance, also returned by context entry.
        """
        try:
            self.read_lock.acquire()
        except DowngradeLockError:
            pass
        else:
            self.acquired = True
        return self

    def release(self, *args: Any) -> None:
        """
        Release the reader only when this adapter recorded an acquisition.

        Reset acquired=False after any needed release succeeds. Repeated release with no acquisition does nothing except optional printing. If underlying release raises, the flag remains set.

        Example:
            >>> safe = SafeReadLock(RWLockWrapper(SHLock()))
            >>> safe.release()
            >>> safe.acquired
            False


        :param args: Optional context-exit arguments; a nonempty argument tuple is printed to stdout.
        :return: None; body exceptions are not suppressed.
        """

        if args:
            print(args)

        if self.acquired:
            self.read_lock.release()
        self.acquired = False

    __enter__ = acquire
    __exit__ = release


P = ParamSpec("P")
R = TypeVar("R")


# Todo: Not sure how to type this
def wrap_simple(lock: Lock, func: Callable[P, R]) -> Callable[P, R]:
    """
    Wrap a callable in a lock context with an already-held-writer fallback.

    Catch DowngradeLockError from the entire with block and call func again outside that context. This includes errors raised by func or context exit, so a partially executed function can be invoked twice. Other exceptions propagate according to the context manager. functools.wraps copies func.__doc__ over the nested wrapper’s source docstring.

    Example:
        >>> wrapped = wrap_simple(Lock(), lambda value: value + 1)
        >>> wrapped(4)
        5


    :param lock: Context manager protecting the call, typically a thread lock or read wrapper.
    :param func: Callable whose arguments, result and metadata are preserved.
    :return: Wrapped callable using the supplied lock on each invocation.
    """

    @wraps(func)
    def call_func_with_lock(*args, **kwargs):
        """
        Invoke the captured callable under its captured lock, retrying a downgrade.

        On DowngradeLockError anywhere in the with block, invoke func outside the context. A failure from that retry propagates. The decorator replaces this source documentation with the wrapped callable’s runtime metadata.

        Example:
            >>> wrapped = wrap_simple(Lock(), lambda left, right=0: left + right)
            >>> wrapped(2, right=3)
            5


        :param args: Positional arguments forwarded unchanged to func.
        :param kwargs: Keyword arguments forwarded unchanged to func.
        :return: func’s result, or None if the context manager suppresses an exception before a return.
        """
        try:
            with lock:
                return func(*args, **kwargs)
        except DowngradeLockError:
            # We already have an exclusive lock, no need to acquire a shared
            # lock. See the safe_read_lock properties' documentation for why
            # this is necessary.
            return func(*args, **kwargs)

    return call_func_with_lock
