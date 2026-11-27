"""Carry an optional caller checkpoint through synchronous storage I/O.

Storage has no dependency on a job engine. A caller supplies a callback which may
raise before another chunk or publication; interruption does not imply rollback.
"""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, BinaryIO, cast

_check: ContextVar[Callable[[], None] | None] = ContextVar(
    "storage_checkpoint", default=None
)


def checkpoint() -> None:
    callback = _check.get()
    if callback is not None:
        callback()


@contextmanager
def operation_scope(callback: Callable[[], None]) -> Iterator[None]:
    token = _check.set(callback)
    try:
        yield
    finally:
        _check.reset(token)


class _CheckedReader:
    def __init__(self, source: BinaryIO):
        self.source = source
        self.callback = _check.get()

    def read(self, size: int = -1) -> bytes:
        if self.callback is not None:
            self.callback()
        return self.source.read(size)

    def __getattr__(self, name: str) -> Any:
        return getattr(self.source, name)


def checked_reader(source: BinaryIO) -> BinaryIO:
    """Check each delegated read while retaining the caller's stream ownership."""
    return source if _check.get() is None else cast(BinaryIO, _CheckedReader(source))
