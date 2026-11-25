"""Pass durable cancellation into application adapters without global job state."""

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

from LiuXin_alpha.jobs.handler_api import JobRunContext

_current: ContextVar[JobRunContext | None] = ContextVar(
    "core_workflow_context", default=None
)
_library: ContextVar[str | None] = ContextVar("core_workflow_library", default=None)


@contextmanager
def workflow_context(
    context: JobRunContext, library_uuid: str | None = None
) -> Iterator[None]:
    token = _current.set(context)
    library_token = _library.set(library_uuid)
    try:
        yield
    finally:
        _library.reset(library_token)
        _current.reset(token)


def expected_library_uuid() -> str | None:
    return _library.get()


def checkpoint() -> None:
    """Check the current claim and cancellation before the next domain effect."""
    context = _current.get()
    if context is not None:
        context.raise_if_cancel_requested()


class Cancellation:
    """Expose the event-like cancellation interface used by metadata providers."""

    def __init__(self) -> None:
        self.context = _current.get()

    def is_set(self) -> bool:
        return self.context is not None and self.context.is_cancel_requested()
