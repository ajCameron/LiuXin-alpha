"""Publish managed projections only after the owning macro transaction commits."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field

from LiuXin_alpha.databases.database_driver_plugins._macro_state import (
    macro_transaction_state,
)


@dataclass
class CompletionFrame:
    """Collect publication callbacks for one owned transaction or nested savepoint."""

    owned: bool
    callbacks: list[Callable[[], None]] = field(default_factory=list)


def after_commit(provider: object, callback: Callable[[], None]) -> None:
    """Append a callback to the current managed transaction or savepoint frame.

    Require an owned outer transaction so delivery cannot precede the real commit.
    Savepoint rollback discards its callbacks. The callback is synchronous and must
    not assume that failure can undo SQL that has already committed."""
    state = macro_transaction_state(provider)
    frames = getattr(state, "completion_frames", ())
    if not frames or not frames[0].owned:
        raise RuntimeError(
            "Projection publication requires a managed owned transaction"
        )
    frames[-1].callbacks.append(callback)


@contextmanager
def completion_frame(provider: object, *, owned: bool) -> Iterator[None]:
    """Match publication lifetime to an outer SQL transaction or nested savepoint.

    On failure discard this frame; on nested success transfer callbacks to its parent.
    After outer success invoke queued callbacks in order, collecting ordinary errors
    into an exception group that explicitly warns against retrying committed writes.
    The caller must exit the SQL scope before completing this publication frame."""
    state = macro_transaction_state(provider, create=True)
    frames: list[CompletionFrame] | None = getattr(state, "completion_frames", None)
    if frames is None:
        frames = []
        state.completion_frames = frames
    frame = CompletionFrame(owned)
    frames.append(frame)
    try:
        yield
    except BaseException:
        frames.pop()
        raise
    else:
        frames.pop()
        if frames:
            frames[-1].callbacks.extend(frame.callbacks)
        else:
            failures = []
            for callback in frame.callbacks:
                try:
                    callback()
                except Exception as error:
                    error.add_note(
                        "SQL committed; projection delivery failed. Do not retry the write."
                    )
                    failures.append(error)
            if failures:
                raise ExceptionGroup(
                    "Committed transaction publication failed", failures
                )
