"""Release managed participants on completion and publish projections on commit."""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field

from LiuXin_alpha.databases.database_driver_plugins._macro_state import (
    macro_transaction_state,
)


@dataclass
class CompletionFrame:
    """Collect publication and completion callbacks for a managed SQL scope."""

    owned: bool
    callbacks: list[Callable[[], None]] = field(default_factory=list)
    completion_callbacks: list[Callable[[], None]] = field(default_factory=list)


def owned_transaction_active(provider: object) -> bool:
    """Report whether this thread's managed outer unit owns its SQL completion.

    Borrowed raw transactions have no observable completion at the macro boundary.
    Cache-facing services use this distinction before promising deferred recovery.
    """
    state = macro_transaction_state(provider)
    frames = getattr(state, "completion_frames", ())
    return bool(frames and frames[0].owned)


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


def after_transaction(provider: object, callback: Callable[[], None]) -> None:
    """Release a participant after the owned outer unit commits or rolls back.

    Register on the outer frame, including when called inside a savepoint. Inner
    rollback does not release the participant while its enclosing unit is open.
    Completion runs before commit-only publication, after SQL has finished.
    """
    state = macro_transaction_state(provider)
    frames = getattr(state, "completion_frames", ())
    if not frames or not frames[0].owned:
        raise RuntimeError(
            "Transaction completion requires a managed owned transaction"
        )
    frames[0].completion_callbacks.append(callback)


def _complete_participants(frame: CompletionFrame) -> list[Exception]:
    """Attempt every release even when an earlier participant fails."""
    failures = []
    for callback in frame.completion_callbacks:
        try:
            callback()
        except Exception as error:
            failures.append(error)
    return failures


@contextmanager
def completion_frame(provider: object, *, owned: bool) -> Iterator[None]:
    """Match publication lifetime to an outer SQL transaction or nested savepoint.

    On failure discard publication; on nested success transfer it to the parent.
    Release outer participants after commit or rollback, before any publication.
    After outer success invoke queued publication in order, collecting ordinary
    errors into an exception group; rollback preserves the original exception.
    The caller must exit the SQL scope before completing this frame."""
    state = macro_transaction_state(provider, create=True)
    frames: list[CompletionFrame] | None = getattr(state, "completion_frames", None)
    if frames is None:
        frames = []
        state.completion_frames = frames
    frame = CompletionFrame(owned)
    frames.append(frame)
    try:
        yield
    except BaseException as original:
        frames.pop()
        if not frames:
            for error in _complete_participants(frame):
                original.add_note(f"Transaction participant release failed: {error!r}")
        raise
    else:
        frames.pop()
        if frames:
            frames[-1].callbacks.extend(frame.callbacks)
        else:
            failures = _complete_participants(frame)
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
