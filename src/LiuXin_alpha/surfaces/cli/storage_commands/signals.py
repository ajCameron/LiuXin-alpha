"""
Translate main-thread SIGINT/SIGTERM delivery into cooperative ingest cancellation.

The first signal records a request; the next raises KeyboardInterrupt. Temporary
handler installation/restoration is separate from the latched cancellation state.
No worker, subprocess, or Core job is automatically terminated by the request.
"""

from __future__ import annotations

import signal
import threading
from types import FrameType
from typing import cast, final


@final
class SignalCancellation:
    """
    Latch a first cancellation signal and force Python unwinding on a later signal.

    A second signal raises ``KeyboardInterrupt`` so an operator can still force
    the Python workflow boundary to unwind if graceful cancellation stalls in
    a parser or external program. Only main-thread context entry installs handlers.
    Instances retain their request across exits/re-entry and are not a reentrant
    stack of handler scopes. Installation/restoration errors are not rolled back.

    Example:
        >>> cancellation = SignalCancellation()
        >>> cancellation.requested(), cancellation.signal_number
        (False, None)
    """

    def __init__(self) -> None:
        """
        Initialize an unset request event and empty signal-handler bookkeeping.

        Example:
            >>> SignalCancellation().requested()
            False


        :return: None; create state without installing any process signal handlers.
        """
        self._requested = threading.Event()
        self._signal_number: int | None = None
        self._previous: dict[int, object] = {}
        self._installed = False

    @property
    def signal_number(self) -> int | None:
        """
        Report the first signal received by this instance without clearing it.

        Example:
            >>> SignalCancellation().signal_number is None
            True


        :return: Integer first-signal number, or None before any cancellation request.
        """
        return self._signal_number

    def requested(self) -> bool:
        """
        Observe whether cooperative cancellation has been latched.

        Example:
            >>> SignalCancellation().requested()
            False


        :return: Current event state; observation does not consume or reset the request.
        """
        return self._requested.is_set()

    def __enter__(self) -> SignalCancellation:
        """
        Save and replace SIGINT/SIGTERM handlers when running on the main thread.

        Non-main threads return the instance unchanged. Set installed only after
        both replacements succeed; failure during installation can leave a partial
        replacement, and existing request state is not reset.

        Example:
            >>> with SignalCancellation() as cancellation:  # doctest: +SKIP
            ...     run_ingest(cancel_requested=cancellation.requested)


        :return: This instance, whether handlers were installed or entry was a no-op.
        """
        if threading.current_thread() is not threading.main_thread():
            return self
        for signal_number in (signal.SIGINT, signal.SIGTERM):
            self._previous[int(signal_number)] = signal.getsignal(signal_number)
            _ = signal.signal(signal_number, self._receive)
        self._installed = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback_value: object,
    ) -> None:
        """
        Restore saved handlers after successful installation, without suppressing errors.

        No-op when installed is false. Restoration errors propagate before the
        flag is cleared; the request event and first signal are always retained.

        Example:
            >>> SignalCancellation().__exit__(None, None, None)


        :param exc_type: Ignored exception type from the context body.
        :param exc: Ignored exception instance from the context body.
        :param traceback_value: Ignored body traceback information.
        :return: None; body exceptions remain unsuppressed.
        """
        del exc_type, exc, traceback_value
        if not self._installed:
            return
        for signal_number, previous in self._previous.items():
            _ = signal.signal(
                signal_number,
                cast("signal._HANDLER", previous),
            )
        self._installed = False

    def _receive(self, signal_number: int, _frame: FrameType | None) -> None:
        """
        Record the first signal or raise KeyboardInterrupt for any later delivery.

        The second signal does not replace the original signal_number. Direct
        calls exercise the same state machine without sending an operating-system signal.

        Example:
            >>> cancellation = SignalCancellation()
            >>> cancellation._receive(signal.SIGTERM, None)
            >>> cancellation.signal_number == signal.SIGTERM and cancellation.requested()
            True


        :param signal_number: Delivered signal, integer-converted on the first request.
        :param _frame: Ignored frame supplied by Python's signal machinery.
        :return: None after latching the first request.
        :raises KeyboardInterrupt: A request was already latched, forcing local unwinding.
        """
        if self._requested.is_set():
            raise KeyboardInterrupt(
                f"received signal {signal_number} after cancellation was requested"
            )
        self._signal_number = int(signal_number)
        self._requested.set()
