"""Storage CLI signals ownership."""

from __future__ import annotations

import signal
import threading
from types import FrameType
from typing import cast, final


@final
class SignalCancellation:
    """Convert the first interrupt/termination signal into graceful cancellation.

    A second signal raises ``KeyboardInterrupt`` so an operator can still force
    the Python workflow boundary to unwind if graceful cancellation stalls in
    a parser or external program.
    """

    def __init__(self) -> None:
        self._requested = threading.Event()
        self._signal_number: int | None = None
        self._previous: dict[int, object] = {}
        self._installed = False

    @property
    def signal_number(self) -> int | None:
        return self._signal_number

    def requested(self) -> bool:
        return self._requested.is_set()

    def __enter__(self) -> SignalCancellation:
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
        if self._requested.is_set():
            raise KeyboardInterrupt(
                f"received signal {signal_number} after cancellation was requested"
            )
        self._signal_number = int(signal_number)
        self._requested.set()
