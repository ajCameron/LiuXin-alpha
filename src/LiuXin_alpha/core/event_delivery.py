"""Deliver detached, bounded event hints without running observers on dispatch threads."""

from __future__ import annotations

import json
import threading
import time
from collections import deque
from collections.abc import Callable
from copy import deepcopy
from dataclasses import replace
from typing import Any

from LiuXin_alpha.core.events import CoreEvent
from LiuXin_alpha.core.wire import to_wire


class Subscription:
    """One serial observer with a bounded queue and explicit loss notifications."""

    def __init__(self, callback: Callable[[CoreEvent], None]):
        self.callback = callback
        self.condition = threading.Condition()
        self.queue: deque[tuple[CoreEvent, int]] = deque()
        self.bytes = 0
        self.dropped = 0
        self.failures = 0
        self.needs_resync = False
        self.active = False
        self.closed = False
        self.thread = threading.Thread(
            target=self._deliver, name="core-events", daemon=True
        )
        self.thread.start()

    def offer(self, event: CoreEvent, size: int) -> None:
        with self.condition:
            if self.closed:
                return
            if self.needs_resync:
                marker = replace(
                    event,
                    event_type="events.resync_required",
                    payload={"reason": "callback_failed"},
                )
                self.queue.append((marker, 128))
                self.bytes += 128
                self.needs_resync = False
            if len(self.queue) >= 128 or self.bytes + size > 1024 * 1024:
                self.dropped += len(self.queue)
                self.queue.clear()
                self.bytes = 0
                marker = replace(
                    event,
                    event_type="events.resync_required",
                    payload={"reason": "subscriber_overflow"},
                )
                self.queue.append((marker, 128))
                self.bytes = 128
            self.queue.append((deepcopy(event), size))
            self.bytes += size
            self.condition.notify_all()

    def _deliver(self) -> None:
        while True:
            with self.condition:
                self.condition.wait_for(lambda: self.queue or self.closed)
                if not self.queue:
                    return
                event, size = self.queue.popleft()
                self.bytes -= size
                self.active = True
            try:
                self.callback(event)
            except Exception:
                with self.condition:
                    self.failures += 1
                    self.needs_resync = True
                    # The remaining queue cannot be trusted as a complete history.
                    self.dropped += len(self.queue)
                    self.queue.clear()
                    self.bytes = 0
                from LiuXin_alpha.utils.logging import default_log

                default_log.error(
                    "Core event subscriber failed; authoritative resynchronization required"
                )
            finally:
                with self.condition:
                    self.active = False
                    self.condition.notify_all()

    def close(self) -> None:
        with self.condition:
            self.closed = True
            self.condition.notify_all()

    def drain(self, deadline: float) -> bool:
        if threading.current_thread() is self.thread:
            return False
        with self.condition:
            return self.condition.wait_for(
                lambda: not self.queue and not self.active,
                max(0.0, deadline - time.monotonic()),
            )


class EventDelivery:
    """Own at most 32 subscriber queues; events are hints, never recovery receipts."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.subscriptions: list[Subscription] = []
        # Unsubscribing cannot stop an executing Python callback. Keep counting
        # its thread until it exits so repeated removal cannot evade admission.
        self.retired: list[Subscription] = []
        self.closed = False

    def subscribe(self, callback: Callable[[CoreEvent], None]) -> Callable[[], None]:
        if not callable(callback):
            raise TypeError("Event subscriber must be callable")
        with self.lock:
            if self.closed:
                raise RuntimeError("Core event delivery is closed")
            self.retired[:] = [item for item in self.retired if item.thread.is_alive()]
            if len(self.subscriptions) + len(self.retired) >= 32:
                raise ValueError("Core subscriber limit reached")
            subscription = Subscription(callback)
            self.subscriptions.append(subscription)

        def remove() -> None:
            with self.lock:
                if subscription in self.subscriptions:
                    self.subscriptions.remove(subscription)
                    self.retired.append(subscription)
                    subscription.close()

        return remove

    def unsubscribe(self, callback: Callable[[CoreEvent], None]) -> None:
        with self.lock:
            for subscription in self.subscriptions:
                if subscription.callback == callback:
                    self.subscriptions.remove(subscription)
                    self.retired.append(subscription)
                    subscription.close()
                    return

    def emit(self, event: CoreEvent) -> None:
        try:
            payload = to_wire(event.payload, max_bytes=64 * 1024)
        except Exception:
            payload = {
                "reason": "event_payload_limit",
                "original_type": event.event_type,
            }
            event = replace(event, event_type="events.resync_required")
        detached = replace(event, payload=payload)
        size = len(json.dumps(payload, ensure_ascii=True)) + 512
        with self.lock:
            for subscription in self.subscriptions:
                subscription.offer(detached, size)

    def drain(self, timeout: float = 1.0) -> bool:
        deadline = time.monotonic() + timeout
        with self.lock:
            subscriptions = tuple(self.subscriptions)
        return all(subscription.drain(deadline) for subscription in subscriptions)

    def statistics(self) -> dict[str, Any]:
        with self.lock:
            return {
                "subscribers": len(self.subscriptions),
                "retired_threads": sum(item.thread.is_alive() for item in self.retired),
                "closed": self.closed,
                "dropped": sum(item.dropped for item in self.subscriptions),
                "callback_failures": sum(item.failures for item in self.subscriptions),
            }

    def close(self) -> None:
        with self.lock:
            self.closed = True
            for subscription in self.subscriptions:
                subscription.close()
