"""Bound HTTP sockets and retained buffers before creating request threads."""

from __future__ import annotations

import socket
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import ThreadingHTTPServer
from typing import Any


class BufferBudget:
    """Reserve raw request/response bytes without waiting behind stalled sockets."""

    def __init__(self, capacity: int = 64 * 1024 * 1024):
        self.capacity = capacity
        self.used = 0
        self._lock = threading.Lock()

    @contextmanager
    def reserve(self, size: int) -> Iterator[None]:
        with self._lock:
            if size < 0 or self.used + size > self.capacity:
                raise ValueError("Core HTTP buffer budget exhausted; retry later")
            self.used += size
        try:
            yield
        finally:
            with self._lock:
                self.used -= size


class DeadlineReader:
    """Give all reads in one protocol phase a single monotonic deadline."""

    def __init__(self, reader: Any, connection: socket.socket, seconds: float) -> None:
        self.reader = reader
        self.connection = connection
        self.reset(seconds)

    def reset(self, seconds: float) -> None:
        self.deadline = time.monotonic() + seconds

    def _read(self, method: str, size: int = -1) -> bytes:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Core HTTP read deadline exceeded")
        self.connection.settimeout(remaining)
        return getattr(self.reader, method)(size)

    def readline(self, size: int = -1) -> bytes:
        # Buffered readline can repeatedly receive a byte just before its socket
        # timeout. Consume through read1 so the total deadline is rechecked.
        data = bytearray()
        while size < 0 or len(data) < size:
            byte = self._read("read1", 1)
            if not byte:
                break
            data.extend(byte)
            if byte == b"\n":
                break
        return bytes(data)

    def read(self, size: int) -> bytes:
        data = bytearray()
        while len(data) < size:
            chunk = self._read("read1", min(size - len(data), 64 * 1024))
            if not chunk:
                raise ValueError("Request body ended before Content-Length")
            data.extend(chunk)
        return bytes(data)

    def close(self) -> None:
        self.reader.close()


class BoundedHTTPServer(ThreadingHTTPServer):
    """Reject excess connections before allocating an unbounded handler thread."""

    daemon_threads = True
    block_on_close = False
    request_queue_size = 32

    def __init__(self, *args: Any, max_requests: int = 16, **kwargs: Any) -> None:
        self._admission = threading.BoundedSemaphore(max_requests)
        super().__init__(*args, **kwargs)

    def process_request(
        self, request: socket.socket | tuple[bytes, socket.socket], client_address: Any
    ) -> None:
        if not isinstance(request, socket.socket):
            raise TypeError("HTTP admission requires a TCP socket")
        if not self._admission.acquire(blocking=False):
            try:
                request.settimeout(0.05)
                request.sendall(
                    b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\nRetry-After: 1\r\n\r\n"
                )
            except OSError:
                pass
            finally:
                self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._admission.release()
            raise

    def process_request_thread(
        self, request: socket.socket | tuple[bytes, socket.socket], client_address: Any
    ) -> None:
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._admission.release()
