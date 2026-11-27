"""Exercise hostile payloads, slow readers and pre-thread HTTP admission."""

import socket
import threading
import time
from dataclasses import dataclass
from unittest.mock import Mock

import pytest

from LiuXin_alpha.core.transport.bounds import BufferBudget, DeadlineReader
from LiuXin_alpha.core.transport.http import CoreHttpDaemon
from LiuXin_alpha.core.wire import CoreWireError, to_wire


def test_structural_and_encoded_byte_budgets():
    value = []
    for _ in range(50):
        value = [value]
    with pytest.raises(CoreWireError, match="structural"):
        to_wire(value)
    with pytest.raises(CoreWireError, match="structural"):
        to_wire([0] * 100_001)
    with pytest.raises(CoreWireError, match="byte budget"):
        to_wire("雪" * 100, max_bytes=300)
    # Repeated references are permitted; actual cycles are not.
    shared = {"a": 1}
    assert to_wire([shared, shared]) == [shared, shared]
    shared["self"] = shared
    with pytest.raises(CoreWireError, match="cycle"):
        to_wire(shared)


def test_dataclass_cycles_and_duck_mapping_collisions():
    @dataclass
    class Node:
        next: object = None

    value = Node()
    value.next = value
    with pytest.raises(CoreWireError, match="cycle"):
        to_wire(value)

    class Row:
        def keys(self):
            return [1, "1"]

        def __getitem__(self, key):
            return str(type(key))

    with pytest.raises(CoreWireError, match="colliding"):
        to_wire(Row())


def test_buffer_budget_releases_on_error_and_never_waits():
    budget = BufferBudget(10)
    with pytest.raises(RuntimeError):
        with budget.reserve(8):
            with pytest.raises(ValueError, match="exhausted"):
                with budget.reserve(3):
                    pass
            raise RuntimeError("socket failed")
    assert budget.used == 0
    with budget.reserve(10):
        assert budget.used == 10


@pytest.mark.parametrize("method", ["read", "readline"])
def test_total_read_deadline_stops_drip_feed(method):
    receiver, sender = socket.socketpair()
    reader = DeadlineReader(receiver.makefile("rb"), receiver, 0.15)
    stop = threading.Event()

    def drip():
        while not stop.wait(0.025):
            try:
                sender.sendall(b"x")
            except OSError:
                return

    thread = threading.Thread(target=drip)
    thread.start()
    started = time.monotonic()
    try:
        with pytest.raises(TimeoutError):
            getattr(reader, method)(100)
        assert time.monotonic() - started < 0.6
    finally:
        stop.set()
        sender.close()
        reader.close()
        receiver.close()
        thread.join(1)


def test_http_rejects_excess_connections_before_spawning_threads():
    runtime = Mock()
    runtime.subscribe.return_value = lambda: None
    daemon = CoreHttpDaemon(runtime)
    daemon.start()
    sockets = []
    try:
        # Hold the admitted handlers in their header phase.
        for _ in range(16):
            sock = socket.create_connection(daemon.server_address, timeout=2)
            sockets.append(sock)
            sock.sendall(b"G")
        deadline = time.monotonic() + 2
        while daemon._server._admission._value and time.monotonic() < deadline:
            time.sleep(0.01)
        with socket.create_connection(daemon.server_address, timeout=2) as excess:
            assert b"503 Service Unavailable" in excess.recv(4096)
        assert not runtime.execute_command.called
    finally:
        for sock in sockets:
            sock.close()
        daemon.stop()
