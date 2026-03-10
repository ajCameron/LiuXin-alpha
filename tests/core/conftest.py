from __future__ import annotations

import json
import socket
import urllib.parse
import urllib.request

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator

import pytest

from LiuXin_alpha.core import CoreHttpDaemon, CoreRuntime, RemoteLibraryProxy


@dataclass
class FakeDatabase:
    value: int = 0

    def get_value(self) -> int:
        return int(self.value)

    def set_value(self, new_value: int) -> int:
        self.value = int(new_value)
        return self.value


@dataclass
class FakeStorage:
    ping_count: int = 0

    def ping(self) -> str:
        self.ping_count += 1
        return "pong"


@dataclass
class FakeLibrary:
    database: FakeDatabase
    storage: FakeStorage

    def echo(self, text: str) -> str:
        return "echo:{}".format(text)


@pytest.fixture
def core_runtime_factory() -> Callable[..., CoreRuntime]:
    def _build_runtime(*, initial_value: int = 0, core_version: str = "test-core") -> CoreRuntime:
        library = FakeLibrary(database=FakeDatabase(value=initial_value), storage=FakeStorage())
        return CoreRuntime(library=library, core_version=core_version)

    return _build_runtime


@pytest.fixture
def free_port() -> Callable[..., int]:
    def _reserve_free_port(host: str = "127.0.0.1") -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((host, 0))
            return int(sock.getsockname()[1])

    return _reserve_free_port


@pytest.fixture
def daemon_factory() -> Callable[..., Any]:
    @contextmanager
    def _start_daemon(
        runtime: CoreRuntime,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        endpoint_namespace: str | None = None,
    ) -> Iterator[CoreHttpDaemon]:
        daemon = CoreHttpDaemon(runtime, host=host, port=port, endpoint_namespace=endpoint_namespace)
        daemon.start()
        try:
            yield daemon
        finally:
            daemon.stop()

    return _start_daemon


@pytest.fixture
def fetch_json() -> Callable[..., dict[str, Any]]:
    def _fetch_json(url: str, *, timeout: float = 3.0) -> dict[str, Any]:
        request = urllib.request.Request(url=url, method="GET")
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))

    return _fetch_json


@pytest.fixture
def remote_proxy() -> Callable[..., RemoteLibraryProxy]:
    def _build_remote_proxy(*, daemon: CoreHttpDaemon, timeout_seconds: float = 10.0) -> RemoteLibraryProxy:
        return RemoteLibraryProxy(endpoint=daemon.base_url, timeout_seconds=timeout_seconds)

    return _build_remote_proxy


@pytest.fixture
def daemon_with_proxy_factory(
    daemon_factory: Callable[..., Any],
    remote_proxy: Callable[..., RemoteLibraryProxy],
) -> Callable[..., Any]:
    @contextmanager
    def _start_daemon_with_proxy(
        runtime: CoreRuntime,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        endpoint_namespace: str | None = None,
        timeout_seconds: float = 10.0,
    ) -> Iterator[tuple[CoreHttpDaemon, RemoteLibraryProxy]]:
        with daemon_factory(runtime, host=host, port=port, endpoint_namespace=endpoint_namespace) as daemon:
            yield daemon, remote_proxy(daemon=daemon, timeout_seconds=timeout_seconds)

    return _start_daemon_with_proxy


@pytest.fixture
def event_poller(fetch_json: Callable[..., dict[str, Any]]) -> Callable[..., tuple[list[dict[str, Any]], int]]:
    def _poll_events(
        daemon: CoreHttpDaemon,
        *,
        after: int = 0,
        timeout: float = 1.0,
        max_polls: int = 8,
        stop_when: Callable[[dict[str, Any]], bool] | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        sequence = int(after)
        events: list[dict[str, Any]] = []
        for _ in range(int(max_polls)):
            query = urllib.parse.urlencode({"after": sequence, "timeout": timeout})
            payload = fetch_json("{}?{}".format(daemon.events_next_url, query), timeout=max(2.0, timeout + 1.0))
            result = payload.get("result", {}) if isinstance(payload, dict) else {}
            event = result.get("event") if isinstance(result, dict) else None
            next_sequence = result.get("next_sequence") if isinstance(result, dict) else None
            if next_sequence is not None:
                try:
                    sequence = int(next_sequence)
                except Exception:
                    pass
            if not isinstance(event, dict):
                continue
            event_obj = dict(event)
            events.append(event_obj)
            if stop_when is not None and stop_when(event_obj):
                break
        return events, sequence

    return _poll_events
