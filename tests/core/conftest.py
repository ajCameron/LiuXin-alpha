"""
Provide conftest utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise conftest through a consuming regression::

        python -m pytest -q tests/core/conftest.py
"""
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
    """
    Provide the fakedatabase contract for validated ebook processing.

    Example:
        Exercise FakeDatabase through a consuming regression::

            python -m pytest -q tests/core/conftest.py
    """
    value: int = 0

    def get_value(self) -> int:
        """
        Return value under the format's safety and compatibility rules.

        Example:
            Exercise FakeDatabase.get value through a consuming regression::

                python -m pytest -q tests/core/conftest.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return int(self.value)

    def set_value(self, new_value: int) -> int:
        """
        Set value under the format's safety and compatibility rules.

        Example:
            Exercise FakeDatabase.set value through a consuming regression::

                python -m pytest -q tests/core/conftest.py


        :param new_value: Value supplied for new value under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.value = int(new_value)
        return self.value


@dataclass
class FakeStorage:
    """
    Provide the fakestorage contract for validated ebook processing.

    Example:
        Exercise FakeStorage through a consuming regression::

            python -m pytest -q tests/core/conftest.py
    """
    ping_count: int = 0

    def ping(self) -> str:
        """
        Perform the ping operation under explicit file-format and conversion rules.

        Example:
            Exercise FakeStorage.ping through a consuming regression::

                python -m pytest -q tests/core/conftest.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.ping_count += 1
        return "pong"


@dataclass
class FakeLibrary:
    """
    Provide the fakelibrary contract for validated ebook processing.

    Example:
        Exercise FakeLibrary through a consuming regression::

            python -m pytest -q tests/core/conftest.py
    """
    database: FakeDatabase
    storage: FakeStorage

    def echo(self, text: str) -> str:
        """
        Perform the echo operation under explicit file-format and conversion rules.

        Example:
            Exercise FakeLibrary.echo through a consuming regression::

                python -m pytest -q tests/core/conftest.py


        :param text: Text parsed, normalized or rendered.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return "echo:{}".format(text)


@pytest.fixture
def core_runtime_factory() -> Callable[..., CoreRuntime]:
    """
    Perform the core runtime factory operation under explicit file-format and conversion rules.

    Example:
        Exercise core runtime factory through a consuming regression::

            python -m pytest -q tests/core/conftest.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def _build_runtime(*, initial_value: int = 0, core_version: str = "test-core") -> CoreRuntime:
        """
        Perform the build runtime operation under explicit file-format and conversion rules.

        Example:
            Exercise core runtime factory. build runtime through a consuming regression::

                python -m pytest -q tests/core/conftest.py


        :param initial_value: Value supplied for initial value under the utility contract.
        :param core_version: Value supplied for core version under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        library = FakeLibrary(database=FakeDatabase(value=initial_value), storage=FakeStorage())
        return CoreRuntime(library=library, core_version=core_version)

    return _build_runtime


@pytest.fixture
def free_port() -> Callable[..., int]:
    """
    Perform the free port operation under explicit file-format and conversion rules.

    Example:
        Exercise free port through a consuming regression::

            python -m pytest -q tests/core/conftest.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def _reserve_free_port(host: str = "127.0.0.1") -> int:
        """
        Perform the reserve free port operation under explicit file-format and conversion rules.

        Example:
            Exercise free port. reserve free port through a consuming regression::

                python -m pytest -q tests/core/conftest.py


        :param host: Value supplied for host under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind((host, 0))
            return int(sock.getsockname()[1])

    return _reserve_free_port


@pytest.fixture
def daemon_factory() -> Callable[..., Any]:
    """
    Perform the daemon factory operation under explicit file-format and conversion rules.

    Example:
        Exercise daemon factory through a consuming regression::

            python -m pytest -q tests/core/conftest.py


    :return: An iterator yielding the normalized values described above.
    """
    @contextmanager
    def _start_daemon(
        runtime: CoreRuntime,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        endpoint_namespace: str | None = None,
    ) -> Iterator[CoreHttpDaemon]:
        """
        Perform the start daemon operation under explicit file-format and conversion rules.

        Example:
            Exercise daemon factory. start daemon through a consuming regression::

                python -m pytest -q tests/core/conftest.py


        :param runtime: Value supplied for runtime under the utility contract.
        :param host: Value supplied for host under the utility contract.
        :param port: Value supplied for port under the utility contract.
        :param endpoint_namespace: Value supplied for endpoint namespace under the utility
            contract.
        :return: An iterator yielding the normalized values described above.
        """
        daemon = CoreHttpDaemon(runtime, host=host, port=port, endpoint_namespace=endpoint_namespace)
        daemon.start()
        try:
            yield daemon
        finally:
            daemon.stop()

    return _start_daemon


@pytest.fixture
def fetch_json() -> Callable[..., dict[str, Any]]:
    """
    Perform the fetch json operation under explicit file-format and conversion rules.

    Example:
        Exercise fetch json through a consuming regression::

            python -m pytest -q tests/core/conftest.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def _fetch_json(url: str, *, timeout: float = 3.0) -> dict[str, Any]:
        """
        Perform the fetch json operation under explicit file-format and conversion rules.

        Example:
            Exercise fetch json. fetch json through a consuming regression::

                python -m pytest -q tests/core/conftest.py


        :param url: Value supplied for url under the utility contract.
        :param timeout: Maximum wait time before the operation fails.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        request = urllib.request.Request(url=url, method="GET")
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))

    return _fetch_json


@pytest.fixture
def remote_proxy() -> Callable[..., RemoteLibraryProxy]:
    """
    Perform the remote proxy operation under explicit file-format and conversion rules.

    Example:
        Exercise remote proxy through a consuming regression::

            python -m pytest -q tests/core/conftest.py


    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def _build_remote_proxy(*, daemon: CoreHttpDaemon, timeout_seconds: float = 10.0) -> RemoteLibraryProxy:
        """
        Perform the build remote proxy operation under explicit file-format and conversion rules.

        Example:
            Exercise remote proxy. build remote proxy through a consuming regression::

                python -m pytest -q tests/core/conftest.py


        :param daemon: Value supplied for daemon under the utility contract.
        :param timeout_seconds: Value supplied for timeout seconds under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return RemoteLibraryProxy(endpoint=daemon.base_url, timeout_seconds=timeout_seconds)

    return _build_remote_proxy


@pytest.fixture
def daemon_with_proxy_factory(
    daemon_factory: Callable[..., Any],
    remote_proxy: Callable[..., RemoteLibraryProxy],
) -> Callable[..., Any]:
    """
    Perform the daemon with proxy factory operation under explicit file-format and conversion rules.

    Example:
        Exercise daemon with proxy factory through a consuming regression::

            python -m pytest -q tests/core/conftest.py


    :param daemon_factory: Value supplied for daemon factory under the utility contract.
    :param remote_proxy: Value supplied for remote proxy under the utility contract.
    :return: An iterator yielding the normalized values described above.
    """
    @contextmanager
    def _start_daemon_with_proxy(
        runtime: CoreRuntime,
        *,
        host: str = "127.0.0.1",
        port: int = 0,
        endpoint_namespace: str | None = None,
        timeout_seconds: float = 10.0,
    ) -> Iterator[tuple[CoreHttpDaemon, RemoteLibraryProxy]]:
        """
        Perform the start daemon with proxy operation under explicit file-format and conversion rules.

        Example:
            Exercise daemon with proxy factory. start daemon with proxy through a consuming regression::

                python -m pytest -q tests/core/conftest.py


        :param runtime: Value supplied for runtime under the utility contract.
        :param host: Value supplied for host under the utility contract.
        :param port: Value supplied for port under the utility contract.
        :param endpoint_namespace: Value supplied for endpoint namespace under the utility
            contract.
        :param timeout_seconds: Value supplied for timeout seconds under the utility
            contract.
        :return: An iterator yielding the normalized values described above.
        """
        with daemon_factory(runtime, host=host, port=port, endpoint_namespace=endpoint_namespace) as daemon:
            yield daemon, remote_proxy(daemon=daemon, timeout_seconds=timeout_seconds)

    return _start_daemon_with_proxy


@pytest.fixture
def event_poller(fetch_json: Callable[..., dict[str, Any]]) -> Callable[..., tuple[list[dict[str, Any]], int]]:
    """
    Perform the event poller operation under explicit file-format and conversion rules.

    Example:
        Exercise event poller through a consuming regression::

            python -m pytest -q tests/core/conftest.py


    :param fetch_json: Value supplied for fetch json under the utility contract.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    def _poll_events(
        daemon: CoreHttpDaemon,
        *,
        after: int = 0,
        timeout: float = 1.0,
        max_polls: int = 8,
        stop_when: Callable[[dict[str, Any]], bool] | None = None,
    ) -> tuple[list[dict[str, Any]], int]:
        """
        Perform the poll events operation under explicit file-format and conversion rules.

        Example:
            Exercise event poller. poll events through a consuming regression::

                python -m pytest -q tests/core/conftest.py


        :param daemon: Value supplied for daemon under the utility contract.
        :param after: Value supplied for after under the utility contract.
        :param timeout: Maximum wait time before the operation fails.
        :param max_polls: Value supplied for max polls under the utility contract.
        :param stop_when: Value supplied for stop when under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
