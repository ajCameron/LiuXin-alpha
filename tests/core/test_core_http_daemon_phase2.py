from __future__ import annotations

import urllib.error

from contextlib import AbstractContextManager
from typing import Any, Callable

import pytest

from LiuXin_alpha.core import CoreHttpDaemon, CoreRuntime, RemoteLibraryProxy


def test_core_http_daemon_start_stop_and_health_endpoint(
    core_runtime_factory: Callable[..., CoreRuntime],
    daemon_with_proxy_factory: Callable[..., AbstractContextManager[tuple[CoreHttpDaemon, RemoteLibraryProxy]]],
    fetch_json: Callable[..., dict[str, Any]],
) -> None:
    runtime = core_runtime_factory(initial_value=3, core_version="test-phase2")

    with daemon_with_proxy_factory(runtime, endpoint_namespace="daemon_a") as (daemon, _proxy):
        assert daemon.is_running is True
        payload = fetch_json(daemon.health_url)
        assert payload["ok"] is True
        assert payload["result"]["core_version"] == "test-phase2"

    assert daemon.is_running is False
    with pytest.raises(Exception):
        fetch_json(daemon.health_url, timeout=1.0)


def test_core_http_daemon_supports_isolated_endpoints(
    core_runtime_factory: Callable[..., CoreRuntime],
    daemon_with_proxy_factory: Callable[..., AbstractContextManager[tuple[CoreHttpDaemon, RemoteLibraryProxy]]],
    free_port: Callable[..., int],
    fetch_json: Callable[..., dict[str, Any]],
) -> None:
    runtime_a = core_runtime_factory(initial_value=10, core_version="test-phase2")
    runtime_b = core_runtime_factory(initial_value=20, core_version="test-phase2")

    # Explicit per-daemon ports provide hard endpoint isolation in tests.
    with daemon_with_proxy_factory(runtime_a, endpoint_namespace="alpha", port=free_port()) as (
        daemon_a,
        proxy_a,
    ), daemon_with_proxy_factory(runtime_b, endpoint_namespace="beta", port=free_port()) as (daemon_b, proxy_b):

        assert proxy_a.database.get_value() == 10
        assert proxy_b.database.get_value() == 20

        proxy_a.database.set_value(111)
        assert proxy_a.database.get_value() == 111
        assert proxy_b.database.get_value() == 20

        # Wrong namespace on the right host/port should fail.
        wrong_url = "http://{}:{}/beta/health".format(*daemon_a.server_address)
        with pytest.raises(urllib.error.HTTPError):
            fetch_json(wrong_url)


def test_core_http_daemon_events_next_poll_scaffold(
    core_runtime_factory: Callable[..., CoreRuntime],
    daemon_with_proxy_factory: Callable[..., AbstractContextManager[tuple[CoreHttpDaemon, RemoteLibraryProxy]]],
    fetch_json: Callable[..., dict[str, Any]],
    event_poller: Callable[..., tuple[list[dict[str, Any]], int]],
) -> None:
    runtime = core_runtime_factory(initial_value=1, core_version="test-phase2")
    with daemon_with_proxy_factory(runtime, endpoint_namespace="events") as (daemon, proxy):

        # No events yet.
        empty = fetch_json(daemon.events_next_url + "?after=0&timeout=0")
        assert empty["ok"] is True
        assert empty["result"]["event"] is None
        assert empty["result"]["next_sequence"] == 0

        proxy.database.set_value(42)

        events, _ = event_poller(
            daemon,
            after=0,
            timeout=1.0,
            max_polls=8,
            stop_when=lambda event: str(event.get("event_type")) == "command.finished",
        )
        seen_types = [str(event.get("event_type")) for event in events]

        assert "command.started" in seen_types
        assert "command.finished" in seen_types


def test_core_http_daemon_and_remote_proxy_expose_api_description(
    core_runtime_factory: Callable[..., CoreRuntime],
    daemon_with_proxy_factory: Callable[..., AbstractContextManager[tuple[CoreHttpDaemon, RemoteLibraryProxy]]],
    fetch_json: Callable[..., dict[str, Any]],
) -> None:
    runtime = core_runtime_factory(core_version="test-phase2")

    with daemon_with_proxy_factory(runtime, endpoint_namespace="describe") as (daemon, proxy):
        payload = fetch_json(daemon.describe_url + "?target=database&include_targets=1")
        assert payload["ok"] is True
        described = payload["result"]
        assert described["core_version"] == "test-phase2"
        assert [entry["name"] for entry in described["targets"]] == ["database"]

        proxy_described = proxy.describe_api(target="storage")
        assert [entry["name"] for entry in proxy_described["targets"]] == ["storage"]
        query_names = {entry["name"] for entry in proxy_described["queries"]}
        assert "api.describe" in query_names
