from __future__ import annotations

from typing import Callable

from LiuXin_alpha.core import CoreCommand, CoreQuery, CoreRuntime
from LiuXin_alpha.core.proxies import LocalLibraryProxy


def test_core_runtime_health_and_invoke_paths(core_runtime_factory: Callable[..., CoreRuntime]) -> None:
    runtime = core_runtime_factory(core_version="test-phase1")

    health = runtime.execute_query(CoreQuery(name="health")).result
    assert health["core_version"] == "test-phase1"
    assert health["shutdown"] is False

    result = runtime.invoke_query(target="library", method="echo", args=("hello",))
    assert result == "echo:hello"

    write_result = runtime.invoke_command(target="database", method="set_value", args=(7,))
    assert write_result == 7
    assert runtime.invoke_query(target="database", method="get_value") == 7


def test_core_runtime_emits_command_lifecycle_events(core_runtime_factory: Callable[..., CoreRuntime]) -> None:
    runtime = core_runtime_factory(core_version="test-phase1")
    events = []
    runtime.subscribe(events.append)

    command = CoreCommand(
        name="invoke",
        payload={
            "target": "database",
            "method": "set_value",
            "args": (11,),
            "kwargs": {},
        },
    )
    result = runtime.execute_command(command)
    assert result.ok is True

    event_types = [event.event_type for event in events]
    assert "command.started" in event_types
    assert "command.finished" in event_types


def test_local_proxy_auto_dispatches_read_and_write(core_runtime_factory: Callable[..., CoreRuntime]) -> None:
    runtime = core_runtime_factory(core_version="test-phase1")
    proxy = LocalLibraryProxy(runtime)

    assert proxy.database.get_value() == 0
    assert proxy.database.set_value(19) == 19
    assert proxy.database.get_value() == 19
    assert proxy.storage.ping() == "pong"
    assert proxy.health()["core_version"] == "test-phase1"
