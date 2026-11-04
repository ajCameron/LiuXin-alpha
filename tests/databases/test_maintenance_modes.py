"""Keep declarations shared and prevent prompt work entering the legacy queue.

Transactional and durable contracts are exercised in test_maintenance_runtime.
"""

from types import SimpleNamespace

import pytest

from LiuXin_alpha.databases.api import MaintenanceMode as APIMaintenanceMode
from LiuXin_alpha.databases.api.maintenance_api import MaintenancePluginAPI
from LiuXin_alpha.databases.maintenance import MaintenanceMode
from LiuXin_alpha.databases.maintenance.builtin_plugins import NullMaintenancePlugin
from LiuXin_alpha.databases.maintenance.engine import MaintenanceEngine
from LiuXin_alpha.databases.maintenance.events import DirtyRowEvent


def test_lazy_default_and_api_declarations_share_the_same_mode():
    """The default concrete plugin and API describe the same execution mode."""
    assert APIMaintenanceMode is MaintenanceMode
    assert MaintenancePluginAPI.mode is MaintenanceMode.LAZY
    plugin = NullMaintenancePlugin()
    assert plugin.mode is MaintenanceMode.LAZY
    engine = MaintenanceEngine(SimpleNamespace(), [plugin])
    engine.enqueue(DirtyRowEvent("works", 1))
    assert engine.run_once()[plugin.name].handled == 1
    assert not engine.is_alive()


def test_legacy_duck_plugin_and_explicit_lazy_keep_priority_order():
    """An omitted mode stays compatible and priority still orders lazy plugins."""
    calls = []

    def plugin(name, priority, **declaration):
        return SimpleNamespace(
            name=name,
            priority=priority,
            wants_event=lambda event: True,
            coalesce_key=lambda event: None,
            handle_events=lambda context, events: calls.append(name),
            **declaration,
        )

    engine = MaintenanceEngine(
        SimpleNamespace(),
        [plugin("old", 10), plugin("explicit", 20, mode="lazy")],
    )
    engine.enqueue(DirtyRowEvent("works", 1))
    engine.run_once()
    assert calls == ["explicit", "old"]


@pytest.mark.parametrize(
    ("mode", "error"),
    [
        (MaintenanceMode.PROMPT, NotImplementedError),
        ("prompt", NotImplementedError),
        ("urgent", ValueError),
        (None, ValueError),
    ],
)
def test_unsupported_mode_cannot_be_constructed_or_registered(monkeypatch, mode, error):
    """No unsupported plugin enters the worker or gets a startup side effect."""
    plugin = NullMaintenancePlugin()
    plugin.mode = mode
    with pytest.raises(error):
        MaintenanceEngine(SimpleNamespace(), [plugin])

    engine = MaintenanceEngine(SimpleNamespace(), [])
    starts = []
    monkeypatch.setattr(engine, "is_alive", lambda: True)
    monkeypatch.setattr(plugin, "startup", lambda context: starts.append(context))
    with pytest.raises(error):
        engine.register_plugin(plugin)
    assert engine.iter_plugins() == ()
    assert starts == []


@pytest.mark.parametrize("dispatch", ("queued", "synchronous", "startup"))
def test_changed_mode_is_refused_before_any_handler_runs(dispatch):
    """Changing a declaration cannot turn prompt work into best-effort dispatch."""
    calls = []

    class Recorder(NullMaintenancePlugin):
        def startup(self, context):
            calls.append("startup")

        def handle_events(self, context, events):
            calls.append(self.name)
            return super().handle_events(context, events)

    lazy, changed = Recorder(), Recorder()
    lazy.name, changed.name = "first-lazy", "changed"
    lazy.priority = 100
    engine = MaintenanceEngine(SimpleNamespace(), [lazy, changed])
    changed.mode = MaintenanceMode.PROMPT
    event = DirtyRowEvent("works", 1)
    engine.enqueue(event)
    with pytest.raises(NotImplementedError, match="originating write transaction"):
        if dispatch == "queued":
            engine.run_once()
        elif dispatch == "synchronous":
            engine.rename_item(1, "works", "Renamed", now=True)
        else:
            engine.run()
    assert calls == []
    changed.mode = MaintenanceMode.LAZY
    assert all(result.handled == 1 for result in engine.run_once().values())
    assert calls == ["first-lazy", "changed"]
