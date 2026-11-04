"""Exercise malformed trusted plugins without permitting silent receipts or partial effects."""

from types import SimpleNamespace

import pytest

from LiuXin_alpha.databases.db_types.maintenance_types import MaintenanceMode
from LiuXin_alpha.databases.maintenance.plugins import (
    MaintenancePluginResult,
    MaintenancePluginUpgrade,
)
from LiuXin_alpha.databases.maintenance.runtime import (
    MaintenanceFailure,
    MaintenanceRuntime,
)
from tests.databases.test_maintenance_runtime import Projection
from tests.databases.test_maintenance_runtime import runtime_factory as runtime_factory


async def async_hook(*args):
    raise AssertionError("Async hooks must never execute")


def generator_hook(*args):
    raise AssertionError("Generator hooks must never execute")
    yield  # pragma: no cover


class AsyncCallable:
    async def __call__(self, *args):
        raise AssertionError("Async callable must never execute")


class GeneratorCallable:
    def __call__(self, *args):
        raise AssertionError("Generator callable must never execute")
        yield  # pragma: no cover


@pytest.mark.parametrize(
    "field,value",
    [
        ("name", None),
        ("name", ""),
        ("name", "bad\x00name"),
        ("version", ""),
        ("version", "bad\x00version"),
        ("tables", "works"),
        ("tables", ("works", "works")),
        ("tables", ("maintenance_changes",)),
        ("tables", ("Maintenance_Changes",)),
        ("dependencies", "projection"),
        ("dependencies", ("",)),
        ("priority", True),
        ("priority", 2**63),
        ("retry_limit", -1),
        ("retry_limit", True),
        ("retry_limit", 2**63),
        ("retry_backoff_s", float("nan")),
        ("retry_backoff_s", float("inf")),
        ("retry_backoff_s", -1),
        ("retry_backoff_s", 10**1000),
        ("retry_backoff_s", 1e100),
        ("enabled_by_default", "false"),
        ("enabled_by_default", 2),
    ],
)
def test_invalid_declaration_never_attaches_to_driver(field, value):
    plugin = Projection()
    setattr(plugin, field, value)
    driver = SimpleNamespace(_open_connections=[])
    with pytest.raises((ValueError, TypeError)):
        MaintenanceRuntime(SimpleNamespace(driver=driver), [plugin])
    assert "_maintenance_runtime" not in vars(driver)


@pytest.mark.parametrize("hook", ["wants_event", "coalesce_key", "handle_events"])
@pytest.mark.parametrize(
    "value", [None, 3, async_hook, generator_hook, AsyncCallable(), GeneratorCallable()]
)
def test_invalid_hook_is_refused_before_attachment(hook, value):
    plugin = Projection()
    setattr(plugin, hook, value)
    driver = SimpleNamespace(_open_connections=[])
    with pytest.raises((ValueError, TypeError)):
        MaintenanceRuntime(SimpleNamespace(driver=driver), [plugin])
    assert "_maintenance_runtime" not in vars(driver)


@pytest.mark.parametrize(
    "definition",
    [
        "CREATE TABLE bad_source (id TEXT PRIMARY KEY)",
        "CREATE TABLE bad_source (first INTEGER, second INTEGER, PRIMARY KEY (first, second))",
        "CREATE TABLE bad_source (id INTEGER)",
        "CREATE VIEW bad_source AS SELECT work_id FROM works",
    ],
)
def test_unsupported_subscription_rolls_back_installation(runtime_factory, definition):
    seed = runtime_factory([])
    with seed.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'preserved')")
        connection.execute(definition)
    plugin = Projection()
    plugin.tables = ("bad_source",)
    runtime = runtime_factory([plugin])
    with pytest.raises(ValueError, match="primary key|integer row identities"):
        runtime.install()
    assert not runtime.status()["installed"]
    check = runtime.driver.get_connection()
    assert check.execute("SELECT * FROM works").fetchall() == [(1, "preserved")]


@pytest.mark.parametrize("mode", [MaintenanceMode.PROMPT, MaintenanceMode.LAZY])
@pytest.mark.parametrize("selected", [None, 0, 1, "yes", [], {}])
def test_non_boolean_selection_cannot_acknowledge_work(runtime_factory, mode, selected):
    plugin = Projection()
    plugin.mode = mode
    plugin.wants_event = lambda event: selected
    runtime = runtime_factory([plugin])
    runtime.install()
    if mode is MaintenanceMode.PROMPT:
        with pytest.raises(MaintenanceFailure, match="boolean"):
            with runtime.unit() as connection:
                connection.execute("INSERT INTO works VALUES (1, 'source')")
    else:
        with runtime.unit() as connection:
            connection.execute("INSERT INTO works VALUES (1, 'source')")
        identity = runtime.status()["pending"][0]["obligation_id"]
        with pytest.raises(MaintenanceFailure, match="boolean"):
            runtime.deliver(plugin.name, [identity])
        pending = runtime.status()["pending"]
        assert len(pending) == 1 and pending[0]["attempts"] == 1
    connection = runtime.driver.get_connection()
    assert connection.execute("SELECT * FROM effects").fetchall() == []
    assert len(connection.execute("SELECT * FROM works").fetchall()) == int(
        mode is MaintenanceMode.LAZY
    )
    # A fixed implementation can resume after the rejected invocation.
    plugin.wants_event = lambda event: True
    if mode is MaintenanceMode.LAZY:
        runtime.retry(plugin.name)
        assert runtime.deliver(plugin.name, [identity]) == {"handled": 1}
    else:
        with runtime.unit() as connection:
            connection.execute("INSERT INTO works VALUES (1, 'recovered')")
    check = runtime.driver.get_connection()
    assert check.execute("SELECT COUNT(*) FROM effects").fetchone()[0] == 1


@pytest.mark.parametrize("mode", [MaintenanceMode.PROMPT, MaintenanceMode.LAZY])
@pytest.mark.parametrize(
    "result",
    [
        None,
        {},
        MaintenancePluginResult(handled=True),
        MaintenancePluginResult(handled=-1),
        MaintenancePluginResult(handled=2),
        MaintenancePluginResult(errors=1),
        MaintenancePluginResult(deferred=1),
    ],
)
def test_malformed_handler_results_roll_back_effects_and_recover(
    runtime_factory, mode, result
):
    class Broken(Projection):
        def handle_events(self, context, events):
            super().handle_events(context, events)
            return result

    plugin = Broken()
    plugin.mode = mode
    runtime = runtime_factory([plugin])
    runtime.install()
    if mode is MaintenanceMode.PROMPT:
        with pytest.raises(MaintenanceFailure):
            with runtime.unit() as connection:
                connection.execute("INSERT INTO works VALUES (1, 'source')")
    else:
        with runtime.unit() as connection:
            connection.execute("INSERT INTO works VALUES (1, 'source')")
        identity = runtime.status()["pending"][0]["obligation_id"]
        with pytest.raises(MaintenanceFailure):
            runtime.deliver(plugin.name, [identity])
        pending = runtime.status()["pending"]
        assert len(pending) == 1 and pending[0]["attempts"] == 1
        assert pending[0]["last_error"]
    connection = runtime.driver.get_connection()
    assert connection.execute("SELECT * FROM effects").fetchall() == []
    assert len(connection.execute("SELECT * FROM works").fetchall()) == int(
        mode is MaintenanceMode.LAZY
    )
    plugin.handle_events = Projection.handle_events.__get__(plugin)
    if mode is MaintenanceMode.LAZY:
        runtime.retry(plugin.name)
        runtime.deliver(plugin.name, [identity])
    else:
        with runtime.unit() as connection:
            connection.execute("INSERT INTO works VALUES (1, 'recovered')")
    check = runtime.driver.get_connection()
    assert check.execute("SELECT COUNT(*) FROM effects").fetchone()[0] == 1


@pytest.mark.parametrize("callback", [async_hook, generator_hook])
def test_non_synchronous_migration_never_publishes_upgrade(runtime_factory, callback):
    old = Projection()
    old.mode = MaintenanceMode.LAZY
    original = runtime_factory([old])
    original.install()
    with original.unit() as connection:
        connection.execute("INSERT INTO works VALUES (1, 'source')")
    before = original.status()
    new = Projection()
    new.mode = MaintenanceMode.LAZY
    new.version = "2"
    new.upgrades = (
        MaintenancePluginUpgrade("1", replay_pending=True, migrate=callback),
    )
    with pytest.raises((ValueError, TypeError, MaintenanceFailure)):
        upgraded = runtime_factory([new])
        upgraded.upgrade(new.name, expected_version="1", dry_run=False)
    after = original.status()
    assert after["plugins"] == before["plugins"]
    assert [
        (row["obligation_id"], row["plugin_version"]) for row in after["pending"]
    ] == [(row["obligation_id"], row["plugin_version"]) for row in before["pending"]]
    assert after["upgrades"] == []
