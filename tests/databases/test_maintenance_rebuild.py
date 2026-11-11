"""Verify bounded rebuild scheduling, recovery and freshness on both engines."""

from __future__ import annotations

import pytest

from LiuXin_alpha.catalog.maintenance import (
    WorkRevisionPlugin,
    WorkSummaryPlugin,
    rebuild_work_projections,
    work_summary,
)
from LiuXin_alpha.databases.maintenance.projection_sql import put_projection
from tests.databases.test_maintenance_runtime import runtime_factory as runtime_factory


def _make(runtime_factory):
    return runtime_factory([WorkRevisionPlugin(), WorkSummaryPlugin()])


def _seed(runtime, ids):
    with runtime.unit() as connection:
        connection.executemany(
            "INSERT INTO works VALUES (?,?)",
            [(value, f"Work {value}") for value in ids],
        )


def _schedule(runtime, *, limit=2):
    for _ in range(100):
        receipt = rebuild_work_projections(runtime.db, limit=limit)
        assert receipt["scheduled_this_page"] <= limit
        if receipt["scheduling_complete"]:
            return receipt
    pytest.fail("Rebuild failed to finish its bounded scan")


def _drain(runtime):
    while records := runtime.status()["pending"]:
        runtime.deliver(
            WorkSummaryPlugin.name, [row["obligation_id"] for row in records]
        )


def test_historical_rebuild_resumes_and_clears_corrupt_and_deleted_projections(
    runtime_factory,
):
    runtime = _make(runtime_factory)
    _seed(runtime, [0, 1, 2, 4])
    with runtime.unit() as connection:
        connection.executemany(
            "INSERT INTO expression_work_links VALUES (?,?,?)",
            [(1, 1, 10), (2, 1, 10), (3, 1, 11)],
        )
    runtime.install()
    assert not work_summary(runtime.db, 1)["current"]
    with runtime.unit() as connection, runtime._internal():
        for name in (WorkRevisionPlugin.name, WorkSummaryPlugin.name):
            # Deliberately corrupt high revisions must not prevent explicit repair.
            put_projection(connection, name, "1", '{"corrupt":true}', 999999)
            put_projection(connection, name, "9", '{"corrupt":true}', 999999)
    first = rebuild_work_projections(runtime.db, limit=2)
    assert first["source_cursor"] == 1
    assert first["phase"] == "source"
    assert first["scheduled"] == 2
    assert not work_summary(runtime.db, 1)["current"]
    restarted = _make(runtime_factory)
    result = _schedule(restarted)
    assert result["run_uuid"] == first["run_uuid"]
    assert result["scheduled"] == 5  # Four live sources and one deleted key.
    assert result["phase"] == "waiting"
    assert result["pending_through_checkpoint"] == 5
    _drain(restarted)
    assert work_summary(restarted.db, 1)["summary"]["expression_count"] == 2
    assert work_summary(restarted.db, 1)["current"]
    assert work_summary(restarted.db, 9)["summary"] == {
        "deleted": True,
        "expression_count": 0,
    }
    assert work_summary(restarted.db, 9)["current"]
    assert restarted.status()["rebuilds"][0]["phase"] == "complete"
    assert restarted.prune_completed(limit=500) == 5
    assert restarted.status()["rebuilds"][0]["phase"] == "complete"
    repeated = rebuild_work_projections(restarted.db, limit=2)
    assert repeated["scheduled_this_page"] == 0
    assert repeated["run_uuid"] == first["run_uuid"]
    with restarted.unit() as connection:
        assert connection.execute(
            "SELECT * FROM works ORDER BY work_id"
        ).fetchall() == [(value, f"Work {value}") for value in [0, 1, 2, 4]]
    new = rebuild_work_projections(restarted.db, limit=2, restart=True)
    assert new["run_uuid"] != first["run_uuid"]
    assert new["scheduled"] == 2


def test_prompt_failure_rolls_back_entire_page_and_preserves_checkpoint(
    runtime_factory, monkeypatch
):
    runtime = _make(runtime_factory)
    _seed(runtime, [1, 2, 3])
    runtime.install()
    first = rebuild_work_projections(runtime.db, limit=1)
    before = runtime.status()
    original = WorkRevisionPlugin.handle_events

    def fail(self, context, events):
        original(self, context, events)
        raise RuntimeError("rebuild prompt failure")

    monkeypatch.setattr(WorkRevisionPlugin, "handle_events", fail)
    with pytest.raises(RuntimeError, match="rebuild prompt failure"):
        rebuild_work_projections(runtime.db, limit=2)
    state = runtime.status()
    assert state["rebuilds"] == before["rebuilds"]
    assert [r["obligation_id"] for r in state["pending"]] == [
        r["obligation_id"] for r in before["pending"]
    ]
    assert work_summary(runtime.db, 2)["source_revision"] is None
    monkeypatch.setattr(WorkRevisionPlugin, "handle_events", original)
    result = _schedule(runtime)
    assert result["run_uuid"] == first["run_uuid"]
    assert result["scheduled"] == 3
    _drain(runtime)
    assert all(work_summary(runtime.db, value)["current"] for value in [1, 2, 3])


def test_normal_capture_covers_inserts_updates_and_deletes_between_pages(
    runtime_factory,
):
    runtime = _make(runtime_factory)
    _seed(runtime, [1, 3, 5])
    runtime.install()
    first = rebuild_work_projections(runtime.db, limit=1)
    assert first["source_bound"] == 5
    with runtime.unit() as connection:
        connection.execute("DELETE FROM works WHERE work_id=3")
        connection.execute("INSERT INTO works VALUES (0,'insert behind cursor')")
        connection.execute("INSERT INTO works VALUES (7,'insert beyond bound')")
        connection.execute("INSERT INTO expression_work_links VALUES (1,1,100)")
    result = _schedule(runtime, limit=1)
    assert result["source_bound"] == 5
    assert result["scheduled"] == 2
    _drain(runtime)
    for value in [0, 1, 3, 5, 7]:
        assert work_summary(runtime.db, value)["current"]
    assert work_summary(runtime.db, 3)["summary"]["deleted"]
    assert work_summary(runtime.db, 1)["summary"]["expression_count"] == 1


def test_empty_rebuild_and_status_do_not_create_jobs_or_source_rows(runtime_factory):
    runtime = _make(runtime_factory)
    with pytest.raises(RuntimeError, match="Install maintenance"):
        rebuild_work_projections(runtime.db)
    assert not runtime.status()["installed"]
    runtime.install()
    result = _schedule(runtime)
    assert result["phase"] == "complete"
    assert result["scheduled"] == result["scanned"] == 0
    assert runtime.status()["pending"] == []


@pytest.mark.parametrize("limit", [0, -1, 501, True, 1.5])
def test_rebuild_refuses_unbounded_or_inexact_page_sizes(runtime_factory, limit):
    runtime = _make(runtime_factory)
    with pytest.raises(ValueError, match="page size"):
        rebuild_work_projections(runtime.db, limit=limit)
    assert not runtime.status()["installed"]


def test_lazy_failure_keeps_rebuild_waiting_until_explicit_retry(
    runtime_factory, monkeypatch
):
    runtime = _make(runtime_factory)
    _seed(runtime, [1])
    runtime.install()
    _schedule(runtime)
    original = WorkSummaryPlugin.handle_events

    def fail(self, context, events):
        original(self, context, events)
        raise RuntimeError("lazy rebuild failed")

    monkeypatch.setattr(WorkSummaryPlugin, "handle_events", fail)
    with pytest.raises(RuntimeError, match="lazy rebuild failed"):
        _drain(runtime)
    assert runtime.status()["rebuilds"][0]["phase"] == "waiting"
    assert not work_summary(runtime.db, 1)["current"]
    monkeypatch.setattr(WorkSummaryPlugin, "handle_events", original)
    runtime.retry(WorkSummaryPlugin.name)
    _drain(runtime)
    assert runtime.status()["rebuilds"][0]["phase"] == "complete"
