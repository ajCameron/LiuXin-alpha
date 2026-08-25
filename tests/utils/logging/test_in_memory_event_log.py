from __future__ import annotations

import json
import threading
import time

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from LiuXin_alpha.utils.logging.event_logs import InMemoryEventLog


def test_in_memory_event_log_put_get_filter_and_persist(tmp_path: Path) -> None:
    path = tmp_path / "events.jsonl"
    event_log = InMemoryEventLog(
        max_entries=3,
        persist_path=path,
        include_level_name_in_jsonl=True,
    )
    t0 = datetime(2020, 1, 1, tzinfo=timezone.utc)
    _ = event_log.put_event("hello\nworld", level=20, ts=t0, context={"a": 1})
    _ = event_log.put_event("warn", level=30, ts=t0 + timedelta(seconds=1))
    _ = event_log.put_event("err", level=40, ts=t0 + timedelta(seconds=2))

    assert any("hello\\nworld" in rendered for rendered in event_log.get())
    assert [event.message for event in event_log.get_events(level_min=40)] == [
        "err"
    ]
    assert [event.message for event in event_log.get_events(contains="ar")] == [
        "warn"
    ]
    assert [
        event.message
        for event in event_log.get_events(
            since_ts=t0 + timedelta(seconds=1)
        )
    ] == ["err"]

    _ = event_log.put_event("new", level=20)
    assert [
        event.message for event in event_log.get_events(reverse=False)
    ] == ["warn", "err", "new"]
    # Each put is flushed, so the durable stream is readable before close.
    lines = path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 4
    assert json.loads(lines[0])["level_name"] == "INFO"
    event_log.close()


def test_in_memory_event_log_resize_and_level_names() -> None:
    with InMemoryEventLog(max_entries=5) as event_log:
        for index in range(5):
            event_log.put(f"m{index}")
        event_log.set_max_entries(2)
        assert event_log.max_entries == 2
        assert [
            event.message for event in event_log.get_events(reverse=False)
        ] == ["m3", "m4"]
        event_log.set_level_names({20: "NOTICE"})
        assert event_log.level_name(20) == "NOTICE"
        event_log.set_level_names({7: "TRACE"}, replace=True)
        assert event_log.get_level_names() == {7: "TRACE"}
        assert event_log.level_name(20) == "LVL20"
        with pytest.raises(ValueError):
            event_log.set_max_entries(0)


def test_in_memory_event_log_follow_drains_events_before_close() -> None:
    event_log = InMemoryEventLog(max_entries=100)
    observed: list[str] = []

    def producer() -> None:
        for index in range(3):
            time.sleep(0.02)
            event_log.put(f"p{index}")
        event_log.close()

    thread = threading.Thread(target=producer)
    thread.start()
    for event in event_log.follow(poll_interval_s=0.01):
        observed.append(event.message)
    thread.join(timeout=1)

    assert not thread.is_alive()
    assert observed == ["p0", "p1", "p2"]


def test_in_memory_event_log_rejects_bad_inputs_and_closed_writes() -> None:
    event_log = InMemoryEventLog()
    with pytest.raises(TypeError):
        event_log.put_event(123)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        event_log.put_event("x", level="INFO")  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        event_log.put_event("x", context=[("a", 1)])  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        event_log.put_event("x", context={1: "bad"})  # type: ignore[dict-item]
    with pytest.raises(ValueError):
        tuple(event_log.follow(poll_interval_s=0))
    event_log.close()
    with pytest.raises(RuntimeError, match="closed"):
        event_log.put("after close")
