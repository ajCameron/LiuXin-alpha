from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


# def test_in_memory_event_log_put_get_and_filtering(tmp_path: Path) -> None:
#     from LiuXin_alpha.utils.logging.event_logs.in_memory_list import InMemoryEventLog
#
#     log = InMemoryEventLog(max_entries=3, persist_path=tmp_path / "events.jsonl", include_level_name_in_jsonl=True)
#
#     t0 = datetime(2020, 1, 1, tzinfo=timezone.utc)
#     log.put_event("hello\nworld", level=20, ts=t0, context={"a": 1})
#     log.put_event("warn", level=30, ts=t0 + timedelta(seconds=1))
#     log.put_event("err", level=40, ts=t0 + timedelta(seconds=2))
#
#     # normalize_multiline True by default
#     rendered = list(log.get())
#     assert any("hello\\nworld" in s for s in rendered)
#
#     # filters
#     assert [e.message for e in log.get_events(level_min=40)] == ["err"]
#     assert [e.message for e in log.get_events(contains="ar")] == ["warn"]
#     assert [e.message for e in log.get_events(since_ts=t0 + timedelta(seconds=1))] == ["err"]
#
#     # retention: add one more -> drops oldest
#     log.put_event("new", level=20)
#     msgs = [e.message for e in log.get_events(reverse=False)]
#     assert "hello\\nworld" not in msgs
#
#     # persistence written
#     lines = (tmp_path / "events.jsonl").read_text(encoding="utf-8").splitlines()
#     assert lines
#     payload = json.loads(lines[0])
#     assert payload["id"] >= 1
#     assert payload["level_name"]
#
#
# def test_in_memory_event_log_set_max_entries_resizes_and_validates() -> None:
#     from LiuXin_alpha.utils.logging.event_logs.in_memory_list import InMemoryEventLog
#
#     log = InMemoryEventLog(max_entries=5)
#     for i in range(5):
#         log.put_event(f"m{i}")
#     log.set_max_entries(2)
#     assert log.max_entries == 2
#     assert [e.message for e in log.get_events(reverse=False)] == ["m3", "m4"]
#     with pytest.raises(ValueError):
#         log.set_max_entries(0)
#
#
# def test_in_memory_event_log_follow_yields_new_events_and_stops_on_close() -> None:
#     from LiuXin_alpha.utils.logging.event_logs.in_memory_list import InMemoryEventLog
#
#     log = InMemoryEventLog(max_entries=100)
#
#     out: list[str] = []
#
#     def producer() -> None:
#         for i in range(3):
#             time.sleep(0.02)
#             log.put_event(f"p{i}")
#         log.close()
#
#     t = threading.Thread(target=producer)
#     t.start()
#
#     for ev in log.follow(poll_interval_s=0.01):
#         out.append(ev.message)
#     t.join(timeout=1)
#
#     assert out[:3] == ["p0", "p1", "p2"]
#
#
# def test_in_memory_event_log_rejects_bad_inputs() -> None:
#     from LiuXin_alpha.utils.logging.event_logs.in_memory_list import InMemoryEventLog
#
#     log = InMemoryEventLog()
#     with pytest.raises(TypeError):
#         log.put_event(123)  # type: ignore[arg-type]
#     with pytest.raises(TypeError):
#         log.put_event("x", level="INFO")  # type: ignore[arg-type]
#     with pytest.raises(TypeError):
#         log.put_event("x", context=[("a", 1)])  # type: ignore[arg-type]
#     with pytest.raises(ValueError):
#         list(log.follow(poll_interval_s=0))