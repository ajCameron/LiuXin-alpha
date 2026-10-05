"""
Provide test event log handler utility behavior.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise test event log handler through a consuming regression::

        python -m pytest -q tests/utils/logging/test_event_log_handler.py
"""
from __future__ import annotations

import json
import logging
import threading

from pathlib import Path
from uuid import uuid4

from LiuXin_alpha.utils.logging.event_logs import EventLogHandler, InMemoryEventLog
from LiuXin_alpha.utils.logging.run_logging import (
    LiuXinTextLogFormatter,
    LoggingTextStream,
    RunLoggingSession,
)


def _jsonl(path: Path) -> list[dict[str, object]]:
    """
    Perform the jsonl utility operation under explicit compatibility rules.

    Example:
        Exercise  jsonl through a consuming regression::

            python -m pytest -q tests/utils/logging/test_event_log_handler.py


    :param path: Filesystem path read, written, normalized or validated by the
        operation.
    :return: The normalized value, metadata record, path, stream result or collection
        described above.
    """
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_persisted_level_name_does_not_deadlock(tmp_path: Path) -> None:
    """
    Perform the test persisted level name does not deadlock utility operation under explicit compatibility rules.

    Example:
        Exercise test persisted level name does not deadlock through a consuming regression::

            python -m pytest -q tests/utils/logging/test_event_log_handler.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    event_log = InMemoryEventLog(
        persist_path=tmp_path / "events.jsonl",
        include_level_name_in_jsonl=True,
    )
    finished = threading.Event()

    def write() -> None:
        """
        Forward the write operation while preserving adapter ownership rules.

        Example:
            Exercise test persisted level name does not deadlock.write through a consuming regression::

                python -m pytest -q tests/utils/logging/test_event_log_handler.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        _ = event_log.put_event("ready", level=logging.INFO)
        finished.set()

    thread = threading.Thread(target=write, daemon=True)
    thread.start()
    thread.join(timeout=2)
    assert finished.is_set(), "persisted event logging deadlocked"
    event_log.close()

    [event] = _jsonl(tmp_path / "events.jsonl")
    assert event["message"] == "ready"
    assert event["level_name"] == "INFO"


def test_persistence_round_trips_surrogateescaped_paths(tmp_path: Path) -> None:
    """
    Perform the test persistence round trips surrogateescaped paths utility operation under explicit compatibility rules.

    Example:
        Exercise test persistence round trips surrogateescaped paths through a consuming regression::

            python -m pytest -q tests/utils/logging/test_event_log_handler.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    path = tmp_path / "events.jsonl"
    tortured = "pack-\udcff/深い/📚 e\u0301.epub"
    with InMemoryEventLog(persist_path=path) as event_log:
        _ = event_log.put_event(
            "path observed",
            context={"path": tortured},
        )

    [event] = _jsonl(path)
    assert event["context"] == {"path": tortured}
    assert "\\udcff" in path.read_text(encoding="utf-8")


def test_logging_handler_retains_structured_context_and_traceback() -> None:
    """
    Perform the test logging handler retains structured context and traceback utility operation under explicit compatibility rules.

    Example:
        Exercise test logging handler retains structured context and traceback through a consuming regression::

            python -m pytest -q tests/utils/logging/test_event_log_handler.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    event_log = InMemoryEventLog()
    handler = EventLogHandler(event_log)
    logger = logging.getLogger("tests.liuxin.event-handler")
    old_level = logger.level
    old_propagate = logger.propagate
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    logger.addHandler(handler)
    try:
        try:
            raise ValueError("broken archive")
        except ValueError:
            logger.exception(
                "member failed",
                extra={
                    "liuxin_event": "member_error",
                    "liuxin_context": {
                        "member_path": "bad/\udcff.epub",
                        "attempt": 2,
                    },
                },
            )
    finally:
        logger.removeHandler(handler)
        handler.close()
        logger.setLevel(old_level)
        logger.propagate = old_propagate

    [event] = tuple(event_log.get_events())
    assert event.message == "member failed"
    assert event.context["event"] == "member_error"
    assert event.context["details"] == {
        "member_path": "bad/\udcff.epub",
        "attempt": 2,
    }
    assert "ValueError: broken archive" in event.context["traceback"]
    event_log.close()


def test_run_logging_session_writes_complete_jsonl_and_rotating_text(
    tmp_path: Path,
) -> None:
    """
    Perform the test run logging session writes complete jsonl and rotating text utility operation under explicit compatibility rules.

    Example:
        Exercise test run logging session writes complete jsonl and rotating text through a consuming regression::

            python -m pytest -q tests/utils/logging/test_event_log_handler.py


    :param tmp_path: Value supplied for tmp path under the utility contract.
    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    run_id = uuid4()
    logger = logging.getLogger("tests.liuxin.run-session")
    with RunLoggingSession(
        tmp_path,
        run_id=run_id,
        prefix="test-run",
        max_text_bytes=350,
        text_backup_count=2,
    ) as session:
        assert session.paths is not None
        paths = session.paths
        for number in range(20):
            logger.info(
                "object %d %s",
                number,
                "x" * 80,
                extra={
                    "liuxin_event": "object_seen",
                    "liuxin_context": {"run_id": str(run_id), "number": number},
                },
            )
        stream = LoggingTextStream(
            logger,
            level=logging.DEBUG,
            stream_name="legacy_stdout",
        )
        _ = stream.write("legacy line\npartial")
        stream.flush()

    events = _jsonl(paths.event_log)
    assert len(events) == 22
    assert [event["context"]["event"] for event in events[-2:]] == [
        "captured_output",
        "captured_output",
    ]
    assert events[0]["context"]["details"] == {
        "number": 0,
        "run_id": str(run_id),
    }
    assert paths.human_log.is_file()
    rotated = tuple(tmp_path.glob(paths.human_log.name + ".*"))
    assert 1 <= len(rotated) <= 2
    combined_text = "".join(
        path.read_text(encoding="utf-8") for path in (paths.human_log, *rotated)
    )
    assert "legacy line" in combined_text or "partial" in combined_text


def test_human_formatter_bounds_cyclic_context() -> None:
    """
    Perform the test human formatter bounds cyclic context utility operation under explicit compatibility rules.

    Example:
        Exercise test human formatter bounds cyclic context through a consuming regression::

            python -m pytest -q tests/utils/logging/test_event_log_handler.py


    :return: None; the operation mutates state, writes output or performs cleanup in
        place.
    """
    cyclic: dict[str, object] = {}
    cyclic["again"] = cyclic
    record = logging.LogRecord(
        "tests.liuxin.cyclic",
        logging.INFO,
        __file__,
        1,
        "cyclic context",
        (),
        None,
    )
    record.liuxin_event = "cyclic_event"
    record.liuxin_context = cyclic

    rendered = LiuXinTextLogFormatter("%(levelname)s %(message)s").format(record)

    assert "INFO cyclic context" in rendered
    assert "event=cyclic_event" in rendered
    assert "context=" in rendered
