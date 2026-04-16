"""SQLite-backed persistence for durable jobs.

This repository stores job definitions, job runs, and append-only events. It is
intentionally light-weight and pragmatic: payloads and schedules are kept as
JSON strings until a clearer normalization need emerges.
"""

from __future__ import annotations

import contextlib
import json
import pathlib
import sqlite3

from typing import Iterable

from LiuXin_alpha.jobs.models import (
    JobConcurrencyPolicy,
    JobDefinition,
    JobDefinitionState,
    JobEventKind,
    JobProgressUpdate,
    JobResultPolicy,
    JobRun,
    JobRunEvent,
    JobRunState,
    JobTriggerKind,
    now_ep_k,
)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS job_definitions (
    job_definition_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_kind TEXT NOT NULL,
    job_name TEXT NOT NULL,
    state TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    schedule_json TEXT,
    priority INTEGER NOT NULL DEFAULT 100,
    concurrency_policy TEXT NOT NULL,
    result_policy TEXT NOT NULL,
    timeout_s REAL,
    max_retries INTEGER NOT NULL DEFAULT 0,
    retry_backoff_s REAL NOT NULL DEFAULT 0,
    heartbeat_timeout_s REAL,
    created_timestamp_ep_k INTEGER NOT NULL,
    modified_timestamp_ep_k INTEGER NOT NULL,
    last_queued_timestamp_ep_k INTEGER,
    last_started_timestamp_ep_k INTEGER,
    last_finished_timestamp_ep_k INTEGER
);
CREATE TABLE IF NOT EXISTS job_runs (
    job_run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_definition_id INTEGER NOT NULL,
    job_kind TEXT NOT NULL,
    trigger_kind TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt_number INTEGER NOT NULL DEFAULT 1,
    worker_id TEXT,
    lease_expires_timestamp_ep_k INTEGER,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    not_before_timestamp_ep_k INTEGER,
    progress_current INTEGER,
    progress_total INTEGER,
    progress_unit TEXT,
    progress_message TEXT,
    result_json TEXT,
    error_text TEXT,
    log_path TEXT,
    queued_timestamp_ep_k INTEGER,
    started_timestamp_ep_k INTEGER,
    heartbeat_timestamp_ep_k INTEGER,
    finished_timestamp_ep_k INTEGER,
    FOREIGN KEY(job_definition_id) REFERENCES job_definitions(job_definition_id)
);
CREATE INDEX IF NOT EXISTS idx_job_runs_state_not_before ON job_runs(state, not_before_timestamp_ep_k);
CREATE INDEX IF NOT EXISTS idx_job_runs_definition ON job_runs(job_definition_id);
CREATE TABLE IF NOT EXISTS job_run_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_run_id INTEGER NOT NULL,
    event_kind TEXT NOT NULL,
    event_message TEXT,
    event_json TEXT,
    created_timestamp_ep_k INTEGER NOT NULL,
    FOREIGN KEY(job_run_id) REFERENCES job_runs(job_run_id)
);
CREATE INDEX IF NOT EXISTS idx_job_run_events_run ON job_run_events(job_run_id, created_timestamp_ep_k);
"""


class JobRepository:
    """SQLite-backed store for durable jobs and job runs."""

    def __init__(self, sqlite_path: str | pathlib.Path) -> None:
        self.sqlite_path = pathlib.Path(sqlite_path).expanduser()
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        self.ensure_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.sqlite_path))
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(_SCHEMA)
            conn.commit()

    def create_definition(self, definition: JobDefinition) -> JobDefinition:
        now = now_ep_k()
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO job_definitions(
                    job_kind, job_name, state, payload_json, schedule_json, priority,
                    concurrency_policy, result_policy, timeout_s, max_retries,
                    retry_backoff_s, heartbeat_timeout_s,
                    created_timestamp_ep_k, modified_timestamp_ep_k
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    definition.job_kind,
                    definition.job_name,
                    definition.state.value,
                    definition.payload_json,
                    definition.schedule_json,
                    int(definition.priority),
                    definition.concurrency_policy.value,
                    definition.result_policy.value,
                    definition.timeout_s,
                    int(definition.max_retries),
                    float(definition.retry_backoff_s),
                    definition.heartbeat_timeout_s,
                    now,
                    now,
                ),
            )
            conn.commit()
            job_definition_id = int(cur.lastrowid)
        self.append_definition_event(job_definition_id, "created", f"Created definition {definition.job_name}")
        return self.get_definition(job_definition_id)

    def update_definition(self, definition: JobDefinition) -> JobDefinition:
        if definition.job_definition_id is None:
            raise ValueError("update_definition requires job_definition_id")
        now = now_ep_k()
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE job_definitions
                SET job_kind=?, job_name=?, state=?, payload_json=?, schedule_json=?, priority=?,
                    concurrency_policy=?, result_policy=?, timeout_s=?, max_retries=?,
                    retry_backoff_s=?, heartbeat_timeout_s=?, modified_timestamp_ep_k=?
                WHERE job_definition_id=?
                """,
                (
                    definition.job_kind,
                    definition.job_name,
                    definition.state.value,
                    definition.payload_json,
                    definition.schedule_json,
                    int(definition.priority),
                    definition.concurrency_policy.value,
                    definition.result_policy.value,
                    definition.timeout_s,
                    int(definition.max_retries),
                    float(definition.retry_backoff_s),
                    definition.heartbeat_timeout_s,
                    now,
                    int(definition.job_definition_id),
                ),
            )
            conn.commit()
        return self.get_definition(int(definition.job_definition_id))

    def get_definition(self, job_definition_id: int) -> JobDefinition:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM job_definitions WHERE job_definition_id=?", (int(job_definition_id),)).fetchone()
        if row is None:
            raise KeyError(f"Unknown job definition id: {job_definition_id!r}")
        return self._definition_from_row(row)

    def list_definitions(self, *, states: Iterable[JobDefinitionState] | None = None) -> list[JobDefinition]:
        sql = "SELECT * FROM job_definitions"
        params: list[object] = []
        if states is not None:
            states_list = [str(s.value if isinstance(s, JobDefinitionState) else s) for s in states]
            if states_list:
                placeholders = ", ".join("?" for _ in states_list)
                sql += f" WHERE state IN ({placeholders})"
                params.extend(states_list)
        sql += " ORDER BY priority ASC, job_definition_id ASC"
        with self.connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [self._definition_from_row(row) for row in rows]

    def enqueue_run(self, *, job_definition_id: int, trigger_kind: JobTriggerKind = JobTriggerKind.MANUAL, not_before_timestamp_ep_k: int | None = None) -> JobRun:
        definition = self.get_definition(job_definition_id)
        self._apply_concurrency_policy_on_enqueue(definition)
        now = now_ep_k()
        with self.connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO job_runs(
                    job_definition_id, job_kind, trigger_kind, state, attempt_number,
                    not_before_timestamp_ep_k, queued_timestamp_ep_k
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(job_definition_id),
                    definition.job_kind,
                    trigger_kind.value,
                    JobRunState.QUEUED.value,
                    1,
                    not_before_timestamp_ep_k,
                    now,
                ),
            )
            conn.execute(
                "UPDATE job_definitions SET last_queued_timestamp_ep_k=?, modified_timestamp_ep_k=? WHERE job_definition_id=?",
                (now, now, int(job_definition_id)),
            )
            conn.commit()
            job_run_id = int(cur.lastrowid)
        self.append_event(job_run_id, JobEventKind.QUEUED, f"Queued via {trigger_kind.value}")
        return self.get_run(job_run_id)

    def lease_next_run(self, *, worker_id: str, lease_for_s: float) -> JobRun | None:
        now = now_ep_k()
        lease_expires = now + int(max(1.0, float(lease_for_s)) * 1000)
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM job_runs
                WHERE (
                    state = ? OR
                    (state = ? AND lease_expires_timestamp_ep_k IS NOT NULL AND lease_expires_timestamp_ep_k < ?)
                )
                AND (not_before_timestamp_ep_k IS NULL OR not_before_timestamp_ep_k <= ?)
                ORDER BY job_run_id ASC
                LIMIT 1
                """,
                (JobRunState.QUEUED.value, JobRunState.LEASED.value, now, now),
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                """
                UPDATE job_runs
                SET state=?, worker_id=?, lease_expires_timestamp_ep_k=?, heartbeat_timestamp_ep_k=?
                WHERE job_run_id=?
                """,
                (JobRunState.LEASED.value, str(worker_id), lease_expires, now, int(row["job_run_id"])),
            )
            conn.commit()
            run_id = int(row["job_run_id"])
        self.append_event(run_id, JobEventKind.LEASED, f"Leased to {worker_id}")
        return self.get_run(run_id)

    def mark_running(self, job_run_id: int, *, worker_id: str) -> JobRun:
        now = now_ep_k()
        run = self.get_run(job_run_id)
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE job_runs
                SET state=?, worker_id=?, started_timestamp_ep_k=?, heartbeat_timestamp_ep_k=?
                WHERE job_run_id=?
                """,
                (JobRunState.RUNNING.value, str(worker_id), now, now, int(job_run_id)),
            )
            conn.execute(
                """
                UPDATE job_definitions
                SET last_started_timestamp_ep_k=?, modified_timestamp_ep_k=?
                WHERE job_definition_id=?
                """,
                (now, now, int(run.job_definition_id)),
            )
            conn.commit()
        self.append_event(int(job_run_id), JobEventKind.STARTED, f"Started on worker {worker_id}")
        return self.get_run(int(job_run_id))

    def heartbeat(self, job_run_id: int, *, worker_id: str, message: str | None = None, lease_for_s: float | None = 60.0) -> None:
        now = now_ep_k()
        lease_expires = None if lease_for_s is None else now + int(max(1.0, float(lease_for_s)) * 1000)
        with self.connect() as conn:
            conn.execute(
                "UPDATE job_runs SET heartbeat_timestamp_ep_k=?, worker_id=?, lease_expires_timestamp_ep_k=COALESCE(?, lease_expires_timestamp_ep_k) WHERE job_run_id=?",
                (now, str(worker_id), lease_expires, int(job_run_id)),
            )
            conn.commit()
        self.append_event(int(job_run_id), JobEventKind.HEARTBEAT, message or "Heartbeat")

    def refresh_lease(self, job_run_id: int, *, worker_id: str, lease_for_s: float) -> None:
        now = now_ep_k()
        lease_expires = now + int(max(1.0, float(lease_for_s)) * 1000)
        with self.connect() as conn:
            conn.execute(
                "UPDATE job_runs SET worker_id=?, lease_expires_timestamp_ep_k=?, heartbeat_timestamp_ep_k=? WHERE job_run_id=?",
                (str(worker_id), lease_expires, now, int(job_run_id)),
            )
            conn.commit()

    def update_progress(self, job_run_id: int, update: JobProgressUpdate) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE job_runs
                SET progress_current=?, progress_total=?, progress_unit=?, progress_message=?, heartbeat_timestamp_ep_k=?
                WHERE job_run_id=?
                """,
                (
                    update.progress_current,
                    update.progress_total,
                    update.progress_unit,
                    update.progress_message,
                    now_ep_k(),
                    int(job_run_id),
                ),
            )
            conn.commit()
        self.append_event(int(job_run_id), JobEventKind.PROGRESS, update.progress_message or "Progress", event_json=json.dumps({
            "progress_current": update.progress_current,
            "progress_total": update.progress_total,
            "progress_unit": update.progress_unit,
        }))

    def mark_succeeded(self, job_run_id: int, *, result_json: str | None = None) -> JobRun:
        now = now_ep_k()
        run = self.get_run(job_run_id)
        with self.connect() as conn:
            conn.execute(
                "UPDATE job_runs SET state=?, result_json=?, finished_timestamp_ep_k=?, lease_expires_timestamp_ep_k=NULL WHERE job_run_id=?",
                (JobRunState.SUCCEEDED.value, result_json, now, int(job_run_id)),
            )
            conn.execute(
                "UPDATE job_definitions SET last_finished_timestamp_ep_k=?, modified_timestamp_ep_k=? WHERE job_definition_id=?",
                (now, now, int(run.job_definition_id)),
            )
            conn.commit()
        self.append_event(int(job_run_id), JobEventKind.SUCCEEDED, "Succeeded", event_json=result_json)
        self._apply_result_policy(run.job_definition_id)
        return self.get_run(int(job_run_id))

    def mark_failed(self, job_run_id: int, *, error_text: str) -> JobRun:
        now = now_ep_k()
        run = self.get_run(job_run_id)
        with self.connect() as conn:
            conn.execute(
                "UPDATE job_runs SET state=?, error_text=?, finished_timestamp_ep_k=?, lease_expires_timestamp_ep_k=NULL WHERE job_run_id=?",
                (JobRunState.FAILED.value, str(error_text), now, int(job_run_id)),
            )
            conn.execute(
                "UPDATE job_definitions SET last_finished_timestamp_ep_k=?, modified_timestamp_ep_k=? WHERE job_definition_id=?",
                (now, now, int(run.job_definition_id)),
            )
            conn.commit()
        self.append_event(int(job_run_id), JobEventKind.FAILED, str(error_text))
        self._apply_result_policy(run.job_definition_id)
        return self.get_run(int(job_run_id))

    def mark_cancelled(self, job_run_id: int, *, error_text: str | None = None) -> JobRun:
        now = now_ep_k()
        run = self.get_run(job_run_id)
        with self.connect() as conn:
            conn.execute(
                "UPDATE job_runs SET state=?, error_text=?, finished_timestamp_ep_k=?, lease_expires_timestamp_ep_k=NULL WHERE job_run_id=?",
                (JobRunState.CANCELLED.value, error_text, now, int(job_run_id)),
            )
            conn.execute(
                "UPDATE job_definitions SET last_finished_timestamp_ep_k=?, modified_timestamp_ep_k=? WHERE job_definition_id=?",
                (now, now, int(run.job_definition_id)),
            )
            conn.commit()
        self.append_event(int(job_run_id), JobEventKind.CANCELLED, error_text or "Cancelled")
        self._apply_result_policy(run.job_definition_id)
        return self.get_run(int(job_run_id))

    def request_cancel(self, job_run_id: int) -> None:
        with self.connect() as conn:
            conn.execute("UPDATE job_runs SET cancel_requested=1 WHERE job_run_id=?", (int(job_run_id),))
            conn.commit()
        self.append_event(int(job_run_id), JobEventKind.CANCEL_REQUESTED, "Cancellation requested")

    def get_run(self, job_run_id: int) -> JobRun:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM job_runs WHERE job_run_id=?", (int(job_run_id),)).fetchone()
        if row is None:
            raise KeyError(f"Unknown job run id: {job_run_id!r}")
        return self._run_from_row(row)

    def list_runs(self, *, job_definition_id: int | None = None, states: Iterable[JobRunState] | None = None) -> list[JobRun]:
        sql = "SELECT * FROM job_runs"
        clauses: list[str] = []
        params: list[object] = []
        if job_definition_id is not None:
            clauses.append("job_definition_id=?")
            params.append(int(job_definition_id))
        if states is not None:
            state_values = [str(s.value if isinstance(s, JobRunState) else s) for s in states]
            if state_values:
                placeholders = ", ".join("?" for _ in state_values)
                clauses.append(f"state IN ({placeholders})")
                params.extend(state_values)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY job_run_id ASC"
        with self.connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [self._run_from_row(row) for row in rows]

    def list_events(self, job_run_id: int) -> list[JobRunEvent]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT * FROM job_run_events WHERE job_run_id=? ORDER BY event_id ASC",
                (int(job_run_id),),
            ).fetchall()
        return [self._event_from_row(row) for row in rows]

    def append_log(self, job_run_id: int, *, message: str, event_json: str | None = None) -> None:
        self.append_event(job_run_id, JobEventKind.LOG, message, event_json=event_json)

    def append_event(self, job_run_id: int, event_kind: JobEventKind, message: str | None = None, *, event_json: str | None = None) -> None:
        with self.connect() as conn:
            conn.execute(
                "INSERT INTO job_run_events(job_run_id, event_kind, event_message, event_json, created_timestamp_ep_k) VALUES (?, ?, ?, ?, ?)",
                (int(job_run_id), event_kind.value, message, event_json, now_ep_k()),
            )
            conn.commit()

    def append_definition_event(self, job_definition_id: int, event_kind: str, message: str) -> None:
        del job_definition_id, event_kind, message
        # Placeholder for later definition-event stream; run events cover the MVP.

    def get_due_definitions_for_scheduling(self, *, now_timestamp_ep_k: int | None = None) -> list[JobDefinition]:
        now_value = now_ep_k() if now_timestamp_ep_k is None else int(now_timestamp_ep_k)
        due: list[JobDefinition] = []
        for definition in self.list_definitions(states=[JobDefinitionState.ENABLED]):
            schedule = self._load_schedule_json(definition.schedule_json)
            interval_s = schedule.get("interval_seconds")
            if interval_s in (None, "", 0):
                continue
            every_ms = int(float(interval_s) * 1000)
            baseline = definition.last_queued_timestamp_ep_k or definition.created_timestamp_ep_k or 0
            if baseline <= 0 or now_value >= baseline + every_ms:
                due.append(definition)
        return due

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _apply_concurrency_policy_on_enqueue(self, definition: JobDefinition) -> None:
        active = self.list_runs(
            job_definition_id=int(definition.job_definition_id or 0),
            states=[JobRunState.QUEUED, JobRunState.LEASED, JobRunState.RUNNING],
        )
        policy = definition.concurrency_policy
        if policy is JobConcurrencyPolicy.ALLOW_PARALLEL:
            return
        if policy is JobConcurrencyPolicy.SKIP_IF_RUNNING and active:
            raise RuntimeError(f"Job definition {definition.job_name!r} is already active")
        if policy is JobConcurrencyPolicy.QUEUE_ONE and active:
            raise RuntimeError(f"Job definition {definition.job_name!r} already has a queued/running run")
        if policy is JobConcurrencyPolicy.REPLACE_RUNNING:
            for run in active:
                self.request_cancel(int(run.job_run_id))

    def _apply_result_policy(self, job_definition_id: int) -> None:
        definition = self.get_definition(int(job_definition_id))
        if definition.result_policy is JobResultPolicy.KEEP_ALL:
            return
        runs = self.list_runs(job_definition_id=int(job_definition_id))
        if definition.result_policy is JobResultPolicy.KEEP_FAILURES:
            doomed = [r for r in runs if r.state is JobRunState.SUCCEEDED][:-1]
        elif definition.result_policy is JobResultPolicy.KEEP_LATEST_ONLY:
            doomed = runs[:-1]
        else:
            doomed = []
        if not doomed:
            return
        doomed_ids = tuple(int(r.job_run_id) for r in doomed if r.job_run_id is not None)
        if not doomed_ids:
            return
        placeholders = ", ".join("?" for _ in doomed_ids)
        with self.connect() as conn:
            conn.execute(f"DELETE FROM job_run_events WHERE job_run_id IN ({placeholders})", doomed_ids)
            conn.execute(f"DELETE FROM job_runs WHERE job_run_id IN ({placeholders})", doomed_ids)
            conn.commit()

    @staticmethod
    def _load_schedule_json(schedule_json: str | None) -> dict[str, object]:
        if not schedule_json:
            return {}
        with contextlib.suppress(Exception):
            loaded = json.loads(schedule_json)
            if isinstance(loaded, dict):
                return loaded
        return {}

    @staticmethod
    def _definition_from_row(row: sqlite3.Row) -> JobDefinition:
        return JobDefinition(
            job_definition_id=int(row["job_definition_id"]),
            job_kind=str(row["job_kind"]),
            job_name=str(row["job_name"]),
            state=JobDefinitionState(str(row["state"])),
            payload_json=str(row["payload_json"]),
            schedule_json=row["schedule_json"],
            priority=int(row["priority"]),
            concurrency_policy=JobConcurrencyPolicy(str(row["concurrency_policy"])),
            result_policy=JobResultPolicy(str(row["result_policy"])),
            timeout_s=(float(row["timeout_s"]) if row["timeout_s"] is not None else None),
            max_retries=int(row["max_retries"]),
            retry_backoff_s=float(row["retry_backoff_s"]),
            heartbeat_timeout_s=(float(row["heartbeat_timeout_s"]) if row["heartbeat_timeout_s"] is not None else None),
            created_timestamp_ep_k=(int(row["created_timestamp_ep_k"]) if row["created_timestamp_ep_k"] is not None else None),
            modified_timestamp_ep_k=(int(row["modified_timestamp_ep_k"]) if row["modified_timestamp_ep_k"] is not None else None),
            last_queued_timestamp_ep_k=(int(row["last_queued_timestamp_ep_k"]) if row["last_queued_timestamp_ep_k"] is not None else None),
            last_started_timestamp_ep_k=(int(row["last_started_timestamp_ep_k"]) if row["last_started_timestamp_ep_k"] is not None else None),
            last_finished_timestamp_ep_k=(int(row["last_finished_timestamp_ep_k"]) if row["last_finished_timestamp_ep_k"] is not None else None),
        )

    @staticmethod
    def _run_from_row(row: sqlite3.Row) -> JobRun:
        return JobRun(
            job_run_id=int(row["job_run_id"]),
            job_definition_id=int(row["job_definition_id"]),
            job_kind=str(row["job_kind"]),
            trigger_kind=JobTriggerKind(str(row["trigger_kind"])),
            state=JobRunState(str(row["state"])),
            attempt_number=int(row["attempt_number"]),
            worker_id=row["worker_id"],
            lease_expires_timestamp_ep_k=(int(row["lease_expires_timestamp_ep_k"]) if row["lease_expires_timestamp_ep_k"] is not None else None),
            cancel_requested=bool(int(row["cancel_requested"] or 0)),
            not_before_timestamp_ep_k=(int(row["not_before_timestamp_ep_k"]) if row["not_before_timestamp_ep_k"] is not None else None),
            progress_current=(int(row["progress_current"]) if row["progress_current"] is not None else None),
            progress_total=(int(row["progress_total"]) if row["progress_total"] is not None else None),
            progress_unit=row["progress_unit"],
            progress_message=row["progress_message"],
            result_json=row["result_json"],
            error_text=row["error_text"],
            log_path=row["log_path"],
            queued_timestamp_ep_k=(int(row["queued_timestamp_ep_k"]) if row["queued_timestamp_ep_k"] is not None else None),
            started_timestamp_ep_k=(int(row["started_timestamp_ep_k"]) if row["started_timestamp_ep_k"] is not None else None),
            heartbeat_timestamp_ep_k=(int(row["heartbeat_timestamp_ep_k"]) if row["heartbeat_timestamp_ep_k"] is not None else None),
            finished_timestamp_ep_k=(int(row["finished_timestamp_ep_k"]) if row["finished_timestamp_ep_k"] is not None else None),
        )

    @staticmethod
    def _event_from_row(row: sqlite3.Row) -> JobRunEvent:
        return JobRunEvent(
            event_id=int(row["event_id"]),
            job_run_id=int(row["job_run_id"]),
            event_kind=JobEventKind(str(row["event_kind"])),
            event_message=row["event_message"],
            event_json=row["event_json"],
            created_timestamp_ep_k=(int(row["created_timestamp_ep_k"]) if row["created_timestamp_ep_k"] is not None else None),
        )


__all__ = ["JobRepository"]
