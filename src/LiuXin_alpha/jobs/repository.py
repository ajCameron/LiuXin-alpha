"""
Persist and query managed-job state transitions.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise repository through a consuming regression::

        python -m pytest -q tests/jobs/test_jobs_repository.py
"""

from __future__ import annotations

import contextlib
import json
import math
import pathlib
import sqlite3
import uuid
from collections.abc import Iterable, Iterator

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
    lease_token TEXT,
    payload_json TEXT,
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


class JobLeaseLost(RuntimeError):
    """The caller no longer owns an unexpired claim and must stop reporting effects."""


class JobRepository:
    """
    SQLite-backed store for durable jobs and job runs.

    Each operation owns and closes its connection. Enqueue policy, claims and
    worker transitions use immediate transactions; their events commit with
    their state. A worker name is descriptive, while the per-claim token fences
    all worker-originated mutations.

    Example:
        Exercise JobRepository through a consuming regression::

            python -m pytest -q tests/jobs/test_jobs_repository.py
    """

    def __init__(self, sqlite_path: str | pathlib.Path) -> None:
        """
        Initialize and validate the jobrepository state.

        Example:
            Exercise JobRepository.  init   through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param sqlite_path: Value supplied for sqlite path under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.sqlite_path = pathlib.Path(sqlite_path).expanduser()
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        self.ensure_schema()

    def connect(self) -> sqlite3.Connection:
        """
        Open a raw SQLite connection whose lifetime belongs to the caller.

        Example:
            Exercise JobRepository.connect through a consuming regression::

                with contextlib.closing(repository.connect()) as conn, conn:
                    rows = conn.execute("SELECT * FROM job_runs").fetchall()


        :return: An open connection. Its transaction context alone does not close it.
        """
        conn = sqlite3.connect(str(self.sqlite_path))
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_schema(self) -> None:
        """Create the ledger and add claim/payload columns to existing ledgers.

        Legacy runs capture their current definition payload once on upgrade;
        an earlier historical payload cannot be reconstructed.
        """
        with self._connection() as conn:
            conn.executescript(_SCHEMA)
            conn.execute("BEGIN IMMEDIATE")
            columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(job_runs)")
            }
            for name in ("lease_token", "payload_json"):
                if name not in columns:
                    conn.execute(f"ALTER TABLE job_runs ADD COLUMN {name} TEXT")
            conn.execute("""
                UPDATE job_runs SET payload_json=(
                    SELECT payload_json FROM job_definitions
                    WHERE job_definitions.job_definition_id=job_runs.job_definition_id
                ) WHERE payload_json IS NULL
            """)

    def create_definition(self, definition: JobDefinition) -> JobDefinition:
        """
        Create definition under the format's safety and compatibility rules.

        Example:
            Exercise JobRepository.create definition through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param definition: Value supplied for definition under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        now = now_ep_k()
        with self._connection() as conn:
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
        self.append_definition_event(
            job_definition_id, "created", f"Created definition {definition.job_name}"
        )
        return self.get_definition(job_definition_id)

    def update_definition(self, definition: JobDefinition) -> JobDefinition:
        """
        Perform the update definition operation under explicit file-format and conversion rules.

        Example:
            Exercise JobRepository.update definition through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param definition: Value supplied for definition under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if definition.job_definition_id is None:
            raise ValueError("update_definition requires job_definition_id")
        now = now_ep_k()
        with self._connection() as conn:
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
        """
        Return definition under the format's safety and compatibility rules.

        Example:
            Exercise JobRepository.get definition through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param job_definition_id: Value supplied for job definition id under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM job_definitions WHERE job_definition_id=?",
                (int(job_definition_id),),
            ).fetchone()
        if row is None:
            raise KeyError(f"Unknown job definition id: {job_definition_id!r}")
        return self._definition_from_row(row)

    def list_definitions(
        self, *, states: Iterable[JobDefinitionState] | None = None
    ) -> list[JobDefinition]:
        """
        Perform the list definitions operation under explicit file-format and conversion rules.

        Example:
            Exercise JobRepository.list definitions through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param states: Value supplied for states under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        sql = "SELECT * FROM job_definitions"
        params: list[object] = []
        if states is not None:
            states_list = [
                str(s.value if isinstance(s, JobDefinitionState) else s) for s in states
            ]
            if states_list:
                placeholders = ", ".join("?" for _ in states_list)
                sql += f" WHERE state IN ({placeholders})"
                params.extend(states_list)
        sql += " ORDER BY priority ASC, job_definition_id ASC"
        with self._connection() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [self._definition_from_row(row) for row in rows]

    def enqueue_run(
        self,
        *,
        job_definition_id: int,
        trigger_kind: JobTriggerKind = JobTriggerKind.MANUAL,
        not_before_timestamp_ep_k: int | None = None,
    ) -> JobRun:
        """Atomically enforce concurrency, snapshot inputs, enqueue and record the event."""
        now = now_ep_k()
        with self._connection(write=True) as conn:
            definition = self._definition_from_row(
                self._definition_row(conn, job_definition_id)
            )
            self._apply_concurrency_policy_on_enqueue(definition, connection=conn)
            cur = conn.execute(
                """
                INSERT INTO job_runs(job_definition_id, job_kind, payload_json,
                    trigger_kind, state, attempt_number, not_before_timestamp_ep_k,
                    queued_timestamp_ep_k) VALUES (?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (
                    job_definition_id,
                    definition.job_kind,
                    definition.payload_json,
                    trigger_kind.value,
                    JobRunState.QUEUED.value,
                    not_before_timestamp_ep_k,
                    now,
                ),
            )
            run_id = int(cur.lastrowid)
            conn.execute(
                "UPDATE job_definitions SET last_queued_timestamp_ep_k=?, modified_timestamp_ep_k=? WHERE job_definition_id=?",
                (now, now, job_definition_id),
            )
            self._append_event(
                conn, run_id, JobEventKind.QUEUED, f"Queued via {trigger_kind.value}"
            )
            return self._run_from_row(self._run_row(conn, run_id))

    def lease_next_run(self, *, worker_id: str, lease_for_s: float) -> JobRun | None:
        """Claim once under a write transaction; replace expired unstarted claims.

        Expired running attempts become aborted with uncertain effects. They are
        never silently replayed. Each new claim has a fresh fencing token, even
        when the same worker name is reused.
        """
        duration = self._lease_milliseconds(lease_for_s)
        if not str(worker_id).strip():
            raise ValueError("worker_id cannot be blank")
        with self._connection(write=True) as conn:
            now = now_ep_k()
            self._recover_expired(conn, now)
            # Older ledgers may contain cancellation flags on unstarted runs.
            for pending in conn.execute(
                "SELECT * FROM job_runs WHERE cancel_requested=1 AND state IN ('queued', 'leased')"
            ).fetchall():
                self._finish(
                    conn,
                    pending,
                    JobRunState.CANCELLED,
                    error_text="Cancelled before execution",
                )
            row = conn.execute(
                """
                SELECT * FROM job_runs WHERE (
                    state='queued' OR (state='leased' AND lease_expires_timestamp_ep_k <= ?)
                ) AND (not_before_timestamp_ep_k IS NULL OR not_before_timestamp_ep_k <= ?)
                ORDER BY job_run_id LIMIT 1
                """,
                (now, now),
            ).fetchone()
            if row is None:
                return None
            run_id = int(row["job_run_id"])
            conn.execute(
                "UPDATE job_runs SET state='leased', worker_id=?, lease_token=?, lease_expires_timestamp_ep_k=?, heartbeat_timestamp_ep_k=? WHERE job_run_id=?",
                (str(worker_id), uuid.uuid4().hex, now + duration, now, run_id),
            )
            self._append_event(
                conn, run_id, JobEventKind.LEASED, f"Leased to {worker_id}"
            )
            return self._run_from_row(self._run_row(conn, run_id))

    def mark_running(
        self, job_run_id: int, *, worker_id: str, lease_token: str
    ) -> JobRun:
        """Start only the current unexpired claim and record its transition atomically."""
        with self._connection(write=True) as conn:
            run = self._require_claim(
                conn, job_run_id, worker_id, lease_token, states=("leased",)
            )
            now = now_ep_k()
            conn.execute(
                "UPDATE job_runs SET state='running', started_timestamp_ep_k=?, heartbeat_timestamp_ep_k=? WHERE job_run_id=?",
                (now, now, job_run_id),
            )
            conn.execute(
                "UPDATE job_definitions SET last_started_timestamp_ep_k=?, modified_timestamp_ep_k=? WHERE job_definition_id=?",
                (now, now, run["job_definition_id"]),
            )
            self._append_event(
                conn, job_run_id, JobEventKind.STARTED, f"Started on worker {worker_id}"
            )
            return self._run_from_row(self._run_row(conn, job_run_id))

    def heartbeat(
        self,
        job_run_id: int,
        *,
        worker_id: str,
        lease_token: str,
        message: str | None = None,
        lease_for_s: float | None = 60.0,
    ) -> None:
        """Renew only a live claim; never steal or revive an expired lease."""
        duration = (
            None if lease_for_s is None else self._lease_milliseconds(lease_for_s)
        )
        with self._connection(write=True) as conn:
            self._require_claim(conn, job_run_id, worker_id, lease_token)
            now = now_ep_k()
            expires = None if duration is None else now + duration
            conn.execute(
                "UPDATE job_runs SET heartbeat_timestamp_ep_k=?, lease_expires_timestamp_ep_k=COALESCE(?, lease_expires_timestamp_ep_k) WHERE job_run_id=?",
                (now, expires, job_run_id),
            )
            self._append_event(
                conn, job_run_id, JobEventKind.HEARTBEAT, message or "Heartbeat"
            )

    def refresh_lease(
        self, job_run_id: int, *, worker_id: str, lease_token: str, lease_for_s: float
    ) -> None:
        """Refresh through the same fenced heartbeat owner."""
        self.heartbeat(
            job_run_id,
            worker_id=worker_id,
            lease_token=lease_token,
            lease_for_s=lease_for_s,
        )

    def update_progress(
        self,
        job_run_id: int,
        update: JobProgressUpdate,
        *,
        worker_id: str,
        lease_token: str,
    ) -> None:
        """Persist a running owner's progress and cursor event in one transaction."""
        with self._connection(write=True) as conn:
            self._require_claim(
                conn, job_run_id, worker_id, lease_token, states=("running",)
            )
            conn.execute(
                "UPDATE job_runs SET progress_current=?, progress_total=?, progress_unit=?, progress_message=? WHERE job_run_id=?",
                (
                    update.progress_current,
                    update.progress_total,
                    update.progress_unit,
                    update.progress_message,
                    job_run_id,
                ),
            )
            self._append_event(
                conn,
                job_run_id,
                JobEventKind.PROGRESS,
                update.progress_message or "Progress",
                event_json=json.dumps(
                    {
                        "progress_current": update.progress_current,
                        "progress_total": update.progress_total,
                        "progress_unit": update.progress_unit,
                    }
                ),
            )

    def mark_succeeded(
        self,
        job_run_id: int,
        *,
        worker_id: str,
        lease_token: str,
        result_json: str | None = None,
    ) -> JobRun:
        """Record the current running owner's terminal receipt and event atomically."""
        with self._connection(write=True) as conn:
            run = self._require_claim(
                conn, job_run_id, worker_id, lease_token, states=("running",)
            )
            return self._finish(
                conn, run, JobRunState.SUCCEEDED, result_json=result_json
            )

    def mark_failed(
        self, job_run_id: int, *, worker_id: str, lease_token: str, error_text: str
    ) -> JobRun:
        """Record the current running owner's terminal receipt and event atomically."""
        with self._connection(write=True) as conn:
            run = self._require_claim(
                conn, job_run_id, worker_id, lease_token, states=("running",)
            )
            return self._finish(conn, run, JobRunState.FAILED, error_text=error_text)

    def mark_cancelled(
        self,
        job_run_id: int,
        *,
        worker_id: str,
        lease_token: str,
        error_text: str | None = None,
        result_json: str | None = None,
    ) -> JobRun:
        """Record the current running owner's terminal receipt and event atomically."""
        with self._connection(write=True) as conn:
            run = self._require_claim(
                conn, job_run_id, worker_id, lease_token, states=("running",)
            )
            return self._finish(
                conn,
                run,
                JobRunState.CANCELLED,
                error_text=error_text,
                result_json=result_json,
            )

    def request_cancel(self, job_run_id: int) -> bool:
        """Cancel unstarted work immediately; request cooperation from running work.

        Terminal runs are unchanged. True acknowledges a new or existing request,
        not reversal of effects already performed by a running handler.
        """
        with self._connection(write=True) as conn:
            return self._request_cancel(conn, self._run_row(conn, job_run_id))

    def get_run(self, job_run_id: int) -> JobRun:
        """
        Return run under the format's safety and compatibility rules.

        Example:
            Exercise JobRepository.get run through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param job_run_id: Value supplied for job run id under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        with self._connection() as conn:
            row = conn.execute(
                "SELECT * FROM job_runs WHERE job_run_id=?", (int(job_run_id),)
            ).fetchone()
        if row is None:
            raise KeyError(f"Unknown job run id: {job_run_id!r}")
        return self._run_from_row(row)

    def list_runs(
        self,
        *,
        job_definition_id: int | None = None,
        states: Iterable[JobRunState] | None = None,
    ) -> list[JobRun]:
        """
        Perform the list runs operation under explicit file-format and conversion rules.

        Example:
            Exercise JobRepository.list runs through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param job_definition_id: Value supplied for job definition id under the utility
            contract.
        :param states: Value supplied for states under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        sql = "SELECT * FROM job_runs"
        clauses: list[str] = []
        params: list[object] = []
        if job_definition_id is not None:
            clauses.append("job_definition_id=?")
            params.append(int(job_definition_id))
        if states is not None:
            state_values = [
                str(s.value if isinstance(s, JobRunState) else s) for s in states
            ]
            if state_values:
                placeholders = ", ".join("?" for _ in state_values)
                clauses.append(f"state IN ({placeholders})")
                params.extend(state_values)
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY job_run_id ASC"
        with self._connection() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
        return [self._run_from_row(row) for row in rows]

    def list_events(
        self, job_run_id: int, *, after_event_id: int = 0, limit: int | None = None
    ) -> list[JobRunEvent]:
        """Read a stable event-ID cursor, optionally bounded for remote polling."""
        if after_event_id < 0 or (limit is not None and limit < 1):
            raise ValueError("Event cursor must be nonnegative and limit positive")
        sql = "SELECT * FROM job_run_events WHERE job_run_id=? AND event_id>? ORDER BY event_id"
        params = [job_run_id, after_event_id]
        if limit is not None:
            sql += " LIMIT ?"
            params.append(limit)
        with self._connection() as conn:
            return [
                self._event_from_row(row)
                for row in conn.execute(sql, params).fetchall()
            ]

    def append_log(
        self,
        job_run_id: int,
        *,
        worker_id: str,
        lease_token: str,
        message: str,
        event_json: str | None = None,
    ) -> None:
        """Append a log observation only for the current running owner."""
        with self._connection(write=True) as conn:
            self._require_claim(
                conn, job_run_id, worker_id, lease_token, states=("running",)
            )
            self._append_event(
                conn, job_run_id, JobEventKind.LOG, message, event_json=event_json
            )

    def append_event(
        self,
        job_run_id: int,
        event_kind: JobEventKind,
        message: str | None = None,
        *,
        event_json: str | None = None,
    ) -> None:
        """Append an operator observation; lifecycle owners use their own transaction."""
        with self._connection(write=True) as conn:
            self._run_row(conn, job_run_id)
            self._append_event(
                conn, job_run_id, event_kind, message, event_json=event_json
            )

    def append_definition_event(
        self, job_definition_id: int, event_kind: str, message: str
    ) -> None:
        """
        Perform the append definition event operation under explicit file-format and conversion rules.

        Example:
            Exercise JobRepository.append definition event through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param job_definition_id: Value supplied for job definition id under the utility
            contract.
        :param event_kind: Value supplied for event kind under the utility contract.
        :param message: Value supplied for message under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        del job_definition_id, event_kind, message
        # Placeholder for later definition-event stream; run events cover the MVP.

    def get_due_definitions_for_scheduling(
        self, *, now_timestamp_ep_k: int | None = None
    ) -> list[JobDefinition]:
        """
        Return due definitions for scheduling under the format's safety and compatibility rules.

        Example:
            Exercise JobRepository.get due definitions for scheduling through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param now_timestamp_ep_k: Value supplied for now timestamp ep k under the utility
            contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        now_value = (
            now_ep_k() if now_timestamp_ep_k is None else int(now_timestamp_ep_k)
        )
        due: list[JobDefinition] = []
        for definition in self.list_definitions(states=[JobDefinitionState.ENABLED]):
            schedule = self._load_schedule_json(definition.schedule_json)
            interval_s = schedule.get("interval_seconds")
            if interval_s in (None, "", 0):
                continue
            every_ms = int(float(interval_s) * 1000)
            baseline = (
                definition.last_queued_timestamp_ep_k
                or definition.created_timestamp_ep_k
                or 0
            )
            if baseline <= 0 or now_value >= baseline + every_ms:
                due.append(definition)
        return due

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _apply_concurrency_policy_on_enqueue(
        self, definition: JobDefinition, *, connection: sqlite3.Connection
    ) -> None:
        """Check and apply enqueue policy inside the same write unit as insertion."""
        active = connection.execute(
            "SELECT * FROM job_runs WHERE job_definition_id=? AND state IN ('queued', 'leased', 'running')",
            (definition.job_definition_id,),
        ).fetchall()
        policy = definition.concurrency_policy
        if (
            policy
            in (JobConcurrencyPolicy.SKIP_IF_RUNNING, JobConcurrencyPolicy.QUEUE_ONE)
            and active
        ):
            raise RuntimeError(
                f"Job definition {definition.job_name!r} already has a queued/running run"
            )
        if policy is JobConcurrencyPolicy.REPLACE_RUNNING:
            for run in active:
                self._request_cancel(connection, run)

    def _apply_result_policy(
        self, connection: sqlite3.Connection, job_definition_id: int
    ) -> None:
        """Prune terminal result payloads, never live runs, identity or event cursors."""
        definition = self._definition_from_row(
            self._definition_row(connection, job_definition_id)
        )
        if definition.result_policy is JobResultPolicy.KEEP_ALL:
            return
        rows = connection.execute(
            "SELECT * FROM job_runs WHERE job_definition_id=? AND state NOT IN ('queued', 'leased', 'running') ORDER BY job_run_id",
            (job_definition_id,),
        ).fetchall()
        if definition.result_policy is JobResultPolicy.KEEP_FAILURES:
            rows = [row for row in rows if row["state"] == "succeeded"]
        for row in rows[:-1]:
            connection.execute(
                "UPDATE job_runs SET result_json=NULL WHERE job_run_id=?",
                (row["job_run_id"],),
            )
            connection.execute(
                "UPDATE job_run_events SET event_json=NULL WHERE job_run_id=? AND event_kind IN ('succeeded', 'failed', 'cancelled', 'aborted')",
                (row["job_run_id"],),
            )

    @contextlib.contextmanager
    def _connection(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        """Own connection lifetime and, for transitions, one immediate transaction."""
        with contextlib.closing(self.connect()) as conn:
            if write:
                conn.execute("BEGIN IMMEDIATE")
            with conn:
                yield conn

    @staticmethod
    def _lease_milliseconds(seconds: float) -> int:
        value = float(seconds)
        if not math.isfinite(value) or value <= 0:
            raise ValueError("Lease duration must be finite and positive")
        return int(max(1.0, value) * 1000)

    @staticmethod
    def _definition_row(conn: sqlite3.Connection, definition_id: int) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM job_definitions WHERE job_definition_id=?", (definition_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"Unknown job definition id: {definition_id!r}")
        return row

    @staticmethod
    def _run_row(conn: sqlite3.Connection, run_id: int) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM job_runs WHERE job_run_id=?", (run_id,)
        ).fetchone()
        if row is None:
            raise KeyError(f"Unknown job run id: {run_id!r}")
        return row

    def _require_claim(
        self,
        conn: sqlite3.Connection,
        run_id: int,
        worker_id: str,
        lease_token: str,
        *,
        states: tuple[str, ...] = ("leased", "running"),
    ) -> sqlite3.Row:
        row = self._run_row(conn, run_id)
        if (
            not lease_token
            or row["lease_token"] != lease_token
            or row["worker_id"] != worker_id
            or row["state"] not in states
            or (row["lease_expires_timestamp_ep_k"] or 0) <= now_ep_k()
        ):
            raise JobLeaseLost(
                f"Lease for run {run_id} is no longer owned by this claim"
            )
        return row

    def check_claim(
        self, job_run_id: int, *, worker_id: str, lease_token: str
    ) -> JobRun:
        """Validate ownership at a handler checkpoint without extending its lease."""
        with self._connection() as conn:
            return self._run_from_row(
                self._require_claim(conn, job_run_id, worker_id, lease_token)
            )

    @staticmethod
    def _append_event(
        conn: sqlite3.Connection,
        run_id: int,
        kind: JobEventKind,
        message: str | None,
        *,
        event_json: str | None = None,
    ) -> None:
        conn.execute(
            "INSERT INTO job_run_events(job_run_id, event_kind, event_message, event_json, created_timestamp_ep_k) VALUES (?, ?, ?, ?, ?)",
            (run_id, kind.value, message, event_json, now_ep_k()),
        )

    def _finish(
        self,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        state: JobRunState,
        *,
        result_json: str | None = None,
        error_text: str | None = None,
    ) -> JobRun:
        run_id = int(row["job_run_id"])
        # A late request is acknowledged only after handler execution has ended.
        if state is JobRunState.SUCCEEDED and row["cancel_requested"]:
            state = JobRunState.CANCELLED
            error_text = "Cancellation acknowledged after handler returned; receipt retains completed effects"
        now = now_ep_k()
        conn.execute(
            "UPDATE job_runs SET state=?, result_json=?, error_text=?, finished_timestamp_ep_k=?, lease_expires_timestamp_ep_k=NULL, lease_token=NULL WHERE job_run_id=?",
            (state.value, result_json, error_text, now, run_id),
        )
        conn.execute(
            "UPDATE job_definitions SET last_finished_timestamp_ep_k=?, modified_timestamp_ep_k=? WHERE job_definition_id=?",
            (now, now, row["job_definition_id"]),
        )
        self._append_event(
            conn,
            run_id,
            JobEventKind(state.value),
            error_text or state.value.capitalize(),
            event_json=result_json,
        )
        self._apply_result_policy(conn, int(row["job_definition_id"]))
        return self._run_from_row(self._run_row(conn, run_id))

    def _request_cancel(self, conn: sqlite3.Connection, row: sqlite3.Row) -> bool:
        if row["state"] not in ("queued", "leased", "running"):
            return False
        run_id = int(row["job_run_id"])
        if not row["cancel_requested"]:
            conn.execute(
                "UPDATE job_runs SET cancel_requested=1 WHERE job_run_id=?", (run_id,)
            )
            self._append_event(
                conn, run_id, JobEventKind.CANCEL_REQUESTED, "Cancellation requested"
            )
        if row["state"] in ("queued", "leased"):
            self._finish(
                conn,
                row,
                JobRunState.CANCELLED,
                error_text="Cancelled before execution",
            )
        return True

    def _recover_expired(self, conn: sqlite3.Connection, now: int) -> list[int]:
        rows = conn.execute(
            "SELECT * FROM job_runs WHERE state='running' AND (lease_expires_timestamp_ep_k IS NULL OR lease_expires_timestamp_ep_k <= ?)",
            (now,),
        ).fetchall()
        for row in rows:
            self._finish(
                conn,
                row,
                JobRunState.ABORTED,
                error_text="Worker lease expired; effects may have occurred. Inspect domain evidence before an explicit retry.",
            )
        return [int(row["job_run_id"]) for row in rows]

    def recover_expired_runs(self) -> list[int]:
        """Record interrupted running attempts without automatically replaying them."""
        with self._connection(write=True) as conn:
            return self._recover_expired(conn, now_ep_k())

    @staticmethod
    def _load_schedule_json(schedule_json: str | None) -> dict[str, object]:
        """
        Perform the load schedule json operation under explicit file-format and conversion rules.

        Example:
            Exercise JobRepository. load schedule json through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param schedule_json: Value supplied for schedule json under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if not schedule_json:
            return {}
        with contextlib.suppress(Exception):
            loaded = json.loads(schedule_json)
            if isinstance(loaded, dict):
                return loaded
        return {}

    @staticmethod
    def _definition_from_row(row: sqlite3.Row) -> JobDefinition:
        """
        Perform the definition from row operation under explicit file-format and conversion rules.

        Example:
            Exercise JobRepository. definition from row through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param row: Value supplied for row under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
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
            timeout_s=(
                float(row["timeout_s"]) if row["timeout_s"] is not None else None
            ),
            max_retries=int(row["max_retries"]),
            retry_backoff_s=float(row["retry_backoff_s"]),
            heartbeat_timeout_s=(
                float(row["heartbeat_timeout_s"])
                if row["heartbeat_timeout_s"] is not None
                else None
            ),
            created_timestamp_ep_k=(
                int(row["created_timestamp_ep_k"])
                if row["created_timestamp_ep_k"] is not None
                else None
            ),
            modified_timestamp_ep_k=(
                int(row["modified_timestamp_ep_k"])
                if row["modified_timestamp_ep_k"] is not None
                else None
            ),
            last_queued_timestamp_ep_k=(
                int(row["last_queued_timestamp_ep_k"])
                if row["last_queued_timestamp_ep_k"] is not None
                else None
            ),
            last_started_timestamp_ep_k=(
                int(row["last_started_timestamp_ep_k"])
                if row["last_started_timestamp_ep_k"] is not None
                else None
            ),
            last_finished_timestamp_ep_k=(
                int(row["last_finished_timestamp_ep_k"])
                if row["last_finished_timestamp_ep_k"] is not None
                else None
            ),
        )

    @staticmethod
    def _run_from_row(row: sqlite3.Row) -> JobRun:
        """
        Perform the run from row operation under explicit file-format and conversion rules.

        Example:
            Exercise JobRepository. run from row through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param row: Value supplied for row under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return JobRun(
            job_run_id=int(row["job_run_id"]),
            job_definition_id=int(row["job_definition_id"]),
            job_kind=str(row["job_kind"]),
            trigger_kind=JobTriggerKind(str(row["trigger_kind"])),
            state=JobRunState(str(row["state"])),
            attempt_number=int(row["attempt_number"]),
            worker_id=row["worker_id"],
            lease_token=row["lease_token"],
            payload_json=row["payload_json"],
            lease_expires_timestamp_ep_k=(
                int(row["lease_expires_timestamp_ep_k"])
                if row["lease_expires_timestamp_ep_k"] is not None
                else None
            ),
            cancel_requested=bool(int(row["cancel_requested"] or 0)),
            not_before_timestamp_ep_k=(
                int(row["not_before_timestamp_ep_k"])
                if row["not_before_timestamp_ep_k"] is not None
                else None
            ),
            progress_current=(
                int(row["progress_current"])
                if row["progress_current"] is not None
                else None
            ),
            progress_total=(
                int(row["progress_total"])
                if row["progress_total"] is not None
                else None
            ),
            progress_unit=row["progress_unit"],
            progress_message=row["progress_message"],
            result_json=row["result_json"],
            error_text=row["error_text"],
            log_path=row["log_path"],
            queued_timestamp_ep_k=(
                int(row["queued_timestamp_ep_k"])
                if row["queued_timestamp_ep_k"] is not None
                else None
            ),
            started_timestamp_ep_k=(
                int(row["started_timestamp_ep_k"])
                if row["started_timestamp_ep_k"] is not None
                else None
            ),
            heartbeat_timestamp_ep_k=(
                int(row["heartbeat_timestamp_ep_k"])
                if row["heartbeat_timestamp_ep_k"] is not None
                else None
            ),
            finished_timestamp_ep_k=(
                int(row["finished_timestamp_ep_k"])
                if row["finished_timestamp_ep_k"] is not None
                else None
            ),
        )

    @staticmethod
    def _event_from_row(row: sqlite3.Row) -> JobRunEvent:
        """
        Perform the event from row operation under explicit file-format and conversion rules.

        Example:
            Exercise JobRepository. event from row through a consuming regression::

                python -m pytest -q tests/jobs/test_jobs_repository.py


        :param row: Value supplied for row under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return JobRunEvent(
            event_id=int(row["event_id"]),
            job_run_id=int(row["job_run_id"]),
            event_kind=JobEventKind(str(row["event_kind"])),
            event_message=row["event_message"],
            event_json=row["event_json"],
            created_timestamp_ep_k=(
                int(row["created_timestamp_ep_k"])
                if row["created_timestamp_ep_k"] is not None
                else None
            ),
        )


__all__ = ["JobRepository", "JobLeaseLost"]
